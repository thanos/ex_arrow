defmodule ExArrow.GenStage do
  @moduledoc """
  Arrow-native GenStage producers.

  ExArrow provides three demand-driven producers that emit
  `ExArrow.RecordBatch` values from the common ExArrow sources:

  | Producer                          | Source                                    |
  |-----------------------------------|-------------------------------------------|
  | `ExArrow.GenStage.ParquetProducer` | Parquet file or binary                   |
  | `ExArrow.GenStage.FlightProducer`  | Flight `do_get` ticket                   |
  | `ExArrow.GenStage.ADBCProducer`    | ADBC statement / `{connection, sql}`     |

  All three share the same lifecycle:

  - **Demand-driven**: batches are only read when a consumer demands them, so
    slow consumers apply backpressure all the way to the source.
  - **Arrow batch delivery**: each emitted event is an `ExArrow.RecordBatch`
    handle (an opaque reference to native memory) — no row maps.
  - **Clean shutdown**: when the underlying stream is exhausted the producer
    drains remaining batches, sends itself a stop message, and exits with
    reason `:normal`.
  - **Resource cleanup**: the stream handle is released when the producer
    terminates; `terminate/2` is implemented so the stream is closed even on
    non-normal shutdown.

  Requires `{:gen_stage, "~> 1.2"}` in your `mix.exs` dependencies.

  ## Wiring examples

  ### Producer + consumer

      {:ok, prod} =
        ExArrow.GenStage.ParquetProducer.start_link(path: "/data/events.parquet")

      # A minimal consumer that collects batches into the calling process.
      defmodule Collector do
        use GenStage

        def init(pid), do: {:consumer, pid}

        def handle_events(batches, _from, pid) do
          send(pid, {:batches, batches})
          {:noreply, [], pid}
        end
      end

      {:ok, cons} = GenStage.start_link(Collector, self())
      GenStage.sync_subscribe(cons, to: prod, max_demand: 4)

  ### Producer-consumer

  Wrap a producer with a producer-consumer that transforms each batch (e.g.
  via `ExArrow.Batch.select/2`) before forwarding it downstream.

      defmodule MyTransformer do
        use GenStage

        def init(state), do: {:producer_consumer, state}

        def handle_events(batches, _from, state) do
          transformed =
            Enum.map(batches, fn batch ->
              {:ok, slim} = ExArrow.Batch.select(batch, ["id"])
              slim
            end)

          {:noreply, transformed, state}
        end
      end
  """

  defmodule State do
    @moduledoc false
    @type t :: %__MODULE__{
            stream: ExArrow.Stream.t() | nil,
            source: term(),
            demand: non_neg_integer(),
            done: boolean()
          }
    defstruct [:stream, :source, demand: 0, done: false]
  end

  @doc false
  # Shared demand-dispatch loop used by every ExArrow producer.
  #
  # Returns a GenStage `{:noreply, events, state}` reply.  When the stream is
  # exhausted it returns `{:stop, :normal, state}` so the producer exits
  # cleanly after signalling downstream.
  @spec dispatch(State.t(), non_neg_integer()) ::
          {:noreply, [ExArrow.RecordBatch.t()], State.t()}
          | {:stop, :normal, State.t()}
          | {:noreply, [], State.t()}
  def dispatch(%State{done: true} = state, _demand), do: {:noreply, [], state}

  def dispatch(%State{} = state, demand) do
    state = %{state | demand: state.demand + demand}
    do_dispatch(state, [])
  end

  defp do_dispatch(%{demand: 0} = state, acc) do
    {:noreply, Enum.reverse(acc), %{state | demand: 0}}
  end

  defp do_dispatch(%{stream: stream, source: source} = state, acc) do
    case ExArrow.Stream.next(stream) do
      nil ->
        # Stream exhausted: emit whatever we have already pulled, then ask the
        # producer to stop on the next message loop.  Stopping in the same
        # callback would drop `acc`, so we send ourselves a `:stop` message and
        # handle it in `handle_info/2`.
        send(self(), {__MODULE__, :stop})
        {:noreply, Enum.reverse(acc), %{state | done: true, stream: nil}}

      {:error, _reason} ->
        send(self(), {__MODULE__, :stop})
        {:noreply, Enum.reverse(acc), %{state | done: true, stream: nil}}

      batch ->
        emit_batch_telemetry(batch, source)
        do_dispatch(%{state | demand: state.demand - 1}, [batch | acc])
    end
  end

  defp emit_batch_telemetry(batch, source) do
    if ExArrow.RecordBatch.record_batch?(batch) do
      measurements = ExArrow.Telemetry.batch_measurements(batch)
      ExArrow.Telemetry.execute([:ex_arrow, :stream, :batch], measurements, %{source: source})
    end
  end

  @doc false
  # Shared handler for the `{ExArrow.GenStage, :stop}` self-message dispatched
  # when a stream is exhausted.  Returns a `{:stop, :normal, state}` reply so
  # the producer exits cleanly.
  @spec handle_stop(State.t()) :: {:stop, :normal, State.t()}
  def handle_stop(state), do: {:stop, :normal, %{state | done: true, stream: nil}}

  @doc false
  # Shared `terminate/2` — releases the underlying stream handle if still open.
  @spec terminate(State.t()) :: :ok
  def terminate(%State{stream: nil}), do: :ok

  def terminate(%State{stream: stream}) do
    # Drain remaining batches so the native resource is released promptly.
    # The handle is also GC'd, but draining avoids holding file/socket
    # descriptors open until collection.
    _ = drain(stream)
    :ok
  end

  defp drain(stream) do
    case ExArrow.Stream.next(stream) do
      nil -> :ok
      {:error, _} -> :ok
      _batch -> drain(stream)
    end
  end
end

defmodule ExArrow.GenStage.ParquetProducer do
  @moduledoc """
  A `GenStage` producer that emits `ExArrow.RecordBatch` values from a Parquet
  file or in-memory Parquet binary.

  ## Options

  - `:path` — path to a `.parquet` file (opened with
    `ExArrow.Stream.from_parquet/1`).
  - `:binary` — in-memory Parquet bytes (opened with
    `ExArrow.Stream.from_parquet_binary/1`).
  - `:stream` — a pre-opened `ExArrow.Stream.t()` (useful for testing).

  ## Example

      {:ok, producer} =
        ExArrow.GenStage.ParquetProducer.start_link(path: "/data/events.parquet")
  """

  @gen_stage_available Code.ensure_loaded?(GenStage)

  if @gen_stage_available do
    use GenStage

    alias ExArrow.GenStage.State

    def start_link(opts) when is_list(opts) do
      with {:ok, stream, source} <- open_stream(opts) do
        GenStage.start_link(__MODULE__, %State{
          stream: stream,
          source: source,
          demand: 0,
          done: false
        })
      end
    end

    defp open_stream(opts) do
      cond do
        stream = Keyword.get(opts, :stream) ->
          {:ok, stream, ExArrow.Stream.source(stream) || :parquet}

        path = Keyword.get(opts, :path) ->
          case ExArrow.Stream.from_parquet(path) do
            {:ok, stream} -> {:ok, stream, {:parquet, path}}
            {:error, _} = err -> err
          end

        binary = Keyword.get(opts, :binary) ->
          case ExArrow.Stream.from_parquet_binary(binary) do
            {:ok, stream} -> {:ok, stream, {:parquet, :binary}}
            {:error, _} = err -> err
          end

        true ->
          {:error, "ParquetProducer requires one of :path, :binary, or :stream"}
      end
    end

    @impl true
    def init(%State{} = state) do
      {:producer, state}
    end

    @impl true
    def handle_demand(demand, state) do
      ExArrow.GenStage.dispatch(state, demand)
    end

    @impl true
    def handle_info({ExArrow.GenStage, :stop}, state) do
      ExArrow.GenStage.handle_stop(state)
    end

    def handle_info(_msg, state) do
      {:noreply, [], state}
    end

    @impl true
    def terminate(_reason, state) do
      ExArrow.GenStage.terminate(state)
    end
  else
    @doc false
    @spec start_link(keyword()) :: {:error, String.t()}
    def start_link(_opts) do
      {:error,
       "GenStage is not available. Add {:gen_stage, \"~> 1.2\"} to your mix.exs dependencies."}
    end
  end
end

defmodule ExArrow.GenStage.FlightProducer do
  @moduledoc """
  A `GenStage` producer that emits `ExArrow.RecordBatch` values from a Flight
  `do_get` stream.

  ## Options

  - `:client` + `:ticket` — opened with `ExArrow.Stream.from_flight/2`.
  - `:stream` — a pre-opened `ExArrow.Stream.t()` (useful for testing).
  """

  @gen_stage_available Code.ensure_loaded?(GenStage)

  if @gen_stage_available do
    use GenStage

    alias ExArrow.GenStage.State

    def start_link(opts) when is_list(opts) do
      with {:ok, stream, source} <- open_stream(opts) do
        GenStage.start_link(__MODULE__, %State{
          stream: stream,
          source: source,
          demand: 0,
          done: false
        })
      end
    end

    defp open_stream(opts) do
      cond do
        stream = Keyword.get(opts, :stream) ->
          {:ok, stream, ExArrow.Stream.source(stream) || :flight}

        client = Keyword.get(opts, :client) ->
          case ExArrow.Stream.from_flight(client, Keyword.get(opts, :ticket)) do
            {:ok, stream} -> {:ok, stream, {:flight, Keyword.get(opts, :ticket)}}
            {:error, _} = err -> err
          end

        true ->
          {:error, "FlightProducer requires :client + :ticket, or :stream"}
      end
    end

    @impl true
    def init(%State{} = state) do
      {:producer, state}
    end

    @impl true
    def handle_demand(demand, state) do
      ExArrow.GenStage.dispatch(state, demand)
    end

    @impl true
    def handle_info({ExArrow.GenStage, :stop}, state) do
      ExArrow.GenStage.handle_stop(state)
    end

    def handle_info(_msg, state) do
      {:noreply, [], state}
    end

    @impl true
    def terminate(_reason, state) do
      ExArrow.GenStage.terminate(state)
    end
  else
    @doc false
    @spec start_link(keyword()) :: {:error, String.t()}
    def start_link(_opts) do
      {:error,
       "GenStage is not available. Add {:gen_stage, \"~> 1.2\"} to your mix.exs dependencies."}
    end
  end
end

defmodule ExArrow.GenStage.ADBCProducer do
  @moduledoc """
  A `GenStage` producer that emits `ExArrow.RecordBatch` values from an ADBC
  query result stream.

  ## Options

  - `:statement` — a pre-built `ExArrow.ADBC.Statement.t()` (executed with
    `ExArrow.Stream.from_adbc/1`).
  - `:connection` + `:sql` — opened with `ExArrow.Stream.from_adbc/2`.
  - `:stream` — a pre-opened `ExArrow.Stream.t()` (useful for testing).
  """

  @gen_stage_available Code.ensure_loaded?(GenStage)

  if @gen_stage_available do
    use GenStage

    alias ExArrow.GenStage.State

    def start_link(opts) when is_list(opts) do
      with {:ok, stream, source} <- open_stream(opts) do
        GenStage.start_link(__MODULE__, %State{
          stream: stream,
          source: source,
          demand: 0,
          done: false
        })
      end
    end

    defp open_stream(opts) do
      cond do
        stream = Keyword.get(opts, :stream) ->
          {:ok, stream, ExArrow.Stream.source(stream) || :adbc}

        stmt = Keyword.get(opts, :statement) ->
          case ExArrow.Stream.from_adbc(stmt) do
            {:ok, stream} -> {:ok, stream, {:adbc, :statement}}
            {:error, _} = err -> err
          end

        conn = Keyword.get(opts, :connection) ->
          case ExArrow.Stream.from_adbc(conn, Keyword.get(opts, :sql)) do
            {:ok, stream} -> {:ok, stream, {:adbc, Keyword.get(opts, :sql)}}
            {:error, _} = err -> err
          end

        true ->
          {:error, "ADBCProducer requires :statement, :connection + :sql, or :stream"}
      end
    end

    @impl true
    def init(%State{} = state) do
      {:producer, state}
    end

    @impl true
    def handle_demand(demand, state) do
      ExArrow.GenStage.dispatch(state, demand)
    end

    @impl true
    def handle_info({ExArrow.GenStage, :stop}, state) do
      ExArrow.GenStage.handle_stop(state)
    end

    def handle_info(_msg, state) do
      {:noreply, [], state}
    end

    @impl true
    def terminate(_reason, state) do
      ExArrow.GenStage.terminate(state)
    end
  else
    @doc false
    @spec start_link(keyword()) :: {:error, String.t()}
    def start_link(_opts) do
      {:error,
       "GenStage is not available. Add {:gen_stage, \"~> 1.2\"} to your mix.exs dependencies."}
    end
  end
end
