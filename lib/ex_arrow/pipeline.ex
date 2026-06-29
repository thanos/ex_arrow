defmodule ExArrow.Pipeline do
  @moduledoc """
  A thin pipeline abstraction over ExArrow streams.

  `ExArrow.Pipeline` provides a stable, composable API for transforming and
  sinking Arrow streams without exposing the underlying execution mechanism.
  Internally it threads an Elixir `Stream` of `ExArrow.RecordBatch` values
  alongside the stream's schema, so transformations stay lazy and batches are
  never converted to row maps.

  Every function accepts and returns `{:ok, pipeline} | {:error, reason}`, so
  pipelines compose with `|>/2` directly from `ExArrow.Stream.from_*/1`
  constructors (which return `{:ok, stream} | {:error, _}`).  Errors short-
  circuit through every stage.

  ## Example

      ExArrow.Stream.from_flight_sql(client, "SELECT * FROM events")
      |> ExArrow.Pipeline.map_batches(fn batch ->
        {:ok, slim} = ExArrow.Batch.select(batch, ["id", "score"])
        slim
      end)
      |> ExArrow.Pipeline.write_parquet("/data/events.parquet")

  ## Laziness

  `map_batches/2` and `each_batch/2` return a lazy pipeline — no batches are
  consumed until a sink (`write_parquet/2`, `write_flight/3`, or
  `write_dataframe/1`) runs.  This means a pipeline built but never sunk does
  no work.

  ## Telemetry

  `map_batches/2` and `each_batch/2` emit `[:ex_arrow, :pipeline, :batch]` for
  every batch that flows through the stage, carrying `rows`, `columns`, and
  `batch_count` measurements and `%{source: :pipeline}` metadata.  The sinks
  emit their respective `[:ex_arrow, :parquet, :write]` and
  `[:ex_arrow, :flight, :query]` events.
  """

  alias ExArrow.Flight.Client, as: FlightClient
  alias ExArrow.IPC
  alias ExArrow.Parquet.Writer
  alias ExArrow.RecordBatch
  alias ExArrow.Stream, as: ArrowStream

  @enforce_keys [:schema, :enum]
  defstruct [:schema, :enum]

  @type t :: %__MODULE__{schema: ExArrow.Schema.t() | nil, enum: Enumerable.t()}
  @type input :: ExArrow.Stream.t() | t()
  @type threaded :: {:ok, input()} | {:error, term()}

  @doc """
  Wrap an `ExArrow.Stream.t()` (or thread an existing pipeline) and apply
  `fun` to each batch.

  The resulting pipeline is lazy: `fun` runs only when a sink consumes the
  pipeline.  `fun` receives an `ExArrow.RecordBatch.t()` and should return an
  `ExArrow.RecordBatch.t()` (or any term if the downstream sink expects it).

  ## Example

      ExArrow.Stream.from_parquet("/data/events.parquet")
      |> ExArrow.Pipeline.map_batches(&ExArrow.Batch.schema/1)
  """
  @spec map_batches(threaded(), (RecordBatch.t() -> term())) :: threaded()
  def map_batches(threaded, fun)

  def map_batches({:error, _} = err, _fun), do: err

  def map_batches({:ok, input}, fun) when is_function(fun, 1) do
    with {:ok, pipeline} <- wrap(input) do
      enum =
        Stream.map(pipeline.enum, fn batch ->
          result = fun.(batch)
          emit_pipeline_telemetry(batch)
          result
        end)

      {:ok, %{pipeline | enum: enum}}
    end
  end

  @doc """
  Run `fun` for its side effects on each batch without changing the pipeline.

  Lazy: the side effect runs only when a sink consumes the pipeline.  The
  pipeline's batches pass through unchanged.
  """
  @spec each_batch(threaded(), (RecordBatch.t() -> term())) :: threaded()
  def each_batch(threaded, fun)

  def each_batch({:error, _} = err, _fun), do: err

  def each_batch({:ok, input}, fun) when is_function(fun, 1) do
    with {:ok, pipeline} <- wrap(input) do
      enum =
        Stream.map(pipeline.enum, fn batch ->
          fun.(batch)
          emit_pipeline_telemetry(batch)
          batch
        end)

      {:ok, %{pipeline | enum: enum}}
    end
  end

  @doc """
  Consume the pipeline and write every batch to a Parquet file at `path`.

  Triggers evaluation of all upstream stages.  Emits a
  `[:ex_arrow, :parquet, :write]` telemetry event.  Returns `:ok` or
  `{:error, message}`.
  """
  @spec write_parquet(threaded(), Path.t()) :: :ok | {:error, String.t()}
  def write_parquet(threaded, path) when is_binary(path) do
    with {:ok, pipeline} <- unwrap(threaded),
         batches = Enum.to_list(pipeline.enum),
         {:ok, schema} <- resolve_schema(pipeline, batches) do
      rows = Enum.sum(Enum.map(batches, &RecordBatch.num_rows/1))

      ExArrow.Telemetry.execute(
        [:ex_arrow, :parquet, :write],
        %{rows: rows, batch_count: length(batches)},
        %{destination: path, source: :pipeline}
      )

      Writer.to_file(path, schema, batches)
    end
  end

  @doc """
  Consume the pipeline and upload every batch to a Flight server via `client`.

  `opts` are forwarded to `ExArrow.Flight.Client.do_put/4`.  Emits a
  `[:ex_arrow, :flight, :query]` telemetry event.  Returns `:ok` or
  `{:error, reason}`.
  """
  @spec write_flight(threaded(), ExArrow.Flight.Client.t(), keyword()) ::
          :ok | {:error, term()}
  def write_flight(threaded, client, opts \\ [])

  def write_flight({:error, _} = err, _client, _opts), do: err

  def write_flight({:ok, input}, client, opts) do
    with {:ok, pipeline} <- wrap(input),
         batches = Enum.to_list(pipeline.enum),
         {:ok, schema} <- resolve_schema(pipeline, batches) do
      rows = Enum.sum(Enum.map(batches, &RecordBatch.num_rows/1))

      ExArrow.Telemetry.execute(
        [:ex_arrow, :flight, :query],
        %{rows: rows, batch_count: length(batches)},
        %{destination: Keyword.get(opts, :descriptor), source: :pipeline}
      )

      FlightClient.do_put(client, schema, batches, opts)
    end
  end

  @doc """
  Consume the pipeline and convert it into an `Explorer.DataFrame`.

  Requires the optional `{:explorer, "~> 0.11"}` dependency.  Returns
  `{:ok, dataframe}` or `{:error, message}`.
  """
  @spec write_dataframe(threaded()) ::
          {:ok, Explorer.DataFrame.t()} | {:error, String.t()}
  def write_dataframe(threaded) do
    with {:ok, pipeline} <- unwrap(threaded),
         batches = Enum.to_list(pipeline.enum),
         {:ok, schema} <- resolve_schema(pipeline, batches),
         {:ok, ipc_bin} <- IPC.Writer.to_binary(schema, batches),
         {:ok, stream} <- IPC.Reader.from_binary(ipc_bin) do
      ExArrow.DataFrame.from_arrow(stream)
    end
  end

  # ── internals ─────────────────────────────────────────────────────────────────

  defp wrap(%__MODULE__{} = pipeline), do: {:ok, pipeline}

  defp wrap(source) do
    if ArrowStream.stream?(source) do
      case ArrowStream.schema(source) do
        {:ok, schema} -> {:ok, %__MODULE__{schema: schema, enum: source}}
        {:error, _} = err -> err
      end
    else
      {:error, "expected an ExArrow.Stream or ExArrow.Pipeline, got: #{inspect(source)}"}
    end
  end

  defp unwrap({:ok, input}), do: wrap(input)
  defp unwrap({:error, _} = err), do: err

  # The schema is derived from the first emitted batch so that transformations
  # which change the schema (select/drop/rename) are reflected accurately.  The
  # schema captured at wrap time is only used as a fallback when the pipeline
  # produced zero batches (so an empty Parquet file still carries a schema).
  defp resolve_schema(%__MODULE__{schema: captured}, []) do
    if captured, do: {:ok, captured}, else: {:error, "no schema and no batches"}
  end

  defp resolve_schema(_pipeline, [first | _]) do
    if ExArrow.RecordBatch.record_batch?(first) do
      {:ok, RecordBatch.schema(first)}
    else
      {:error, "pipeline produced a non-batch value; cannot derive schema: #{inspect(first)}"}
    end
  end

  defp emit_pipeline_telemetry(batch) do
    if ExArrow.RecordBatch.record_batch?(batch) do
      measurements = ExArrow.Telemetry.batch_measurements(batch)

      ExArrow.Telemetry.execute([:ex_arrow, :pipeline, :batch], measurements, %{source: :pipeline})
    end
  end
end
