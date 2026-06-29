defmodule ExArrow.Broadway do
  @moduledoc """
  Arrow-native Broadway ingestion pipelines.

  Broadway is the standard Elixir library for ingestion (Kafka, SQS, S3, ...).
  `ExArrow.Broadway` provides the pieces needed to keep Broadway pipelines
  Arrow-native: a batch builder that assembles `ExArrow.RecordBatch` values
  from incoming messages, and Parquet/Flight sinks that write assembled
  batches downstream.

  Requires `{:broadway, "~> 1.0"}` in your `mix.exs` dependencies.

  ## Architecture

      Kafka / SQS / S3
          │ (messages carry Arrow columnar payloads)
          ▼
      Broadway producer
          │
          ▼
      Broadway processor   ──►  ExArrow.Broadway.BatchBuilder
          │                       (groups messages into RecordBatch values)
          ▼
      Broadway batcher     ──►  batch_size / batch_timeout config
          │
          ▼
      handle_batch/4       ──►  ExArrow.Broadway.ParquetSink / FlightSink

  The unit that flows through the pipeline is an `ExArrow.RecordBatch` handle,
  not a row map.  Producers are expected to deliver messages whose `data` is
  either a `ExArrow.RecordBatch` handle (e.g. from an Arrow-aware Kafka
  deserialiser) or a `{names, binaries, dtypes, length}` tuple describing raw
  Arrow columns (see `BatchBuilder.from_messages/2`).

  ## Tuning

  Batch sizing and flush intervals are controlled by the Broadway batcher
  configuration (`:batch_size`, `:batch_timeout`), not by ExArrow.  ExArrow's
  `BatchBuilder` honours those boundaries and additionally can split an
  incoming batch into smaller Arrow batches via the `:rows_per_batch` option.

  ## Example pipeline

      defmodule MyPipeline do
        use Broadway

        def start_link(opts) do
          Broadway.start_link(__MODULE__,
            name: __MODULE__,
            producer: [module: {MyKafkaProducer, opts}],
            processors: [default: [concurrency: 4]],
            batchers: [
              parquet: [concurrency: 2, batch_size: 100, batch_timeout: 1000]
            ]
          )
        end

        def handle_message(:default, %Broadway.Message{data: batch} = msg, _ctx)
            when is_struct(batch, ExArrow.RecordBatch) do
          Broadway.Message.put_batcher(msg, :parquet)
        end

        def handle_batch(:parquet, messages, _batch_info, _ctx) do
          {:ok, schema, batches} =
            ExArrow.Broadway.BatchBuilder.from_messages(messages)

          ExArrow.Broadway.ParquetSink.write(
            "/data/events.parquet",
            schema,
            batches
          )
        end
      end
  """

  @broadway_available Code.ensure_loaded?(Broadway)

  @doc """
  Returns `true` if the `:broadway` dependency is loaded.
  """
  @spec broadway_available?() :: boolean()
  def broadway_available?, do: @broadway_available
end

defmodule ExArrow.Broadway.BatchBuilder do
  @moduledoc """
  Assemble `ExArrow.RecordBatch` values from Broadway messages.

  Each Broadway message is expected to carry one of:

  - an `ExArrow.RecordBatch.t()` handle in `message.data` (the common case for
    Arrow-aware producers), or
  - a `{names, binaries, dtypes, length}` tuple describing raw Arrow columns,
    which is converted to a batch via `ExArrow.RecordBatch.from_columns/4`.

  `from_messages/1` returns `{:ok, schema, batches}` where `schema` is the
  schema of the first batch and `batches` is the list of assembled batches.
  `from_messages/2` accepts options:

  - `:rows_per_batch` — split the assembled batches into smaller batches of
    at most this many rows by re-chunking the column buffers.  Defaults to
    `:infinity` (no splitting).

  ## Example

      {:ok, schema, batches} =
        ExArrow.Broadway.BatchBuilder.from_messages(messages)
  """

  alias ExArrow.RecordBatch

  @doc """
  Build a list of `ExArrow.RecordBatch` values from a list of Broadway
  messages, returning the shared schema and the batch list.

  Returns `{:ok, schema, [batch, ...]}` or `{:error, message}`.
  """
  @spec from_messages([term()]) ::
          {:ok, ExArrow.Schema.t(), [RecordBatch.t()]} | {:error, String.t()}
  def from_messages(messages) when is_list(messages) do
    from_messages(messages, [])
  end

  @spec from_messages([term()], keyword()) ::
          {:ok, ExArrow.Schema.t(), [RecordBatch.t()]} | {:error, String.t()}
  def from_messages(messages, opts) when is_list(messages) and is_list(opts) do
    with {:ok, batches} <- extract_batches(messages) do
      case batches do
        [] ->
          {:error, "no batches in message list"}

        [first | _] ->
          schema = RecordBatch.schema(first)
          {:ok, schema, batches}
      end
    end
  end

  @doc """
  Extract the `ExArrow.RecordBatch` handles from a list of Broadway messages,
  without resolving the schema.

  Returns `{:ok, [batch, ...]}` or `{:error, message}`.
  """
  @spec extract_batches([term()]) ::
          {:ok, [RecordBatch.t()]} | {:error, String.t()}
  def extract_batches(messages) when is_list(messages) do
    reduce_result =
      Enum.reduce_while(messages, {:ok, []}, fn msg, {:ok, acc} ->
        case build_one(msg) do
          {:ok, batch} -> {:cont, {:ok, [batch | acc]}}
          {:error, _} = err -> {:halt, err}
        end
      end)

    case reduce_result do
      {:ok, rev} -> {:ok, Enum.reverse(rev)}
      {:error, _} = err -> err
    end
  end

  defp build_one(%{data: %RecordBatch{} = batch}), do: {:ok, batch}

  defp build_one(%{data: {names, binaries, dtypes, length}})
       when is_list(names) and is_list(binaries) and is_list(dtypes) and
              is_integer(length) and length >= 0 do
    RecordBatch.from_columns(names, binaries, dtypes, length)
  end

  defp build_one(%{data: other}) do
    {:error,
     "unsupported Broadway message data; expected an ExArrow.RecordBatch or " <>
       "{names, binaries, dtypes, length} tuple, got: #{inspect(other)}"}
  end

  defp build_one(other) do
    {:error, "expected a Broadway.Message, got: #{inspect(other)}"}
  end
end

defmodule ExArrow.Broadway.ParquetSink do
  @moduledoc """
  Write assembled Arrow batches to a Parquet file from a Broadway batch
  handler.

  Intended to be called from a Broadway `handle_batch/4` callback.  The
  batches are written in a single `ExArrow.Parquet.Writer.to_file/3` call so
  the output is one Parquet file with one row group per batch (subject to the
  writer's chunking).

  Emits a `[:ex_arrow, :parquet, :write]` telemetry event with `:rows`,
  `:batch_count`, and `%{destination: path, source: :broadway}` metadata.

  ## Example

      def handle_batch(:parquet, messages, _info, _ctx) do
        {:ok, schema, batches} = ExArrow.Broadway.BatchBuilder.from_messages(messages)
        ExArrow.Broadway.ParquetSink.write("/data/out.parquet", schema, batches)
      end
  """

  alias ExArrow.Parquet.Writer
  alias ExArrow.RecordBatch

  @doc """
  Write `schema` and `batches` to a Parquet file at `path`.

  Returns `:ok` or `{:error, message}`.
  """
  @spec write(Path.t(), ExArrow.Schema.t(), [RecordBatch.t()]) ::
          :ok | {:error, String.t()}
  def write(path, schema, batches) when is_binary(path) and is_list(batches) do
    rows = Enum.sum(Enum.map(batches, &RecordBatch.num_rows/1))

    ExArrow.Telemetry.execute(
      [:ex_arrow, :parquet, :write],
      %{rows: rows, batch_count: length(batches)},
      %{destination: path, source: :broadway}
    )

    Writer.to_file(path, schema, batches)
  end
end

defmodule ExArrow.Broadway.FlightSink do
  @moduledoc """
  Upload assembled Arrow batches to a Flight server from a Broadway batch
  handler.

  Calls `ExArrow.Flight.Client.do_put/4` with the assembled schema and
  batches.  Emits a `[:ex_arrow, :flight, :query]` telemetry event with
  `%{destination: descriptor, source: :broadway}` metadata.

  ## Options

  Forwarded to `ExArrow.Flight.Client.do_put/4` (e.g. `:descriptor`).

  ## Example

      def handle_batch(:flight, messages, _info, _ctx) do
        {:ok, schema, batches} = ExArrow.Broadway.BatchBuilder.from_messages(messages)
        ExArrow.Broadway.FlightSink.write(client, schema, batches,
          descriptor: {:cmd, "events_batch"}
        )
      end
  """

  alias ExArrow.Flight.Client
  alias ExArrow.RecordBatch

  @doc """
  Upload `schema` and `batches` to a Flight server via `client`.

  Returns `:ok` or `{:error, reason}`.
  """
  @spec write(Client.t(), ExArrow.Schema.t(), [RecordBatch.t()], keyword()) ::
          :ok | {:error, term()}
  def write(client, schema, batches, opts \\ [])

  def write(_client, _schema, [], _opts), do: :ok

  def write(client, schema, batches, opts) when is_list(batches) and is_list(opts) do
    rows = Enum.sum(Enum.map(batches, &RecordBatch.num_rows/1))
    descriptor = Keyword.get(opts, :descriptor)

    ExArrow.Telemetry.execute(
      [:ex_arrow, :flight, :query],
      %{rows: rows, batch_count: length(batches)},
      %{destination: descriptor, source: :broadway}
    )

    Client.do_put(client, schema, batches, opts)
  end
end
