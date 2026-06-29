defmodule ExArrow.Sink.Helpers do
  @moduledoc false

  alias ExArrow.RecordBatch
  alias ExArrow.Stream

  @spec normalise_source(term(), String.t()) ::
          {:ok, ExArrow.Schema.t(), [RecordBatch.t()]} | {:error, String.t()}
  def normalise_source(source, sink_name) do
    cond do
      Stream.stream?(source) ->
        with {:ok, schema} <- Stream.schema(source) do
          {:ok, schema, Stream.to_list(source)}
        end

      is_tuple(source) and tuple_size(source) == 2 ->
        {schema, batches} = source

        if is_list(batches) do
          {:ok, schema, batches}
        else
          {:error, "expected {schema, [batches]}, got: #{inspect(source)}"}
        end

      is_list(source) ->
        normalise_list(source, sink_name)

      true ->
        {:error, "unsupported source for #{sink_name} sink: #{inspect(source)}"}
    end
  end

  defp normalise_list([], sink_name) do
    {:error, "cannot write an empty batch list to #{sink_name}"}
  end

  defp normalise_list([first | _] = batches, _sink_name) do
    if RecordBatch.record_batch?(first) do
      {:ok, RecordBatch.schema(first), batches}
    else
      {:error, "expected a list of ExArrow.RecordBatch, got: #{inspect(first)}"}
    end
  end
end

defmodule ExArrow.Sink.Parquet do
  @moduledoc """
  Write an Arrow stream or batch list to a Parquet file.

  A thin sink that consumes an `ExArrow.Stream.t()` (or a
  `{schema, [batches]}` tuple) and writes it to a Parquet file via
  `ExArrow.Parquet.Writer.to_file/3`.  Emits a
  `[:ex_arrow, :parquet, :write]` telemetry event.

  ## Example

      {:ok, stream} = ExArrow.Stream.from_flight_sql(client, "SELECT * FROM t")
      ExArrow.Sink.Parquet.write(stream, "/data/out.parquet")
  """

  alias ExArrow.Parquet.Writer
  alias ExArrow.RecordBatch
  alias ExArrow.Sink.Helpers
  alias ExArrow.Stream

  @doc """
  Write `source` to a Parquet file at `path`.

  `source` may be:

  - an `ExArrow.Stream.t()` — schema is read with `ExArrow.Stream.schema/1`
    and batches with `ExArrow.Stream.to_list/1`.
  - a `{schema, [batches]}` tuple.
  - a list of `ExArrow.RecordBatch.t()` — schema is taken from the first batch.

  Returns `:ok` or `{:error, message}`.
  """
  @spec write(Stream.t() | {ExArrow.Schema.t(), [RecordBatch.t()]} | [RecordBatch.t()], Path.t()) ::
          :ok | {:error, String.t()}
  def write(source, path) when is_binary(path) do
    with {:ok, schema, batches} <- Helpers.normalise_source(source, "Parquet") do
      rows = Enum.sum(Enum.map(batches, &RecordBatch.num_rows/1))

      ExArrow.Telemetry.execute(
        [:ex_arrow, :parquet, :write],
        %{rows: rows, batch_count: length(batches)},
        %{destination: path, source: :sink}
      )

      Writer.to_file(path, schema, batches)
    end
  end
end

defmodule ExArrow.Sink.Flight do
  @moduledoc """
  Upload an Arrow stream or batch list to a Flight server.

  Wraps `ExArrow.Flight.Client.do_put/4`.  Emits a
  `[:ex_arrow, :flight, :query]` telemetry event with the destination
  descriptor in the metadata.

  ## Example

      {:ok, stream} = ExArrow.Stream.from_parquet("/data/events.parquet")
      ExArrow.Sink.Flight.write(stream, client, descriptor: {:cmd, "events"})
  """

  alias ExArrow.Flight.Client
  alias ExArrow.RecordBatch
  alias ExArrow.Sink.Helpers
  alias ExArrow.Stream

  @doc """
  Upload `source` to a Flight server via `client`.

  Accepts the same `source` shapes as `ExArrow.Sink.Parquet.write/2`.  `opts`
  are forwarded to `ExArrow.Flight.Client.do_put/4` (e.g. `:descriptor`).

  Returns `:ok` or `{:error, reason}`.
  """
  @spec write(
          Stream.t() | {ExArrow.Schema.t(), [RecordBatch.t()]} | [RecordBatch.t()],
          Client.t(),
          keyword()
        ) :: :ok | {:error, term()}
  def write(source, client, opts \\ [])

  def write([], _client, _opts), do: :ok

  def write(source, client, opts) do
    with {:ok, schema, batches} <- Helpers.normalise_source(source, "Flight") do
      emit_flight_telemetry(batches, Keyword.get(opts, :descriptor))
      Client.do_put(client, schema, batches, opts)
    end
  end

  defp emit_flight_telemetry(batches, descriptor) do
    rows = Enum.sum(Enum.map(batches, &RecordBatch.num_rows/1))

    ExArrow.Telemetry.execute(
      [:ex_arrow, :flight, :query],
      %{rows: rows, batch_count: length(batches)},
      %{destination: descriptor, source: :sink}
    )
  end
end

defmodule ExArrow.Sink.DataFrame do
  @moduledoc """
  Convert an Arrow stream or batch into an Explorer DataFrame.

  Delegates to `ExArrow.DataFrame.from_arrow/1`, which accepts either an
  `ExArrow.RecordBatch.t()` or an `ExArrow.Stream.t()`.  Requires the optional
  `{:explorer, "~> 0.11"}` dependency.

  ## Example

      {:ok, df} = ExArrow.Sink.DataFrame.write(stream)
  """

  @doc """
  Convert `source` to an `Explorer.DataFrame`.

  Returns `{:ok, dataframe}` or `{:error, message}`.
  """
  @spec write(ExArrow.RecordBatch.t() | ExArrow.Stream.t()) ::
          {:ok, Explorer.DataFrame.t()} | {:error, String.t()}
  def write(source), do: ExArrow.DataFrame.from_arrow(source)
end

defmodule ExArrow.Sink.Nx do
  @moduledoc """
  Convert an Arrow batch into an `Nx.Tensor`.

  Delegates to `ExArrow.to_nx/1`.  Requires the optional
  `{:nx, "~> 0.12"}` dependency.

  ## Example

      {:ok, tensor} = ExArrow.Sink.Nx.write(batch)
  """

  @doc """
  Convert `batch` to an `Nx.Tensor`.

  Returns `{:ok, tensor}` or `{:error, message}`.
  """
  @spec write(ExArrow.RecordBatch.t()) :: {:ok, Nx.Tensor.t()} | {:error, String.t()}
  def write(batch), do: ExArrow.to_nx(batch)
end
