defmodule ExArrow.Explorer do
  @moduledoc """
  Bridge between ExArrow and Explorer DataFrames.

  Converts between `ExArrow.Stream` / `ExArrow.RecordBatch` and
  `Explorer.DataFrame` via an in-memory IPC round-trip.  No CSV or row-by-row
  conversion is performed — the path is always columnar binary.

  Requires `{:explorer, "~> 0.8"}` in your `mix.exs` dependencies.  When
  Explorer is absent every function returns `{:error, "Explorer is not
  available..."}`.

  ## Typical usage

  **ExArrow → Explorer** (e.g. after a Flight or ADBC query):

      {:ok, stream} = ExArrow.Flight.Client.do_get(client, "sales_2024")
      {:ok, df}     = ExArrow.Explorer.from_stream(stream)
      Explorer.DataFrame.filter(df, score > 0.9)

  **Explorer → ExArrow** (e.g. to write to Parquet or send via Flight):

      df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
      {:ok, stream} = ExArrow.Explorer.to_stream(df)
      :ok = ExArrow.Flight.Client.do_put(client, stream_schema, batches,
              descriptor: {:cmd, "enriched"})
  """

  alias ExArrow.IPC
  alias ExArrow.RecordBatch
  alias ExArrow.Stream

  @explorer_available Code.ensure_loaded?(Explorer.DataFrame)

  if @explorer_available do
    @doc """
    Convert an `ExArrow.Stream` to an `Explorer.DataFrame`.

    Collects all batches from `stream`, serialises them to Arrow IPC, then
    loads the binary with `Explorer.DataFrame.load_ipc_stream!/1`.

    Returns `{:ok, dataframe}` or `{:error, message}`.

    ## Example

        {:ok, stream} = ExArrow.IPC.Reader.from_file("/data/events.arrow")
        {:ok, df}     = ExArrow.Explorer.from_stream(stream)
        Explorer.DataFrame.n_rows(df)
        #=> 1_000_000
    """
    @spec from_stream(Stream.t()) :: {:ok, Explorer.DataFrame.t()} | {:error, String.t()}
    def from_stream(%Stream{} = stream) do
      with {:ok, schema} <- Stream.schema(stream),
           batches = Stream.to_list(stream),
           {:ok, binary} <- IPC.Writer.to_binary(schema, batches) do
        {:ok, Explorer.DataFrame.load_ipc_stream!(binary)}
      end
    rescue
      e -> {:error, Exception.message(e)}
    end

    @doc """
    Convert a single `ExArrow.RecordBatch` to an `Explorer.DataFrame`.

    Returns `{:ok, dataframe}` or `{:error, message}`.

    ## Example

        {:ok, stream} = ExArrow.IPC.Reader.from_file("/data/chunk.arrow")
        batch = ExArrow.Stream.next(stream)
        {:ok, df} = ExArrow.Explorer.from_record_batch(batch)
        Explorer.DataFrame.names(df)
        #=> ["id", "name", "score"]
    """
    @spec from_record_batch(RecordBatch.t()) ::
            {:ok, Explorer.DataFrame.t()} | {:error, String.t()}
    def from_record_batch(%RecordBatch{} = batch) do
      schema = RecordBatch.schema(batch)

      case IPC.Writer.to_binary(schema, [batch]) do
        {:ok, binary} ->
          {:ok, Explorer.DataFrame.load_ipc_stream!(binary)}

        {:error, msg} ->
          {:error, msg}
      end
    rescue
      e -> {:error, Exception.message(e)}
    end

    @doc """
    Convert an `Explorer.DataFrame` to an `ExArrow.Stream`.

    Serialises the dataframe to Arrow IPC via `Explorer.DataFrame.dump_ipc_stream!/1`,
    then opens an `ExArrow.Stream` from the resulting binary.

    Returns `{:ok, stream}` or `{:error, message}`.

    ## Example

        df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
        {:ok, stream} = ExArrow.Explorer.to_stream(df)
        {:ok, schema} = ExArrow.Stream.schema(stream)
        ExArrow.Schema.field_names(schema)
        #=> ["x", "y"]
    """
    @spec to_stream(Explorer.DataFrame.t()) :: {:ok, Stream.t()} | {:error, String.t()}
    def to_stream(df) do
      binary = Explorer.DataFrame.dump_ipc_stream!(df)
      IPC.Reader.from_binary(binary)
    rescue
      e -> {:error, Exception.message(e)}
    end

    @doc """
    Convert an `Explorer.DataFrame` to a list of `ExArrow.RecordBatch` handles.

    Returns `{:ok, [batch]}` or `{:error, message}`.

    ## Example

        df = Explorer.DataFrame.new(a: [10, 20], b: [1.0, 2.0])
        {:ok, batches} = ExArrow.Explorer.to_record_batches(df)
        total_rows = Enum.sum(Enum.map(batches, &ExArrow.RecordBatch.num_rows/1))
        #=> 2
    """
    @spec to_record_batches(Explorer.DataFrame.t()) ::
            {:ok, [RecordBatch.t()]} | {:error, String.t()}
    def to_record_batches(df) do
      with {:ok, stream} <- to_stream(df) do
        {:ok, Stream.to_list(stream)}
      end
    rescue
      e -> {:error, Exception.message(e)}
    end
  else
    @doc false
    def from_stream(_stream), do: {:error, explorer_missing_message()}

    @doc false
    def from_record_batch(_batch), do: {:error, explorer_missing_message()}

    @doc false
    def to_stream(_df), do: {:error, explorer_missing_message()}

    @doc false
    def to_record_batches(_df), do: {:error, explorer_missing_message()}

    defp explorer_missing_message do
      "Explorer is not available. Add {:explorer, \"~> 0.8\"} to your mix.exs dependencies."
    end
  end
end
