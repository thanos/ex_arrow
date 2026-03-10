defmodule ExArrow.Explorer do
  @moduledoc """
  Bridge between ExArrow and Explorer DataFrames.

  Converts between `ExArrow.Stream` / `ExArrow.RecordBatch` and
  `Explorer.DataFrame` via an in-memory IPC round-trip.  No intermediate CSV
  or row-by-row conversion is performed — the serialisation stays columnar.

  Requires `{:explorer, "~> 0.8"}` in your `mix.exs` dependencies.

  ## Examples

      # ExArrow.Stream → Explorer.DataFrame
      {:ok, stream} = ExArrow.IPC.Reader.from_file("/data/events.arrow")
      {:ok, df}     = ExArrow.Explorer.from_stream(stream)

      # ExArrow.RecordBatch → Explorer.DataFrame
      {:ok, df} = ExArrow.Explorer.from_record_batch(batch)

      # Explorer.DataFrame → ExArrow.Stream
      {:ok, stream} = ExArrow.Explorer.to_stream(df)

      # Explorer.DataFrame → [ExArrow.RecordBatch]
      {:ok, batches} = ExArrow.Explorer.to_record_batches(df)
  """

  alias ExArrow.IPC
  alias ExArrow.RecordBatch
  alias ExArrow.Stream

  @explorer_available Code.ensure_loaded?(Explorer.DataFrame)

  if @explorer_available do
    @doc """
    Convert an `ExArrow.Stream` to an `Explorer.DataFrame`.

    All batches are collected, serialised to IPC, then loaded into Explorer.
    Returns `{:ok, dataframe}` or `{:error, message}`.
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

    Returns `{:ok, stream}` or `{:error, message}`.
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
