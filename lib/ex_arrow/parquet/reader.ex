defmodule ExArrow.Parquet.Reader do
  @moduledoc """
  Parquet file reader.

  Opens a Parquet file or binary and returns an `ExArrow.Stream` that yields
  record batches via `ExArrow.Stream.next/1` — the same interface used by IPC
  and ADBC streams.

  Parquet files are read fully into native memory on open (the format requires
  reading the footer before any row groups can be decoded).  Batches remain in
  native Arrow buffers until consumed; only the row group boundaries are
  translated back to BEAM terms.

  ## Examples

      {:ok, stream} = ExArrow.Parquet.Reader.from_file("/data/events.parquet")
      {:ok, schema} = ExArrow.Stream.schema(stream)
      batches = ExArrow.Stream.to_list(stream)

      # Or read from a binary already in memory:
      {:ok, stream} = ExArrow.Parquet.Reader.from_binary(parquet_bytes)
  """

  alias ExArrow.Native
  alias ExArrow.Stream

  @doc """
  Open a Parquet file at `path` for reading.

  Returns `{:ok, stream}` or `{:error, message}`.
  """
  @spec from_file(Path.t()) :: {:ok, Stream.t()} | {:error, String.t()}
  def from_file(path) when is_binary(path) do
    case Native.parquet_reader_from_file(path) do
      {:ok, ref} -> {:ok, %Stream{resource: ref, backend: :parquet}}
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Open a Parquet file from an in-memory binary.

  Returns `{:ok, stream}` or `{:error, message}`.
  """
  @spec from_binary(binary()) :: {:ok, Stream.t()} | {:error, String.t()}
  def from_binary(binary) when is_binary(binary) do
    case Native.parquet_reader_from_binary(binary) do
      {:ok, ref} -> {:ok, %Stream{resource: ref, backend: :parquet}}
      {:error, msg} -> {:error, msg}
    end
  end
end
