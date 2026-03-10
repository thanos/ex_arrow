defmodule ExArrow.Parquet.Reader do
  @moduledoc """
  Parquet file reader: open a `.parquet` file or an in-memory binary and
  receive an `ExArrow.Stream` that yields record batches.

  The stream interface is identical to `ExArrow.IPC.Reader` and ADBC streams —
  use `ExArrow.Stream.schema/1`, `ExArrow.Stream.next/1`, and
  `ExArrow.Stream.to_list/1` to consume it.

  ### How Parquet is read

  Parquet is a columnar format with a footer that must be read before any row
  groups can be decoded.  ExArrow reads all row groups eagerly on `from_file/1`
  / `from_binary/1` and stores the resulting batches in native Arrow memory.
  Individual batches are only moved into BEAM terms when `ExArrow.Stream.next/1`
  is called.

  ## Examples

      # Read all batches from a file
      {:ok, stream}  = ExArrow.Parquet.Reader.from_file("/data/events.parquet")
      {:ok, schema}  = ExArrow.Stream.schema(stream)
      IO.inspect ExArrow.Schema.field_names(schema)
      batches = ExArrow.Stream.to_list(stream)

      # Read from an in-memory binary (e.g. fetched from object storage)
      parquet_bytes = File.read!("/data/events.parquet")
      {:ok, stream} = ExArrow.Parquet.Reader.from_binary(parquet_bytes)
      batch = ExArrow.Stream.next(stream)   # first batch
      nil   = ExArrow.Stream.next(stream)   # nil when exhausted

      # Pipe into Explorer
      {:ok, stream} = ExArrow.Parquet.Reader.from_file("/data/report.parquet")
      {:ok, df}     = ExArrow.Explorer.from_stream(stream)
  """

  alias ExArrow.Native
  alias ExArrow.Stream

  @doc """
  Open a Parquet file at `path` for reading.

  Returns `{:ok, stream}` where `stream` is an `ExArrow.Stream` with
  `:parquet` backend, or `{:error, message}` if the file does not exist or
  is not valid Parquet.

  ## Example

      {:ok, stream} = ExArrow.Parquet.Reader.from_file("/data/events.parquet")
      {:ok, schema} = ExArrow.Stream.schema(stream)
      field_names   = ExArrow.Schema.field_names(schema)
      batches       = ExArrow.Stream.to_list(stream)
  """
  @spec from_file(Path.t()) :: {:ok, Stream.t()} | {:error, String.t()}
  def from_file(path) when is_binary(path) do
    case Native.parquet_reader_from_file(path) do
      {:ok, ref} -> {:ok, %Stream{resource: ref, backend: :parquet}}
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Open a Parquet file from an in-memory `binary`.

  Useful when the Parquet data has already been downloaded (e.g. from S3,
  an HTTP endpoint, or another process).

  Returns `{:ok, stream}` or `{:error, message}`.

  ## Example

      parquet_bytes = File.read!("/data/events.parquet")
      {:ok, stream} = ExArrow.Parquet.Reader.from_binary(parquet_bytes)
      {:ok, schema} = ExArrow.Stream.schema(stream)
      batch         = ExArrow.Stream.next(stream)
      rows          = ExArrow.RecordBatch.num_rows(batch)
  """
  @spec from_binary(binary()) :: {:ok, Stream.t()} | {:error, String.t()}
  def from_binary(binary) when is_binary(binary) do
    case Native.parquet_reader_from_binary(binary) do
      {:ok, ref} -> {:ok, %Stream{resource: ref, backend: :parquet}}
      {:error, msg} -> {:error, msg}
    end
  end
end
