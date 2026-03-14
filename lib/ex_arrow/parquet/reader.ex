defmodule ExArrow.Parquet.Reader do
  @moduledoc """
  Parquet file reader: open a `.parquet` file or an in-memory binary and
  receive an `ExArrow.Stream` that yields record batches.

  The stream interface is identical to `ExArrow.IPC.Reader` and ADBC streams —
  use `ExArrow.Stream.schema/1`, `ExArrow.Stream.next/1`, and
  `ExArrow.Stream.to_list/1` to consume it.

  ### How Parquet is read (lazy row-group streaming)

  Parquet has a footer that is scanned once when the stream is opened, making
  the schema immediately available via `ExArrow.Stream.schema/1`.  Row groups
  are then decoded **on demand**: each call to `ExArrow.Stream.next/1` reads
  and decodes the next row group without touching the rest of the file.

  This means:

  - Peak memory scales with the largest single row group, not the full file.
  - You can stop consuming after N batches and the remaining row groups are
    never decoded.
  - For file-backed streams (`from_file/1`) the underlying file handle stays
    open until the stream resource is garbage-collected.
  - For binary-backed streams (`from_binary/1`) the bytes are held in native
    memory and released when the resource is collected.

  ## Examples

      # Consume lazily — only the requested row groups are decoded
      {:ok, stream}  = ExArrow.Parquet.Reader.from_file("/data/events.parquet")
      {:ok, schema}  = ExArrow.Stream.schema(stream)
      IO.inspect ExArrow.Schema.field_names(schema)
      first_batch = ExArrow.Stream.next(stream)   # decodes row-group 0
      next_batch  = ExArrow.Stream.next(stream)   # decodes row-group 1
      nil         = ExArrow.Stream.next(stream)   # nil when exhausted

      # Collect all row groups at once
      {:ok, stream} = ExArrow.Parquet.Reader.from_file("/data/events.parquet")
      batches = ExArrow.Stream.to_list(stream)

      # Read from an in-memory binary (e.g. fetched from object storage)
      parquet_bytes = File.read!("/data/events.parquet")
      {:ok, stream} = ExArrow.Parquet.Reader.from_binary(parquet_bytes)
      batch = ExArrow.Stream.next(stream)

      # Pipe into Explorer
      {:ok, stream} = ExArrow.Parquet.Reader.from_file("/data/report.parquet")
      {:ok, df}     = ExArrow.Explorer.from_stream(stream)
  """

  alias ExArrow.Native
  alias ExArrow.Stream

  @doc """
  Open a Parquet file at `path` for lazy row-group streaming.

  Scans the Parquet footer to make the schema available, then returns a stream
  whose row groups are decoded on demand by `ExArrow.Stream.next/1`.  The file
  handle remains open until the stream resource is garbage-collected.

  Returns `{:ok, stream}` where `stream` is an `ExArrow.Stream` with
  `:parquet` backend, or `{:error, message}` if the file does not exist or
  is not valid Parquet.

  ## Example

      {:ok, stream} = ExArrow.Parquet.Reader.from_file("/data/events.parquet")
      {:ok, schema} = ExArrow.Stream.schema(stream)
      field_names   = ExArrow.Schema.field_names(schema)
      first_batch   = ExArrow.Stream.next(stream)  # decodes row-group 0
      batches       = ExArrow.Stream.to_list(stream)  # collects remaining
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
