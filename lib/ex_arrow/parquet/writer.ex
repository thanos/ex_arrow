defmodule ExArrow.Parquet.Writer do
  @moduledoc """
  Parquet file writer: serialise Arrow record batches to a `.parquet` file or
  to an in-memory binary.

  Accepts an `ExArrow.Schema` handle and a list of `ExArrow.RecordBatch` handles
  produced by any ExArrow source (IPC reader, ADBC execute, Flight do_get,
  or compute kernels).

  ## Examples

      # Write a query result to Parquet
      {:ok, db}   = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_sqlite",
                      uri: ":memory:")
      {:ok, conn} = ExArrow.ADBC.Connection.open(db)
      {:ok, stmt} = ExArrow.ADBC.Statement.new(conn, "SELECT 1 AS n, 'hello' AS s")
      {:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)
      {:ok, schema} = ExArrow.Stream.schema(stream)
      batches       = ExArrow.Stream.to_list(stream)

      :ok = ExArrow.Parquet.Writer.to_file("/out/result.parquet", schema, batches)

      # Or serialise to an in-memory binary (e.g. to upload to S3)
      {:ok, parquet_bytes} = ExArrow.Parquet.Writer.to_binary(schema, batches)

      # Round-trip: write then read back
      {:ok, rt_stream} = ExArrow.Parquet.Reader.from_binary(parquet_bytes)
      rt_batch = ExArrow.Stream.next(rt_stream)
  """

  alias ExArrow.Native
  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  @doc """
  Write `schema` and `batches` to a Parquet file at `path`.

  Creates or overwrites the file.  Returns `:ok` or `{:error, message}`.

  ## Example

      {:ok, schema}  = ExArrow.Stream.schema(stream)
      batches        = ExArrow.Stream.to_list(stream)
      :ok = ExArrow.Parquet.Writer.to_file("/data/output.parquet", schema, batches)
  """
  @spec to_file(Path.t(), Schema.t(), [RecordBatch.t()]) :: :ok | {:error, String.t()}
  def to_file(path, schema, batches)
      when is_binary(path) and is_list(batches) do
    s = Schema.resource_ref(schema)
    batch_refs = Enum.map(batches, &RecordBatch.resource_ref/1)

    case Native.parquet_writer_to_file(path, s, batch_refs) do
      :ok -> :ok
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Serialise `schema` and `batches` to a Parquet binary in memory.

  Returns `{:ok, binary}` or `{:error, message}`.  The binary can be uploaded
  to object storage, sent over HTTP, or passed to `ExArrow.Parquet.Reader.from_binary/1`
  for a round-trip.

  ## Example

      {:ok, schema} = ExArrow.Stream.schema(stream)
      batches       = ExArrow.Stream.to_list(stream)
      {:ok, bytes}  = ExArrow.Parquet.Writer.to_binary(schema, batches)

      # Upload to S3, write to a socket, etc.
      byte_size(bytes)  #=> e.g. 2048
  """
  @spec to_binary(Schema.t(), [RecordBatch.t()]) :: {:ok, binary()} | {:error, String.t()}
  def to_binary(schema, batches) when is_list(batches) do
    s = Schema.resource_ref(schema)
    batch_refs = Enum.map(batches, &RecordBatch.resource_ref/1)

    case Native.parquet_writer_to_binary(s, batch_refs) do
      {:ok, binary} -> {:ok, binary}
      {:error, msg} -> {:error, msg}
    end
  end
end
