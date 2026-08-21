defmodule ExArrow.Parquet.Writer do
  @moduledoc """
  Parquet file writer: serialise Arrow record batches to a `.parquet` file or
  to an in-memory binary.

  Accepts an `ExArrow.Schema` handle and a list of `ExArrow.RecordBatch` handles
  produced by any ExArrow source (IPC reader, ADBC execute, Flight do_get,
  or compute kernels).

  ### Write options (v0.8+)

  * `:compression` — `:none` (alias `:uncompressed`) | `:snappy` | `:zstd` |
    `{:zstd, level}` (level 1..22) | `:lz4` | `:gzip`
  * `:row_group_size` — positive integer (max rows per row group)
  * `:dictionary` — boolean

  ## Examples

      :ok =
        ExArrow.Parquet.Writer.to_file("/out/result.parquet", schema, batches,
          compression: :zstd,
          row_group_size: 64_000
        )

      {:ok, bytes} =
        ExArrow.Parquet.Writer.to_binary(schema, batches, compression: :snappy)
  """

  alias ExArrow.Native
  alias ExArrow.Parquet.Opts
  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  @doc """
  Write `schema` and `batches` to a Parquet file at `path`.

  Creates or overwrites the file.  Returns `:ok` or `{:error, message}`.
  """
  @spec to_file(Path.t(), Schema.t(), [RecordBatch.t()], keyword()) ::
          :ok | {:error, String.t()}
  def to_file(path, schema, batches, opts \\ [])
      when is_binary(path) and is_list(batches) and is_list(opts) do
    with {:ok, opts} <- Opts.validate_write(opts) do
      s = Schema.resource_ref(schema)
      batch_refs = Enum.map(batches, &RecordBatch.resource_ref/1)

      case Native.parquet_writer_to_file(path, s, batch_refs, opts) do
        :ok -> :ok
        {:error, msg} -> {:error, msg}
      end
    end
  end

  @doc """
  Serialise `schema` and `batches` to a Parquet binary in memory.

  Returns `{:ok, binary}` or `{:error, message}`.
  """
  @spec to_binary(Schema.t(), [RecordBatch.t()], keyword()) ::
          {:ok, binary()} | {:error, String.t()}
  def to_binary(schema, batches, opts \\ []) when is_list(batches) and is_list(opts) do
    with {:ok, opts} <- Opts.validate_write(opts) do
      s = Schema.resource_ref(schema)
      batch_refs = Enum.map(batches, &RecordBatch.resource_ref/1)

      case Native.parquet_writer_to_binary(s, batch_refs, opts) do
        {:ok, binary} -> {:ok, binary}
        {:error, msg} -> {:error, msg}
      end
    end
  end
end
