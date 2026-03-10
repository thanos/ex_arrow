defmodule ExArrow.Parquet.Writer do
  @moduledoc """
  Parquet file writer.

  Accepts an `ExArrow.Schema` handle and a list of `ExArrow.RecordBatch` handles
  and produces Parquet output either to a file path or to an in-memory binary.

  ## Examples

      # Write batches to a file
      :ok = ExArrow.Parquet.Writer.to_file("/out/result.parquet", schema, batches)

      # Serialise to an in-memory binary (e.g. to upload to object storage)
      {:ok, parquet_bytes} = ExArrow.Parquet.Writer.to_binary(schema, batches)
  """

  alias ExArrow.Native
  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  @doc """
  Write `schema` and `batches` to a Parquet file at `path`.

  Returns `:ok` or `{:error, message}`.
  """
  @spec to_file(Path.t(), Schema.t(), [RecordBatch.t()]) :: :ok | {:error, String.t()}
  def to_file(path, %Schema{resource: s}, batches)
      when is_binary(path) and is_list(batches) do
    batch_refs = Enum.map(batches, &RecordBatch.resource_ref/1)

    case Native.parquet_writer_to_file(path, s, batch_refs) do
      :ok -> :ok
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Serialise `schema` and `batches` to a Parquet binary in memory.

  Returns `{:ok, binary}` or `{:error, message}`.
  """
  @spec to_binary(Schema.t(), [RecordBatch.t()]) :: {:ok, binary()} | {:error, String.t()}
  def to_binary(%Schema{resource: s}, batches) when is_list(batches) do
    batch_refs = Enum.map(batches, &RecordBatch.resource_ref/1)

    case Native.parquet_writer_to_binary(s, batch_refs) do
      {:ok, binary} -> {:ok, binary}
      {:error, msg} -> {:error, msg}
    end
  end
end
