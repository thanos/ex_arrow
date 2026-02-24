defmodule ExArrow.IPC.Writer do
  @moduledoc """
  IPC stream/file writer: write Arrow record batches to binary or file.
  """
  alias ExArrow.Native
  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  @doc """
  Writes record batches (as a stream or list) to a binary.
  Batches must share the same schema.
  """
  @spec to_binary(Schema.t(), [RecordBatch.t()] | Enumerable.t()) ::
          {:ok, binary()} | {:error, String.t()}
  def to_binary(schema, batches) do
    schema_ref = Schema.resource_ref(schema)
    batch_refs = Enum.map(batches, &RecordBatch.resource_ref/1)

    case Native.ipc_writer_to_binary(schema_ref, batch_refs) do
      {:ok, binary} -> {:ok, binary}
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Writes record batches to a file at the given path.
  """
  @spec to_file(Path.t(), Schema.t(), [RecordBatch.t()] | Enumerable.t()) ::
          :ok | {:error, String.t()}
  def to_file(path, schema, batches) when is_binary(path) do
    schema_ref = Schema.resource_ref(schema)
    batch_refs = Enum.map(batches, &RecordBatch.resource_ref/1)

    case Native.ipc_writer_to_file(path, schema_ref, batch_refs) do
      :ok -> :ok
      {:error, msg} -> {:error, msg}
    end
  end
end
