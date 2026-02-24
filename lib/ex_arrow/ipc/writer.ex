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
  def to_binary(%Schema{resource: schema_ref}, batches) do
    batch_refs = batches |> Enum.to_list() |> Enum.map(& &1.resource)
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
  def to_file(path, %Schema{resource: schema_ref}, batches) when is_binary(path) do
    batch_refs = batches |> Enum.to_list() |> Enum.map(& &1.resource)
    case Native.ipc_writer_to_file(path, schema_ref, batch_refs) do
      :ok -> :ok
      {:error, msg} -> {:error, msg}
    end
  end
end
