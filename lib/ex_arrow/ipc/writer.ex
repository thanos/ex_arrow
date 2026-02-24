defmodule ExArrow.IPC.Writer do
  @moduledoc """
  IPC stream/file writer: write Arrow record batches to binary or file.
  """
  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  @doc """
  Writes record batches (as a stream or list) to a binary.
  Stub: returns error until NIF is implemented.
  """
  @spec to_binary(Schema.t(), [RecordBatch.t()] | Enumerable.t()) ::
          {:ok, binary()} | {:error, term()}
  def to_binary(_schema, _batches) do
    {:error, :not_implemented}
  end

  @doc """
  Writes record batches to a file at the given path.
  Stub: returns error until NIF is implemented.
  """
  @spec to_file(Path.t(), Schema.t(), [RecordBatch.t()] | Enumerable.t()) ::
          :ok | {:error, term()}
  def to_file(_path, _schema, _batches) do
    {:error, :not_implemented}
  end
end
