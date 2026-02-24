defmodule ExArrow.IPC.Reader do
  @moduledoc """
  IPC stream/file reader: read Arrow data from binary or file path.

  Returns a stream of record batches (handles). Data remains in native memory.
  """
  alias ExArrow.Stream

  @doc """
  Opens an IPC stream from binary data.
  Returns an `ExArrow.Stream` handle that yields record batches via `ExArrow.Stream.next/1`.
  Stub: raises until NIF is implemented.
  """
  @spec from_binary(binary()) :: {:ok, Stream.t()} | {:error, term()}
  def from_binary(_binary) do
    {:error, :not_implemented}
  end

  @doc """
  Opens an IPC stream from a file path.
  Stub: raises until NIF is implemented.
  """
  @spec from_file(Path.t()) :: {:ok, Stream.t()} | {:error, term()}
  def from_file(_path) do
    {:error, :not_implemented}
  end
end
