defmodule ExArrow.IPC.Reader do
  @moduledoc """
  IPC stream/file reader: read Arrow data from binary or file path.

  Returns a stream of record batches (handles). Data remains in native memory.
  """
  alias ExArrow.Native
  alias ExArrow.Stream

  @doc """
  Opens an IPC stream from binary data.
  Returns an `ExArrow.Stream` handle that yields record batches via `ExArrow.Stream.next/1`.
  """
  @spec from_binary(binary()) :: {:ok, Stream.t()} | {:error, String.t()}
  def from_binary(binary) do
    case Native.ipc_reader_from_binary(binary) do
      {:ok, stream_ref} -> {:ok, %Stream{resource: stream_ref}}
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Opens an IPC stream from a file path.
  """
  @spec from_file(Path.t()) :: {:ok, Stream.t()} | {:error, String.t()}
  def from_file(path) when is_binary(path) do
    case Native.ipc_reader_from_file(path) do
      {:ok, stream_ref} -> {:ok, %Stream{resource: stream_ref}}
      {:error, msg} -> {:error, msg}
    end
  end
end
