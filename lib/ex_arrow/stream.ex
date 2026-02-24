defmodule ExArrow.Stream do
  @moduledoc """
  Arrow stream handle (opaque reference to native stream).

  Used for IPC streaming and Flight result streams. Yields record batches
  via an Elixir iterator; data stays in native memory until consumed.
  """
  alias ExArrow.RecordBatch
  alias ExArrow.Schema
  alias ExArrow.Native

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Returns the schema of this stream (without consuming it).
  """
  @spec schema(t()) :: Schema.t()
  def schema(%__MODULE__{resource: ref}) do
    schema_ref = Native.ipc_stream_schema(ref)
    %Schema{resource: schema_ref}
  end

  @doc """
  Returns the next record batch from the stream, or nil when done.
  Returns `{:error, message}` on read error.
  """
  @spec next(t()) :: RecordBatch.t() | nil | {:error, String.t()}
  def next(%__MODULE__{resource: ref}) do
    case Native.ipc_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> %RecordBatch{resource: batch_ref}
      {:error, msg} -> {:error, msg}
    end
  end
end
