defmodule ExArrow.Stream do
  @moduledoc """
  Arrow stream handle (opaque reference to native stream).

  Used for IPC streaming, Flight result streams, and ADBC execute results.
  Yields record batches via an Elixir iterator; data stays in native memory until consumed.

  The `backend` field is `:ipc` for IPC/Flight streams and `:adbc` for ADBC result streams.
  Callers typically do not set it; it is set when the stream is created.
  """
  alias ExArrow.Native
  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  @opaque t :: %__MODULE__{resource: reference(), backend: :ipc | :adbc}
  defstruct [:resource, backend: :ipc]

  @doc """
  Returns the schema of this stream (without consuming it).
  Returns `{:error, message}` if the stream is invalid (e.g. poisoned lock).
  """
  @spec schema(t()) :: {:ok, Schema.t()} | {:error, String.t()}
  def schema(%__MODULE__{resource: ref, backend: :adbc}) do
    case Native.adbc_stream_schema(ref) do
      {:error, msg} -> {:error, msg}
      schema_ref -> {:ok, Schema.from_ref(schema_ref)}
    end
  end

  def schema(%__MODULE__{resource: ref, backend: :ipc}) do
    case Native.ipc_stream_schema(ref) do
      {:error, msg} -> {:error, msg}
      schema_ref -> {:ok, Schema.from_ref(schema_ref)}
    end
  end

  @doc """
  Returns the next record batch from the stream, or nil when done.
  Returns `{:error, message}` on read error.
  """
  @spec next(t()) :: RecordBatch.t() | nil | {:error, String.t()}
  def next(%__MODULE__{resource: ref, backend: :adbc}) do
    case Native.adbc_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> RecordBatch.from_ref(batch_ref)
      {:error, msg} -> {:error, msg}
    end
  end

  def next(%__MODULE__{resource: ref, backend: :ipc}) do
    case Native.ipc_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> RecordBatch.from_ref(batch_ref)
      {:error, msg} -> {:error, msg}
    end
  end
end
