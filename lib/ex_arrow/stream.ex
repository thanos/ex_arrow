defmodule ExArrow.Stream do
  @moduledoc """
  Opaque handle to a native Arrow record-batch stream.

  Provides a unified iterator interface over three backing sources:

  | Backend    | Created by                                              |
  |------------|---------------------------------------------------------|
  | `:ipc`     | `ExArrow.IPC.Reader` — Arrow IPC stream or file format  |
  | `:parquet` | `ExArrow.Parquet.Reader` — lazy row-group Parquet reader |
  | `:adbc`    | `ExArrow.ADBC.Statement.execute/1` — SQL result streams |

  Flight `do_get` results also use the `:ipc` backend (the Flight client
  returns an IPC stream resource).

  All three backends expose the same three functions:

  - `schema/1` — inspect the Arrow schema without consuming any batches
  - `next/1` — read the next batch on demand (`nil` when exhausted)
  - `to_list/1` — collect all remaining batches into a list

  Record batch data stays in native Arrow memory until consumed.  Callers
  never set the `backend` field directly; it is assigned by the function that
  opens the stream.
  """
  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  @opaque t :: %__MODULE__{resource: reference(), backend: :ipc | :adbc | :parquet}
  defstruct [:resource, backend: :ipc]

  @doc """
  Returns the schema of this stream (without consuming it).
  Returns `{:error, message}` if the stream is invalid (e.g. poisoned lock).
  """
  @spec schema(t()) :: {:ok, Schema.t()} | {:error, String.t()}
  def schema(%__MODULE__{resource: ref, backend: :adbc}) do
    case native().adbc_stream_schema(ref) do
      {:error, msg} -> {:error, msg}
      schema_ref -> {:ok, Schema.from_ref(schema_ref)}
    end
  end

  def schema(%__MODULE__{resource: ref, backend: :ipc}) do
    case native().ipc_stream_schema(ref) do
      {:error, msg} -> {:error, msg}
      schema_ref -> {:ok, Schema.from_ref(schema_ref)}
    end
  end

  def schema(%__MODULE__{resource: ref, backend: :parquet}) do
    schema_ref = native().parquet_stream_schema(ref)
    {:ok, Schema.from_ref(schema_ref)}
  end

  @doc """
  Returns the next record batch from the stream, or nil when done.
  Returns `{:error, message}` on read error.
  """
  @spec next(t()) :: RecordBatch.t() | nil | {:error, String.t()}
  def next(%__MODULE__{resource: ref, backend: :adbc}) do
    case native().adbc_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> RecordBatch.from_ref(batch_ref)
      {:error, msg} -> {:error, msg}
    end
  end

  def next(%__MODULE__{resource: ref, backend: :ipc}) do
    case native().ipc_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> RecordBatch.from_ref(batch_ref)
      {:error, msg} -> {:error, msg}
    end
  end

  def next(%__MODULE__{resource: ref, backend: :parquet}) do
    case native().parquet_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> RecordBatch.from_ref(batch_ref)
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Collects all remaining batches from the stream into a list.

  Stops at the first error and raises.  Returns an empty list for an
  already-exhausted stream.
  """
  @spec to_list(t()) :: [RecordBatch.t()]
  def to_list(%__MODULE__{} = stream) do
    do_collect(stream, [])
  end

  defp native, do: Application.get_env(:ex_arrow, :stream_native, ExArrow.Native)

  defp do_collect(stream, acc) do
    case next(stream) do
      nil -> Enum.reverse(acc)
      {:error, msg} -> raise "ExArrow.Stream.to_list/1 failed: #{msg}"
      batch -> do_collect(stream, [batch | acc])
    end
  end
end
