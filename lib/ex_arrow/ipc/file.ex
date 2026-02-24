defmodule ExArrow.IPC.File do
  @moduledoc """
  IPC file format reader: random access to schema, batch count, and batches by index.

  Use `from_file/1` or `from_binary/1` to open, then `schema/1`, `batch_count/1`,
  and `get_batch/2` for random access. Data remains in native memory until you
  use `ExArrow.RecordBatch` / `ExArrow.Schema` accessors.
  """
  alias ExArrow.Native
  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Opens an IPC file (file format) from a path. Enables random access to batches.
  """
  @spec from_file(Path.t()) :: {:ok, t()} | {:error, String.t()}
  def from_file(path) when is_binary(path) do
    case Native.ipc_file_open(path) do
      {:ok, file_ref} -> {:ok, %__MODULE__{resource: file_ref}}
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Opens an IPC file format from in-memory binary (e.g. for tests). Random access as with from_file.
  """
  @spec from_binary(binary()) :: {:ok, t()} | {:error, String.t()}
  def from_binary(binary) do
    case Native.ipc_file_open_from_binary(binary) do
      {:ok, file_ref} -> {:ok, %__MODULE__{resource: file_ref}}
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Returns the schema of the IPC file as an `ExArrow.Schema` handle.
  Returns `{:error, message}` if the file handle is invalid (e.g. poisoned lock).
  """
  @spec schema(t()) :: {:ok, Schema.t()} | {:error, String.t()}
  def schema(%__MODULE__{resource: ref}) do
    case Native.ipc_file_schema(ref) do
      {:error, _} = err -> err
      schema_ref -> {:ok, Schema.from_ref(schema_ref)}
    end
  end

  @doc """
  Returns the number of record batches in the file.
  """
  @spec batch_count(t()) :: non_neg_integer()
  def batch_count(%__MODULE__{resource: ref}) do
    Native.ipc_file_num_batches(ref)
  end

  @doc """
  Returns the record batch at the given 0-based index as an `ExArrow.RecordBatch` handle.
  """
  @spec get_batch(t(), non_neg_integer()) :: {:ok, RecordBatch.t()} | {:error, String.t()}
  def get_batch(%__MODULE__{resource: ref}, index) when is_integer(index) and index >= 0 do
    case Native.ipc_file_get_batch(ref, index) do
      {:ok, batch_ref} -> {:ok, RecordBatch.from_ref(batch_ref)}
      {:error, _} = err -> err
    end
  end

  def get_batch(_file, index), do: {:error, "invalid batch index: #{inspect(index)}"}
end
