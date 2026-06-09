defmodule ExArrow.RecordBatch do
  @moduledoc """
  Arrow record batch handle (opaque reference to native record batch).

  A batch is a collection of column arrays with a shared schema and row count.
  It sits between `ExArrow.Array` (one column) and `ExArrow.Table` or 
  `ExArrow.Stream` (multiple batches).  Data stays in native memory; accessors
  return handles or small metadata.

  ## Position in the hierarchy

      Schema ── Field (metadata)
                  │
      RecordBatch ── Array (one per column)
                        │
      Table / Stream ── RecordBatch (one or more)
  """
  alias ExArrow.Native
  alias ExArrow.Schema

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc false
  @spec record_batch?(term()) :: boolean()
  def record_batch?(%__MODULE__{}), do: true
  def record_batch?(_), do: false

  @doc false
  @spec from_ref(reference()) :: t()
  def from_ref(ref), do: %__MODULE__{resource: ref}

  @doc false
  @spec resource_ref(t()) :: reference()
  def resource_ref(%__MODULE__{resource: ref}), do: ref

  @doc """
  Returns the schema of this record batch.
  """
  @spec schema(t()) :: Schema.t()
  def schema(%__MODULE__{resource: ref}) do
    ref |> Native.record_batch_schema() |> Schema.from_ref()
  end

  @doc """
  Returns the number of rows in this batch.
  """
  @spec num_rows(t()) :: non_neg_integer()
  def num_rows(%__MODULE__{resource: ref}) do
    Native.record_batch_num_rows(ref)
  end

  @doc """
  Returns the number of columns in this batch.

  Derived from the batch's schema — no separate NIF call is needed.

  ## Examples

      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream}  = ExArrow.IPC.Reader.from_binary(ipc_bin)
      batch = ExArrow.Stream.next(stream)
      ExArrow.RecordBatch.num_columns(batch)  #=> 2
  """
  @spec num_columns(t()) :: non_neg_integer()
  def num_columns(%__MODULE__{} = batch) do
    batch |> schema() |> Schema.fields() |> length()
  end

  @doc """
  Returns the column names of this batch.

  Derived from the batch's schema.  Equivalent to
  `ExArrow.Schema.field_names(ExArrow.RecordBatch.schema(batch))`.

  ## Examples

      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream}  = ExArrow.IPC.Reader.from_binary(ipc_bin)
      batch = ExArrow.Stream.next(stream)
      ExArrow.RecordBatch.column_names(batch)  #=> ["id", "name"]
  """
  @spec column_names(t()) :: [String.t()]
  def column_names(%__MODULE__{} = batch) do
    batch |> schema() |> Schema.field_names()
  end
end
