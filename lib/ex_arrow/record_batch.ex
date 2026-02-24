defmodule ExArrow.RecordBatch do
  @moduledoc """
  Arrow record batch handle (opaque reference to native record batch).

  A batch is a collection of arrays (columns) with a shared row count.
  Data stays in native memory; accessors return handles or small metadata.
  """
  alias ExArrow.Native
  alias ExArrow.Schema

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

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
end
