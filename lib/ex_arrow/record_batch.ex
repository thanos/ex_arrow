defmodule ExArrow.RecordBatch do
  @moduledoc """
  Arrow record batch handle (opaque reference to native record batch).

  A batch is a collection of arrays (columns) with a shared row count.
  Data stays in native memory; accessors return handles or small metadata.
  """
  alias ExArrow.Schema

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Returns the schema of this record batch.
  Stub: returns nil until NIF is implemented.
  """
  @spec schema(t()) :: Schema.t() | nil
  def schema(_batch) do
    nil
  end

  @doc """
  Returns the number of rows in this batch.
  Stub: returns 0 until NIF is implemented.
  """
  @spec num_rows(t()) :: non_neg_integer()
  def num_rows(_batch) do
    0
  end
end
