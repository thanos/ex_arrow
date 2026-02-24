defmodule ExArrow.Table do
  @moduledoc """
  Arrow table handle (opaque reference to native table).

  A table is a collection of record batches with a shared schema.
  Data stays in native memory.
  """
  alias ExArrow.Schema

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Returns the schema of this table.
  Stub: returns nil until NIF is implemented.
  """
  @spec schema(t()) :: Schema.t() | nil
  def schema(_table) do
    nil
  end

  @doc """
  Returns the number of rows in this table.
  Stub: returns 0 until NIF is implemented.
  """
  @spec num_rows(t()) :: non_neg_integer()
  def num_rows(_table) do
    0
  end
end
