defmodule ExArrow.Schema do
  @moduledoc """
  Arrow schema handle (opaque reference to native schema).

  Holds metadata (field names and types) for a table or record batch.
  Data lives in native memory; this module provides a stable handle and
  Elixir-friendly accessors for small metadata.
  """
  alias ExArrow.Field
  alias ExArrow.Native

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Returns the list of fields in the schema (Elixir structs).
  """
  @spec fields(t()) :: [Field.t()]
  def fields(%__MODULE__{resource: ref}) do
    ref
    |> Native.schema_fields()
    |> Enum.map(fn {name, type} -> %Field{name: name, type: type} end)
  end
end
