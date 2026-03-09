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

  @doc false
  @spec from_ref(reference()) :: t()
  def from_ref(ref), do: %__MODULE__{resource: ref}

  @doc false
  @spec resource_ref(t()) :: reference()
  def resource_ref(%__MODULE__{resource: ref}), do: ref

  @doc """
  Returns the list of fields in the schema (Elixir structs).
  """
  @spec fields(t()) :: [Field.t()]
  def fields(%__MODULE__{resource: ref}) do
    ref
    |> Native.schema_fields()
    |> Enum.map(fn {name, type} -> %Field{name: name, type: type} end)
  end

  @doc """
  Returns just the field names of the schema as a list of strings.
  """
  @spec field_names(t()) :: [String.t()]
  def field_names(%__MODULE__{} = schema) do
    schema |> fields() |> Enum.map(& &1.name)
  end
end
