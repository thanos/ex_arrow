defmodule ExArrow.Field do
  @moduledoc """
  Arrow field metadata (name, type, and nullability).

  Elixir-friendly struct returned from schema/record-batch metadata.  A field
  corresponds to one column in a record batch.  The `nullable` flag indicates
  whether the column can contain null values; it defaults to `true` (Arrow's
  default for fields without an explicit nullability marker).
  """

  @type t :: %__MODULE__{
          name: String.t(),
          type: term(),
          nullable: boolean()
        }

  defstruct [:name, :type, nullable: true]

  @doc false
  @spec new(String.t(), term(), boolean()) :: t()
  def new(name, type, nullable \\ true), do: %__MODULE__{name: name, type: type, nullable: nullable}
end
