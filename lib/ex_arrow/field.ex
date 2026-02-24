defmodule ExArrow.Field do
  @moduledoc """
  Arrow field metadata (name and type).

  Elixir-friendly struct returned from schema/record-batch metadata.
  """
  @type t :: %__MODULE__{
          name: String.t(),
          type: term()
        }
  defstruct [:name, :type]

  @doc false
  @spec new(String.t(), term()) :: t()
  def new(name, type), do: %__MODULE__{name: name, type: type}
end
