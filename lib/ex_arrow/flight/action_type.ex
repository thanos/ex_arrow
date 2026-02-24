defmodule ExArrow.Flight.ActionType do
  @moduledoc """
  Describes an action type supported by a Flight server.

  Returned as elements of the list from `ExArrow.Flight.Client.list_actions/1`.

  Fields:
  - `type` — machine-readable action name (e.g. `"clear"`, `"ping"`).
  - `description` — human-readable description of what the action does.
  """

  @type t :: %__MODULE__{type: String.t(), description: String.t()}

  defstruct [:type, :description]

  @spec from_native({any(), any()}) :: ExArrow.Flight.ActionType.t()
  @doc false
  def from_native({type, description}), do: %__MODULE__{type: type, description: description}
end
