defmodule ExArrow.Flight.Server do
  @moduledoc """
  Arrow Flight server: minimal echo server for do_put / do_get by ticket.

  Full API (list_flights, get_flight_info, actions) in later milestones.
  """
  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Starts a Flight server on the given port.
  Stub: returns error until NIF is implemented.
  """
  @spec start_link(non_neg_integer(), keyword()) :: {:ok, t()} | {:error, term()}
  def start_link(_port, _opts \\ []) do
    {:error, :not_implemented}
  end
end
