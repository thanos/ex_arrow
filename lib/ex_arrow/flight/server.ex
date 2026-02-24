defmodule ExArrow.Flight.Server do
  @moduledoc """
  Arrow Flight server: minimal echo server for do_put / do_get by ticket.

  Full API (list_flights, get_flight_info, actions) in later milestones.
  """
  alias ExArrow.Native

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Starts a Flight server on the given port (use 0 for any available port).
  """
  @spec start_link(non_neg_integer(), keyword()) :: {:ok, t()} | {:error, term()}
  def start_link(port, _opts \\ []) do
    case Native.flight_server_start(port) do
      {:ok, server_ref} -> {:ok, %__MODULE__{resource: server_ref}}
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Returns the port the server is listening on (for start_link(0) dynamic port).
  """
  @spec port(t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def port(%__MODULE__{resource: ref}) do
    {:ok, Native.flight_server_port(ref)}
  end

  @doc """
  Stops the Flight server.
  """
  @spec stop(t()) :: :ok | {:error, term()}
  def stop(%__MODULE__{resource: ref}) do
    Native.flight_server_stop(ref)
  end
end
