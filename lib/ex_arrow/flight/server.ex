defmodule ExArrow.Flight.Server do
  @moduledoc """
  Arrow Flight server: minimal echo server for do_put / do_get by ticket.

  Full API (list_flights, get_flight_info, actions) in later milestones.
  """
  alias ExArrow.Native

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Starts a Flight server on the given port (use `0` for any available port).

  ## Options

  * `:host` — IP address to bind to. Defaults to `"127.0.0.1"` (loopback
    only, safest default). Pass `"0.0.0.0"` to accept connections on all
    interfaces, e.g. for a server that remote clients need to reach.

  ## Examples

      # Loopback only (default)
      {:ok, server} = ExArrow.Flight.Server.start_link(9999)

      # All interfaces
      {:ok, server} = ExArrow.Flight.Server.start_link(9999, host: "0.0.0.0")

      # OS-assigned port
      {:ok, server} = ExArrow.Flight.Server.start_link(0)
  """
  @spec start_link(non_neg_integer(), keyword()) :: {:ok, t()} | {:error, term()}
  def start_link(port, opts \\ []) do
    host = Keyword.get(opts, :host, "127.0.0.1")

    case Native.flight_server_start(host, port) do
      {:ok, server_ref} -> {:ok, %__MODULE__{resource: server_ref}}
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Returns the port the server is listening on (useful when `port: 0` was passed).
  """
  @spec port(t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def port(%__MODULE__{resource: ref}) do
    {:ok, Native.flight_server_port(ref)}
  end

  @doc """
  Returns the host address the server is bound to, e.g. `"127.0.0.1"` or `"0.0.0.0"`.
  """
  @spec host(t()) :: {:ok, String.t()} | {:error, term()}
  def host(%__MODULE__{resource: ref}) do
    {:ok, Native.flight_server_host(ref)}
  end

  @doc """
  Stops the Flight server, waiting for in-flight requests to drain.
  """
  @spec stop(t()) :: :ok | {:error, term()}
  def stop(%__MODULE__{resource: ref}) do
    Native.flight_server_stop(ref)
  end
end
