defmodule ExArrow.Flight.Client do
  @moduledoc """
  Arrow Flight client: connect to a Flight server and exchange Arrow data.

  Delegates to the configured implementation (see `:flight_client_impl` in
  application config). The default implementation uses NIFs backed by
  `arrow-flight` + tonic.

  ## Connection

      {:ok, client} = ExArrow.Flight.Client.connect("localhost", 9999, [])

  Options accepted by `connect/3`:
  - `:connect_timeout_ms` — TCP connection timeout in milliseconds (default: 0, no timeout).
  - `:tls` — transport security (see below).

  ## TLS

  Transport security is controlled by the `:tls` option and defaults to a
  secure setting for non-loopback hosts:

  | `:tls` value          | behaviour                                                   |
  |-----------------------|-------------------------------------------------------------|
  | not set, loopback host | plaintext HTTP/2 (auto; localhost / 127.x / ::1)           |
  | not set, remote host  | TLS with native OS certificate store (auto, secure default) |
  | `false`               | plaintext HTTP/2 regardless of host                         |
  | `true`                | TLS with native OS certificate store                        |
  | `[ca_cert_pem: pem]`  | TLS verified against the given PEM-encoded CA certificate   |

  Using `tls: false` for a non-loopback host is permitted but exposes traffic on
  untrusted networks — prefer the default or an explicit `tls: true`.

  ### Examples

      # Remote server — TLS enabled automatically
      {:ok, client} = ExArrow.Flight.Client.connect("flight.example.com", 9999, [])

      # Explicit TLS with a custom CA certificate
      pem = File.read!("/etc/ssl/my-ca.pem")
      {:ok, client} = ExArrow.Flight.Client.connect("internal.svc", 9999, tls: [ca_cert_pem: pem])

      # Explicit plaintext (loopback only, development)
      {:ok, client} = ExArrow.Flight.Client.connect("localhost", 9999, tls: false)

  ## Timeouts and cancellation

  Use `:connect_timeout_ms` to bound the initial connection. Per-call timeouts
  and retry policies are not yet exposed through the public API; see the Flight
  guide in `docs/flight_guide.md` for patterns to implement these at the call site.
  """

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  defp impl do
    Application.get_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientImpl)
  end

  @doc """
  Connects to a Flight server at `host`:`port`.

  Options:
  - `:connect_timeout_ms` — connection timeout in milliseconds (0 = no limit).
  - `:tls` — `true | false | [ca_cert_pem: pem]`; see module doc for details.
    Defaults to `:system_certs` for non-loopback hosts and plaintext for loopback.
  """
  @spec connect(String.t(), non_neg_integer(), keyword()) :: {:ok, t()} | {:error, term()}
  def connect(host, port, opts \\ []) do
    impl().connect(host, port, opts)
  end

  @doc """
  Retrieves data for `ticket` as a stream of record batches.
  """
  @spec do_get(t(), term()) :: {:ok, ExArrow.Stream.t()} | {:error, term()}
  def do_get(client, ticket) do
    impl().do_get(client, ticket)
  end

  @doc """
  Uploads `batches` to the server under the given `schema`.
  """
  @spec do_put(t(), ExArrow.Schema.t(), Enumerable.t()) :: :ok | {:error, term()}
  def do_put(client, schema, batches) do
    impl().do_put(client, schema, batches)
  end

  @doc """
  Lists available flights matching `criteria` (empty binary = all).

  Returns a list of `ExArrow.Flight.FlightInfo` structs.
  """
  @spec list_flights(t(), binary()) ::
          {:ok, [ExArrow.Flight.FlightInfo.t()]} | {:error, term()}
  def list_flights(client, criteria \\ <<>>) do
    impl().list_flights(client, criteria)
  end

  @doc """
  Returns metadata for the flight identified by `descriptor`.

  `descriptor` is `{:cmd, binary()}` or `{:path, [String.t()]}`.
  """
  @spec get_flight_info(t(), ExArrow.Flight.ClientBehaviour.descriptor()) ::
          {:ok, ExArrow.Flight.FlightInfo.t()} | {:error, term()}
  def get_flight_info(client, descriptor) do
    impl().get_flight_info(client, descriptor)
  end

  @doc """
  Returns the Arrow schema for the flight identified by `descriptor`.
  """
  @spec get_schema(t(), ExArrow.Flight.ClientBehaviour.descriptor()) ::
          {:ok, ExArrow.Schema.t()} | {:error, term()}
  def get_schema(client, descriptor) do
    impl().get_schema(client, descriptor)
  end

  @doc """
  Lists the action types supported by the server.

  Returns a list of `ExArrow.Flight.ActionType` structs.
  """
  @spec list_actions(t()) ::
          {:ok, [ExArrow.Flight.ActionType.t()]} | {:error, term()}
  def list_actions(client) do
    impl().list_actions(client)
  end

  @doc """
  Executes the named action on the server with an optional binary body.

  Returns `{:ok, results}` where `results` is a list of binary response bodies,
  or `{:error, reason}` on failure.

  ## Examples

      {:ok, ["pong"]} = ExArrow.Flight.Client.do_action(client, "ping", <<>>)
      {:ok, []}       = ExArrow.Flight.Client.do_action(client, "clear", <<>>)
  """
  @spec do_action(t(), String.t(), binary()) :: {:ok, [binary()]} | {:error, term()}
  def do_action(client, action_type, action_body \\ <<>>) do
    impl().do_action(client, action_type, action_body)
  end
end
