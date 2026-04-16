defmodule ExArrow.FlightSQL.Error do
  @moduledoc """
  Structured error type for Flight SQL operations.

  All client and transport failures are returned as `{:error, %ExArrow.FlightSQL.Error{}}` from
  non-bang functions, or raised from bang functions (e.g. `query!/2`).

  ## Error codes

  | Code | Source |
  |------|--------|
  | `:transport_error` | TCP/TLS channel failure before any RPC completes |
  | `:server_error` | gRPC `INTERNAL` status from the server |
  | `:unimplemented` | gRPC `UNIMPLEMENTED` — server does not support the operation |
  | `:unauthenticated` | gRPC `UNAUTHENTICATED` — missing or rejected credentials |
  | `:permission_denied` | gRPC `PERMISSION_DENIED` |
  | `:not_found` | gRPC `NOT_FOUND` |
  | `:invalid_argument` | gRPC `INVALID_ARGUMENT` — bad SQL syntax, wrong parameter types |
  | `:protocol_error` | Malformed or unexpected Flight SQL response structure |
  | `:multi_endpoint` | `FlightInfo` returned more than one endpoint; not supported in v0.5.0 |
  | `:invalid_option` | Invalid connect or query option at the Elixir layer |
  | `:conversion_error` | Arrow → Explorer or Arrow → Nx conversion failure |

  ## Examples

      {:error, %ExArrow.FlightSQL.Error{code: :invalid_argument, message: "syntax error"}} =
        ExArrow.FlightSQL.Client.query(client, "SELECT FROM")

      try do
        ExArrow.FlightSQL.Client.query!(client, "BAD SQL")
      rescue
        e in ExArrow.FlightSQL.Error -> IO.inspect(e.code)
      end
  """

  defexception [:code, :message, :grpc_status, :details]

  @type code ::
          :transport_error
          | :server_error
          | :unimplemented
          | :unauthenticated
          | :permission_denied
          | :not_found
          | :invalid_argument
          | :protocol_error
          | :multi_endpoint
          | :invalid_option
          | :conversion_error

  @type t :: %__MODULE__{
          code: code(),
          message: String.t(),
          grpc_status: integer() | nil,
          details: term() | nil
        }

  @impl true
  def message(%__MODULE__{code: code, message: msg, details: nil}) do
    "[#{code}] #{msg}"
  end

  def message(%__MODULE__{code: code, message: msg, details: details}) do
    "[#{code}] #{msg} — #{inspect(details)}"
  end

  @doc false
  # Build an Error from the 3-tuple `{code_atom, grpc_status_integer, message}` that
  # the NIF encodes for every gRPC-level failure.
  @spec from_nif({atom(), non_neg_integer(), String.t()}) :: t()
  def from_nif({code, grpc_status, message}) do
    status = if grpc_status == 0, do: nil, else: grpc_status
    %__MODULE__{code: code, message: message, grpc_status: status}
  end

  @doc false
  # Build an Error from a plain string (transport or option validation errors).
  @spec from_string(atom(), String.t()) :: t()
  def from_string(code, message) when is_atom(code) and is_binary(message) do
    %__MODULE__{code: code, message: message}
  end
end
