defmodule ExArrow.Flight.Server do
  @moduledoc """
  Arrow Flight server: multi-dataset routing server with optional TLS.

  Each `do_put` stores data under a ticket derived from the Flight descriptor
  (cmd bytes or path segments joined with `/`). If no descriptor is provided
  the legacy ticket `"echo"` is used, preserving backward compatibility.

  ## TLS

  Transport security is controlled by the `:tls` option:

  | `:tls` value                          | behaviour                               |
  |---------------------------------------|-----------------------------------------|
  | not set (default)                     | plaintext HTTP/2                        |
  | `[cert_pem: pem, key_pem: pem]`       | one-way TLS (server presents cert)      |
  | `[cert_pem: pem, key_pem: pem, ca_cert_pem: pem]` | mutual TLS (mTLS)       |

  ## Examples

      # Plaintext (default)
      {:ok, server} = ExArrow.Flight.Server.start_link(9999)

      # One-way TLS
      cert = File.read!("server.crt")
      key  = File.read!("server.key")
      {:ok, server} = ExArrow.Flight.Server.start_link(9999, tls: [cert_pem: cert, key_pem: key])

      # Mutual TLS
      ca = File.read!("ca.crt")
      {:ok, server} = ExArrow.Flight.Server.start_link(9999,
        tls: [cert_pem: cert, key_pem: key, ca_cert_pem: ca])

      # OS-assigned port, all interfaces
      {:ok, server} = ExArrow.Flight.Server.start_link(0, host: "0.0.0.0")
  """
  alias ExArrow.Native

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Starts a Flight server on `port` (use `0` for any available port).

  ## Options

  * `:host`       — bind address (default `"127.0.0.1"`).
  * `:tls`        — TLS config keyword list; see module doc.
  """
  @spec start_link(non_neg_integer(), keyword()) :: {:ok, t()} | {:error, term()}
  def start_link(port, opts \\ []) do
    host = Keyword.get(opts, :host, "127.0.0.1")
    server_tls = build_server_tls(Keyword.get(opts, :tls))

    case server_tls do
      {:error, msg} ->
        {:error, msg}

      tls_term ->
        case Native.flight_server_start(host, port, tls_term) do
          {:ok, server_ref} -> {:ok, %__MODULE__{resource: server_ref}}
          {:error, msg} -> {:error, msg}
        end
    end
  end

  # Translate the `:tls` keyword option into the term expected by the NIF:
  #   nil                                          → :plaintext
  #   [cert_pem: c, key_pem: k]                   → {:tls, c, k}
  #   [cert_pem: c, key_pem: k, ca_cert_pem: ca]  → {:mtls, c, k, ca}
  defp build_server_tls(nil), do: :plaintext

  defp build_server_tls(tls_opts) when is_list(tls_opts) do
    cert = Keyword.get(tls_opts, :cert_pem)
    key = Keyword.get(tls_opts, :key_pem)
    ca = Keyword.get(tls_opts, :ca_cert_pem)

    cond do
      not is_binary(cert) -> {:error, ":tls requires :cert_pem binary"}
      not is_binary(key) -> {:error, ":tls requires :key_pem binary"}
      is_binary(ca) -> {:mtls, cert, key, ca}
      true -> {:tls, cert, key}
    end
  end

  defp build_server_tls(_), do: {:error, ":tls must be a keyword list"}

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
