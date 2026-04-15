defmodule ExArrow.FlightSQL.Options do
  @moduledoc false

  # Internal option validation and normalization for Flight SQL connections.
  # Mirrors the TLS option model from ExArrow.Flight.ClientImpl.

  alias ExArrow.FlightSQL.Error

  @type tls_opt :: false | true | [ca_cert_pem: binary()]
  @type header :: {String.t(), String.t()}

  @type connect_opts :: %{
          host: String.t(),
          port: :inet.port_number(),
          tls_mode: :plaintext | :system_certs | {:custom_ca, binary()},
          headers: [header()]
        }

  # Hosts treated as loopback for the auto-TLS heuristic.
  # IPv6 addresses use the canonical short form that parse_uri/1 produces after
  # stripping brackets from "[::1]:port" style URIs.
  @loopback_hosts ~w[localhost 127.0.0.1 ::1 ip6-localhost]

  @doc """
  Parse a `"host:port"` URI string and keyword options into a normalized options map.

  Accepted URI formats:
  - `"hostname:port"` — e.g. `"localhost:32010"`
  - `"hostname"` — port defaults to 31337
  - `"[::1]:port"` — bracketed IPv6 address with explicit port

  Accepted keyword options:
  - `:tls` — `false | true | [ca_cert_pem: pem_binary]` (default: auto based on host).
  - `:headers` — list of `{name, value}` string tuples sent as gRPC metadata.

  Returns `{:ok, opts}` or `{:error, %Error{code: :invalid_option}}`.
  """
  @spec parse(String.t(), keyword()) :: {:ok, connect_opts()} | {:error, Error.t()}
  def parse(uri, opts) when is_binary(uri) and is_list(opts) do
    with {:ok, {host, port}} <- parse_uri(uri),
         {:ok, tls_mode} <- parse_tls(host, Keyword.get(opts, :tls)),
         {:ok, headers} <- parse_headers(Keyword.get(opts, :headers, [])) do
      {:ok, %{host: host, port: port, tls_mode: tls_mode, headers: headers}}
    end
  end

  # ── URI parsing ───────────────────────────────────────────────────────────────

  defp parse_uri(uri) do
    cond do
      # Bracketed IPv6: "[::1]:32010" — must have both brackets and port.
      String.starts_with?(uri, "[") ->
        case Regex.run(~r/^\[([^\]]+)\]:(\d+)$/, uri) do
          [_, host, port_str] ->
            parse_port(port_str, host, uri)

          _ ->
            invalid_option(
              "invalid URI #{inspect(uri)}: IPv6 addresses must use the form [host]:port"
            )
        end

      # Hostname or IPv4 with optional port: "host:port" or "host".
      true ->
        case String.split(uri, ":", parts: 2) do
          [host, port_str] ->
            parse_port(port_str, host, uri)

          [host] ->
            # No port supplied — use the Flight SQL default.
            {:ok, {host, 31_337}}
        end
    end
  end

  defp parse_port(port_str, host, uri) do
    case Integer.parse(port_str) do
      {port, ""} when port > 0 and port <= 65_535 ->
        {:ok, {host, port}}

      _ ->
        invalid_option("invalid port in URI #{inspect(uri)}: expected an integer 1-65535")
    end
  end

  # ── TLS parsing ───────────────────────────────────────────────────────────────

  defp parse_tls(host, nil) do
    mode = if host in @loopback_hosts, do: :plaintext, else: :system_certs
    {:ok, mode}
  end

  defp parse_tls(_host, false), do: {:ok, :plaintext}
  defp parse_tls(_host, true), do: {:ok, :system_certs}

  defp parse_tls(_host, tls_opts) when is_list(tls_opts) do
    case Keyword.fetch(tls_opts, :ca_cert_pem) do
      {:ok, pem} when is_binary(pem) ->
        {:ok, {:custom_ca, pem}}

      _ ->
        invalid_option(":tls list must include a :ca_cert_pem binary")
    end
  end

  defp parse_tls(_host, other) do
    invalid_option(
      "invalid :tls option #{inspect(other)}; expected false, true, or [ca_cert_pem: pem]"
    )
  end

  # ── Headers parsing ───────────────────────────────────────────────────────────

  defp parse_headers(headers) when is_list(headers) do
    result =
      Enum.reduce_while(headers, {:ok, []}, fn
        {k, v}, {:ok, acc} when is_binary(k) and is_binary(v) ->
          {:cont, {:ok, [{k, v} | acc]}}

        other, _ ->
          {:halt,
           invalid_option(
             "each :headers entry must be a {name_string, value_string} tuple, got: #{inspect(other)}"
           )}
      end)

    case result do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      error -> error
    end
  end

  defp parse_headers(other) do
    invalid_option(":headers must be a list of {name, value} tuples, got: #{inspect(other)}")
  end

  # ── Helpers ───────────────────────────────────────────────────────────────────

  defp invalid_option(msg) do
    {:error, Error.from_string(:invalid_option, msg)}
  end
end
