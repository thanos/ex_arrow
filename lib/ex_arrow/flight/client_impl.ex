defmodule ExArrow.Flight.ClientImpl do
  @moduledoc false
  @behaviour ExArrow.Flight.ClientBehaviour

  alias ExArrow.{Flight.ActionType, Flight.Client, Flight.FlightInfo, Native, Schema, Stream}

  @impl true
  def connect(host, port, opts) do
    with {:ok, tls_mode} <- tls_mode_for(host, opts) do
      connect_timeout_ms = Keyword.get(opts, :connect_timeout_ms, 0)

      case Native.flight_client_connect(host, port, connect_timeout_ms, tls_mode) do
        {:ok, client_ref} -> {:ok, %Client{resource: client_ref}}
        {:error, msg} -> {:error, msg}
      end
    end
  end

  # Determine the TLS mode to pass to the NIF based on the `:tls` option and host.
  #
  # | `:tls` value            | result                           |
  # |-------------------------|----------------------------------|
  # | `false`                 | `:plaintext` (explicit opt-out)  |
  # | `true`                  | `:system_certs`                  |
  # | `[ca_cert_pem: pem]`    | `{:custom_ca, pem}`              |
  # | not set, loopback host  | `:plaintext` (auto)              |
  # | not set, remote host    | `:system_certs` (auto, default)  |
  defp tls_mode_for(host, opts) do
    case Keyword.get(opts, :tls) do
      false ->
        {:ok, :plaintext}

      true ->
        {:ok, :system_certs}

      tls_opts when is_list(tls_opts) ->
        case Keyword.fetch(tls_opts, :ca_cert_pem) do
          {:ok, pem} when is_binary(pem) ->
            {:ok, {:custom_ca, pem}}

          _ ->
            {:error, {:invalid_tls_opt, ":tls list must include a :ca_cert_pem binary"}}
        end

      nil ->
        {:ok, if(loopback?(host), do: :plaintext, else: :system_certs)}
    end
  end

  # Hosts considered safe for unencrypted connections by default.
  defp loopback?(host) do
    host in ~w[localhost 127.0.0.1 ::1 0:0:0:0:0:0:0:1 ip6-localhost]
  end

  @impl true
  def do_get(client, ticket) do
    ticket_binary = to_string(ticket)

    case Native.flight_client_do_get(client.resource, ticket_binary) do
      {:ok, stream_ref} -> {:ok, %Stream{resource: stream_ref}}
      {:error, msg} -> {:error, msg}
    end
  end

  @impl true
  def do_put(client, schema, batches) do
    batch_refs = Enum.map(batches, &ExArrow.RecordBatch.resource_ref/1)

    case Native.flight_client_do_put(
           client.resource,
           Schema.resource_ref(schema),
           batch_refs
         ) do
      :ok -> :ok
      {:error, msg} -> {:error, msg}
    end
  end

  @impl true
  def list_flights(client, criteria) do
    case Native.flight_client_list_flights(client.resource, criteria, 0) do
      {:ok, raw_list} ->
        {:ok, Enum.map(raw_list, &FlightInfo.from_native/1)}

      {:error, msg} ->
        {:error, msg}
    end
  end

  @impl true
  def get_flight_info(client, descriptor) do
    case Native.flight_client_get_flight_info(client.resource, descriptor, 0) do
      {:ok, raw} -> {:ok, FlightInfo.from_native(raw)}
      {:error, msg} -> {:error, msg}
    end
  end

  @impl true
  def get_schema(client, descriptor) do
    case Native.flight_client_get_schema(client.resource, descriptor, 0) do
      {:ok, schema_ref} -> {:ok, Schema.from_ref(schema_ref)}
      {:error, msg} -> {:error, msg}
    end
  end

  @impl true
  def list_actions(client) do
    case Native.flight_client_list_actions(client.resource, 0) do
      {:ok, raw_list} ->
        {:ok, Enum.map(raw_list, &ActionType.from_native/1)}

      {:error, msg} ->
        {:error, msg}
    end
  end

  @impl true
  def do_action(client, action_type, action_body) do
    case Native.flight_client_do_action(client.resource, action_type, action_body, 0) do
      {:ok, results} -> {:ok, results}
      {:error, msg} -> {:error, msg}
    end
  end
end
