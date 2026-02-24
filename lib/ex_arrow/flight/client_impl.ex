defmodule ExArrow.Flight.ClientImpl do
  @moduledoc false
  @behaviour ExArrow.Flight.ClientBehaviour

  # TLS note: the native client always uses a plaintext http:// connection.
  # Passing `tls: true` is rejected explicitly below rather than silently ignored.
  # Full TLS support (certificate verification, mutual TLS) is deferred to a later
  # milestone; until then, only localhost / loopback endpoints are safe to use.

  alias ExArrow.{Flight.ActionType, Flight.Client, Flight.FlightInfo, Native, Schema, Stream}

  @impl true
  def connect(host, port, opts) do
    if Keyword.get(opts, :tls, false) do
      {:error, :tls_not_supported}
    else
      connect_timeout_ms = Keyword.get(opts, :connect_timeout_ms, 0)

      case Native.flight_client_connect(host, port, connect_timeout_ms) do
        {:ok, client_ref} -> {:ok, %Client{resource: client_ref}}
        {:error, msg} -> {:error, msg}
      end
    end
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
