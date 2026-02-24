defmodule ExArrow.Flight.ClientImpl do
  @moduledoc false
  @behaviour ExArrow.Flight.ClientBehaviour

  # TLS note: the native client always uses a plaintext http:// connection.
  # Passing `tls: true` is rejected explicitly below rather than silently ignored.
  # Full TLS support (certificate verification, mutual TLS) is deferred to a later
  # milestone; until then, only localhost / loopback endpoints are safe to use.

  alias ExArrow.Native
  alias ExArrow.Stream, as: ExArrowStream

  @impl true
  def connect(host, port, opts) do
    if Keyword.get(opts, :tls, false) do
      {:error, :tls_not_supported}
    else
      case Native.flight_client_connect(host, port) do
        {:ok, client_ref} -> {:ok, %ExArrow.Flight.Client{resource: client_ref}}
        {:error, msg} -> {:error, msg}
      end
    end
  end

  @impl true
  def do_get(client, ticket) do
    ticket_binary = to_string(ticket)

    case Native.flight_client_do_get(client.resource, ticket_binary) do
      {:ok, stream_ref} -> {:ok, %ExArrowStream{resource: stream_ref}}
      {:error, msg} -> {:error, msg}
    end
  end

  @impl true
  def do_put(client, schema, batches) do
    batch_refs = batches |> Enum.to_list() |> Enum.map(& &1.resource)

    case Native.flight_client_do_put(client.resource, schema.resource, batch_refs) do
      :ok -> :ok
      {:error, msg} -> {:error, msg}
    end
  end
end
