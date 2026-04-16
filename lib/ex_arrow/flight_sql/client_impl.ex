defmodule ExArrow.FlightSQL.ClientImpl do
  @moduledoc false

  @behaviour ExArrow.FlightSQL.ClientBehaviour

  alias ExArrow.FlightSQL.{Client, Error, Options}
  alias ExArrow.Stream

  require Logger

  @impl true
  def connect(uri, opts) do
    with {:ok, %{host: host, port: port, tls_mode: tls_mode, headers: headers}} <-
           Options.parse(uri, opts) do
      case native().flight_sql_connect(host, port, tls_mode, headers) do
        {:ok, ref} -> {:ok, %Client{resource: ref}}
        {:error, msg} when is_binary(msg) -> {:error, Error.from_string(:transport_error, msg)}
        {:error, nif_err} -> {:error, wrap_nif_error(nif_err)}
      end
    end
  end

  @impl true
  def query(%Client{resource: ref}, sql, _opts) do
    case native().flight_sql_query(ref, sql) do
      {:ok, stream_ref} -> {:ok, %Stream{resource: stream_ref, backend: :flight_sql}}
      {:error, nif_err} -> {:error, wrap_nif_error(nif_err)}
    end
  end

  @impl true
  def execute(%Client{resource: ref}, sql, _opts) do
    case native().flight_sql_execute(ref, sql) do
      {:ok, :unknown} -> {:ok, :unknown}
      {:ok, n} when is_integer(n) -> {:ok, n}
      {:error, nif_err} -> {:error, wrap_nif_error(nif_err)}
    end
  end

  @impl true
  def close(_client) do
    # The underlying channel is released when the resource is garbage-collected.
    # Explicit close is a no-op in v0.5.0; the resource handle is simply dropped.
    :ok
  end

  @impl true
  def get_tables(%Client{resource: ref}, opts) do
    catalog = Keyword.get(opts, :catalog)
    db_schema_filter = Keyword.get(opts, :db_schema_filter)
    table_name_filter = Keyword.get(opts, :table_name_filter)
    table_types = Keyword.get(opts, :table_types, [])
    include_schema = Keyword.get(opts, :include_schema, false)

    case native().flight_sql_get_tables(
           ref,
           catalog,
           db_schema_filter,
           table_name_filter,
           table_types,
           include_schema
         ) do
      {:ok, stream_ref} -> {:ok, %Stream{resource: stream_ref, backend: :flight_sql}}
      {:error, nif_err} -> {:error, wrap_nif_error(nif_err)}
    end
  end

  @impl true
  def get_db_schemas(%Client{resource: ref}, opts) do
    catalog = Keyword.get(opts, :catalog)
    db_schema_filter = Keyword.get(opts, :db_schema_filter)

    case native().flight_sql_get_db_schemas(ref, catalog, db_schema_filter) do
      {:ok, stream_ref} -> {:ok, %Stream{resource: stream_ref, backend: :flight_sql}}
      {:error, nif_err} -> {:error, wrap_nif_error(nif_err)}
    end
  end

  @impl true
  def get_sql_info(%Client{resource: ref}, _opts) do
    case native().flight_sql_get_sql_info(ref) do
      {:ok, stream_ref} -> {:ok, %Stream{resource: stream_ref, backend: :flight_sql}}
      {:error, nif_err} -> {:error, wrap_nif_error(nif_err)}
    end
  end

  defp native, do: Application.get_env(:ex_arrow, :flight_sql_client_native, ExArrow.Native)

  # ── Error helpers ─────────────────────────────────────────────────────────────

  # NIF errors are one of:
  #   {code_atom, grpc_status_integer, message_binary}  — gRPC-level errors
  #   message_binary                                     — transport/option errors

  defp wrap_nif_error({code, grpc_status, msg})
       when is_atom(code) and is_integer(grpc_status) and is_binary(msg) do
    Error.from_nif({code, grpc_status, msg})
  end

  defp wrap_nif_error(msg) when is_binary(msg) do
    Error.from_string(:transport_error, msg)
  end

  defp wrap_nif_error(other) do
    Logger.warning(
      "[ExArrow.FlightSQL] unexpected NIF error shape #{inspect(other)}; " <>
        "expected {code, grpc_status, msg} or binary"
    )

    Error.from_string(:transport_error, inspect(other))
  end
end
