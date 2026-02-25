defmodule ExArrow.ADBC.ConnectionImpl do
  @moduledoc false
  @behaviour ExArrow.ADBC.ConnectionBehaviour

  alias ExArrow.ADBC.{Connection, Database}
  alias ExArrow.{Schema, Stream}

  @impl true
  def open(%Database{resource: db_ref}) do
    case native().adbc_connection_open(db_ref) do
      {:ok, ref} -> {:ok, %Connection{resource: ref}}
      {:error, msg} -> {:error, msg}
    end
  end

  @impl true
  def get_table_types(%Connection{resource: conn_ref}) do
    case native().adbc_connection_get_table_types(conn_ref) do
      {:ok, stream_ref} -> {:ok, %Stream{resource: stream_ref, backend: :adbc}}
      {:error, msg} -> {:error, msg}
    end
  end

  @impl true
  def get_table_schema(%Connection{resource: conn_ref}, catalog, db_schema, table_name) do
    case native().adbc_connection_get_table_schema(conn_ref, catalog, db_schema, table_name) do
      {:ok, schema_ref} -> {:ok, Schema.from_ref(schema_ref)}
      {:error, msg} -> {:error, msg}
    end
  end

  @impl true
  def get_objects(%Connection{resource: conn_ref}, opts) do
    depth = Keyword.get(opts, :depth, "all")
    catalog = Keyword.get(opts, :catalog)
    db_schema = Keyword.get(opts, :db_schema)
    table_name = Keyword.get(opts, :table_name)
    column_name = Keyword.get(opts, :column_name)

    case native().adbc_connection_get_objects(
           conn_ref,
           depth,
           catalog,
           db_schema,
           table_name,
           column_name
         ) do
      {:ok, stream_ref} -> {:ok, %Stream{resource: stream_ref, backend: :adbc}}
      {:error, msg} -> {:error, msg}
    end
  end

  defp native do
    Application.get_env(:ex_arrow, :adbc_native, ExArrow.Native)
  end
end
