defmodule ExArrow.ADBC.Connection do
  @moduledoc """
  ADBC Connection: open from Database, then create Statements.
  Delegates to the configured implementation (see `:adbc_connection_impl` in application config).

  Metadata APIs (get_table_types, get_table_schema, get_objects) are supported only when
  the driver implements them; otherwise they return `{:error, message}`.
  """
  alias ExArrow.ADBC.Database
  alias ExArrow.{Schema, Stream}

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  defp impl do
    Application.get_env(:ex_arrow, :adbc_connection_impl, ExArrow.ADBC.ConnectionImpl)
  end

  @doc """
  Opens a connection from a database handle.
  """
  @spec open(Database.t()) :: {:ok, t()} | {:error, term()}
  def open(database) do
    impl().open(database)
  end

  @doc """
  Returns a stream of table types (e.g. TABLE, VIEW) from the database.
  Not all drivers support this; returns `{:error, message}` if unsupported.
  """
  @spec get_table_types(t()) :: {:ok, Stream.t()} | {:error, term()}
  def get_table_types(conn) do
    impl().get_table_types(conn)
  end

  @doc """
  Returns the Arrow schema of the given table.
  `catalog` and `db_schema` may be `nil` if not applicable for the driver.
  Not all drivers support this.
  """
  @spec get_table_schema(t(), String.t() | nil, String.t() | nil, String.t()) ::
          {:ok, Schema.t()} | {:error, term()}
  def get_table_schema(conn, catalog, db_schema, table_name) do
    impl().get_table_schema(conn, catalog, db_schema, table_name)
  end

  @doc """
  Returns a hierarchical view of catalogs, schemas, tables, and/or columns.

  Options (all optional):
  - `:depth` — `"all"`, `"catalogs"`, `"schemas"`, `"tables"`, or `"columns"` (default `"all"`).
  - `:catalog`, `:db_schema`, `:table_name`, `:column_name` — filter by name (nil = no filter).

  Not all drivers support this; returns `{:error, message}` if unsupported.
  """
  @spec get_objects(t(), keyword()) :: {:ok, Stream.t()} | {:error, term()}
  def get_objects(conn, opts \\ []) do
    impl().get_objects(conn, opts)
  end
end
