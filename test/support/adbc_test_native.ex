# Test-only modules used to cover ADBC impl success/error branches without a real driver.
# Set Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeSuccess)
# or ExArrow.ADBC.TestNativeError in tests.

defmodule ExArrow.ADBC.TestNativeSuccess do
  @moduledoc false

  @spec adbc_database_open(term()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_database_open(_spec), do: {:ok, make_ref()}

  @spec adbc_connection_open(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_connection_open(_db_ref), do: {:ok, make_ref()}

  @spec adbc_connection_get_table_types(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_connection_get_table_types(_conn_ref), do: {:error, "test stub: not implemented"}

  @spec adbc_connection_get_table_schema(reference(), term(), term(), String.t()) ::
          {:ok, reference()} | {:error, String.t()}
  def adbc_connection_get_table_schema(_conn_ref, _catalog, _db_schema, _table_name),
    do: {:error, "test stub: not implemented"}

  @spec adbc_connection_get_objects(reference(), String.t(), term(), term(), term(), term()) ::
          {:ok, reference()} | {:error, String.t()}
  def adbc_connection_get_objects(
        _conn_ref,
        _depth,
        _catalog,
        _db_schema,
        _table_name,
        _column_name
      ),
      do: {:error, "test stub: not implemented"}

  @spec adbc_statement_new(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_statement_new(_conn_ref), do: {:ok, make_ref()}

  @spec adbc_statement_set_sql(reference(), String.t()) :: :ok | {:error, String.t()}
  def adbc_statement_set_sql(_stmt_ref, _sql), do: :ok

  @spec adbc_statement_bind(reference(), reference()) :: :ok | {:error, String.t()}
  def adbc_statement_bind(_stmt_ref, _batch_ref), do: :ok

  @spec adbc_statement_execute(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_statement_execute(_stmt_ref), do: {:ok, make_ref()}
end

defmodule ExArrow.ADBC.TestNativeError do
  @moduledoc false

  @spec adbc_database_open(term()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_database_open(_spec), do: {:error, "test error"}

  @spec adbc_connection_open(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_connection_open(_db_ref), do: {:error, "test error"}

  @spec adbc_connection_get_table_types(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_connection_get_table_types(_conn_ref), do: {:error, "test error"}

  @spec adbc_connection_get_table_schema(reference(), term(), term(), String.t()) ::
          {:ok, reference()} | {:error, String.t()}
  def adbc_connection_get_table_schema(_conn_ref, _catalog, _db_schema, _table_name),
    do: {:error, "test error"}

  @spec adbc_connection_get_objects(reference(), String.t(), term(), term(), term(), term()) ::
          {:ok, reference()} | {:error, String.t()}
  def adbc_connection_get_objects(
        _conn_ref,
        _depth,
        _catalog,
        _db_schema,
        _table_name,
        _column_name
      ),
      do: {:error, "test error"}

  @spec adbc_statement_new(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_statement_new(_conn_ref), do: {:error, "test error"}

  @spec adbc_statement_set_sql(reference(), String.t()) :: :ok | {:error, String.t()}
  def adbc_statement_set_sql(_stmt_ref, _sql), do: {:error, "test error"}

  @spec adbc_statement_bind(reference(), reference()) :: :ok | {:error, String.t()}
  def adbc_statement_bind(_stmt_ref, _batch_ref), do: {:error, "test error"}

  @spec adbc_statement_execute(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_statement_execute(_stmt_ref), do: {:error, "test error"}
end
