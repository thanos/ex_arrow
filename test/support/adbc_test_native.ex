# Test-only modules used to cover ADBC impl success/error branches without a real driver.
# Set Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeSuccess)
# or ExArrow.ADBC.TestNativeError in tests.

defmodule ExArrow.ADBC.TestNativeSuccess do
  @moduledoc false

  @spec adbc_database_open(term()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_database_open(_spec), do: {:ok, make_ref()}

  @spec adbc_connection_open(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_connection_open(_db_ref), do: {:ok, make_ref()}

  @spec adbc_statement_new(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_statement_new(_conn_ref), do: {:ok, make_ref()}

  @spec adbc_statement_set_sql(reference(), String.t()) :: :ok | {:error, String.t()}
  def adbc_statement_set_sql(_stmt_ref, _sql), do: :ok

  @spec adbc_statement_execute(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_statement_execute(_stmt_ref), do: {:ok, make_ref()}
end

defmodule ExArrow.ADBC.TestNativeError do
  @moduledoc false

  @spec adbc_database_open(term()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_database_open(_spec), do: {:error, "test error"}

  @spec adbc_connection_open(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_connection_open(_db_ref), do: {:error, "test error"}

  @spec adbc_statement_new(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_statement_new(_conn_ref), do: {:error, "test error"}

  @spec adbc_statement_set_sql(reference(), String.t()) :: :ok | {:error, String.t()}
  def adbc_statement_set_sql(_stmt_ref, _sql), do: {:error, "test error"}

  @spec adbc_statement_execute(reference()) :: {:ok, reference()} | {:error, String.t()}
  def adbc_statement_execute(_stmt_ref), do: {:error, "test error"}
end
