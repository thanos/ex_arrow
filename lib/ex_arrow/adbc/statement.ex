defmodule ExArrow.ADBC.Statement do
  @moduledoc """
  ADBC Statement: set_sql, execute, returns Arrow stream (record batches).
  Delegates to the configured implementation (see `:adbc_statement_impl` in application config).
  """
  alias ExArrow.ADBC.Connection
  alias ExArrow.Stream

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  defp impl do
    Application.get_env(:ex_arrow, :adbc_statement_impl, ExArrow.ADBC.StatementImpl)
  end

  @doc """
  Creates a new statement from a connection.
  """
  @spec new(Connection.t()) :: {:ok, t()} | {:error, term()}
  def new(conn) do
    impl().new(conn)
  end

  @doc """
  Sets the SQL for this statement.
  """
  @spec set_sql(t(), String.t()) :: :ok | {:error, term()}
  def set_sql(stmt, sql) do
    impl().set_sql(stmt, sql)
  end

  @doc """
  Binds a record batch to the statement (e.g. for prepared statements or bulk insert).
  Not all drivers support binding; returns `{:error, message}` if unsupported.
  """
  @spec bind(t(), ExArrow.RecordBatch.t()) :: :ok | {:error, term()}
  def bind(stmt, batch) do
    impl().bind(stmt, batch)
  end

  @doc """
  Executes the statement and returns a stream of record batches.
  Use `ExArrow.Stream.schema/1` and `ExArrow.Stream.next/1` to read the result.
  """
  @spec execute(t()) :: {:ok, Stream.t()} | {:error, term()}
  def execute(stmt) do
    impl().execute(stmt)
  end
end
