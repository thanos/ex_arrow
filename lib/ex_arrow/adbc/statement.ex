defmodule ExArrow.ADBC.Statement do
  @moduledoc """
  ADBC Statement: create with `new(conn, sql)` or `new(conn, sql, bind: batch)`, then `execute` to get an Arrow stream (record batches).
  Use `set_sql/2` and `bind/2` when reusing a statement. Delegates to the configured implementation (see `:adbc_statement_impl` in application config).
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

  Optionally pass SQL and/or an initial bind as a record batch:

      {:ok, stmt} = Statement.new(conn, "SELECT 1 AS n")
      {:ok, stmt} = Statement.new(conn, "INSERT INTO t SELECT * FROM ?", bind: record_batch)

  Use `bind/2` to rebind after creation.
  """
  @spec new(Connection.t()) :: {:ok, t()} | {:error, term()}
  def new(conn) do
    impl().new(conn)
  end

  @spec new(Connection.t(), String.t()) :: {:ok, t()} | {:error, term()}
  def new(conn, sql) when is_binary(sql) do
    with {:ok, stmt} <- impl().new(conn),
         :ok <- impl().set_sql(stmt, sql) do
      {:ok, stmt}
    end
  end

  @spec new(Connection.t(), String.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(conn, sql, opts) when is_binary(sql) and is_list(opts) do
    with {:ok, stmt} <- new(conn, sql),
         :ok <- maybe_bind(stmt, opts) do
      {:ok, stmt}
    end
  end

  defp maybe_bind(stmt, opts) do
    case Keyword.get(opts, :bind) do
      nil -> :ok
      batch -> impl().bind(stmt, batch)
    end
  end

  @doc """
  Sets the SQL for this statement (e.g. when creating with `new/1` for reuse).
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
