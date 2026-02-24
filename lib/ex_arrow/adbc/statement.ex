defmodule ExArrow.ADBC.Statement do
  @moduledoc """
  ADBC Statement: set_sql, execute, returns Arrow stream (record batches).
  """
  alias ExArrow.ADBC.Connection
  alias ExArrow.Stream

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Creates a new statement from a connection.
  Stub: returns error until NIF is implemented.
  """
  @spec new(Connection.t()) :: {:ok, t()} | {:error, term()}
  def new(_conn) do
    {:error, :not_implemented}
  end

  @doc """
  Sets the SQL for this statement.
  Stub: returns error until NIF is implemented.
  """
  @spec set_sql(t(), String.t()) :: :ok | {:error, term()}
  def set_sql(_stmt, _sql) do
    {:error, :not_implemented}
  end

  @doc """
  Executes the statement and returns a stream of record batches.
  Stub: returns error until NIF is implemented.
  """
  @spec execute(t()) :: {:ok, Stream.t()} | {:error, term()}
  def execute(_stmt) do
    {:error, :not_implemented}
  end
end
