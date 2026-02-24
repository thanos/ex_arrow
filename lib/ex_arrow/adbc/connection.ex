defmodule ExArrow.ADBC.Connection do
  @moduledoc """
  ADBC Connection: open from Database, then create Statements.
  Delegates to the configured implementation (see `:adbc_connection_impl` in application config).
  """
  alias ExArrow.ADBC.Database

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  defp impl do
    Application.get_env(:ex_arrow, :adbc_connection_impl, ExArrow.ADBC.ConnectionImpl)
  end

  @doc """
  Opens a connection from a database handle.
  Stub: returns error until NIF is implemented.
  """
  @spec open(Database.t()) :: {:ok, t()} | {:error, term()}
  def open(database) do
    impl().open(database)
  end
end
