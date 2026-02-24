defmodule ExArrow.ADBC.Connection do
  @moduledoc """
  ADBC Connection: open from Database, then create Statements.
  """
  alias ExArrow.ADBC.Database

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Opens a connection from a database handle.
  Stub: returns error until NIF is implemented.
  """
  @spec open(Database.t()) :: {:ok, t()} | {:error, term()}
  def open(_database) do
    {:error, :not_implemented}
  end
end
