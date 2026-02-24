defmodule ExArrow.ADBC.StatementBehaviour do
  @moduledoc """
  Behaviour for ADBC Statement implementations. Used with Mox in tests.
  """
  @callback new(connection :: ExArrow.ADBC.Connection.t()) ::
              {:ok, ExArrow.ADBC.Statement.t()} | {:error, term()}
  @callback set_sql(statement :: ExArrow.ADBC.Statement.t(), sql :: String.t()) ::
              :ok | {:error, term()}
  @callback execute(statement :: ExArrow.ADBC.Statement.t()) ::
              {:ok, ExArrow.Stream.t()} | {:error, term()}
end
