defmodule ExArrow.ADBC.ConnectionBehaviour do
  @moduledoc """
  Behaviour for ADBC Connection implementations. Used with Mox in tests.
  """
  @callback open(database :: ExArrow.ADBC.Database.t()) ::
              {:ok, ExArrow.ADBC.Connection.t()} | {:error, term()}
end
