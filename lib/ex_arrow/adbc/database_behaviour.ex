defmodule ExArrow.ADBC.DatabaseBehaviour do
  @moduledoc """
  Behaviour for ADBC Database implementations. Used with Mox in tests.
  """
  @callback open(driver_path_or_opts :: String.t() | keyword()) ::
              {:ok, ExArrow.ADBC.Database.t()} | {:error, term()}
end
