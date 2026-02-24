defmodule ExArrow.Flight.ClientBehaviour do
  @moduledoc """
  Behaviour for Arrow Flight client implementations.
  Allows swapping the real client for a mock in tests (e.g. with Mox).
  """
  @callback connect(host :: String.t(), port :: non_neg_integer(), opts :: keyword()) ::
              {:ok, ExArrow.Flight.Client.t()} | {:error, term()}
  @callback do_get(client :: ExArrow.Flight.Client.t(), ticket :: term()) ::
              {:ok, ExArrow.Stream.t()} | {:error, term()}
  @callback do_put(
              client :: ExArrow.Flight.Client.t(),
              schema :: ExArrow.Schema.t(),
              batches :: Enumerable.t()
            ) ::
              :ok | {:error, term()}
end
