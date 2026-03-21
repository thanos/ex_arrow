defmodule ExArrow.NimblePoolBehaviour do
  @moduledoc false

  @doc """
  Starts a pool process.  Mirrors `NimblePool.start_link/1`.
  """
  @callback start_link(keyword()) :: {:ok, pid()} | {:error, term()}

  @doc """
  Checks out a worker from the pool, runs `fun.(from, worker_state)` and
  returns its first element.  Mirrors `NimblePool.checkout!/4`.
  """
  @callback checkout!(
              pool :: term(),
              command :: term(),
              fun :: (term(), term() -> {term(), term(), term()}),
              timeout :: non_neg_integer()
            ) :: term()
end
