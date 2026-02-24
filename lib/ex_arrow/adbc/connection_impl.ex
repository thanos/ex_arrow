defmodule ExArrow.ADBC.ConnectionImpl do
  @moduledoc false
  @behaviour ExArrow.ADBC.ConnectionBehaviour
  @impl true
  def open(_database), do: {:error, :not_implemented}
end
