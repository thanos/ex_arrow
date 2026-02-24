defmodule ExArrow.ADBC.DatabaseImpl do
  @moduledoc false
  @behaviour ExArrow.ADBC.DatabaseBehaviour
  @impl true
  def open(_driver_path_or_opts), do: {:error, :not_implemented}
end
