defmodule ExArrow.ADBC.StatementImpl do
  @moduledoc false
  @behaviour ExArrow.ADBC.StatementBehaviour
  @impl true
  def new(_conn), do: {:error, :not_implemented}
  @impl true
  def set_sql(_stmt, _sql), do: {:error, :not_implemented}
  @impl true
  def execute(_stmt), do: {:error, :not_implemented}
end
