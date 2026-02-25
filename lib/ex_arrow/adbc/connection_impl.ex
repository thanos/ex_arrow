defmodule ExArrow.ADBC.ConnectionImpl do
  @moduledoc false
  @behaviour ExArrow.ADBC.ConnectionBehaviour

  alias ExArrow.ADBC.{Connection, Database}

  @impl true
  def open(%Database{resource: db_ref}) do
    case native().adbc_connection_open(db_ref) do
      {:ok, ref} -> {:ok, %Connection{resource: ref}}
      {:error, msg} -> {:error, msg}
    end
  end

  defp native do
    Application.get_env(:ex_arrow, :adbc_native, ExArrow.Native)
  end
end
