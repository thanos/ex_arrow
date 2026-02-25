defmodule ExArrow.ADBC.ConnectionImpl do
  @moduledoc false
  @behaviour ExArrow.ADBC.ConnectionBehaviour

  alias ExArrow.ADBC.{Connection, Database}
  alias ExArrow.Native

  @impl true
  def open(%Database{resource: db_ref}) do
    case Native.adbc_connection_open(db_ref) do
      {:ok, ref} -> {:ok, %Connection{resource: ref}}
      {:error, msg} -> {:error, msg}
    end
  end
end
