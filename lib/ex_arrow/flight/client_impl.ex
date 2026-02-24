defmodule ExArrow.Flight.ClientImpl do
  @moduledoc false
  @behaviour ExArrow.Flight.ClientBehaviour

  @impl true
  def connect(_host, _port, _opts), do: {:error, :not_implemented}

  @impl true
  def do_get(_client, _ticket), do: {:error, :not_implemented}

  @impl true
  def do_put(_client, _schema, _batches), do: {:error, :not_implemented}
end
