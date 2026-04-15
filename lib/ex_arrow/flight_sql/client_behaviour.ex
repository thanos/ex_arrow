defmodule ExArrow.FlightSQL.ClientBehaviour do
  @moduledoc """
  Behaviour for Flight SQL client implementations.

  Swap the real implementation for a mock in tests by setting the
  `:flight_sql_client_impl` application environment key:

      Application.put_env(:ex_arrow, :flight_sql_client_impl, MyMock)
  """

  alias ExArrow.{FlightSQL.Error, Stream}

  @type client :: ExArrow.FlightSQL.Client.t()
  @type sql :: String.t()
  @type affected_rows :: non_neg_integer() | :unknown

  @callback connect(uri :: String.t(), opts :: keyword()) ::
              {:ok, client()} | {:error, Error.t()}

  @callback query(client(), sql(), opts :: keyword()) ::
              {:ok, Stream.t()} | {:error, Error.t()}

  @callback execute(client(), sql(), opts :: keyword()) ::
              {:ok, affected_rows()} | {:error, Error.t()}

  @callback close(client()) :: :ok
end
