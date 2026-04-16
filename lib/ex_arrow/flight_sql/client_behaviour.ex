defmodule ExArrow.FlightSQL.ClientBehaviour do
  @moduledoc false
  # Internal behaviour for Flight SQL client implementations.
  #
  # Swap the real implementation for a test mock by setting the
  # `:flight_sql_client_impl` application environment key before calling
  # any `ExArrow.FlightSQL.Client` function:
  #
  #     Application.put_env(:ex_arrow, :flight_sql_client_impl, MyMock)
  #
  # Mox mock definition (test_helper.exs):
  #
  #     Mox.defmock(MyMock, for: ExArrow.FlightSQL.ClientBehaviour)

  alias ExArrow.{FlightSQL.Error, FlightSQL.Statement, Stream}

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

  @callback get_tables(client(), opts :: keyword()) ::
              {:ok, Stream.t()} | {:error, Error.t()}

  @callback get_db_schemas(client(), opts :: keyword()) ::
              {:ok, Stream.t()} | {:error, Error.t()}

  @callback get_sql_info(client(), opts :: keyword()) ::
              {:ok, Stream.t()} | {:error, Error.t()}

  @callback prepare(client(), sql(), opts :: keyword()) ::
              {:ok, Statement.t()} | {:error, Error.t()}
end
