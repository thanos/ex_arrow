defmodule ExArrow.Flight.ClientBehaviour do
  @moduledoc """
  Behaviour for Arrow Flight client implementations.
  Allows swapping the real client for a mock in tests (e.g. with Mox).
  """

  @type descriptor :: {:cmd, binary()} | {:path, [String.t()]}

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

  @callback list_flights(client :: ExArrow.Flight.Client.t(), criteria :: binary()) ::
              {:ok, [ExArrow.Flight.FlightInfo.t()]} | {:error, term()}

  @callback get_flight_info(client :: ExArrow.Flight.Client.t(), descriptor :: descriptor()) ::
              {:ok, ExArrow.Flight.FlightInfo.t()} | {:error, term()}

  @callback get_schema(client :: ExArrow.Flight.Client.t(), descriptor :: descriptor()) ::
              {:ok, ExArrow.Schema.t()} | {:error, term()}

  @callback list_actions(client :: ExArrow.Flight.Client.t()) ::
              {:ok, [ExArrow.Flight.ActionType.t()]} | {:error, term()}

  @callback do_action(
              client :: ExArrow.Flight.Client.t(),
              action_type :: String.t(),
              action_body :: binary()
            ) ::
              {:ok, [binary()]} | {:error, term()}
end
