defmodule ExArrow.ADBC.ConnectionBehaviour do
  @moduledoc """
  Behaviour for ADBC Connection implementations. Used with Mox in tests.
  """
  @callback open(database :: ExArrow.ADBC.Database.t()) ::
              {:ok, ExArrow.ADBC.Connection.t()} | {:error, term()}

  @callback get_table_types(connection :: ExArrow.ADBC.Connection.t()) ::
              {:ok, ExArrow.Stream.t()} | {:error, term()}

  @callback get_table_schema(connection :: ExArrow.ADBC.Connection.t(), catalog :: String.t() | nil,
              db_schema :: String.t() | nil, table_name :: String.t()) ::
              {:ok, ExArrow.Schema.t()} | {:error, term()}

  @callback get_objects(connection :: ExArrow.ADBC.Connection.t(), opts :: keyword()) ::
              {:ok, ExArrow.Stream.t()} | {:error, term()}
end
