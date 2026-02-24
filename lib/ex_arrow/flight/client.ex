defmodule ExArrow.Flight.Client do
  @moduledoc """
  Arrow Flight client: connect to a Flight server, do_get, do_put.

  Opaque handle: `ExArrow.Flight.ClientRef`. TLS and options in later milestones.
  Delegates to the configured implementation (see `:flight_client_impl` in application config).
  """
  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  defp impl do
    Application.get_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientImpl)
  end

  @doc """
  Connects to a Flight server at the given host and port.
  Stub: returns error until NIF is implemented.
  """
  @spec connect(String.t(), non_neg_integer(), keyword()) :: {:ok, t()} | {:error, term()}
  def connect(host, port, opts \\ []) do
    impl().connect(host, port, opts)
  end

  @doc """
  Performs do_get with the given ticket, returns a stream of record batches.
  Stub: returns error until NIF is implemented.
  """
  @spec do_get(t(), term()) :: {:ok, ExArrow.Stream.t()} | {:error, term()}
  def do_get(client, ticket) do
    impl().do_get(client, ticket)
  end

  @doc """
  Performs do_put: uploads the given stream of record batches.
  Stub: returns error until NIF is implemented.
  """
  @spec do_put(t(), ExArrow.Schema.t(), Enumerable.t()) :: :ok | {:error, term()}
  def do_put(client, schema, batches) do
    impl().do_put(client, schema, batches)
  end
end
