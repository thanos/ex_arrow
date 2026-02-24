defmodule ExArrow.Flight.Client do
  @moduledoc """
  Arrow Flight client: connect to a Flight server, do_get, do_put.

  Opaque handle: `ExArrow.Flight.ClientRef`. TLS and options in later milestones.
  """
  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Connects to a Flight server at the given host and port.
  Stub: returns error until NIF is implemented.
  """
  @spec connect(String.t(), non_neg_integer(), keyword()) :: {:ok, t()} | {:error, term()}
  def connect(_host, _port, _opts \\ []) do
    {:error, :not_implemented}
  end

  @doc """
  Performs do_get with the given ticket, returns a stream of record batches.
  Stub: returns error until NIF is implemented.
  """
  @spec do_get(t(), term()) :: {:ok, ExArrow.Stream.t()} | {:error, term()}
  def do_get(_client, _ticket) do
    {:error, :not_implemented}
  end

  @doc """
  Performs do_put: uploads the given stream of record batches.
  Stub: returns error until NIF is implemented.
  """
  @spec do_put(t(), ExArrow.Schema.t(), Enumerable.t()) :: :ok | {:error, term()}
  def do_put(_client, _schema, _batches) do
    {:error, :not_implemented}
  end
end
