defmodule ExArrow.ADBC.Database do
  @moduledoc """
  ADBC Database handle: open a database via driver (shared library / env).

  Canonical API from adbc.h: Database -> Connection -> Statement -> Arrow stream.
  """
  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Opens a database using the given driver path or driver name (from env).
  Stub: returns error until NIF is implemented.
  """
  @spec open(String.t() | keyword()) :: {:ok, t()} | {:error, term()}
  def open(_driver_path_or_opts) do
    {:error, :not_implemented}
  end
end
