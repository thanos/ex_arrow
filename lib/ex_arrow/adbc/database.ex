defmodule ExArrow.ADBC.Database do
  @moduledoc """
  ADBC Database handle: open a database via driver (shared library / env).

  Canonical API from adbc.h: Database -> Connection -> Statement -> Arrow stream.
  Delegates to the configured implementation (see `:adbc_database_impl` in application config).
  """
  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  defp impl do
    Application.get_env(:ex_arrow, :adbc_database_impl, ExArrow.ADBC.DatabaseImpl)
  end

  @doc """
  Opens a database using the given driver path or driver name (from env).
  Stub: returns error until NIF is implemented.
  """
  @spec open(String.t() | keyword()) :: {:ok, t()} | {:error, term()}
  def open(driver_path_or_opts) do
    impl().open(driver_path_or_opts)
  end
end
