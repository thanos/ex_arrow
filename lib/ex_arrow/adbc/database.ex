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
  Opens a database using the given driver path or options.

  - **String** — path to the driver shared library (e.g. `libadbc_driver_sqlite.so`).
  - **Keyword list** — `driver_path: path`, or `driver_name: name` with optional `uri: uri`.
    With `driver_name`, the driver manager looks up the library by name (e.g. env, system paths).
    If `uri` is provided, it is passed to the driver as the database URI (e.g. SQLite `uri: ":memory:"`).
    If `uri` is omitted, no URI option is set; drivers that require one may fail later at connection open.

  Returns `{:error, message}` if the driver cannot be loaded.
  """
  @spec open(String.t() | keyword()) :: {:ok, t()} | {:error, term()}
  def open(driver_path_or_opts) do
    impl().open(driver_path_or_opts)
  end
end
