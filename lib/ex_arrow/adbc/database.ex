defmodule ExArrow.ADBC.Database do
  @moduledoc """
  ADBC Database handle: open a database via driver (shared library / env).

  Canonical API from adbc.h: Database -> Connection -> Statement -> Arrow stream.
  Delegates to the configured implementation (see `:adbc_database_impl` in application config).
  """
  alias ExArrow.ADBC.DatabaseAdbcPackageImpl

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  defp impl do
    Application.get_env(:ex_arrow, :adbc_database_impl, ExArrow.ADBC.DatabaseImpl)
  end

  @doc """
  Opens a database using the given driver path or options.

  - **`:adbc_package`** — use the supervised adbc-package connection when `config :ex_arrow, :adbc_package` is set (e.g. `[driver: :sqlite, uri: ":memory:"]`). The adbc Database and Connection are started under ExArrow’s supervisor; no native driver is loaded.
  - **String** — path to the driver shared library (e.g. `libadbc_driver_sqlite.so`).
  - **Keyword list** — `driver_path: path`, or `driver_name: name` with optional `uri: uri`.
    With `driver_name`, the driver manager looks up the library by name (e.g. env, system paths).
    If `uri` is provided, it is passed to the driver as the database URI (e.g. SQLite `uri: ":memory:"`).
    If `uri` is omitted, no URI option is set; drivers that require one may fail later at connection open.

  Returns `{:error, message}` if the driver cannot be loaded or the adbc_package backend is not configured.
  """
  @spec open(String.t() | keyword() | :adbc_package) :: {:ok, t()} | {:error, term()}
  def open(:adbc_package) do
    DatabaseAdbcPackageImpl.open(:adbc_package)
  end

  def open(driver_path_or_opts) do
    impl().open(driver_path_or_opts)
  end
end
