defmodule ExArrow.ADBC.DriverHelper do
  @moduledoc """
  Convenience helpers for using ExArrow's ADBC APIs together with the
  [`adbc`](https://hex.pm/packages/adbc) package.

  This module is entirely optional:

  - If the `:adbc` package is available, it can be used to download drivers
    before opening a database with `ExArrow.ADBC.Database.open/1`.
  - If `:adbc` is not available, helpers fall back to calling
    `ExArrow.ADBC.Database.open/1` directly.

  **Important:** ExArrow's ADBC layer uses the C driver manager and expects a
  **loadable shared library** (e.g. `libadbc_driver_sqlite.so`). The `adbc`
  Hex package has its own process-based Database/Connection and native stack;
  its drivers are not necessarily installed in a form the C driver manager can
  load. So `ensure_driver_and_open/2` may return `{:error, _}` even after
  `Adbc.download_driver/1` succeeds. In that case, use a standalone ADBC C
  driver (see the project's livebook/INSTALL_ADBC_DRIVER.md or docs).

  For tests, you can inject the download module via application config
  `:ex_arrow`, `:adbc_download_module` (default: `Adbc`).
  """

  alias ExArrow.ADBC.Database

  @doc """
  Ensures the given ADBC driver is available (using `Adbc.download_driver/1`
  when the `:adbc` package is present), then opens a database via
  `ExArrow.ADBC.Database.open/1`. Returns `{:error, reason}` if the download
  fails or if `Database.open/1` fails.

  This is a convenience around the common pattern:

    * use `adbc` to manage/download drivers (e.g. `:sqlite`, `:postgresql`)
    * open the database with ExArrow so you get Arrow streams from `execute/1`

  ## Examples

      # Ensure the SQLite driver is available (if :adbc is in your deps)
      # and open an in-memory database via ExArrow:
      {:ok, db} =
        ExArrow.ADBC.DriverHelper.ensure_driver_and_open(:sqlite, ":memory:")

      {:ok, conn} = ExArrow.ADBC.Connection.open(db)
      {:ok, stmt} = ExArrow.ADBC.Statement.new(conn)
      :ok = ExArrow.ADBC.Statement.set_sql(stmt, "SELECT 1 AS n")
      {:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)

  If the `:adbc` package is not installed, this function still works; it simply
  skips the download step and calls `ExArrow.ADBC.Database.open/1` with the
  inferred options.
  """
  @spec ensure_driver_and_open(atom(), String.t()) :: {:ok, Database.t()} | {:error, term()}
  def ensure_driver_and_open(driver_key, uri) when is_atom(driver_key) and is_binary(uri) do
    # When config :ex_arrow, :adbc_package is set, use the supervised adbc connection (no native driver needed).
    if adbc_package_configured?() do
      Database.open(:adbc_package)
    else
      case maybe_download_driver(driver_key) do
        :ok ->
          opts = [
            driver_name: driver_name_from_key(driver_key),
            uri: uri
          ]

          Database.open(opts)

        {:error, _} = err ->
          err
      end
    end
  end

  defp adbc_package_configured? do
    case Application.get_env(:ex_arrow, :adbc_package) do
      opts when is_list(opts) and opts != [] -> true
      _ -> false
    end
  end

  defp maybe_download_driver(driver_key) do
    module = Application.get_env(:ex_arrow, :adbc_download_module, Adbc)

    if Code.ensure_loaded?(module) and function_exported?(module, :download_driver, 1) do
      case module.download_driver(driver_key) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
      end
    else
      :ok
    end
  end

  defp driver_name_from_key(driver_key) do
    "adbc_driver_#{driver_key}"
  end
end
