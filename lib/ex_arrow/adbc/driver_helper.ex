defmodule ExArrow.ADBC.DriverHelper do
  @moduledoc """
  Convenience helpers for using ExArrow's ADBC APIs together with the
  [`adbc`](https://hex.pm/packages/adbc) package.

  This module is entirely optional:

  - If the `:adbc` package is available, it can be used to download drivers
    before opening a database with `ExArrow.ADBC.Database.open/1`.
  - If `:adbc` is not available, helpers fall back to calling
    `ExArrow.ADBC.Database.open/1` directly.
  """

  alias ExArrow.ADBC.Database

  @doc """
  Ensures the given ADBC driver is available (using `Adbc.download_driver!/1`
  when the `:adbc` package is present), then opens a database via
  `ExArrow.ADBC.Database.open/1`.

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
    maybe_download_driver(driver_key)

    opts = [
      driver_name: driver_name_from_key(driver_key),
      uri: uri
    ]

    Database.open(opts)
  end

  defp maybe_download_driver(driver_key) do
    if Code.ensure_loaded?(Adbc) and function_exported?(Adbc, :download_driver!, 1) do
      _ = Adbc.download_driver!(driver_key)
    end

    :ok
  end

  defp driver_name_from_key(driver_key) do
    "adbc_driver_#{driver_key}"
  end
end

