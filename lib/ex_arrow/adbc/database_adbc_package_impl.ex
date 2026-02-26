defmodule ExArrow.ADBC.DatabaseAdbcPackageImpl do
  @moduledoc false
  @behaviour ExArrow.ADBC.DatabaseBehaviour

  alias ExArrow.ADBC.{AdbcPackageManager, Database}

  @impl true
  def open(:adbc_package) do
    case AdbcPackageManager.get_pids() do
      {:ok, {_db_pid, _conn_pid}} ->
        {:ok, %Database{resource: :adbc_package}}

      {:error, :not_configured} ->
        {:error,
         "adbc_package backend not configured. Set config :ex_arrow, :adbc_package to a keyword list (e.g. [driver: :sqlite, uri: \":memory:\"]) and ensure the :adbc dependency is available."}

      {:error, reason} ->
        {:error, "adbc_package backend failed to start: #{inspect(reason)}"}
    end
  end

  def open(_) do
    {:error, "DatabaseAdbcPackageImpl only supports open(:adbc_package)"}
  end
end
