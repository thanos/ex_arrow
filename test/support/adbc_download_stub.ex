# Stubs for testing ExArrow.ADBC.DriverHelper. Set
# Application.put_env(:ex_arrow, :adbc_download_module, ExArrow.ADBC.AdbcStubOk)
# (or AdbcStubError / AdbcStubNoDownload) in tests to exercise the download path
# without the real :adbc package.

defmodule ExArrow.ADBC.AdbcStubOk do
  @moduledoc false
  @spec download_driver(term()) :: :ok
  def download_driver(_driver_key), do: :ok
end

defmodule ExArrow.ADBC.AdbcStubError do
  @moduledoc false
  @spec download_driver(term()) :: {:error, String.t()}
  def download_driver(_driver_key), do: {:error, "download failed"}
end

defmodule ExArrow.ADBC.AdbcStubNoDownload do
  @moduledoc false
  # No download_driver/1 — simulates :adbc not present or not providing the function.
  # function_exported?(AdbcStubNoDownload, :download_driver, 1) is false.
end
