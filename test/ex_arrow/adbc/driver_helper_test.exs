defmodule ExArrow.ADBC.DriverHelperTest do
  # Application.put_env/delete_env are global state — async: false prevents
  # on_exit callbacks from one test deleting env keys that another concurrent
  # test is still relying on (which caused the `:postgresql` dlopen failure).
  use ExUnit.Case, async: false

  alias ExArrow.ADBC.{Database, DriverHelper}

  setup context do
    Mox.set_mox_from_context(context)
    :ok
  end

  describe "ensure_driver_and_open/2" do
    @expected_sqlite_opts [driver_name: "adbc_driver_sqlite", uri: ":memory:"]

    test "when adbc_download_module has no download_driver/1, skips download and calls Database.open/1 with inferred opts" do
      opts = @expected_sqlite_opts
      Application.put_env(:ex_arrow, :adbc_download_module, ExArrow.ADBC.AdbcStubNoDownload)
      Application.put_env(:ex_arrow, :adbc_database_impl, ExArrow.ADBC.DatabaseMock)

      on_exit(fn ->
        Application.delete_env(:ex_arrow, :adbc_download_module)
        Application.delete_env(:ex_arrow, :adbc_database_impl)
      end)

      fake_db = %Database{resource: make_ref()}

      ExArrow.ADBC.DatabaseMock
      |> Mox.expect(:open, fn ^opts -> {:ok, fake_db} end)

      assert {:ok, ^fake_db} = DriverHelper.ensure_driver_and_open(:sqlite, ":memory:")
    end

    test "when adbc_download_module has download_driver/1 returning :ok, invokes it then calls Database.open/1" do
      opts = @expected_sqlite_opts
      Application.put_env(:ex_arrow, :adbc_download_module, ExArrow.ADBC.AdbcStubOk)
      Application.put_env(:ex_arrow, :adbc_database_impl, ExArrow.ADBC.DatabaseMock)

      on_exit(fn ->
        Application.delete_env(:ex_arrow, :adbc_download_module)
        Application.delete_env(:ex_arrow, :adbc_database_impl)
      end)

      fake_db = %Database{resource: make_ref()}

      ExArrow.ADBC.DatabaseMock
      |> Mox.expect(:open, fn ^opts -> {:ok, fake_db} end)

      assert {:ok, ^fake_db} = DriverHelper.ensure_driver_and_open(:sqlite, ":memory:")
    end

    test "when download_driver/1 returns {:error, reason}, returns that error without calling Database.open/1" do
      Application.put_env(:ex_arrow, :adbc_download_module, ExArrow.ADBC.AdbcStubError)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_download_module) end)

      assert {:error, "download failed"} =
               DriverHelper.ensure_driver_and_open(:sqlite, ":memory:")
    end

    test "infers driver_name from driver_key" do
      Application.put_env(:ex_arrow, :adbc_download_module, ExArrow.ADBC.AdbcStubNoDownload)
      Application.put_env(:ex_arrow, :adbc_database_impl, ExArrow.ADBC.DatabaseMock)

      on_exit(fn ->
        Application.delete_env(:ex_arrow, :adbc_download_module)
        Application.delete_env(:ex_arrow, :adbc_database_impl)
      end)

      fake_db = %Database{resource: make_ref()}

      ExArrow.ADBC.DatabaseMock
      |> Mox.expect(:open, fn [driver_name: name, uri: uri] ->
        assert name == "adbc_driver_postgresql"
        assert uri == "postgresql://localhost/mydb"
        {:ok, fake_db}
      end)

      assert {:ok, ^fake_db} =
               DriverHelper.ensure_driver_and_open(:postgresql, "postgresql://localhost/mydb")
    end

    test "propagates Database.open/1 error" do
      opts = @expected_sqlite_opts
      Application.put_env(:ex_arrow, :adbc_download_module, ExArrow.ADBC.AdbcStubNoDownload)
      Application.put_env(:ex_arrow, :adbc_database_impl, ExArrow.ADBC.DatabaseMock)

      on_exit(fn ->
        Application.delete_env(:ex_arrow, :adbc_download_module)
        Application.delete_env(:ex_arrow, :adbc_database_impl)
      end)

      ExArrow.ADBC.DatabaseMock
      |> Mox.expect(:open, fn ^opts -> {:error, "driver load failed"} end)

      assert {:error, "driver load failed"} =
               DriverHelper.ensure_driver_and_open(:sqlite, ":memory:")
    end
  end
end
