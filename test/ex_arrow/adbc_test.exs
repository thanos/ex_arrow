defmodule ExArrow.ADBCTest do
  use ExUnit.Case, async: true

  setup context do
    Mox.set_mox_from_context(context)
    :ok
  end

  describe "Database (stub)" do
    test "open/1 returns not_implemented" do
      assert ExArrow.ADBC.Database.open("/path/to/driver") == {:error, :not_implemented}
    end

    test "open/1 with keyword opts returns not_implemented" do
      assert ExArrow.ADBC.Database.open(uri: "postgresql://localhost") ==
               {:error, :not_implemented}
    end
  end

  describe "Connection (stub)" do
    test "open/1 returns not_implemented" do
      db = %ExArrow.ADBC.Database{resource: make_ref()}
      assert ExArrow.ADBC.Connection.open(db) == {:error, :not_implemented}
    end
  end

  describe "Statement (stub)" do
    test "new/1 returns not_implemented" do
      conn = %ExArrow.ADBC.Connection{resource: make_ref()}
      assert ExArrow.ADBC.Statement.new(conn) == {:error, :not_implemented}
    end

    test "set_sql/2 returns not_implemented" do
      stmt = %ExArrow.ADBC.Statement{resource: make_ref()}
      assert ExArrow.ADBC.Statement.set_sql(stmt, "SELECT 1") == {:error, :not_implemented}
    end

    test "execute/1 returns not_implemented" do
      stmt = %ExArrow.ADBC.Statement{resource: make_ref()}
      assert ExArrow.ADBC.Statement.execute(stmt) == {:error, :not_implemented}
    end
  end

  describe "Database with Mox mock" do
    test "open/1 uses mock when configured and returns success" do
      Application.put_env(:ex_arrow, :adbc_database_impl, ExArrow.ADBC.DatabaseMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_database_impl) end)

      fake_db = %ExArrow.ADBC.Database{resource: make_ref()}

      ExArrow.ADBC.DatabaseMock
      |> Mox.expect(:open, fn "driver.so" ->
        {:ok, fake_db}
      end)

      assert {:ok, ^fake_db} = ExArrow.ADBC.Database.open("driver.so")
    end
  end
end
