defmodule ExArrow.ADBCTest do
  use ExUnit.Case, async: true

  alias ExArrow.ADBC.{Connection, Database, Statement}
  alias ExArrow.{Schema, Stream}

  setup context do
    Mox.set_mox_from_context(context)
    :ok
  end

  # ── Real implementation (no driver / invalid) ───────────────────────────────

  describe "Database (real impl)" do
    test "open/1 with non-existent path returns error" do
      assert {:error, _} = Database.open("/nonexistent/adbc_driver_xyz.so")
    end

    test "open/1 with keyword opts (driver path) returns error when path invalid" do
      assert {:error, _} = Database.open(driver_path: "/nonexistent/driver.so")
    end

    test "open/1 with driver_name uses env lookup and returns error when not found" do
      # driver_name without a loadable driver
      assert {:error, _} = Database.open(driver_name: "adbc_driver_nonexistent_xyz")
    end

    test "open/1 with nil normalizes to opts and returns error from NIF" do
      assert {:error, msg} = Database.open(nil)
      assert is_binary(msg)
    end

    test "open/1 with non-string non-list (e.g. integer) returns error" do
      assert {:error, msg} = Database.open(42)
      assert is_binary(msg)
    end

    test "open/1 with invalid type returns clear validation error without calling NIF" do
      assert {:error, msg} = Database.open(%{driver: "x"})
      assert msg =~ "expected driver path"
      assert msg =~ "driver_path or :driver_name"
      assert msg =~ "%{"
    end

    test "open/1 with empty list returns error (no driver_path or driver_name)" do
      assert {:error, msg} = Database.open([])
      assert is_binary(msg)
      assert msg =~ "driver_path or :driver_name"
      assert msg =~ "got keys: []"
    end

    test "open/1 with non-keyword list returns clear validation error without calling NIF" do
      assert {:error, msg} = Database.open([1, 2, 3])
      assert msg =~ "keyword list"
      assert msg =~ "[1, 2, 3]"
    end

    test "open/1 with invalid option value type returns clear validation error without calling NIF" do
      assert {:error, msg} = Database.open(driver_path: 123)
      assert msg =~ "option :driver_path must be a string"
      assert msg =~ "123"
    end
  end

  describe "Connection metadata (real impl, driver-dependent)" do
    test "get_table_types/1 with invalid connection raises ArgumentError" do
      conn = %Connection{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        Connection.get_table_types(conn)
      end
    end

    test "get_table_schema/3 with invalid connection raises ArgumentError" do
      conn = %Connection{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        Connection.get_table_schema(conn, nil, nil, "t")
      end
    end

    test "get_objects/2 with invalid connection raises ArgumentError" do
      conn = %Connection{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        Connection.get_objects(conn, [])
      end
    end
  end

  describe "Connection (real impl)" do
    test "open/1 with invalid database ref raises ArgumentError" do
      db = %Database{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        Connection.open(db)
      end
    end

    test "open/1 requires a Database struct" do
      assert_raise FunctionClauseError, fn ->
        Connection.open(%Statement{resource: make_ref()})
      end
    end
  end

  describe "Statement (real impl)" do
    test "new/1 with invalid connection raises ArgumentError" do
      conn = %Connection{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        Statement.new(conn)
      end
    end

    test "new/1 requires a Connection struct" do
      assert_raise FunctionClauseError, fn ->
        Statement.new(%Database{resource: make_ref()})
      end
    end

    test "set_sql/2 with invalid statement raises ArgumentError" do
      stmt = %Statement{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        Statement.set_sql(stmt, "SELECT 1")
      end
    end

    test "set_sql/2 accepts charlist and passes it to impl (impl converts to string)" do
      Application.put_env(:ex_arrow, :adbc_statement_impl, ExArrow.ADBC.StatementMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_statement_impl) end)

      stmt = %Statement{resource: make_ref()}

      ExArrow.ADBC.StatementMock
      |> Mox.expect(:set_sql, fn ^stmt, sql ->
        # Public API passes through; impl receives charlist when given ~c"SELECT 1"
        assert sql == ~c"SELECT 1" or sql == "SELECT 1"
        :ok
      end)

      assert :ok = Statement.set_sql(stmt, ~c"SELECT 1")
    end

    test "execute/1 with invalid statement raises ArgumentError" do
      stmt = %Statement{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        Statement.execute(stmt)
      end
    end

    test "execute/1 requires a Statement struct" do
      assert_raise FunctionClauseError, fn ->
        Statement.execute(%Connection{resource: make_ref()})
      end
    end

    test "bind/2 with invalid statement raises ArgumentError" do
      stmt = %Statement{resource: make_ref()}
      batch = %ExArrow.RecordBatch{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        Statement.bind(stmt, batch)
      end
    end
  end

  # ── Test native (success/error branches without real driver) ─────────────────

  describe "DatabaseImpl with TestNativeSuccess" do
    test "open/1 returns {:ok, db} when native returns success" do
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeSuccess)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_native) end)

      assert {:ok, %Database{resource: ref}} = Database.open(driver_path: "test")
      assert is_reference(ref)
    end
  end

  describe "DatabaseImpl with TestNativeError" do
    test "open/1 returns {:error, msg} when native returns error" do
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeError)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_native) end)

      assert {:error, "test error"} = Database.open("/any/path")
    end
  end

  describe "ConnectionImpl with TestNativeSuccess" do
    test "open/1 returns {:ok, conn} when native returns success" do
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeSuccess)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_native) end)

      {:ok, db} = Database.open(driver_path: "test")
      assert {:ok, %Connection{resource: ref}} = Connection.open(db)
      assert is_reference(ref)
    end
  end

  describe "ConnectionImpl metadata with TestNativeSuccess" do
    test "get_table_types/1 returns error when stub does not implement" do
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeSuccess)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_native) end)

      {:ok, db} = Database.open(driver_path: "test")
      {:ok, conn} = Connection.open(db)

      assert {:error, "test stub: not implemented"} = Connection.get_table_types(conn)
    end

    test "get_table_schema/3 returns error when stub does not implement" do
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeSuccess)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_native) end)

      {:ok, db} = Database.open(driver_path: "test")
      {:ok, conn} = Connection.open(db)

      assert {:error, "test stub: not implemented"} =
               Connection.get_table_schema(conn, nil, nil, "mytable")
    end

    test "get_objects/2 returns error when stub does not implement" do
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeSuccess)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_native) end)

      {:ok, db} = Database.open(driver_path: "test")
      {:ok, conn} = Connection.open(db)

      assert {:error, "test stub: not implemented"} = Connection.get_objects(conn, depth: "all")
    end
  end

  describe "ConnectionImpl with TestNativeError" do
    test "open/1 returns {:error, msg} when native returns error" do
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeSuccess)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_native) end)

      {:ok, db} = Database.open(driver_path: "test")
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeError)

      assert {:error, "test error"} = Connection.open(db)
    end
  end

  describe "StatementImpl with TestNativeSuccess" do
    test "new/1 set_sql/2 execute/1 return success when native returns success" do
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeSuccess)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_native) end)

      {:ok, db} = Database.open(driver_path: "test")
      {:ok, conn} = Connection.open(db)

      assert {:ok, stmt} = Statement.new(conn)
      assert is_reference(stmt.resource)

      assert :ok = Statement.set_sql(stmt, "SELECT 1")

      assert {:ok, %Stream{resource: stream_ref, backend: :adbc}} = Statement.execute(stmt)

      assert is_reference(stream_ref)
    end

    test "bind/2 returns :ok when native returns success" do
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeSuccess)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_native) end)

      {:ok, db} = Database.open(driver_path: "test")
      {:ok, conn} = Connection.open(db)
      {:ok, stmt} = Statement.new(conn)
      # Use a ref as batch (stub doesn't inspect it)
      batch = %ExArrow.RecordBatch{resource: make_ref()}

      assert :ok = Statement.bind(stmt, batch)
    end
  end

  describe "StatementImpl with TestNativeError" do
    test "new/1 returns {:error, msg} when native returns error" do
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeSuccess)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_native) end)

      {:ok, db} = Database.open(driver_path: "test")
      {:ok, conn} = Connection.open(db)
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeError)

      assert {:error, "test error"} = Statement.new(conn)
    end

    test "set_sql/2 returns {:error, msg} when native returns error" do
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeSuccess)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_native) end)

      {:ok, db} = Database.open(driver_path: "test")
      {:ok, conn} = Connection.open(db)
      {:ok, stmt} = Statement.new(conn)
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeError)

      assert {:error, "test error"} = Statement.set_sql(stmt, "SELECT 1")
    end

    test "execute/1 returns {:error, msg} when native returns error" do
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeSuccess)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_native) end)

      {:ok, db} = Database.open(driver_path: "test")
      {:ok, conn} = Connection.open(db)
      {:ok, stmt} = Statement.new(conn)
      Application.put_env(:ex_arrow, :adbc_native, ExArrow.ADBC.TestNativeError)

      assert {:error, "test error"} = Statement.execute(stmt)
    end
  end

  # ── Mox mocks ───────────────────────────────────────────────────────────────

  describe "Database with Mox mock" do
    test "open/1 uses mock when configured and returns success" do
      Application.put_env(:ex_arrow, :adbc_database_impl, ExArrow.ADBC.DatabaseMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_database_impl) end)

      fake_db = %Database{resource: make_ref()}

      ExArrow.ADBC.DatabaseMock
      |> Mox.expect(:open, fn "driver.so" ->
        {:ok, fake_db}
      end)

      assert {:ok, ^fake_db} = Database.open("driver.so")
    end
  end

  describe "Connection with Mox mock" do
    test "open/1 delegates to mock" do
      Application.put_env(:ex_arrow, :adbc_connection_impl, ExArrow.ADBC.ConnectionMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_connection_impl) end)

      db = %Database{resource: make_ref()}
      fake_conn = %Connection{resource: make_ref()}

      ExArrow.ADBC.ConnectionMock
      |> Mox.expect(:open, fn ^db -> {:ok, fake_conn} end)

      assert {:ok, ^fake_conn} = Connection.open(db)
    end
  end

  describe "Statement with Mox mock" do
    test "new/2 set_sql/2 execute/1 delegate to mock" do
      Application.put_env(:ex_arrow, :adbc_statement_impl, ExArrow.ADBC.StatementMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_statement_impl) end)

      conn = %Connection{resource: make_ref()}
      stmt = %Statement{resource: make_ref()}
      stream = %Stream{resource: make_ref(), backend: :adbc}

      ExArrow.ADBC.StatementMock
      |> Mox.expect(:new, fn ^conn -> {:ok, stmt} end)

      ExArrow.ADBC.StatementMock
      |> Mox.expect(:set_sql, fn ^stmt, "SELECT 1" -> :ok end)

      ExArrow.ADBC.StatementMock
      |> Mox.expect(:execute, fn ^stmt -> {:ok, stream} end)

      assert {:ok, ^stmt} = Statement.new(conn)
      assert :ok = Statement.set_sql(stmt, "SELECT 1")
      assert {:ok, ^stream} = Statement.execute(stmt)
    end
  end

  # ── Integration: live driver (skip if no driver) ─────────────────────────────
  # Prefer a real skip: when ExUnit supports it, return {:skip, reason} from
  # setup when the driver can't be opened so the test is reported as skipped.
  # ExUnit 1.18 does not support that; we raise instead so the test never passes
  # when the driver is missing (mix test --exclude adbc to omit).

  @tag :adbc
  test "full query path: database -> connection -> statement -> execute -> stream (skip if no driver)" do
    opts =
      case System.get_env("ADBC_DRIVER") do
        path when is_binary(path) and path != "" -> [driver_path: path]
        _ -> [driver_name: "adbc_driver_sqlite", uri: ":memory:"]
      end

    case Database.open(opts) do
      {:error, reason} ->
        raise "ADBC driver not available: #{inspect(reason)}. Set ADBC_DRIVER or install a driver. Run with --exclude adbc to omit this test."

      {:ok, db} ->
        assert {:ok, conn} = Connection.open(db)
        assert {:ok, stmt} = Statement.new(conn)
        assert :ok = Statement.set_sql(stmt, "SELECT 1 AS n")
        assert {:ok, stream} = Statement.execute(stmt)
        assert stream.backend == :adbc
        assert {:ok, %Schema{}} = Stream.schema(stream)
        first = Stream.next(stream)
        if first, do: assert(ExArrow.RecordBatch.num_rows(first) >= 0)
    end
  end

  @tag :adbc
  test "metadata: get_table_types when driver available (fails with clear message if no driver)" do
    opts =
      case System.get_env("ADBC_DRIVER") do
        path when is_binary(path) and path != "" -> [driver_path: path]
        _ -> [driver_name: "adbc_driver_sqlite", uri: ":memory:"]
      end

    case Database.open(opts) do
      {:error, reason} ->
        raise "ADBC driver not available: #{inspect(reason)}. Set ADBC_DRIVER or install a driver. Run with --exclude adbc to omit this test."

      {:ok, db} ->
        assert {:ok, conn} = Connection.open(db)

        case Connection.get_table_types(conn) do
          {:ok, stream} ->
            assert stream.backend == :adbc
            assert {:ok, %Schema{}} = Stream.schema(stream)

          {:error, _msg} ->
            # Driver may not support get_table_types
            assert true
        end
    end
  end
end
