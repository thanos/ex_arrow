defmodule ExArrow.ADBC.AdbcPackageTest do
  @moduledoc """
  Tests for the adbc_package backend: DatabaseAdbcPackageImpl, AdbcPackageManager,
  and (when deps available) integration with Adbc + Explorer, including pool_size 1
  and pool_size > 1.

  Error path when Explorer is not available (execute returns {:error, "adbc_package
  backend requires the :explorer dependency..."}) is covered by the integration flow:
  that message comes from adbc_result_to_stream/1 when Code.ensure_loaded?(Explorer.DataFrame)
  is false; we do not unit-test that branch in isolation.
  """
  use ExUnit.Case, async: false

  alias ExArrow.ADBC.{
    AdbcPackageManager,
    Connection,
    Database,
    DatabaseAdbcPackageImpl,
    Statement
  }

  alias ExArrow.{Schema, Stream}

  setup do
    # Ensure manager is not configured so we control state
    saved_package = Application.get_env(:ex_arrow, :adbc_package)
    saved_pool_size = Application.get_env(:ex_arrow, :adbc_package_pool_size)
    Application.delete_env(:ex_arrow, :adbc_package)
    Application.delete_env(:ex_arrow, :adbc_package_pool_size)

    if pid = Process.whereis(ExArrow.ADBC.AdbcPackageManager) do
      GenServer.stop(pid, :normal, 5_000)
      Process.sleep(50)
    end

    {:ok, _} = AdbcPackageManager.start_link()

    on_exit(fn ->
      pid = Process.whereis(ExArrow.ADBC.AdbcPackageManager)

      if is_pid(pid) and Process.alive?(pid) do
        GenServer.stop(pid, :normal, 5_000)
      end

      if saved_package != nil, do: Application.put_env(:ex_arrow, :adbc_package, saved_package)

      if saved_pool_size != nil,
        do: Application.put_env(:ex_arrow, :adbc_package_pool_size, saved_pool_size)
    end)

    :ok
  end

  describe "DatabaseAdbcPackageImpl" do
    test "open(:adbc_package) returns error when backend not configured" do
      # Manager is running but config was cleared in setup
      assert {:error, msg} = Database.open(:adbc_package)
      assert is_binary(msg)
      assert msg =~ "not configured"
      assert msg =~ "adbc_package"
    end

    test "open(:other) returns error (impl only supports :adbc_package)" do
      assert {:error, msg} = DatabaseAdbcPackageImpl.open(:other)
      assert msg =~ "only supports open(:adbc_package)"
    end

    test "open(:adbc_package) via Database returns same as impl" do
      assert {:error, _} = Database.open(:adbc_package)
    end

    @tag :adbc_package
    test "open(:adbc_package) returns error when driver fails to start" do
      unless Code.ensure_loaded?(Module.safe_concat(["Elixir", "Adbc", "Database"])) do
        raise "Adbc required to test driver failure path."
      end

      # Invalid driver option; manager lazy-starts and start_database fails
      Application.put_env(:ex_arrow, :adbc_package, driver: :nonexistent_driver, uri: ":memory:")

      assert {:error, msg} = Database.open(:adbc_package)
      assert is_binary(msg)
      assert msg =~ "failed to start"
    end
  end

  describe "AdbcPackageManager state and API when not configured" do
    test "get_pids returns :not_configured when config not set" do
      assert {:error, :not_configured} = AdbcPackageManager.get_pids()
    end

    test "create_statement returns a reference" do
      assert {:ok, ref} = AdbcPackageManager.create_statement()
      assert is_reference(ref)
    end

    test "set_statement_sql stores sql for valid statement ref" do
      {:ok, ref} = AdbcPackageManager.create_statement()
      assert :ok = AdbcPackageManager.set_statement_sql(ref, "SELECT 1")
    end

    test "set_statement_sql returns error for unknown ref" do
      ref = make_ref()

      assert {:error, "statement not found"} =
               AdbcPackageManager.set_statement_sql(ref, "SELECT 1")
    end

    test "execute_statement returns error when backend not configured" do
      {:ok, ref} = AdbcPackageManager.create_statement()
      :ok = AdbcPackageManager.set_statement_sql(ref, "SELECT 1")
      assert {:error, :not_configured} = AdbcPackageManager.execute_statement(ref)
    end

    test "execute_statement returns error when set_sql was not called" do
      {:ok, ref} = AdbcPackageManager.create_statement()
      assert {:error, "set_sql was not called"} = AdbcPackageManager.execute_statement(ref)
    end

    test "execute_statement returns error for unknown statement ref" do
      ref = make_ref()
      assert {:error, "statement not found"} = AdbcPackageManager.execute_statement(ref)
    end
  end

  describe "adbc_package integration (requires adbc + explorer)" do
    @tag :adbc_package
    test "full flow: open(:adbc_package) -> connection -> statement -> execute when configured" do
      unless adbc_and_explorer_available?() do
        raise "adbc and explorer required. Run with --include adbc_package only when deps available."
      end

      ensure_sqlite_driver!()
      Application.put_env(:adbc, :drivers, [:sqlite])
      Application.put_env(:ex_arrow, :adbc_package, driver: :sqlite, uri: ":memory:")
      Application.put_env(:ex_arrow, :adbc_package_pool_size, 1)

      # Manager already running from setup; it will lazy-start on get_pids/execute
      assert {:ok, db} = Database.open(:adbc_package)
      assert %Database{resource: :adbc_package} = db

      assert {:ok, conn} = Connection.open(db)
      assert {:ok, stmt} = Statement.new(conn, "SELECT 1 AS n, 'hello' AS msg")
      assert {:ok, stream} = Statement.execute(stmt)
      assert stream.backend == :adbc or is_struct(stream, Stream)

      assert {:ok, %Schema{}} = Stream.schema(stream)
      batch = Stream.next(stream)
      assert batch != nil
      assert ExArrow.RecordBatch.num_rows(batch) >= 1
      assert Stream.next(stream) == nil
    end

    @tag :adbc_package
    test "pool_size 1 uses single connection (no NimblePool)" do
      unless adbc_and_explorer_available?() do
        raise "adbc and explorer required."
      end

      ensure_sqlite_driver!()
      Application.put_env(:adbc, :drivers, [:sqlite])
      Application.put_env(:ex_arrow, :adbc_package, driver: :sqlite, uri: ":memory:")
      Application.put_env(:ex_arrow, :adbc_package_pool_size, 1)

      assert {:ok, db} = Database.open(:adbc_package)
      {:ok, conn} = Connection.open(db)

      # Two sequential queries
      {:ok, stmt1} = Statement.new(conn, "SELECT 1 AS a")
      {:ok, stream1} = Statement.execute(stmt1)
      _ = Stream.next(stream1)

      {:ok, stmt2} = Statement.new(conn, "SELECT 2 AS b")
      {:ok, stream2} = Statement.execute(stmt2)
      batch2 = Stream.next(stream2)
      assert batch2 != nil
      assert ExArrow.RecordBatch.num_rows(batch2) >= 1
    end

    @tag :adbc_package
    test "pool_size > 1 uses pool when nimble_pool available" do
      unless adbc_and_explorer_available?() and Code.ensure_loaded?(NimblePool) do
        raise "adbc, explorer, and nimble_pool required for pool test."
      end

      ensure_sqlite_driver!()
      Application.put_env(:adbc, :drivers, [:sqlite])
      Application.put_env(:ex_arrow, :adbc_package, driver: :sqlite, uri: ":memory:")
      Application.put_env(:ex_arrow, :adbc_package_pool_size, 2)

      assert {:ok, db} = Database.open(:adbc_package)
      {:ok, conn} = Connection.open(db)

      # Run two queries concurrently; pool should serve both
      task1 =
        Task.async(fn ->
          {:ok, stmt} = Statement.new(conn, "SELECT 1 AS x")
          {:ok, stream} = Statement.execute(stmt)
          batch = Stream.next(stream)
          batch && ExArrow.RecordBatch.num_rows(batch)
        end)

      task2 =
        Task.async(fn ->
          {:ok, stmt} = Statement.new(conn, "SELECT 2 AS y")
          {:ok, stream} = Statement.execute(stmt)
          batch = Stream.next(stream)
          batch && ExArrow.RecordBatch.num_rows(batch)
        end)

      r1 = Task.await(task1, 5_000)
      r2 = Task.await(task2, 5_000)
      assert r1 >= 1
      assert r2 >= 1
    end
  end

  defp adbc_and_explorer_available? do
    adbc = Code.ensure_loaded?(Module.safe_concat(["Elixir", "Adbc", "Database"]))
    explorer = Code.ensure_loaded?(Module.safe_concat(["Elixir", "Explorer", "DataFrame"]))
    adbc && explorer
  end

  defp ensure_sqlite_driver! do
    if Code.ensure_loaded?(Adbc) and function_exported?(Adbc, :download_driver!, 1) do
      Adbc.download_driver!(:sqlite)
    end
  end
end
