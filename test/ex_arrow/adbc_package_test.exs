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

  import Mox

  alias ExArrow.ADBC.{
    AdbcPackageManager,
    Connection,
    Database,
    DatabaseAdbcPackageImpl,
    Statement
  }

  alias ExArrow.{Schema, Stream}

  setup context do
    Mox.set_mox_from_context(context)
    :ok
  end

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

    test "open(:adbc_package) returns {:ok, db} when pids are available" do
      # Inject a state that looks like a live backend using sys.replace_state.
      mgr = Process.whereis(ExArrow.ADBC.AdbcPackageManager)
      db_pid = spawn(fn -> Process.sleep(5_000) end)
      conn_pid = spawn(fn -> Process.sleep(5_000) end)
      :sys.replace_state(mgr, fn _s -> %{db: db_pid, conn: conn_pid} end)

      assert {:ok, %Database{resource: :adbc_package}} = Database.open(:adbc_package)
    end

    test "open(:adbc_package) returns error when backend previously failed" do
      # Inject a cached-error state so the {:error, reason} branch is hit.
      mgr = Process.whereis(ExArrow.ADBC.AdbcPackageManager)
      :sys.replace_state(mgr, fn _s -> {:error, "driver_load_failed"} end)

      assert {:error, msg} = Database.open(:adbc_package)
      assert msg =~ "failed to start"
      assert msg =~ "driver_load_failed"
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

    test "get_pids returns error when config set but backend fails to start" do
      Application.put_env(:ex_arrow, :adbc_package, driver: :nonexistent_driver, uri: ":memory:")
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_package) end)

      # First call triggers ensure_started -> start_database fails -> state becomes {:error, reason}
      assert {:error, _reason} = AdbcPackageManager.get_pids()
      # Second call hits handle_call(:get_pids, _from, {:error, reason})
      assert {:error, _reason} = AdbcPackageManager.get_pids()
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

  # ── StatementImpl :adbc_package backend ─────────────────────────────────────
  # AdbcPackageManager is running from setup (no driver configured).
  # These tests drive StatementImpl through the public Statement API so that
  # the :adbc_package function clauses in statement_impl.ex are covered.

  describe "StatementImpl :adbc_package backend (via Statement API)" do
    test "new/1 returns a statement with an {:adbc_package, ref} resource" do
      conn = %Connection{resource: :adbc_package}
      assert {:ok, %Statement{resource: {:adbc_package, ref}}} = Statement.new(conn)
      assert is_reference(ref)
    end

    test "set_sql/2 stores SQL for an :adbc_package statement" do
      conn = %Connection{resource: :adbc_package}
      {:ok, stmt} = Statement.new(conn)
      assert :ok = Statement.set_sql(stmt, "SELECT 1")
    end

    test "execute/1 returns error when backend not configured" do
      conn = %Connection{resource: :adbc_package}
      {:ok, stmt} = Statement.new(conn)
      :ok = Statement.set_sql(stmt, "SELECT 1")
      # No driver configured → AdbcPackageManager cannot start → {:error, _}
      assert {:error, _reason} = Statement.execute(stmt)
    end
  end

  # ── AdbcPackagePool callback unit tests ────────────────────────────────────
  # Call NimblePool callbacks directly — no pool process needed.

  describe "AdbcPackagePool NimblePool callbacks" do
    test "handle_checkout/4 returns the conn_pid for checkout" do
      conn_pid = self()
      assert {:ok, ^conn_pid, ^conn_pid, :state} =
               ExArrow.ADBC.AdbcPackagePool.handle_checkout(:checkout, :from, conn_pid, :state)
    end

    test "handle_checkin/4 with :ok keeps the conn_pid" do
      conn_pid = self()
      assert {:ok, ^conn_pid, :state} =
               ExArrow.ADBC.AdbcPackagePool.handle_checkin(:ok, :from, conn_pid, :state)
    end

    test "handle_checkin/4 with {:remove, reason} removes the worker" do
      conn_pid = self()
      assert {:remove, :some_error, :state} =
               ExArrow.ADBC.AdbcPackagePool.handle_checkin(
                 {:remove, :some_error},
                 :from,
                 conn_pid,
                 :state
               )
    end

    test "terminate_worker/3 kills a live connection process" do
      conn_pid = spawn(fn -> Process.sleep(5_000) end)
      assert Process.alive?(conn_pid)
      assert {:ok, :state} = ExArrow.ADBC.AdbcPackagePool.terminate_worker(:reason, conn_pid, :state)
      Process.sleep(10)
      refute Process.alive?(conn_pid)
    end

    test "terminate_worker/3 handles a non-pid conn gracefully" do
      assert {:ok, :state} =
               ExArrow.ADBC.AdbcPackagePool.terminate_worker(:reason, :not_a_pid, :state)
    end

    test "init_worker/1 returns error when Adbc.Connection cannot be opened" do
      # Use a dead PID so the connection attempt fails immediately with noproc.
      # Trap exits so the EXIT signal from the failed start_link doesn't crash
      # the test process before we can inspect the return value.
      dead = spawn(fn -> :ok end)
      Process.sleep(10)
      refute Process.alive?(dead)

      Process.flag(:trap_exit, true)
      assert {:error, _reason} = ExArrow.ADBC.AdbcPackagePool.init_worker(dead)
      receive do {:EXIT, _, _} -> :ok after 0 -> :ok end
    end
  end

  # ── AdbcPackagePool start_link / query with NimblePoolMock ─────────────────

  describe "AdbcPackagePool start_link/1 and query/3 with NimblePoolMock" do
    setup do
      Application.put_env(:ex_arrow, :nimble_pool_mod, ExArrow.NimblePoolMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :nimble_pool_mod) end)
      :ok
    end

    test "start_link/1 delegates to NimblePool.start_link with correct opts" do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      db_pid = self()

      stub(ExArrow.NimblePoolMock, :start_link, fn opts ->
        assert opts[:worker] == {ExArrow.ADBC.AdbcPackagePool, db_pid}
        assert opts[:pool_size] == 3
        {:ok, pid}
      end)

      assert {:ok, ^pid} =
               ExArrow.ADBC.AdbcPackagePool.start_link(database: db_pid, pool_size: 3)
    end

    test "start_link/1 uses __MODULE__ as default name" do
      pid = spawn(fn -> Process.sleep(:infinity) end)

      stub(ExArrow.NimblePoolMock, :start_link, fn opts ->
        assert opts[:name] == ExArrow.ADBC.AdbcPackagePool
        {:ok, pid}
      end)

      ExArrow.ADBC.AdbcPackagePool.start_link(database: self())
    end

    test "query/3 calls checkout! with the sql and returns its result" do
      # Do NOT invoke fun — calling Adbc.Connection.query with a fake pid
      # causes a recursive GenServer.call on the test process.
      stub(ExArrow.NimblePoolMock, :checkout!, fn _pool, sql, _fun, _timeout ->
        assert sql == "SELECT 42"
        :mocked_result
      end)

      assert :mocked_result = ExArrow.ADBC.AdbcPackagePool.query(:fake_pool, "SELECT 42")
    end

    test "query/3 forwards pool_timeout option to checkout!" do
      stub(ExArrow.NimblePoolMock, :checkout!, fn _pool, _sql, _fun, timeout ->
        assert timeout == 8_000
        :ok
      end)

      ExArrow.ADBC.AdbcPackagePool.query(:fake_pool, "SELECT 1", pool_timeout: 8_000)
    end
  end

  # ── AdbcPackageManager injectable-module unit tests ────────────────────────
  # Cover start_if_configured, start_database, start_connection, start_pool,
  # use_pool?, start_pool_or_connection, query, and adbc_result_to_stream
  # without needing a real ADBC driver or Explorer installation.

  describe "AdbcPackageManager with injectable stubs" do
    @env_keys [
      :adbc_db_module,
      :adbc_conn_module,
      :adbc_result_module,
      :explorer_df_module,
      :nimble_pool_mod,
      :adbc_package,
      :adbc_package_pool_size
    ]

    setup do
      saved = Enum.map(@env_keys, fn k -> {k, Application.get_env(:ex_arrow, k)} end)

      on_exit(fn ->
        Enum.each(saved, fn
          {k, nil} -> Application.delete_env(:ex_arrow, k)
          {k, v} -> Application.put_env(:ex_arrow, k, v)
        end)
      end)

      :ok
    end

    defp put_stubs(overrides) do
      Enum.each(overrides, fn {k, v} -> Application.put_env(:ex_arrow, k, v) end)
    end

    # Returns the manager pid and preserves the ETS table when replacing state.
    defp inject_state(extra) do
      mgr = Process.whereis(AdbcPackageManager)
      table = :sys.get_state(mgr) |> Map.get(:table)

      :sys.replace_state(mgr, fn _s ->
        Map.merge(%{table: table}, extra)
      end)

      mgr
    end

    test "start_connection path: start_if_configured and start_connection success" do
      put_stubs(
        adbc_package: [driver: :test, uri: ":memory:"],
        adbc_db_module: ExArrow.ADBC.AdbcDbStub,
        adbc_conn_module: ExArrow.ADBC.AdbcConnStub
      )

      assert {:ok, {db_pid, conn_pid}} = AdbcPackageManager.get_pids()
      assert is_pid(db_pid)
      assert is_pid(conn_pid)
    end

    test "start_connection error path: start_pool_or_connection kills db_pid on failure" do
      put_stubs(
        adbc_package: [driver: :test, uri: ":memory:"],
        adbc_db_module: ExArrow.ADBC.AdbcDbStub,
        adbc_conn_module: ExArrow.ADBC.AdbcConnErrStub
      )

      assert {:error, :stub_conn_failed} = AdbcPackageManager.get_pids()
    end

    test "start_pool path: use_pool? true covers start_pool success" do
      pool_pid = spawn(fn -> Process.sleep(:infinity) end)

      stub(ExArrow.NimblePoolMock, :start_link, fn _opts -> {:ok, pool_pid} end)

      put_stubs(
        adbc_package: [driver: :test, uri: ":memory:"],
        adbc_db_module: ExArrow.ADBC.AdbcDbStub,
        nimble_pool_mod: ExArrow.NimblePoolMock,
        adbc_package_pool_size: 2
      )

      assert {:ok, {db_pid, nil}} = AdbcPackageManager.get_pids()
      assert is_pid(db_pid)
    end

    test "start_pool error path: start_pool_or_connection kills db_pid on pool failure" do
      stub(ExArrow.NimblePoolMock, :start_link, fn _opts -> {:error, :stub_pool_failed} end)

      put_stubs(
        adbc_package: [driver: :test, uri: ":memory:"],
        adbc_db_module: ExArrow.ADBC.AdbcDbStub,
        nimble_pool_mod: ExArrow.NimblePoolMock,
        adbc_package_pool_size: 2
      )

      assert {:error, :stub_pool_failed} = AdbcPackageManager.get_pids()
    end

    test "execute_statement success (conn): covers query/conn, ensure_started/%{db:_}, adbc_result_to_stream" do
      put_stubs(
        adbc_conn_module: ExArrow.ADBC.AdbcConnStub,
        adbc_result_module: ExArrow.ADBC.AdbcResultStub,
        explorer_df_module: ExArrow.ADBC.ExplorerDfStub
      )

      db_pid = spawn(fn -> Process.sleep(:infinity) end)
      conn_pid = spawn(fn -> Process.sleep(:infinity) end)
      inject_state(%{db: db_pid, conn: conn_pid})

      {:ok, ref} = AdbcPackageManager.create_statement()
      :ok = AdbcPackageManager.set_statement_sql(ref, "SELECT 1")

      assert {:ok, %ExArrow.Stream{}} = AdbcPackageManager.execute_statement(ref)
    end

    test "execute_statement success (pool): covers query/pool path" do
      stub(ExArrow.NimblePoolMock, :checkout!, fn _pool, _sql, _fun, _timeout ->
        {:ok, :stub_pool_query_result}
      end)

      put_stubs(
        nimble_pool_mod: ExArrow.NimblePoolMock,
        adbc_result_module: ExArrow.ADBC.AdbcResultStub,
        explorer_df_module: ExArrow.ADBC.ExplorerDfStub
      )

      db_pid = spawn(fn -> Process.sleep(:infinity) end)
      inject_state(%{db: db_pid, pool: ExArrow.ADBC.AdbcPackagePool})

      {:ok, ref} = AdbcPackageManager.create_statement()
      :ok = AdbcPackageManager.set_statement_sql(ref, "SELECT 1")

      assert {:ok, %ExArrow.Stream{}} = AdbcPackageManager.execute_statement(ref)
    end

    test "execute_statement query error: covers {:error, _} reply in handle_call" do
      put_stubs(adbc_conn_module: ExArrow.ADBC.AdbcConnQueryErrStub)

      db_pid = spawn(fn -> Process.sleep(:infinity) end)
      conn_pid = spawn(fn -> Process.sleep(:infinity) end)
      inject_state(%{db: db_pid, conn: conn_pid})

      {:ok, ref} = AdbcPackageManager.create_statement()
      :ok = AdbcPackageManager.set_statement_sql(ref, "SELECT 1")

      assert {:error, :stub_query_failed} = AdbcPackageManager.execute_statement(ref)
    end

    test "adbc_result_to_stream without Explorer: returns missing-dep error" do
      # Use a non-existent module so Code.ensure_loaded? returns false.
      put_stubs(
        adbc_conn_module: ExArrow.ADBC.AdbcConnStub,
        adbc_result_module: ExArrow.ADBC.AdbcResultStub,
        explorer_df_module: ExArrow.ADBC.NonExistentExplorer
      )

      db_pid = spawn(fn -> Process.sleep(:infinity) end)
      conn_pid = spawn(fn -> Process.sleep(:infinity) end)
      inject_state(%{db: db_pid, conn: conn_pid})

      {:ok, ref} = AdbcPackageManager.create_statement()
      :ok = AdbcPackageManager.set_statement_sql(ref, "SELECT 1")

      assert {:error, msg} = AdbcPackageManager.execute_statement(ref)
      assert msg =~ "adbc_package backend requires the :explorer dependency"
    end

    test "set_statement_sql when state has no ETS table returns :not_configured" do
      :sys.replace_state(Process.whereis(AdbcPackageManager), fn _s ->
        {:error, :no_table_state}
      end)

      assert {:error, :not_configured} =
               AdbcPackageManager.set_statement_sql(make_ref(), "SELECT 1")
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
