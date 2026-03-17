defmodule ExArrow.ADBC.ConnectionPoolTest do
  # async: false — mutates multiple application env keys.
  # set_mox_global — NimblePool workers are spawned processes; global mode
  # ensures Mox stubs set on the test process are visible in worker processes.
  use ExUnit.Case, async: false

  import Mox

  alias ExArrow.ADBC.{Connection, ConnectionPool, Database, Statement}

  alias ExArrow.ADBC.ConnectionPool.Worker
  alias ExArrow.Stream

  setup :set_mox_global

  setup do
    prev = %{
      conn: Application.get_env(:ex_arrow, :adbc_connection_impl),
      stmt: Application.get_env(:ex_arrow, :adbc_statement_impl),
      pool: Application.get_env(:ex_arrow, :nimble_pool_mod)
    }

    Application.put_env(:ex_arrow, :adbc_connection_impl, ExArrow.ADBC.ConnectionMock)
    Application.put_env(:ex_arrow, :adbc_statement_impl, ExArrow.ADBC.StatementMock)

    on_exit(fn ->
      restore(:adbc_connection_impl, prev.conn)
      restore(:adbc_statement_impl, prev.stmt)
      restore(:nimble_pool_mod, prev.pool)
    end)

    fake_db = %Database{resource: make_ref()}
    {:ok, db: fake_db}
  end

  defp restore(key, nil), do: Application.delete_env(:ex_arrow, key)
  defp restore(key, val), do: Application.put_env(:ex_arrow, key, val)

  defp new_conn, do: %Connection{resource: make_ref()}
  defp new_stmt, do: %Statement{resource: make_ref()}
  defp new_stream, do: %Stream{resource: make_ref(), backend: :adbc}

  # ── NimblePoolBehaviour ──────────────────────────────────────────────────────

  describe "NimblePoolBehaviour" do
    test "declares start_link/1 and checkout!/4 callbacks" do
      callbacks = ExArrow.NimblePoolBehaviour.behaviour_info(:callbacks)
      assert {:start_link, 1} in callbacks
      assert {:checkout!, 4} in callbacks
    end

    test "NimblePoolMock satisfies the behaviour" do
      behaviours =
        ExArrow.NimblePoolMock.__info__(:attributes)
        |> Keyword.get(:behaviour, [])

      assert ExArrow.NimblePoolBehaviour in behaviours
    end
  end

  # ── NimblePool callback unit tests ──────────────────────────────────────────
  # Call the NimblePool callbacks directly — no pool process needed.

  describe "init_worker/1" do
    test "opens a connection from a Database struct and wraps it in a Worker", %{db: db} do
      conn = new_conn()
      expect(ExArrow.ADBC.ConnectionMock, :open, fn ^db -> {:ok, conn} end)

      assert {:ok, %Worker{db: ^db, conn: ^conn}, ^db} = ConnectionPool.init_worker(db)
    end

    test "propagates connection errors", %{db: db} do
      expect(ExArrow.ADBC.ConnectionMock, :open, fn _db -> {:error, "cannot connect"} end)

      assert {:error, "cannot connect"} = ConnectionPool.init_worker(db)
    end

    test "resolves an atom name via DatabaseServer before opening the connection" do
      # Start a real DatabaseServer backed by DatabaseMock so init_worker can
      # look it up by name.
      fake_db = %Database{resource: make_ref()}
      conn = new_conn()

      stub(ExArrow.ADBC.DatabaseMock, :open, fn _opts -> {:ok, fake_db} end)
      stub(ExArrow.ADBC.ConnectionMock, :open, fn _db -> {:ok, conn} end)

      name = :"db_server_#{:erlang.unique_integer([:positive])}"

      prev_db_impl = Application.get_env(:ex_arrow, :adbc_database_impl)
      Application.put_env(:ex_arrow, :adbc_database_impl, ExArrow.ADBC.DatabaseMock)

      {:ok, srv} =
        ExArrow.ADBC.DatabaseServer.start_link(name: name, driver_path: "fake.so")

      # init_worker with an atom name resolves via DatabaseServer.get/1
      assert {:ok, %Worker{conn: ^conn}, ^name} = ConnectionPool.init_worker(name)

      GenServer.stop(srv)

      restore(:adbc_database_impl, prev_db_impl)
    end
  end

  describe "handle_checkout/4" do
    test "returns the worker unchanged for :checkout" do
      worker = %Worker{db: make_ref(), conn: new_conn()}

      assert {:ok, ^worker, ^worker, :state} =
               ConnectionPool.handle_checkout(:checkout, :from, worker, :state)
    end
  end

  describe "handle_checkin/4" do
    test ":ok keeps the worker in the pool" do
      worker = %Worker{db: make_ref(), conn: new_conn()}

      assert {:ok, ^worker, :state} =
               ConnectionPool.handle_checkin(:ok, :from, worker, :state)
    end

    test "{:remove, reason} removes the worker from the pool" do
      worker = %Worker{db: make_ref(), conn: new_conn()}

      assert {:remove, :closed, :state} =
               ConnectionPool.handle_checkin({:remove, :error}, :from, worker, :state)
    end
  end

  describe "terminate_worker/3" do
    test "closes the connection (struct no-op) and returns {:ok, pool_state}" do
      conn = new_conn()
      worker = %Worker{db: make_ref(), conn: conn}

      # Connection.close/1 is implemented directly on the struct — always :ok.
      assert {:ok, :state} = ConnectionPool.terminate_worker(:reason, worker, :state)
    end
  end

  # ── start_link/1 with NimblePoolMock ────────────────────────────────────────

  describe "start_link/1 with NimblePoolMock" do
    setup do
      Application.put_env(:ex_arrow, :nimble_pool_mod, ExArrow.NimblePoolMock)
      :ok
    end

    test "delegates to NimblePool.start_link with correct options", %{db: db} do
      pid = spawn(fn -> Process.sleep(:infinity) end)

      expect(ExArrow.NimblePoolMock, :start_link, fn opts ->
        assert opts[:worker] == {ConnectionPool, db}
        assert opts[:pool_size] == 3
        assert opts[:lazy] == true
        {:ok, pid}
      end)

      assert {:ok, ^pid} = ConnectionPool.start_link(database: db, pool_size: 3, lazy: true)
    end

    test "includes :name in pool opts when provided", %{db: db} do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      name = :"pool_#{:erlang.unique_integer([:positive])}"

      expect(ExArrow.NimblePoolMock, :start_link, fn opts ->
        assert opts[:name] == name
        {:ok, pid}
      end)

      assert {:ok, ^pid} = ConnectionPool.start_link(database: db, name: name)
    end

    test "omits :name from pool opts when not provided", %{db: db} do
      pid = spawn(fn -> Process.sleep(:infinity) end)

      expect(ExArrow.NimblePoolMock, :start_link, fn opts ->
        refute Keyword.has_key?(opts, :name)
        {:ok, pid}
      end)

      assert {:ok, ^pid} = ConnectionPool.start_link(database: db)
    end
  end

  # ── query/3 with NimblePoolMock ──────────────────────────────────────────────

  describe "query/3 with NimblePoolMock" do
    setup %{db: db} do
      Application.put_env(:ex_arrow, :nimble_pool_mod, ExArrow.NimblePoolMock)
      {:ok, worker: %Worker{db: db, conn: new_conn()}}
    end

    test "invokes checkout closure, runs statement, returns stream", %{worker: worker} do
      stmt = new_stmt()
      stream = new_stream()

      stub(ExArrow.ADBC.StatementMock, :new, fn _conn -> {:ok, stmt} end)
      stub(ExArrow.ADBC.StatementMock, :set_sql, fn _stmt, _sql -> :ok end)
      stub(ExArrow.ADBC.StatementMock, :execute, fn _stmt -> {:ok, stream} end)

      expect(ExArrow.NimblePoolMock, :checkout!, fn _pool, :checkout, fun, _timeout ->
        {result, _checkin, _w} = fun.(:from, worker)
        result
      end)

      assert {:ok, ^stream} = ConnectionPool.query(:fake_pool, "SELECT 1")
    end

    test "checkin tag is {:remove, :error} when statement fails", %{worker: worker} do
      stub(ExArrow.ADBC.StatementMock, :new, fn _conn -> {:error, "stmt error"} end)

      expect(ExArrow.NimblePoolMock, :checkout!, fn _pool, :checkout, fun, _timeout ->
        {result, checkin, _w} = fun.(:from, worker)
        assert checkin == {:remove, :error}
        result
      end)

      assert {:error, "stmt error"} = ConnectionPool.query(:fake_pool, "SELECT 1")
    end

    test "checkin tag is :ok when statement succeeds", %{worker: worker} do
      stream = new_stream()

      stub(ExArrow.ADBC.StatementMock, :new, fn _conn -> {:ok, new_stmt()} end)
      stub(ExArrow.ADBC.StatementMock, :set_sql, fn _stmt, _sql -> :ok end)
      stub(ExArrow.ADBC.StatementMock, :execute, fn _stmt -> {:ok, stream} end)

      expect(ExArrow.NimblePoolMock, :checkout!, fn _pool, :checkout, fun, _timeout ->
        {result, checkin, _w} = fun.(:from, worker)
        assert checkin == :ok
        result
      end)

      assert {:ok, ^stream} = ConnectionPool.query(:fake_pool, "SELECT 42")
    end

    test "forwards pool_timeout option to checkout!", %{worker: _worker} do
      stream = new_stream()

      stub(ExArrow.ADBC.StatementMock, :new, fn _conn -> {:ok, new_stmt()} end)
      stub(ExArrow.ADBC.StatementMock, :set_sql, fn _stmt, _sql -> :ok end)
      stub(ExArrow.ADBC.StatementMock, :execute, fn _stmt -> {:ok, stream} end)

      expect(ExArrow.NimblePoolMock, :checkout!, fn _pool, :checkout, _fun, timeout ->
        assert timeout == 9_000
        {:ok, stream}
      end)

      ConnectionPool.query(:fake_pool, "SELECT 1", pool_timeout: 9_000)
    end
  end

  # ── with_connection/3 with NimblePoolMock ────────────────────────────────────

  describe "with_connection/3 with NimblePoolMock" do
    setup %{db: db} do
      Application.put_env(:ex_arrow, :nimble_pool_mod, ExArrow.NimblePoolMock)
      conn = new_conn()
      {:ok, worker: %Worker{db: db, conn: conn}, conn: conn}
    end

    test "passes the connection to the user function and returns its result",
         %{worker: worker, conn: conn} do
      expect(ExArrow.NimblePoolMock, :checkout!, fn _pool, :checkout, fun, _timeout ->
        {result, _checkin, _w} = fun.(:from, worker)
        result
      end)

      assert {:got, ^conn} =
               ConnectionPool.with_connection(:fake_pool, fn c -> {{:got, c}, :ok} end)
    end

    test "honours {:remove, reason} checkin tag returned by user function",
         %{worker: worker} do
      expect(ExArrow.NimblePoolMock, :checkout!, fn _pool, :checkout, fun, _timeout ->
        {result, checkin, _w} = fun.(:from, worker)
        assert checkin == {:remove, :user_error}
        result
      end)

      ConnectionPool.with_connection(:fake_pool, fn _conn ->
        {:whatever, {:remove, :user_error}}
      end)
    end
  end
end
