defmodule ExArrow.FlightSQL.StatementTest do
  use ExUnit.Case, async: false

  alias ExArrow.FlightSQL.{Client, Error, Statement}
  alias ExArrow.Stream

  # ── Helpers ───────────────────────────────────────────────────────────────────

  setup do
    prev_stmt = Application.get_env(:ex_arrow, :flight_sql_statement_native)
    prev_client = Application.get_env(:ex_arrow, :flight_sql_client_native)
    prev_impl = Application.get_env(:ex_arrow, :flight_sql_client_impl)

    on_exit(fn ->
      restore(:flight_sql_statement_native, prev_stmt)
      restore(:flight_sql_client_native, prev_client)
      restore(:flight_sql_client_impl, prev_impl)
    end)

    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:ex_arrow, key)
  defp restore(key, val), do: Application.put_env(:ex_arrow, key, val)

  defp fake_stmt, do: %Statement{resource: make_ref()}
  defp fake_closed_stmt, do: %Statement{resource: make_ref(), closed: true}

  # ── Statement struct ─────────────────────────────────────────────────────────

  describe "struct" do
    test "holds a resource ref and defaults to open" do
      ref = make_ref()
      stmt = %Statement{resource: ref}
      assert stmt.resource == ref
      assert stmt.closed == false
    end

    test "closed field tracks close state" do
      stmt = %Statement{resource: make_ref(), closed: true}
      assert stmt.closed == true
    end
  end

  # ── Statement.execute/1 ──────────────────────────────────────────────────────

  describe "execute/1 — success (stub native)" do
    setup do
      Application.put_env(:ex_arrow, :flight_sql_statement_native, ExArrow.FlightSQL.StmtNativeOk)
      :ok
    end

    test "returns {:ok, %Stream{backend: :flight_sql}}" do
      assert {:ok, %Stream{backend: :flight_sql}} = Statement.execute(fake_stmt())
    end
  end

  describe "execute/1 — gRPC triple error" do
    setup do
      Application.put_env(
        :ex_arrow,
        :flight_sql_statement_native,
        ExArrow.FlightSQL.StmtNativeError
      )

      :ok
    end

    test "returns {:error, %Error{}} with structured code" do
      assert {:error, %Error{code: :server_error, grpc_status: 13, message: msg}} =
               Statement.execute(fake_stmt())

      assert msg =~ "internal error"
    end
  end

  describe "execute/1 — binary error" do
    setup do
      Application.put_env(
        :ex_arrow,
        :flight_sql_statement_native,
        ExArrow.FlightSQL.StmtNativeBinaryError
      )

      :ok
    end

    test "returns {:error, %Error{code: :transport_error}}" do
      assert {:error, %Error{code: :transport_error, message: "stream failed"}} =
               Statement.execute(fake_stmt())
    end
  end

  describe "execute/1 — invalid resource (real NIF)" do
    @tag no_nif: true
    test "raises ArgumentError when NIF is loaded with bad ref" do
      assert_raise ArgumentError, fn -> Statement.execute(fake_stmt()) end
    end
  end

  describe "execute/1 — closed statement" do
    test "returns {:error, %Error{code: :protocol_error}}" do
      assert {:error, %Error{code: :protocol_error, message: msg}} =
               Statement.execute(fake_closed_stmt())

      assert msg =~ "statement is closed"
    end
  end

  # ── Statement.execute_update/1 ───────────────────────────────────────────────

  describe "execute_update/1 — success with row count" do
    setup do
      Application.put_env(:ex_arrow, :flight_sql_statement_native, ExArrow.FlightSQL.StmtNativeOk)
      :ok
    end

    test "returns {:ok, n} for a positive row count" do
      assert {:ok, 5} = Statement.execute_update(fake_stmt())
    end
  end

  describe "execute_update/1 — :unknown row count" do
    setup do
      Application.put_env(
        :ex_arrow,
        :flight_sql_statement_native,
        ExArrow.FlightSQL.StmtNativeUnknown
      )

      :ok
    end

    test "returns {:ok, :unknown} when server omits row count" do
      assert {:ok, :unknown} = Statement.execute_update(fake_stmt())
    end
  end

  describe "execute_update/1 — gRPC error" do
    setup do
      Application.put_env(
        :ex_arrow,
        :flight_sql_statement_native,
        ExArrow.FlightSQL.StmtNativeError
      )

      :ok
    end

    test "returns {:error, %Error{code: :permission_denied}}" do
      assert {:error, %Error{code: :permission_denied, grpc_status: 7}} =
               Statement.execute_update(fake_stmt())
    end
  end

  describe "execute_update/1 — binary error" do
    setup do
      Application.put_env(
        :ex_arrow,
        :flight_sql_statement_native,
        ExArrow.FlightSQL.StmtNativeBinaryError
      )

      :ok
    end

    test "returns {:error, %Error{code: :transport_error}}" do
      assert {:error, %Error{code: :transport_error, message: "dml failed"}} =
               Statement.execute_update(fake_stmt())
    end
  end

  describe "execute_update/1 — invalid resource (real NIF)" do
    @tag no_nif: true
    test "raises ArgumentError when NIF is loaded with bad ref" do
      assert_raise ArgumentError, fn -> Statement.execute_update(fake_stmt()) end
    end
  end

  describe "execute_update/1 — closed statement" do
    test "returns {:error, %Error{code: :protocol_error}}" do
      assert {:error, %Error{code: :protocol_error}} =
               Statement.execute_update(fake_closed_stmt())
    end
  end

  # ── Statement.bind/2 ─────────────────────────────────────────────────────────

  describe "bind/2 — success (stub native)" do
    setup do
      Application.put_env(:ex_arrow, :flight_sql_statement_native, ExArrow.FlightSQL.StmtNativeOk)
      :ok
    end

    test "returns :ok" do
      batch = %ExArrow.RecordBatch{resource: make_ref()}
      assert :ok = Statement.bind(fake_stmt(), batch)
    end
  end

  describe "bind/2 — schema mismatch error" do
    setup do
      Application.put_env(
        :ex_arrow,
        :flight_sql_statement_native,
        ExArrow.FlightSQL.StmtNativeError
      )

      :ok
    end

    test "returns {:error, %Error{code: :invalid_argument}}" do
      batch = %ExArrow.RecordBatch{resource: make_ref()}

      assert {:error, %Error{code: :invalid_argument, message: msg}} =
               Statement.bind(fake_stmt(), batch)

      assert msg =~ "schema mismatch"
    end
  end

  describe "bind/2 — closed statement" do
    test "returns {:error, %Error{code: :protocol_error}}" do
      batch = %ExArrow.RecordBatch{resource: make_ref()}

      assert {:error, %Error{code: :protocol_error, message: msg}} =
               Statement.bind(fake_closed_stmt(), batch)

      assert msg =~ "statement is closed"
    end
  end

  # ── Statement.parameter_schema/1 ─────────────────────────────────────────────

  describe "parameter_schema/1 — success (stub native)" do
    setup do
      Application.put_env(:ex_arrow, :flight_sql_statement_native, ExArrow.FlightSQL.StmtNativeOk)
      :ok
    end

    test "returns {:ok, schema_ref}" do
      assert {:ok, _schema} = Statement.parameter_schema(fake_stmt())
    end
  end

  describe "parameter_schema/1 — unimplemented error" do
    setup do
      Application.put_env(
        :ex_arrow,
        :flight_sql_statement_native,
        ExArrow.FlightSQL.StmtNativeError
      )

      :ok
    end

    test "returns {:error, %Error{code: :unimplemented}}" do
      assert {:error, %Error{code: :unimplemented, message: msg}} =
               Statement.parameter_schema(fake_stmt())

      assert msg =~ "parameter schema not available"
    end
  end

  describe "parameter_schema/1 — closed statement" do
    test "returns {:error, %Error{code: :protocol_error}}" do
      assert {:error, %Error{code: :protocol_error}} =
               Statement.parameter_schema(fake_closed_stmt())
    end
  end

  # ── Statement.close/1 ───────────────────────────────────────────────────────

  describe "close/1 — success (stub native)" do
    setup do
      Application.put_env(:ex_arrow, :flight_sql_statement_native, ExArrow.FlightSQL.StmtNativeOk)
      :ok
    end

    test "returns :ok" do
      stmt = fake_stmt()
      assert :ok = Statement.close(stmt)
    end
  end

  describe "close/1 — idempotent" do
    test "calling close twice returns :ok" do
      stmt = %Statement{resource: make_ref(), closed: true}
      assert :ok = Statement.close(stmt)
    end
  end

  describe "close/1 — server error (stub native)" do
    setup do
      Application.put_env(
        :ex_arrow,
        :flight_sql_statement_native,
        ExArrow.FlightSQL.StmtNativeError
      )

      :ok
    end

    test "returns {:error, %Error{}} when server fails" do
      assert {:error, %Error{}} = Statement.close(fake_stmt())
    end
  end

  # ── Lifecycle: bind → execute → close ────────────────────────────────────────

  describe "lifecycle — bind, execute, close" do
    setup do
      Application.put_env(:ex_arrow, :flight_sql_statement_native, ExArrow.FlightSQL.StmtNativeOk)
      :ok
    end

    test "full lifecycle works with stubs" do
      stmt = fake_stmt()
      batch = %ExArrow.RecordBatch{resource: make_ref()}

      assert :ok = Statement.bind(stmt, batch)
      assert {:ok, %Stream{}} = Statement.execute(stmt)
      assert :ok = Statement.close(stmt)
    end
  end

  # ── Operations after close should fail ────────────────────────────────────────

  describe "operations after close" do
    test "bind after close returns protocol error" do
      batch = %ExArrow.RecordBatch{resource: make_ref()}

      assert {:error, %Error{code: :protocol_error}} =
               Statement.bind(fake_closed_stmt(), batch)
    end

    test "execute after close returns protocol error" do
      assert {:error, %Error{code: :protocol_error}} = Statement.execute(fake_closed_stmt())
    end

    test "execute_update after close returns protocol error" do
      assert {:error, %Error{code: :protocol_error}} =
               Statement.execute_update(fake_closed_stmt())
    end

    test "parameter_schema after close returns protocol error" do
      assert {:error, %Error{code: :protocol_error}} =
               Statement.parameter_schema(fake_closed_stmt())
    end
  end

  # ── Client.prepare/2 — via ClientImpl stub ────────────────────────────────────

  describe "Client.prepare/2 — success (stub native)" do
    setup do
      Application.put_env(
        :ex_arrow,
        :flight_sql_client_native,
        ExArrow.FlightSQL.TestNativePrepareOk
      )

      :ok
    end

    test "returns {:ok, %Statement{}}" do
      fake_client = %Client{resource: make_ref()}
      assert {:ok, %Statement{}} = Client.prepare(fake_client, "SELECT 1")
    end
  end

  describe "Client.prepare/2 — unimplemented (stub native)" do
    setup do
      Application.put_env(
        :ex_arrow,
        :flight_sql_client_native,
        ExArrow.FlightSQL.TestNativePrepareUnimplemented
      )

      :ok
    end

    test "returns {:error, %Error{code: :unimplemented}}" do
      fake_client = %Client{resource: make_ref()}

      assert {:error, %Error{code: :unimplemented, message: msg}} =
               Client.prepare(fake_client, "SELECT 1")

      assert msg =~ "prepared statements not supported"
    end
  end

  describe "Client.prepare/2 — invalid resource (real NIF)" do
    @tag no_nif: true
    test "raises ArgumentError when NIF is loaded with bad ref" do
      fake_client = %Client{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        Client.prepare(fake_client, "SELECT 1")
      end
    end
  end

  # ── Mox — Client.prepare/2 delegation ────────────────────────────────────────

  describe "with mock — Client.prepare/2" do
    setup context do
      Mox.set_mox_from_context(context)
      Application.put_env(:ex_arrow, :flight_sql_client_impl, ExArrow.FlightSQL.ClientMock)
      :ok
    end

    test "delegates to configured impl and returns Statement" do
      client = %Client{resource: make_ref()}
      fake_stmt_handle = %Statement{resource: make_ref()}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :prepare, fn ^client, "SELECT 1", [] ->
        {:ok, fake_stmt_handle}
      end)

      assert {:ok, ^fake_stmt_handle} = Client.prepare(client, "SELECT 1")
    end

    test "propagates error from impl" do
      client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :prepare, fn _, _, _ ->
        {:error, %Error{code: :unimplemented, message: "not supported"}}
      end)

      assert {:error, %Error{code: :unimplemented}} = Client.prepare(client, "SELECT 1")
    end
  end
end
