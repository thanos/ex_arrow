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

  # ── Statement struct ─────────────────────────────────────────────────────────

  describe "struct" do
    test "holds a resource ref" do
      ref = make_ref()
      stmt = %Statement{resource: ref}
      assert stmt.resource == ref
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
    test "raises ArgumentError when resource is a plain Erlang ref" do
      assert_raise ArgumentError, fn -> Statement.execute(fake_stmt()) end
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
    test "raises ArgumentError when resource is a plain Erlang ref" do
      assert_raise ArgumentError, fn -> Statement.execute_update(fake_stmt()) end
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
    test "raises ArgumentError when client ref is a plain Erlang ref" do
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
