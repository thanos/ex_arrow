defmodule ExArrow.FlightSQL.ClientImplTest do
  use ExUnit.Case, async: false

  alias ExArrow.FlightSQL.{Client, ClientImpl, Error}

  setup do
    prev = Application.get_env(:ex_arrow, :flight_sql_client_native)

    on_exit(fn ->
      case prev do
        nil -> Application.delete_env(:ex_arrow, :flight_sql_client_native)
        val -> Application.put_env(:ex_arrow, :flight_sql_client_native, val)
      end
    end)

    :ok
  end

  # ── connect/2 — options validation ───────────────────────────────────────────

  describe "connect/2 — invalid options (no NIF called)" do
    test "invalid URI returns {:error, %Error{code: :invalid_option}}" do
      assert {:error, %Error{code: :invalid_option}} = ClientImpl.connect("not:valid:uri", [])
    end

    test "invalid tls option returns invalid_option" do
      assert {:error, %Error{code: :invalid_option}} =
               ClientImpl.connect("localhost:32010", tls: :bad)
    end

    test "invalid headers return invalid_option" do
      assert {:error, %Error{code: :invalid_option}} =
               ClientImpl.connect("localhost:32010", headers: ["not-a-tuple"])
    end
  end

  # ── connect/2 — no server (real NIF) ─────────────────────────────────────────

  describe "connect/2 — unreachable server" do
    test "returns {:error, %Error{}} for unreachable loopback" do
      assert {:error, %Error{code: code}} = ClientImpl.connect("localhost:39920", [])
      assert code in [:transport_error, :server_error]
    end

    test "non-loopback host triggers TLS and returns connection error" do
      assert {:error, %Error{}} = ClientImpl.connect("flight.example.invalid:32010", [])
    end
  end

  # ── connect/2 — wrap_nif_error/1 all three clauses ───────────────────────────

  describe "connect/2 — wrap_nif_error clause: binary" do
    test "binary NIF error becomes {:error, %Error{code: :transport_error}}" do
      Application.put_env(
        :ex_arrow,
        :flight_sql_client_native,
        ExArrow.FlightSQL.TestNativeBinaryError
      )

      assert {:error, %Error{code: :transport_error, message: "connection refused"}} =
               ClientImpl.connect("localhost:32010", [])
    end
  end

  describe "connect/2 — wrap_nif_error clause: 3-tuple" do
    test "3-tuple NIF error becomes structured Error with grpc_status" do
      Application.put_env(
        :ex_arrow,
        :flight_sql_client_native,
        ExArrow.FlightSQL.TestNativeTupleError
      )

      assert {:error, %Error{code: :unauthenticated, grpc_status: 16, message: "missing token"}} =
               ClientImpl.connect("localhost:32010", [])
    end
  end

  describe "connect/2 — wrap_nif_error clause: fallback" do
    test "unexpected NIF error becomes {:error, %Error{code: :transport_error}} with inspect" do
      Application.put_env(
        :ex_arrow,
        :flight_sql_client_native,
        ExArrow.FlightSQL.TestNativeFallbackError
      )

      assert {:error, %Error{code: :transport_error, message: msg}} =
               ClientImpl.connect("localhost:32010", [])

      # The fallback clause calls inspect/1 on the unrecognised term.
      assert msg =~ "unexpected_atom"
    end
  end

  # ── close/1 ──────────────────────────────────────────────────────────────────

  describe "close/1" do
    test "returns :ok for any client — no-op in v0.5.0" do
      fake_client = %Client{resource: make_ref()}
      assert :ok = ClientImpl.close(fake_client)
    end
  end

  # ── query/3 and execute/3 with invalid ref ────────────────────────────────────

  describe "query/3 with invalid resource" do
    test "raises ArgumentError when client ref is a plain Erlang ref" do
      fake_client = %Client{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        ClientImpl.query(fake_client, "SELECT 1", [])
      end
    end
  end

  describe "execute/3 with invalid resource" do
    test "raises ArgumentError when client ref is a plain Erlang ref" do
      fake_client = %Client{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        ClientImpl.execute(fake_client, "DELETE FROM t", [])
      end
    end
  end

  describe "get_tables/2 with invalid resource" do
    test "raises ArgumentError when client ref is a plain Erlang ref" do
      fake_client = %Client{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        ClientImpl.get_tables(fake_client, [])
      end
    end
  end

  describe "get_db_schemas/2 with invalid resource" do
    test "raises ArgumentError when client ref is a plain Erlang ref" do
      fake_client = %Client{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        ClientImpl.get_db_schemas(fake_client, [])
      end
    end
  end

  describe "get_sql_info/2 with invalid resource" do
    test "raises ArgumentError when client ref is a plain Erlang ref" do
      fake_client = %Client{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        ClientImpl.get_sql_info(fake_client, [])
      end
    end
  end
end
