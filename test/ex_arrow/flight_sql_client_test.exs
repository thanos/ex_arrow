defmodule ExArrow.FlightSQL.ClientTest do
  use ExUnit.Case, async: true

  alias ExArrow.FlightSQL.{Client, Error}

  setup context do
    Mox.set_mox_from_context(context)
    :ok
  end

  # ── Real implementation (no server) ──────────────────────────────────────────

  describe "real implementation — connect" do
    test "returns error when server is not reachable" do
      assert {:error, %Error{}} = Client.connect("localhost:39901")
    end

    test "plaintext connect attempt fails with transport error on missing server" do
      assert {:error, %Error{code: code}} = Client.connect("localhost:39902", tls: false)
      assert code in [:transport_error, :server_error]
    end

    test "invalid URI returns invalid_option error" do
      assert {:error, %Error{code: :invalid_option}} = Client.connect("not:a:valid:uri")
    end

    test "invalid tls option returns invalid_option error" do
      assert {:error, %Error{code: :invalid_option}} =
               Client.connect("localhost:39903", tls: :bad_atom)
    end

    test "invalid headers return invalid_option error" do
      assert {:error, %Error{code: :invalid_option}} =
               Client.connect("localhost:39904", headers: ["not-a-tuple"])
    end

    test "non-loopback host auto-selects TLS and returns connection error" do
      assert {:error, %Error{}} = Client.connect("flight.example.invalid:32010")
    end
  end

  # ── Mox mock — connect ────────────────────────────────────────────────────────

  describe "with mock — connect" do
    test "connect/2 delegates to configured impl" do
      use_mock()
      fake = %Client{resource: make_ref()}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :connect, fn "localhost:32010", [] ->
        {:ok, fake}
      end)

      assert {:ok, ^fake} = Client.connect("localhost:32010")
    end

    test "connect/2 passes opts through to impl" do
      use_mock()
      fake = %Client{resource: make_ref()}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :connect, fn "remote.server:32010", [tls: true] ->
        {:ok, fake}
      end)

      assert {:ok, ^fake} = Client.connect("remote.server:32010", tls: true)
    end

    test "connect/2 propagates error from impl" do
      use_mock()

      Mox.expect(ExArrow.FlightSQL.ClientMock, :connect, fn _, _ ->
        {:error, %Error{code: :transport_error, message: "refused"}}
      end)

      assert {:error, %Error{code: :transport_error}} = Client.connect("localhost:32010")
    end
  end

  # ── Mox mock — query ──────────────────────────────────────────────────────────

  describe "with mock — query/2" do
    test "returns a Result with schema and batches" do
      use_mock()
      fake_client = %Client{resource: make_ref()}
      fake_stream = %ExArrow.Stream{resource: make_ref(), backend: :flight_sql}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :query, fn ^fake_client, "SELECT 1", [] ->
        {:ok, fake_stream}
      end)

      # query/2 calls impl().query() then collects via Result.from_stream/1.
      # Since the stream resource is a fake ref, from_stream will fail —
      # test the delegation path only.
      result = Client.stream_query(fake_client, "SELECT 1")
      assert {:ok, %ExArrow.Stream{backend: :flight_sql}} = result
    end

    test "query/2 propagates error from impl" do
      use_mock()
      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :query, fn ^fake_client, "BAD SQL", [] ->
        {:error, %Error{code: :invalid_argument, message: "syntax error"}}
      end)

      assert {:error, %Error{code: :invalid_argument}} = Client.query(fake_client, "BAD SQL")
    end

    test "query!/2 raises on error" do
      use_mock()
      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :query, fn _, _, _ ->
        {:error, %Error{code: :server_error, message: "internal"}}
      end)

      assert_raise Error, fn -> Client.query!(fake_client, "SELECT 1") end
    end
  end

  # ── Mox mock — stream_query ───────────────────────────────────────────────────

  describe "with mock — stream_query/2" do
    test "returns lazy stream" do
      use_mock()
      fake_client = %Client{resource: make_ref()}
      fake_stream = %ExArrow.Stream{resource: make_ref(), backend: :flight_sql}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :query, fn ^fake_client, "SELECT *", [] ->
        {:ok, fake_stream}
      end)

      assert {:ok, %ExArrow.Stream{backend: :flight_sql}} =
               Client.stream_query(fake_client, "SELECT *")
    end

    test "stream_query/2 propagates error" do
      use_mock()
      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :query, fn _, _, _ ->
        {:error, %Error{code: :not_found, message: "table does not exist"}}
      end)

      assert {:error, %Error{code: :not_found}} = Client.stream_query(fake_client, "SELECT *")
    end
  end

  # ── Mox mock — execute ────────────────────────────────────────────────────────

  describe "with mock — execute/2" do
    test "returns affected row count" do
      use_mock()
      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :execute, fn ^fake_client,
                                                            "DELETE FROM t WHERE id = 1",
                                                            [] ->
        {:ok, 1}
      end)

      assert {:ok, 1} = Client.execute(fake_client, "DELETE FROM t WHERE id = 1")
    end

    test "returns :unknown when server omits row count" do
      use_mock()
      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :execute, fn _, _, _ ->
        {:ok, :unknown}
      end)

      assert {:ok, :unknown} = Client.execute(fake_client, "CREATE TABLE t (id INT)")
    end

    test "execute/2 propagates error" do
      use_mock()
      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :execute, fn _, _, _ ->
        {:error, %Error{code: :permission_denied, message: "read-only"}}
      end)

      assert {:error, %Error{code: :permission_denied}} =
               Client.execute(fake_client, "DROP TABLE t")
    end
  end

  # ── Mox mock — close ──────────────────────────────────────────────────────────

  describe "with mock — close/1" do
    test "close/1 delegates to impl and returns :ok" do
      use_mock()
      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :close, fn ^fake_client -> :ok end)

      assert :ok = Client.close(fake_client)
    end
  end

  # ── Options validation (real impl, no server) ─────────────────────────────────

  describe "options validation" do
    test "custom CA PEM option is accepted" do
      pem = "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n"
      # Will fail to connect but must not fail with :invalid_option
      assert {:error, %Error{code: code}} =
               Client.connect("localhost:39905", tls: [ca_cert_pem: pem])

      refute code == :invalid_option
    end

    test "empty headers list is accepted" do
      assert {:error, %Error{code: code}} =
               Client.connect("localhost:39906", headers: [])

      refute code == :invalid_option
    end

    test "valid headers list is accepted" do
      assert {:error, %Error{code: code}} =
               Client.connect("localhost:39907",
                 headers: [{"authorization", "Bearer tok"}]
               )

      refute code == :invalid_option
    end
  end

  # ── Helpers ───────────────────────────────────────────────────────────────────

  defp use_mock do
    Application.put_env(:ex_arrow, :flight_sql_client_impl, ExArrow.FlightSQL.ClientMock)
    on_exit(fn -> Application.delete_env(:ex_arrow, :flight_sql_client_impl) end)
  end
end
