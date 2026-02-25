defmodule ExArrow.Flight.ClientImplTest do
  use ExUnit.Case, async: false

  alias ExArrow.Flight.{ActionType, Client, ClientImpl, FlightInfo}
  alias ExArrow.{Schema, Stream}

  # ── helpers ─────────────────────────────────────────────────────────────────

  defp fixture do
    {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
    {:ok, schema} = Stream.schema(stream)
    batch = Stream.next(stream)
    {schema, batch}
  end

  defp collect(stream, acc \\ []) do
    case Stream.next(stream) do
      nil -> Enum.reverse(acc)
      {:error, msg} -> raise "stream error: #{msg}"
      batch -> collect(stream, [batch | acc])
    end
  end

  defp fake_client, do: %Client{resource: make_ref()}

  # ── unit tests (no server) ───────────────────────────────────────────────────

  describe "connect/3 (no server)" do
    test "returns error tuple for non-existent loopback server" do
      assert {:error, _msg} = ClientImpl.connect("localhost", 39_281, [])
    end

    test "tls: true attempts TLS and returns a connection error" do
      # tls: true is now supported — it no longer returns :tls_not_supported.
      # Connecting to a non-existent server still fails with a transport error.
      assert {:error, _msg} = ClientImpl.connect("localhost", 39_282, tls: true)
    end

    test "tls: false uses plaintext for any host" do
      assert {:error, _msg} = ClientImpl.connect("localhost", 39_283, tls: false)
    end

    test "tls: [ca_cert_pem: pem] uses custom CA" do
      fake_pem = "-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----\n"
      # Should attempt connection (and fail — no server), not return :tls_not_supported.
      assert {:error, _msg} =
               ClientImpl.connect("localhost", 39_284, tls: [ca_cert_pem: fake_pem])
    end

    test "tls: [invalid_opt: true] returns {:error, {:invalid_tls_opt, _}}" do
      assert {:error, {:invalid_tls_opt, msg}} =
               ClientImpl.connect("localhost", 39_285, tls: [no_cert: true])

      assert is_binary(msg)
    end

    test "tls: [ca_cert_pem: non_binary] returns {:error, {:invalid_tls_opt, _}}" do
      assert {:error, {:invalid_tls_opt, msg}} =
               ClientImpl.connect("localhost", 39_285, tls: [ca_cert_pem: nil])

      assert is_binary(msg)
    end

    test "loopback ::1 with no tls opt uses plaintext (connection fails)" do
      assert {:error, _msg} = ClientImpl.connect("::1", 39_287, [])
    end

    test "loopback ip6-localhost with no tls opt uses plaintext" do
      assert {:error, _msg} = ClientImpl.connect("ip6-localhost", 39_288, [])
    end

    test "non-loopback host with no tls opt auto-selects TLS (system_certs)" do
      # Connection fails (no server), but the error is a TLS/transport error,
      # not :tls_not_supported.
      assert {:error, _msg} = ClientImpl.connect("flight.example.invalid", 9999, [])
    end

    test "connect_timeout_ms: 1 times out quickly against a non-listening port" do
      assert {:error, _msg} =
               ClientImpl.connect("localhost", 39_286, connect_timeout_ms: 1)
    end
  end

  describe "do_get/2 (invalid resource)" do
    test "raises ArgumentError" do
      assert_raise ArgumentError, fn ->
        ClientImpl.do_get(fake_client(), "ticket")
      end
    end
  end

  describe "do_put/3 (invalid resource)" do
    test "raises ArgumentError" do
      schema = %Schema{resource: make_ref()}
      batch = %ExArrow.RecordBatch{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        ClientImpl.do_put(fake_client(), schema, [batch])
      end
    end
  end

  describe "list_flights/2 (invalid resource)" do
    test "raises ArgumentError" do
      assert_raise ArgumentError, fn ->
        ClientImpl.list_flights(fake_client(), <<>>)
      end
    end
  end

  describe "get_flight_info/2 (invalid resource)" do
    test "raises ArgumentError" do
      assert_raise ArgumentError, fn ->
        ClientImpl.get_flight_info(fake_client(), {:cmd, "echo"})
      end
    end
  end

  describe "get_schema/2 (invalid resource)" do
    test "raises ArgumentError" do
      assert_raise ArgumentError, fn ->
        ClientImpl.get_schema(fake_client(), {:cmd, "echo"})
      end
    end
  end

  describe "list_actions/1 (invalid resource)" do
    test "raises ArgumentError" do
      assert_raise ArgumentError, fn ->
        ClientImpl.list_actions(fake_client())
      end
    end
  end

  describe "do_action/3 (invalid resource)" do
    test "raises ArgumentError" do
      assert_raise ArgumentError, fn ->
        ClientImpl.do_action(fake_client(), "ping", <<>>)
      end
    end
  end

  # ── live server tests ────────────────────────────────────────────────────────

  @tag :flight
  test "connect/3 to running echo server returns %Client{}" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)

    assert {:ok, %Client{} = client} = ClientImpl.connect("localhost", port, [])
    assert is_reference(client.resource)

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "connect/3 with tls: false succeeds against live plaintext server" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)

    assert {:ok, %Client{}} = ClientImpl.connect("localhost", port, tls: false)

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "connect/3 with connect_timeout_ms succeeds against a live server" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)

    assert {:ok, %Client{}} =
             ClientImpl.connect("localhost", port, connect_timeout_ms: 5_000)

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "do_put/3 and do_get/2 roundtrip via impl" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)
    {:ok, client} = ClientImpl.connect("localhost", port, [])
    {schema, batch} = fixture()

    assert :ok = ClientImpl.do_put(client, schema, [batch])

    assert {:ok, get_stream} = ClientImpl.do_get(client, "echo")
    assert {:ok, %Schema{}} = Stream.schema(get_stream)

    batches = collect(get_stream)
    assert length(batches) == 1
    assert ExArrow.RecordBatch.num_rows(hd(batches)) == 2

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "do_get/2 before do_put returns error" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)
    {:ok, client} = ClientImpl.connect("localhost", port, [])

    assert {:error, msg} = ClientImpl.do_get(client, "echo")
    assert msg =~ "no data"

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "list_flights/2 returns empty list when no data is stored" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)
    {:ok, client} = ClientImpl.connect("localhost", port, [])

    assert {:ok, []} = ClientImpl.list_flights(client, <<>>)

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "list_flights/2 returns one FlightInfo after do_put" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)
    {:ok, client} = ClientImpl.connect("localhost", port, [])
    {schema, batch} = fixture()

    assert :ok = ClientImpl.do_put(client, schema, [batch])

    assert {:ok, [%FlightInfo{} = info]} = ClientImpl.list_flights(client, <<>>)
    assert info.total_records == 2
    assert info.descriptor == {:cmd, "echo"}
    assert is_binary(info.schema_bytes)
    assert [%{ticket: "echo"}] = info.endpoints

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "get_flight_info/2 returns FlightInfo for echo descriptor after do_put" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)
    {:ok, client} = ClientImpl.connect("localhost", port, [])
    {schema, batch} = fixture()

    assert :ok = ClientImpl.do_put(client, schema, [batch])

    assert {:ok, %FlightInfo{} = info} =
             ClientImpl.get_flight_info(client, {:cmd, "echo"})

    assert info.total_records == 2
    assert info.descriptor == {:cmd, "echo"}

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "get_flight_info/2 returns error for unknown descriptor" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)
    {:ok, client} = ClientImpl.connect("localhost", port, [])

    assert {:error, _} = ClientImpl.get_flight_info(client, {:cmd, "unknown"})

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "get_schema/2 returns a Schema with correct field names after do_put" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)
    {:ok, client} = ClientImpl.connect("localhost", port, [])
    {schema, batch} = fixture()

    assert :ok = ClientImpl.do_put(client, schema, [batch])

    assert {:ok, %Schema{} = got} = ClientImpl.get_schema(client, {:cmd, "echo"})
    fields = ExArrow.Schema.fields(got)
    assert length(fields) == 2
    names = Enum.map(fields, & &1.name)
    assert "id" in names
    assert "name" in names

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "get_schema/2 returns error for unknown descriptor" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)
    {:ok, client} = ClientImpl.connect("localhost", port, [])

    assert {:error, _} = ClientImpl.get_schema(client, {:cmd, "no_such"})

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "list_actions/1 returns ActionType structs for clear and ping" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)
    {:ok, client} = ClientImpl.connect("localhost", port, [])

    assert {:ok, actions} = ClientImpl.list_actions(client)
    assert Enum.all?(actions, &match?(%ActionType{}, &1))

    types = Enum.map(actions, & &1.type)
    assert "clear" in types
    assert "ping" in types
    assert Enum.all?(actions, fn a -> is_binary(a.description) end)

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "do_action/3 ping returns [pong]" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)
    {:ok, client} = ClientImpl.connect("localhost", port, [])

    assert {:ok, ["pong"]} = ClientImpl.do_action(client, "ping", <<>>)

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "do_action/3 clear removes stored data so subsequent do_get fails" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)
    {:ok, client} = ClientImpl.connect("localhost", port, [])
    {schema, batch} = fixture()

    assert :ok = ClientImpl.do_put(client, schema, [batch])
    assert {:ok, []} = ClientImpl.do_action(client, "clear", <<>>)

    assert {:error, msg} = ClientImpl.do_get(client, "echo")
    assert msg =~ "no data"

    ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "do_action/3 returns error for unknown action" do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)
    {:ok, client} = ClientImpl.connect("localhost", port, [])

    assert {:error, _} = ClientImpl.do_action(client, "no_such_action", <<>>)

    ExArrow.Flight.Server.stop(server)
  end
end
