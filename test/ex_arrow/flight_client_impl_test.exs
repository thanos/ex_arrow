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
    test "returns error tuple for non-existent server" do
      assert {:error, _msg} = ClientImpl.connect("localhost", 39_281, [])
    end

    test "tls: true is rejected with :tls_not_supported" do
      assert {:error, :tls_not_supported} = ClientImpl.connect("host", 9999, tls: true)
    end

    test "connect_timeout_ms: 1 times out quickly against a non-listening port" do
      assert {:error, _msg} =
               ClientImpl.connect("localhost", 39_282, connect_timeout_ms: 1)
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
