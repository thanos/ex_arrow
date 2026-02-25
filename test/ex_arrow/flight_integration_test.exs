defmodule ExArrow.FlightIntegrationTest do
  @moduledoc """
  Integration tests: real Flight echo server + client.

  Requires Flight NIFs (Milestone 3/4). Excluded by default:
    mix test --include flight
  """
  use ExUnit.Case, async: false

  # Start a fresh echo server + connected client for every test.
  # The TCP readiness probe inside flight_server_start ensures the server
  # is accepting connections before setup returns — no sleep needed.
  setup do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)
    {:ok, client} = ExArrow.Flight.Client.connect("localhost", port, [])

    on_exit(fn -> ExArrow.Flight.Server.stop(server) end)
    {:ok, client: client}
  end

  # ── Milestone 3: do_put / do_get roundtrip ─────────────────────────────────

  @tag :flight
  test "do_put then do_get returns same schema and row count", %{client: client} do
    {schema, [batch]} = fixture_schema_and_batches()

    assert :ok = ExArrow.Flight.Client.do_put(client, schema, [batch])

    assert {:ok, get_stream} = ExArrow.Flight.Client.do_get(client, "echo")
    assert {:ok, get_schema} = ExArrow.Stream.schema(get_stream)

    put_fields = ExArrow.Schema.fields(schema)
    get_fields = ExArrow.Schema.fields(get_schema)
    assert length(get_fields) == length(put_fields)

    assert Enum.all?(put_fields, fn pf ->
             Enum.any?(get_fields, fn gf -> gf.name == pf.name and gf.type == pf.type end)
           end)

    batches = collect_batches(get_stream)
    assert length(batches) == 1
    assert ExArrow.RecordBatch.num_rows(hd(batches)) == 2
  end

  @tag :flight
  test "do_get before any do_put returns not-found error", %{client: client} do
    assert {:error, msg} = ExArrow.Flight.Client.do_get(client, "echo")
    assert msg =~ "no data"
  end

  @tag :flight
  test "do_get with unknown ticket returns not-found error", %{client: client} do
    {schema, [batch]} = fixture_schema_and_batches()
    assert :ok = ExArrow.Flight.Client.do_put(client, schema, [batch])

    assert {:error, msg} = ExArrow.Flight.Client.do_get(client, "bad_ticket")
    assert msg =~ "unknown ticket"
  end

  @tag :flight
  test "do_put with empty batch list is rejected by the server", %{client: client} do
    {schema, _} = fixture_schema_and_batches()

    assert {:error, msg} = ExArrow.Flight.Client.do_put(client, schema, [])
    assert msg =~ "no record batches"
  end

  # ── Milestone 4: list_flights ───────────────────────────────────────────────

  @tag :flight
  test "list_flights with no data returns empty list", %{client: client} do
    assert {:ok, []} = ExArrow.Flight.Client.list_flights(client, <<>>)
  end

  @tag :flight
  test "list_flights after do_put returns one FlightInfo", %{client: client} do
    {schema, [batch]} = fixture_schema_and_batches()
    assert :ok = ExArrow.Flight.Client.do_put(client, schema, [batch])

    assert {:ok, [%ExArrow.Flight.FlightInfo{} = info]} =
             ExArrow.Flight.Client.list_flights(client, <<>>)

    assert info.total_records == 2
    assert length(info.endpoints) == 1
    assert hd(info.endpoints).ticket == "echo"
    assert info.descriptor == {:cmd, "echo"}
    assert is_binary(info.schema_bytes)
  end

  # ── Milestone 4: get_flight_info ────────────────────────────────────────────

  @tag :flight
  test "get_flight_info with echo cmd descriptor returns FlightInfo", %{client: client} do
    {schema, [batch]} = fixture_schema_and_batches()
    assert :ok = ExArrow.Flight.Client.do_put(client, schema, [batch])

    assert {:ok, %ExArrow.Flight.FlightInfo{} = info} =
             ExArrow.Flight.Client.get_flight_info(client, {:cmd, "echo"})

    assert info.total_records == 2
    assert info.descriptor == {:cmd, "echo"}
  end

  @tag :flight
  test "get_flight_info with unknown descriptor returns error", %{client: client} do
    assert {:error, _msg} = ExArrow.Flight.Client.get_flight_info(client, {:cmd, "unknown"})
  end

  # ── Milestone 4: get_schema ─────────────────────────────────────────────────

  @tag :flight
  test "get_schema returns a schema handle with correct fields", %{client: client} do
    {schema, [batch]} = fixture_schema_and_batches()
    assert :ok = ExArrow.Flight.Client.do_put(client, schema, [batch])

    assert {:ok, %ExArrow.Schema{} = got_schema} =
             ExArrow.Flight.Client.get_schema(client, {:cmd, "echo"})

    fields = ExArrow.Schema.fields(got_schema)
    assert length(fields) == 2
    assert Enum.any?(fields, &(&1.name == "id"))
    assert Enum.any?(fields, &(&1.name == "name"))
  end

  @tag :flight
  test "get_schema with unknown descriptor returns error", %{client: client} do
    assert {:error, _} = ExArrow.Flight.Client.get_schema(client, {:cmd, "nope"})
  end

  # ── Milestone 4: list_actions ───────────────────────────────────────────────

  @tag :flight
  test "list_actions returns at least clear and ping action types", %{client: client} do
    assert {:ok, actions} = ExArrow.Flight.Client.list_actions(client)

    assert Enum.all?(actions, &match?(%ExArrow.Flight.ActionType{}, &1))
    types = Enum.map(actions, & &1.type)
    assert "clear" in types
    assert "ping" in types
  end

  # ── Milestone 4: do_action ──────────────────────────────────────────────────

  @tag :flight
  test "do_action ping returns pong", %{client: client} do
    assert {:ok, results} = ExArrow.Flight.Client.do_action(client, "ping", <<>>)
    assert results == ["pong"]
  end

  @tag :flight
  test "do_action clear removes stored data", %{client: client} do
    {schema, [batch]} = fixture_schema_and_batches()
    assert :ok = ExArrow.Flight.Client.do_put(client, schema, [batch])

    assert {:ok, []} = ExArrow.Flight.Client.do_action(client, "clear", <<>>)

    assert {:error, msg} = ExArrow.Flight.Client.do_get(client, "echo")
    assert msg =~ "no data"
  end

  @tag :flight
  test "do_action with unknown type returns error", %{client: client} do
    assert {:error, _} = ExArrow.Flight.Client.do_action(client, "no_such_action", <<>>)
  end

  # ── Milestone 4: connect timeout ────────────────────────────────────────────

  @tag :flight
  test "connect with connect_timeout_ms option succeeds", %{} do
    {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    {:ok, port} = ExArrow.Flight.Server.port(server)

    assert {:ok, %ExArrow.Flight.Client{}} =
             ExArrow.Flight.Client.connect("localhost", port, connect_timeout_ms: 5_000)

    ExArrow.Flight.Server.stop(server)
  end

  # ── helpers ─────────────────────────────────────────────────────────────────

  defp fixture_schema_and_batches do
    {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
    {:ok, schema} = ExArrow.Stream.schema(stream)
    batches = collect_batches(stream)
    {schema, batches}
  end

  defp collect_batches(stream, acc \\ []) do
    case ExArrow.Stream.next(stream) do
      nil -> Enum.reverse(acc)
      {:error, _} = err -> err
      batch -> collect_batches(stream, [batch | acc])
    end
  end
end
