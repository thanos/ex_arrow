defmodule ExArrow.FlightIntegrationTest do
  @moduledoc """
  Integration test: real Flight echo server + client do_put / do_get roundtrip.

  Requires Flight NIFs (Milestone 3). Exclude when not implemented:
    mix test --exclude flight
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

  # ---- helpers -------------------------------------------------------

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
