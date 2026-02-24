defmodule ExArrow.FlightIntegrationTest do
  @moduledoc """
  Integration test: real Flight echo server + client do_put / do_get roundtrip.

  Requires Flight NIFs (Milestone 3). Exclude when not implemented:
    mix test --exclude flight
  """
  use ExUnit.Case, async: false

  @tag :flight
  test "do_put then do_get returns same schema and row count" do
    # Start echo server on any port
    assert {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    assert {:ok, port} = ExArrow.Flight.Server.port(server)

    # Give the server a moment to accept connections
    Process.sleep(300)

    # Connect client
    assert {:ok, client} = ExArrow.Flight.Client.connect("localhost", port, [])

    # Fixture: schema + one batch (id int64, name utf8; 2 rows)
    {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
    {:ok, schema} = ExArrow.Stream.schema(stream)
    batch = ExArrow.Stream.next(stream)
    assert batch != nil
    assert ExArrow.Stream.next(stream) == nil

    # do_put
    assert :ok = ExArrow.Flight.Client.do_put(client, schema, [batch])

    # do_get with ticket "echo"
    assert {:ok, get_stream} = ExArrow.Flight.Client.do_get(client, "echo")

    # Same schema
    assert {:ok, get_schema} = ExArrow.Stream.schema(get_stream)
    put_fields = ExArrow.Schema.fields(schema)
    get_fields = ExArrow.Schema.fields(get_schema)
    assert length(get_fields) == length(put_fields)

    assert Enum.all?(put_fields, fn pf ->
             Enum.any?(get_fields, fn gf -> gf.name == pf.name and gf.type == pf.type end)
           end)

    # One batch, 2 rows
    batches = collect_batches(get_stream)
    assert length(batches) == 1
    assert ExArrow.RecordBatch.num_rows(hd(batches)) == 2

    # Cleanup
    assert :ok = ExArrow.Flight.Server.stop(server)
  end

  defp collect_batches(stream, acc \\ []) do
    case ExArrow.Stream.next(stream) do
      nil -> Enum.reverse(acc)
      {:error, _} = err -> err
      batch -> collect_batches(stream, [batch | acc])
    end
  end
end
