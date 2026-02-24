defmodule ExArrow.Flight.ClientImplTest do
  use ExUnit.Case, async: false

  describe "unit behaviour (no server)" do
    test "connect/3 to non-existent server returns error tuple" do
      assert {:error, _msg} = ExArrow.Flight.ClientImpl.connect("localhost", 39_281, [])
    end

    test "do_get/2 with invalid client resource raises ArgumentError" do
      client = %ExArrow.Flight.Client{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        ExArrow.Flight.ClientImpl.do_get(client, "ticket")
      end
    end

    test "do_put/3 with invalid client/schema resources raises ArgumentError" do
      client = %ExArrow.Flight.Client{resource: make_ref()}
      schema = %ExArrow.Schema{resource: make_ref()}
      batch = %ExArrow.RecordBatch{resource: make_ref()}

      assert_raise ArgumentError, fn ->
        ExArrow.Flight.ClientImpl.do_put(client, schema, [batch])
      end
    end
  end

  @tag :flight
  test "connect/3 to running Flight echo server returns client" do
    assert {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    assert {:ok, port} = ExArrow.Flight.Server.port(server)

    assert {:ok, %ExArrow.Flight.Client{} = client} =
             ExArrow.Flight.ClientImpl.connect("localhost", port, [])

    assert is_reference(client.resource)

    assert :ok = ExArrow.Flight.Server.stop(server)
  end

  @tag :flight
  test "do_put/3 and do_get/2 roundtrip via impl" do
    assert {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
    assert {:ok, port} = ExArrow.Flight.Server.port(server)
    assert {:ok, client} = ExArrow.Flight.ClientImpl.connect("localhost", port, [])

    # Fixture: schema + one batch (id int64, name utf8; 2 rows)
    {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
    {:ok, schema} = ExArrow.Stream.schema(stream)
    batch = ExArrow.Stream.next(stream)
    assert batch != nil
    assert ExArrow.Stream.next(stream) == nil

    # do_put via impl
    assert :ok = ExArrow.Flight.ClientImpl.do_put(client, schema, [batch])

    # do_get via impl with ticket "echo"
    assert {:ok, get_stream} = ExArrow.Flight.ClientImpl.do_get(client, "echo")

    # Same schema field count
    assert {:ok, get_schema} = ExArrow.Stream.schema(get_stream)
    fields = ExArrow.Schema.fields(get_schema)
    assert length(fields) == 2

    # One batch, 2 rows
    batches = collect_batches(get_stream, [])
    assert length(batches) == 1
    assert ExArrow.RecordBatch.num_rows(hd(batches)) == 2

    assert :ok = ExArrow.Flight.Server.stop(server)
  end

  defp collect_batches(stream, acc) do
    case ExArrow.Stream.next(stream) do
      nil -> Enum.reverse(acc)
      {:error, msg} -> raise "stream error: #{msg}"
      batch -> collect_batches(stream, [batch | acc])
    end
  end
end
