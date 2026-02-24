# Flight echo client example.
# Usage: mix run examples/flight_echo/client.exs
#
# Start the server first: mix run examples/flight_echo/server.exs
# Then run this to connect, do_put a fixture, do_get by ticket "echo".

IO.puts("ExArrow Flight echo client")
IO.puts("Native NIF version: #{ExArrow.native_version()}")

port = 9999

case ExArrow.Flight.Client.connect("localhost", port, []) do
  {:ok, client} ->
    IO.puts("Connected to localhost:#{port}")

    {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
    {:ok, schema} = ExArrow.Stream.schema(stream)
    batch = ExArrow.Stream.next(stream)

    case ExArrow.Flight.Client.do_put(client, schema, [batch]) do
      :ok ->
        IO.puts("do_put OK")

        case ExArrow.Flight.Client.do_get(client, "echo") do
          {:ok, get_stream} ->
            {:ok, _schema} = ExArrow.Stream.schema(get_stream)

            count = fn me, s, acc ->
              case ExArrow.Stream.next(s) do
                nil -> acc
                _ -> me.(me, s, acc + 1)
              end
            end

            n = count.(count, get_stream, 0)
            IO.puts("do_get OK: #{n} batch(es)")

          {:error, e} ->
            IO.puts("do_get error: #{inspect(e)}")
        end

      {:error, e} ->
        IO.puts("do_put error: #{inspect(e)}")
    end

  {:error, reason} ->
    IO.puts("Connect failed (is the server running?): #{inspect(reason)}")
end
