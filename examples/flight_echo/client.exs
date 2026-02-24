# Flight echo client example (stub until Milestone 3).
# Usage: mix run examples/flight_echo/client.exs
#
# When implemented: connect, do_put batches, do_get by ticket.

IO.puts("ExArrow Flight echo client (stub)")
IO.puts("Native NIF version: #{ExArrow.native_version()}")

case ExArrow.Flight.Client.connect("localhost", 9999) do
  {:ok, _client} -> IO.puts("Connected")
  {:error, reason} -> IO.puts("Connect (expected stub): #{inspect(reason)}")
end
