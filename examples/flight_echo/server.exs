# Flight echo server example (stub until Milestone 3).
# Usage: mix run examples/flight_echo/server.exs
#
# When implemented: start Flight server, accept do_put, respond to do_get by ticket.

IO.puts("ExArrow Flight echo server (stub)")
IO.puts("Native NIF version: #{ExArrow.native_version()}")

case ExArrow.Flight.Server.start_link(9999) do
  {:ok, _pid} -> IO.puts("Server started")
  {:error, reason} -> IO.puts("Start (expected stub): #{inspect(reason)}")
end
