# Flight echo server example.
# Usage: mix run examples/flight_echo/server.exs
#
# Starts echo server on port 9999. Run client.exs in another terminal to do put/get.

IO.puts("ExArrow Flight echo server")
IO.puts("Native NIF version: #{ExArrow.native_version()}")

case ExArrow.Flight.Server.start_link(9999) do
  {:ok, server} ->
    {:ok, port} = ExArrow.Flight.Server.port(server)

    IO.puts(
      "Server listening on port #{port}. Run client: mix run examples/flight_echo/client.exs"
    )

    # Keep running until Ctrl+C (in production you'd link to the app or wait on a signal)
    Process.sleep(:infinity)

  {:error, reason} ->
    IO.puts("Start failed: #{inspect(reason)}")
end
