# Flight echo server example.
# Usage: mix run examples/flight_echo/server.exs
#
# Starts echo server on port 9999. Press Ctrl+C once to trigger a graceful
# shutdown (SIGINT → :shutdown message → server stop → clean exit).

IO.puts("ExArrow Flight echo server")
IO.puts("Native NIF version: #{ExArrow.native_version()}")

case ExArrow.Flight.Server.start_link(9999) do
  {:ok, server} ->
    {:ok, port} = ExArrow.Flight.Server.port(server)

    IO.puts(
      "Server listening on port #{port}. Run client: mix run examples/flight_echo/client.exs"
    )

    # Trap exits so that OS signals (SIGINT / SIGTERM) delivered by the Mix
    # runtime arrive as {:EXIT, pid, reason} or :shutdown messages rather than
    # killing the process immediately.
    Process.flag(:trap_exit, true)

    receive do
      # Mix sends :shutdown when the user presses Ctrl+C or the script ends.
      :shutdown ->
        IO.puts("\nShutting down…")

      # Linked process (e.g. the server NIF resource) exited.
      {:EXIT, _pid, reason} ->
        IO.puts("\nLinked process exited: #{inspect(reason)}")
    end

    :ok = ExArrow.Flight.Server.stop(server)
    IO.puts("Server stopped.")

  {:error, reason} ->
    IO.puts("Start failed: #{inspect(reason)}")
end
