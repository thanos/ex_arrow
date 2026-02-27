Code.require_file("bench_helper.exs", __DIR__)

# ---------------------------------------------------------------------------
# Flight Benchmark
#
# Measures Arrow Flight round-trip latency and throughput using an in-process
# server (no external process needed).
#
# Scenarios:
#
#   1. do_put (10 batches)    – Upload record batches via the client.
#
#   2. do_get (10 batches)    – Download the previously uploaded stream by
#                               ticket and collect all batches.
#
#   3. roundtrip (10 batches) – Full put → get cycle in one benchmark function.
#                               This shows the cost of a real IPC Flight
#                               exchange over the loopback interface.
#
#   4. get_schema             – Fetch schema metadata only (no data transfer).
#
# Key insight: ExArrow Flight transfers Arrow buffers end-to-end without
# converting to/from any BEAM binary term representation. The BEAM process
# is never on the hot path for column data.
# ---------------------------------------------------------------------------

IO.puts("\n== Flight Benchmark ==\n")

{schema, batches} = Bench.DataGen.schema_and_batches(10)

# Start a local Flight server on a free port.
{:ok, server} = ExArrow.Flight.Server.start_link(0)
{:ok, port} = ExArrow.Flight.Server.port(server)

# A reusable client connection for measuring individual operations.
{:ok, client} = ExArrow.Flight.Client.connect("localhost", port, tls: false)

# Pre-upload some data so do_get has something to retrieve.
ticket = "bench_default"
:ok = ExArrow.Flight.Client.do_put(client, schema, batches)

output_dir = Bench.DataGen.output_dir()

Benchee.run(
  %{
    "do_put (10 batches)" => fn ->
      :ok = ExArrow.Flight.Client.do_put(client, schema, batches)
    end,
    "do_get + collect (10 batches)" => fn ->
      {:ok, stream} = ExArrow.Flight.Client.do_get(client, ticket)
      Bench.DataGen.collect_stream(stream)
    end,
    "do_get stream_handle only (10 batches)" => fn ->
      {:ok, _stream} = ExArrow.Flight.Client.do_get(client, ticket)
    end,
    "roundtrip put→get (10 batches)" => fn ->
      :ok = ExArrow.Flight.Client.do_put(client, schema, batches)
      {:ok, stream} = ExArrow.Flight.Client.do_get(client, ticket)
      Bench.DataGen.collect_stream(stream)
    end,
    "list_flights" => fn ->
      {:ok, _flights} = ExArrow.Flight.Client.list_flights(client)
    end
  },
  time: 5,
  memory_time: 2,
  warmup: 2,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML,
     file: Path.join(output_dir, "flight.html"), auto_open: false},
    {Benchee.Formatters.JSON,
     file: Path.join(output_dir, "flight.json")}
  ]
)

ExArrow.Flight.Server.stop(server)
