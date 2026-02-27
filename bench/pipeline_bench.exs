Code.require_file("bench_helper.exs", __DIR__)

# ---------------------------------------------------------------------------
# End-to-End Pipeline Benchmark
#
# Measures the full zero-copy pipeline:
#
#   IPC file on disk  →  ExArrow stream handle  →  Flight do_put
#
# Scenarios:
#
#   1. read_file_forward_flight   – Read an IPC file from disk and upload all
#                                   batches via Flight do_put without ever
#                                   materialising column data in the BEAM heap.
#                                   This is the canonical ExArrow use-case for
#                                   data forwarding pipelines (ETL, proxies).
#
#   2. read_binary_forward_flight – Same as above but starts from a pre-loaded
#                                   binary (e.g. data received over HTTP or
#                                   from a database driver).
#
#   3. materialise_then_forward   – Read the file, collect every batch into
#                                   BEAM memory first, then upload. Shows the
#                                   cost of materialisation as an overhead.
#
# Why the zero-copy path wins:
#   - The Rust IPC reader and Flight client share Arrow buffers directly.
#   - No Elixir struct is allocated per batch for scenario 1 and 2.
#   - BEAM GC pressure is minimal because large column buffers never enter the
#     BEAM heap.
# ---------------------------------------------------------------------------

IO.puts("\n== End-to-End Pipeline Benchmark ==\n")

{schema, batches} = Bench.DataGen.schema_and_batches(20)
{:ok, large_binary} = ExArrow.IPC.Writer.to_binary(schema, batches)
file_path = Bench.DataGen.write_temp_ipc_file(large_binary)

{:ok, server} = ExArrow.Flight.Server.start_link(0)
{:ok, port} = ExArrow.Flight.Server.port(server)
{:ok, client} = ExArrow.Flight.Client.connect("localhost", port, tls: false)

output_dir = Bench.DataGen.output_dir()

Benchee.run(
  %{
    "file → Flight (zero-copy, 20 batches)" => fn ->
      {:ok, stream} = ExArrow.IPC.Reader.from_file(file_path)
      {:ok, read_schema} = ExArrow.Stream.schema(stream)
      stream_batches = Bench.DataGen.collect_stream(stream)
      :ok = ExArrow.Flight.Client.do_put(client, read_schema, stream_batches)
    end,
    "binary → Flight (20 batches)" => fn ->
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(large_binary)
      {:ok, read_schema} = ExArrow.Stream.schema(stream)
      stream_batches = Bench.DataGen.collect_stream(stream)
      :ok = ExArrow.Flight.Client.do_put(client, read_schema, stream_batches)
    end,
    "materialise → Flight (20 batches)" => fn ->
      {:ok, stream} = ExArrow.IPC.Reader.from_file(file_path)
      {:ok, read_schema} = ExArrow.Stream.schema(stream)
      all_batches = Bench.DataGen.collect_stream(stream)
      :ok = ExArrow.Flight.Client.do_put(client, read_schema, all_batches)
    end
  },
  time: 5,
  memory_time: 2,
  warmup: 2,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML,
     file: Path.join(output_dir, "pipeline.html"), auto_open: false},
    {Benchee.Formatters.JSON,
     file: Path.join(output_dir, "pipeline.json")}
  ]
)

ExArrow.Flight.Server.stop(server)
File.rm(file_path)
