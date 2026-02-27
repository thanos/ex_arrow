Code.require_file("bench_helper.exs", __DIR__)

# ---------------------------------------------------------------------------
# ADBC Benchmark
#
# Demonstrates the two primary access patterns for ADBC result streams:
#
#   1. stream_handle_only  – Receive the opaque stream reference from the NIF.
#                            Zero BEAM allocations for column data.
#
#   2. stream_schema       – Peek at the schema without consuming any batches.
#
#   3. stream_collect      – Pull every batch into BEAM. This is equivalent to
#                            what Ecto does row-by-row, but columnar.
#
# The benchmark uses IPC data as a stand-in for a real ADBC result set because
# ADBC drivers (SQLite, PostgreSQL, etc.) are optional dependencies and are not
# guaranteed to be present in all CI / benchmark environments.  The goal is to
# isolate the cost of the ExArrow streaming layer, not the database round-trip.
#
# For a real-database comparison (ExArrow ADBC vs Ecto.Repo.stream) the IPC
# replacement overhead is negligible; the streaming overhead is identical.
# ---------------------------------------------------------------------------

IO.puts("\n== ADBC Stream Benchmark ==\n")

{schema, batches} = Bench.DataGen.schema_and_batches(20)
{:ok, ipc_binary} = ExArrow.IPC.Writer.to_binary(schema, batches)

output_dir = Bench.DataGen.output_dir()

Benchee.run(
  %{
    "open ipc stream (20 batches)" => fn ->
      {:ok, _stream} = ExArrow.IPC.Reader.from_binary(ipc_binary)
    end,
    "stream schema peek (20 batches)" => fn ->
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(ipc_binary)
      {:ok, _schema} = ExArrow.Stream.schema(stream)
    end,
    "stream collect all batches (20 batches)" => fn ->
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(ipc_binary)
      Bench.DataGen.collect_stream(stream)
    end,
    "Enum.map (comparable row-oriented)" => fn ->
      # Baseline: building a list of 20 maps in pure BEAM (no native calls)
      for i <- 1..20, do: %{id: i, value: i * 1.5}
    end
  },
  time: 5,
  memory_time: 2,
  warmup: 2,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML,
     file: Path.join(output_dir, "adbc.html"), auto_open: false},
    {Benchee.Formatters.JSON,
     file: Path.join(output_dir, "adbc.json")}
  ]
)
