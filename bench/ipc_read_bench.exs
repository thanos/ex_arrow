Code.require_file("bench_helper.exs", __DIR__)

# ---------------------------------------------------------------------------
# IPC Read Benchmark
#
# Measures the cost of decoding Arrow IPC data into usable record batches,
# comparing two access patterns:
#
#   1. stream_handle  – Open a stream and keep the opaque handle. Data stays
#                       in native (Rust) memory; BEAM holds only a reference.
#                       This is the zero-copy path.
#
#   2. materialise    – Open a stream and pull every batch into BEAM memory
#                       as ExArrow.RecordBatch structs. Shows the full cost
#                       when callers need random-access to all batches.
#
#   3. from_file      – Read the same data via the IPC file API (memory-mapped
#                       path) instead of a pre-loaded binary.
#
# Why ExArrow beats a plain binary decode:
#   - batch count is never copied into BEAM heap
#   - the opaque stream handle is one word in BEAM
#   - downstream consumers (Flight do_put, ADBC bind) accept the native ref
# ---------------------------------------------------------------------------

IO.puts("\n== IPC Read Benchmark ==\n")

small_binary = Bench.DataGen.ipc_binary(10)
large_binary = Bench.DataGen.large_ipc_binary()
temp_file = Bench.DataGen.write_temp_ipc_file(large_binary)

on_exit = fn -> File.rm(temp_file) end

Benchee.run(
  %{
    "stream_handle (10 batches)" => fn ->
      {:ok, _stream} = ExArrow.IPC.Reader.from_binary(small_binary)
    end,
    "materialise (10 batches)" => fn ->
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(small_binary)
      Bench.DataGen.collect_stream(stream)
    end,
    "stream_handle (50 batches)" => fn ->
      {:ok, _stream} = ExArrow.IPC.Reader.from_binary(large_binary)
    end,
    "materialise (50 batches)" => fn ->
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(large_binary)
      Bench.DataGen.collect_stream(stream)
    end,
    "from_file handle (50 batches)" => fn ->
      {:ok, _stream} = ExArrow.IPC.Reader.from_file(temp_file)
    end,
    "from_file + materialise (50 batches)" => fn ->
      {:ok, stream} = ExArrow.IPC.Reader.from_file(temp_file)
      Bench.DataGen.collect_stream(stream)
    end
  },
  time: 5,
  memory_time: 2,
  warmup: 2,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML,
     file: Path.join(Bench.DataGen.output_dir(), "ipc_read.html"), auto_open: false},
    {Benchee.Formatters.JSON,
     file: Path.join(Bench.DataGen.output_dir(), "ipc_read.json")}
  ]
)

on_exit.()
