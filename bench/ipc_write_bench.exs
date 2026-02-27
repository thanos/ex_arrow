Code.require_file("bench_helper.exs", __DIR__)

# ---------------------------------------------------------------------------
# IPC Write Benchmark
#
# Measures the cost of serialising Arrow record batches to wire formats,
# comparing:
#
#   1. to_binary (10 / 50 batches)   – Stream format; produces an in-memory
#                                       binary directly from native buffers.
#
#   2. to_file (50 batches)          – IPC file format written to a temp path.
#                                       Shows the cost of a disk write vs a
#                                       binary accumulation in memory.
#
#   3. json_encode_equivalent        – JSON-encodes an equal-sized Elixir map
#                                       to demonstrate the overhead of a
#                                       traditional row-oriented approach.
#
# Why ExArrow wins:
#   - Arrow columnar layout serialises with near-zero copies (memcpy)
#   - JSON must build per-field binary representations row-by-row
# ---------------------------------------------------------------------------

IO.puts("\n== IPC Write Benchmark ==\n")

{schema10, batches10} = Bench.DataGen.schema_and_batches(10)
{schema50, batches50} = Bench.DataGen.schema_and_batches(50)

# Build a comparable Erlang-term payload: list of maps, same conceptual size
# as a test fixture batch (~small number of rows * 3 fields). We use
# :erlang.term_to_binary as a universally available baseline for the cost of
# serialising row-oriented BEAM data (no extra deps needed).
term_rows = for i <- 1..100, do: %{"id" => i, "value" => i * 1.5, "label" => "row_#{i}"}

output_dir = Bench.DataGen.output_dir()
temp_file = Path.join(System.tmp_dir!(), "ex_arrow_write_bench_#{System.unique_integer([:positive])}.arrow")
on_exit = fn -> File.rm(temp_file) end

Benchee.run(
  %{
    "ipc to_binary (10 batches)" => fn ->
      {:ok, _bin} = ExArrow.IPC.Writer.to_binary(schema10, batches10)
    end,
    "ipc to_binary (50 batches)" => fn ->
      {:ok, _bin} = ExArrow.IPC.Writer.to_binary(schema50, batches50)
    end,
    "ipc to_file (50 batches)" => fn ->
      :ok = ExArrow.IPC.Writer.to_file(temp_file, schema50, batches50)
    end,
    "term_to_binary (100 rows, 3 fields)" => fn ->
      :erlang.term_to_binary(term_rows)
    end
  },
  time: 5,
  memory_time: 2,
  warmup: 2,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML,
     file: Path.join(output_dir, "ipc_write.html"), auto_open: false},
    {Benchee.Formatters.JSON,
     file: Path.join(output_dir, "ipc_write.json")}
  ]
)

on_exit.()
