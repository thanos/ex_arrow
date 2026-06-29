Code.require_file("bench_helper.exs", __DIR__)

# ---------------------------------------------------------------------------
# ExArrow v0.7.0 — Arrow-native stream / Flow / pipeline benchmarks.
#
# Scenarios:
#
#   1. parquet_stream        – open and drain a Parquet stream.
#   2. ipc_stream            – open and drain an IPC stream.
#   3. flow_from_batches     – drain the same stream through ExArrow.Flow.
#   4. pipeline_map_parquet  – run a Pipeline.map_batches + write_parquet.
#
# Datasets: 1K, 100K, 1M rows (replicated batches of the IPC fixture).
# ---------------------------------------------------------------------------

IO.puts("\n== ExArrow v0.7.0 Stream / Flow / Pipeline Benchmark ==\n")

alias ExArrow.IPC
alias ExArrow.Stream

# Build a single-batch IPC binary with `rows` rows by replicating the
# fixture batch `rows / fixture_rows` times within one batch list and
# concatenating through the IPC writer.  For simplicity we scale by
# replicating *batches* and accept that the row count is approximate.
{_schema, batches} = Bench.DataGen.schema_and_batches(1)
fixture_batch = hd(batches)
fixture_rows = ExArrow.RecordBatch.num_rows(fixture_batch)

# Build IPC binaries for each target row count by replicating the fixture
# batch enough times to reach at least the target.
build_ipc_bin = fn target_rows ->
  batches_needed = div(target_rows + fixture_rows - 1, fixture_rows)
  {schema, batches} = Bench.DataGen.schema_and_batches(batches_needed)
  {:ok, bin} = ExArrow.IPC.Writer.to_binary(schema, batches)
  {schema, bin, batches_needed * fixture_rows}
end

{schema_1k, bin_1k, rows_1k} = build_ipc_bin.(1_000)
{schema_100k, bin_100k, rows_100k} = build_ipc_bin.(100_000)
{schema_1m, bin_1m, rows_1m} = build_ipc_bin.(1_000_000)

# Write Parquet fixtures for the parquet scenarios.
write_parquet_fixture = fn schema, bin, name ->
  {:ok, stream} = IPC.Reader.from_binary(bin)
  batches = Stream.to_list(stream)
  path = Path.join(System.tmp_dir!(), "ex_arrow_bench_#{name}.parquet")
  :ok = ExArrow.Parquet.Writer.to_file(path, schema, batches)
  {path, batches}
end

{pq_1k, _} = write_parquet_fixture.(schema_1k, bin_1k, "1k")
{pq_100k, _} = write_parquet_fixture.(schema_100k, bin_100k, "100k")
{pq_1m, _} = write_parquet_fixture.(schema_1m, bin_1m, "1m")

output_dir = Bench.DataGen.output_dir()

# ── Stream drain benchmarks ───────────────────────────────────────────────────

Benchee.run(
  %{
    "Parquet stream drain (1K rows)" => fn ->
      {:ok, s} = ExArrow.Stream.from_parquet(pq_1k)
      _ = Stream.to_list(s)
    end,
    "Parquet stream drain (100K rows)" => fn ->
      {:ok, s} = ExArrow.Stream.from_parquet(pq_100k)
      _ = Stream.to_list(s)
    end,
    "Parquet stream drain (1M rows)" => fn ->
      {:ok, s} = ExArrow.Stream.from_parquet(pq_1m)
      _ = Stream.to_list(s)
    end,
    "IPC stream drain (1K rows)" => fn ->
      {:ok, s} = ExArrow.Stream.from_ipc(bin_1k)
      _ = Stream.to_list(s)
    end,
    "IPC stream drain (100K rows)" => fn ->
      {:ok, s} = ExArrow.Stream.from_ipc(bin_100k)
      _ = Stream.to_list(s)
    end,
    "IPC stream drain (1M rows)" => fn ->
      {:ok, s} = ExArrow.Stream.from_ipc(bin_1m)
      _ = Stream.to_list(s)
    end
  },
  time: 5,
  memory_time: 2,
  warmup: 2,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: Path.join(output_dir, "v070_stream.html"), auto_open: false},
    {Benchee.Formatters.JSON, file: Path.join(output_dir, "v070_stream.json")}
  ]
)

# ── Flow benchmarks ───────────────────────────────────────────────────────────

Benchee.run(
  %{
    "Flow.map over IPC stream (100K rows)" => fn ->
      {:ok, s} = ExArrow.Stream.from_ipc(bin_100k)

      s
      |> ExArrow.Flow.from_batches()
      |> Flow.map(&ExArrow.RecordBatch.num_rows/1)
      |> Enum.to_list()
    end,
    "Flow.map over IPC stream (1M rows)" => fn ->
      {:ok, s} = ExArrow.Stream.from_ipc(bin_1m)

      s
      |> ExArrow.Flow.from_batches()
      |> Flow.map(&ExArrow.RecordBatch.num_rows/1)
      |> Enum.to_list()
    end
  },
  time: 5,
  memory_time: 2,
  warmup: 2,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: Path.join(output_dir, "v070_flow.html"), auto_open: false},
    {Benchee.Formatters.JSON, file: Path.join(output_dir, "v070_flow.json")}
  ]
)

# ── Pipeline benchmarks ───────────────────────────────────────────────────────

pipeline_out = Path.join(System.tmp_dir!(), "ex_arrow_bench_pipeline_out.parquet")

Benchee.run(
  %{
    "Pipeline.map_batches + write_parquet (100K rows)" => fn ->
      {:ok, s} = ExArrow.Stream.from_ipc(bin_100k)

      {:ok, s}
      |> ExArrow.Pipeline.map_batches(& &1)
      |> ExArrow.Pipeline.write_parquet(pipeline_out)

      File.rm(pipeline_out)
    end,
    "Pipeline.map_batches + write_parquet (1M rows)" => fn ->
      {:ok, s} = ExArrow.Stream.from_ipc(bin_1m)

      {:ok, s}
      |> ExArrow.Pipeline.map_batches(& &1)
      |> ExArrow.Pipeline.write_parquet(pipeline_out)

      File.rm(pipeline_out)
    end
  },
  time: 5,
  memory_time: 2,
  warmup: 2,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: Path.join(output_dir, "v070_pipeline.html"), auto_open: false},
    {Benchee.Formatters.JSON, file: Path.join(output_dir, "v070_pipeline.json")}
  ]
)

# Cleanup
File.rm(pq_1k)
File.rm(pq_100k)
File.rm(pq_1m)

IO.puts("""
  Approximate row counts:
    1K   -> #{rows_1k} rows
    100K -> #{rows_100k} rows
    1M   -> #{rows_1m} rows
""")
