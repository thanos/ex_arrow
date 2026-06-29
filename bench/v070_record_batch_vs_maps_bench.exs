Code.require_file("bench_helper.exs", __DIR__)

# ---------------------------------------------------------------------------
# ExArrow v0.7.0 — Arrow RecordBatch vs list(map()) throughput.
#
# Compares the cost of building, transforming (select), and draining a
# dataset held as Arrow RecordBatch values versus a list of row maps.
#
# Datasets: 1K, 100K, 1M rows.
# ---------------------------------------------------------------------------

IO.puts("\n== Arrow RecordBatch vs list(map()) Benchmark ==\n")

alias ExArrow.RecordBatch

# Build a single s64 column "v" with `n` rows as an Arrow batch.
build_batch = fn n ->
  bin =
    1..n
    |> Enum.map(&<<&1::little-signed-64>>)
    |> IO.iodata_to_binary()

  {:ok, batch} = RecordBatch.from_columns(["v"], [bin], ["s64"], n)
  batch
end

# Build the equivalent list of row maps.
build_maps = fn n ->
  for i <- 1..n, do: %{"v" => i}
end

batch_1k = build_batch.(1_000)
batch_100k = build_batch.(100_000)
batch_1m = build_batch.(1_000_000)

maps_1k = build_maps.(1_000)
maps_100k = build_maps.(100_000)
# 1M maps is large; build lazily inside the scenario to avoid holding it
# in memory for the whole run.

# ── Build benchmarks ──────────────────────────────────────────────────────────

Benchee.run(
  %{
    "build RecordBatch (1K rows)" => fn -> build_batch.(1_000) end,
    "build RecordBatch (100K rows)" => fn -> build_batch.(100_000) end,
    "build RecordBatch (1M rows)" => fn -> build_batch.(1_000_000) end,
    "build list(map()) (1K rows)" => fn -> build_maps.(1_000) end,
    "build list(map()) (100K rows)" => fn -> build_maps.(100_000) end,
    "build list(map()) (1M rows)" => fn -> build_maps.(1_000_000) end
  },
  time: 5,
  memory_time: 2,
  warmup: 2,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML,
     file: Path.join(Bench.DataGen.output_dir(), "v070_build.html"), auto_open: false},
    {Benchee.Formatters.JSON, file: Path.join(Bench.DataGen.output_dir(), "v070_build.json")}
  ]
)

# ── Transform benchmarks (select / map a single field) ────────────────────────

Benchee.run(
  %{
    "Arrow select 1 column (1K rows)" => fn ->
      {:ok, _} = ExArrow.Batch.select(batch_1k, ["v"])
    end,
    "Arrow select 1 column (100K rows)" => fn ->
      {:ok, _} = ExArrow.Batch.select(batch_100k, ["v"])
    end,
    "Arrow select 1 column (1M rows)" => fn ->
      {:ok, _} = ExArrow.Batch.select(batch_1m, ["v"])
    end,
    "Enum.map row maps (1K rows)" => fn ->
      _ = Enum.map(maps_1k, & &1["v"])
    end,
    "Enum.map row maps (100K rows)" => fn ->
      _ = Enum.map(maps_100k, & &1["v"])
    end,
    "Enum.map row maps (1M rows)" => fn ->
      maps = build_maps.(1_000_000)
      _ = Enum.map(maps, & &1["v"])
    end
  },
  time: 5,
  memory_time: 2,
  warmup: 2,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML,
     file: Path.join(Bench.DataGen.output_dir(), "v070_transform.html"), auto_open: false},
    {Benchee.Formatters.JSON, file: Path.join(Bench.DataGen.output_dir(), "v070_transform.json")}
  ]
)

# ── Drain benchmarks (sum a column) ───────────────────────────────────────────

# For Arrow we extract the column buffer and sum the s64 values.
sum_arrow = fn batch ->
  ref = RecordBatch.resource_ref(batch)
  {:ok, {binary, "s64", _n}} = ExArrow.Native.record_batch_column_buffer(ref, "v")
  sum = for <<v::little-signed-64 <- binary>>, reduce: 0, do: (acc -> acc + v)
  sum
end

sum_maps = fn maps ->
  Enum.reduce(maps, 0, fn m, acc -> acc + m["v"] end)
end

Benchee.run(
  %{
    "Arrow sum column (1K rows)" => fn -> sum_arrow.(batch_1k) end,
    "Arrow sum column (100K rows)" => fn -> sum_arrow.(batch_100k) end,
    "Arrow sum column (1M rows)" => fn -> sum_arrow.(batch_1m) end,
    "Enum.reduce row maps (1K rows)" => fn -> sum_maps.(maps_1k) end,
    "Enum.reduce row maps (100K rows)" => fn -> sum_maps.(maps_100k) end,
    "Enum.reduce row maps (1M rows)" => fn ->
      maps = build_maps.(1_000_000)
      sum_maps.(maps)
    end
  },
  time: 5,
  memory_time: 2,
  warmup: 2,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML,
     file: Path.join(Bench.DataGen.output_dir(), "v070_drain.html"), auto_open: false},
    {Benchee.Formatters.JSON, file: Path.join(Bench.DataGen.output_dir(), "v070_drain.json")}
  ]
)

:ok
