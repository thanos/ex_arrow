# Arrow and Flow

`ExArrow.Flow` brings parallel, batch-oriented processing to Arrow streams by
wrapping the [Flow](https://github.com/elixir-lang/flow) library.  This guide
explains how it works, when to use it, and the performance tradeoffs.

## Why Flow?

`Enum` and `Stream` run in a single process.  For CPU-bound transformations
on large Arrow datasets that single process becomes the bottleneck.  Flow
spreads the work across multiple stages (processes), each consuming batches
independently.

The unit of work in `ExArrow.Flow` is the **batch**, not the row.  A Flow
stage receives an `ExArrow.RecordBatch` handle and returns one.  Because the
handle is an opaque reference to native memory, no column buffers are copied
to the BEAM heap when a batch moves between stages — only the small reference
term is sent over the mailbox.

## Building a Flow

`ExArrow.Flow.from_batches/1` accepts an `ExArrow.Stream.t()` (or any
`Enumerable.t()` of batches) and returns a `Flow.t()`:

    {:ok, stream} = ExArrow.Stream.from_parquet("/data/events.parquet")

    stream
    |> ExArrow.Flow.from_batches()
    |> Flow.map(&ExArrow.RecordBatch.num_rows/1)
    |> Enum.to_list()

The function also unwraps `{:ok, stream}` results so it composes directly with
`ExArrow.Stream.from_*/1` in a pipe:

    ExArrow.Stream.from_parquet("/data/events.parquet")
    |> ExArrow.Flow.from_batches()
    |> Flow.map(&ExArrow.RecordBatch.num_rows/1)
    |> Enum.to_list()

`opts` are forwarded to `Flow.from_enumerable/2`:

    ExArrow.Flow.from_batches(stream, stages: 8, max_demand: 4)

## Combinators

All standard `Flow` combinators work:

- `Flow.map/2` — transform each batch.
- `Flow.flat_map/2` — expand one batch into many.
- `Flow.partition/2` — partition batches by key for shuffled reductions.
- `Flow.reduce/3` — reduce batches within a window/partition.

`ExArrow.Flow` adds two telemetry-emitting helpers:

- `ExArrow.Flow.map_batches/2` — `Flow.map/2` plus a
  `[:ex_arrow, :pipeline, :batch]` event per batch.
- `ExArrow.Flow.each_batch/2` — run a side effect per batch, batches pass
  through unchanged.

## Performance implications

### Parallelism

Flow spins up a configurable number of producer and consumer stages
(`:stages`, `:max_demand`, `:min_demand`).  Each stage decodes and transforms
batches independently, so wall-clock time scales with available cores for
CPU-bound work.

### Memory

Only batch references cross process boundaries; the Arrow buffers stay in
native memory until a stage explicitly extracts them.  Peak memory is roughly
`stages * largest_batch` rather than the whole dataset.

### Backpressure

GenStage demand is honoured end-to-end, so a slow consumer slows the producer
without piling up batches.

### Not a row API

Converting batches to row maps inside a Flow stage defeats the purpose.  Keep
transformations column-wise — use `ExArrow.Batch` or `ExArrow.Compute` to
project, filter, or sort within a stage.

## Example: parallel column projection

    {:ok, stream} = ExArrow.Stream.from_parquet("/data/events.parquet")

    stream
    |> ExArrow.Flow.from_batches(stages: 4)
    |> Flow.map(fn batch ->
      {:ok, slim} = ExArrow.Batch.select(batch, ["id", "score"])
      slim
    end)
    |> Enum.to_list()

## Example: partitioned reduction

    {:ok, stream} = ExArrow.Stream.from_flight_sql(client, "SELECT user_id, amount FROM sales")

    stream
    |> ExArrow.Flow.from_batches()
    |> Flow.partition(key: fn batch -> ExArrow.RecordBatch.column_names(batch) end)
    |> Flow.reduce(fn -> %{} end, fn batch, acc ->
      # merge batch into acc keyed by user_id
      Map.merge(acc, summarise(batch), fn _k, a, b -> a + b end)
    end)
    |> Enum.to_list()

## When to use Flow vs Pipeline

| Use `ExArrow.Flow` when                 | Use `ExArrow.Pipeline` when                 |
|-----------------------------------------|---------------------------------------------|
| You need explicit parallelism control   | You want a thin, stable abstraction         |
| You want partition/reduce semantics     | You are doing map-only transformations      |
| You are comfortable with Flow's API     | You want telemetry wired in automatically   |

`ExArrow.Pipeline` may internally use Flow in future releases; the public API
will stay the same.
