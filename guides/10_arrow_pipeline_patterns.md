# Arrow Pipeline Patterns

`ExArrow.Pipeline` is the stable, composable abstraction for transforming and
sinking Arrow streams.  This guide walks through the common patterns and the
tradeoffs of each.

## The Pipeline abstraction

A pipeline wraps an `ExArrow.Stream.t()` with an Elixir `Stream` of
`ExArrow.RecordBatch` values alongside the stream's schema.  Every function
accepts and returns `{:ok, pipeline} | {:error, reason}`, so pipelines compose
with `|>/2` directly from `ExArrow.Stream.from_*/1` constructors.  Errors
short-circuit through every stage.

The pipeline is **lazy**: `map_batches/2` and `each_batch/2` do no work until
a sink (`write_parquet/2`, `write_flight/3`, `write_dataframe/1`) runs.

## map_batches/2

Transform each batch lazily.  `fun` receives an `ExArrow.RecordBatch.t()` and
should return an `ExArrow.RecordBatch.t()` (or any term the downstream sink
expects).

    ExArrow.Stream.from_parquet("/data/events.parquet")
    |> ExArrow.Pipeline.map_batches(fn batch ->
      {:ok, slim} = ExArrow.Batch.select(batch, ["id", "score"])
      slim
    end)
    |> ExArrow.Pipeline.write_parquet("/data/slim.parquet")

`map_batches/2` emits a `[:ex_arrow, :pipeline, :batch]` telemetry event per
batch.

## each_batch/2

Run a side effect per batch without changing the pipeline.  The batches pass
through unchanged.

    ExArrow.Stream.from_parquet("/data/events.parquet")
    |> ExArrow.Pipeline.each_batch(fn batch ->
      IO.puts("rows: #{ExArrow.RecordBatch.num_rows(batch)}")
    end)
    |> ExArrow.Pipeline.write_parquet("/data/copy.parquet")

## write_parquet/2

Consume the pipeline and write every batch to a Parquet file.  Triggers
evaluation of all upstream stages.  Emits a
`[:ex_arrow, :parquet, :write]` telemetry event.

    ExArrow.Stream.from_flight_sql(client, "SELECT * FROM events")
    |> ExArrow.Pipeline.write_parquet("/data/events.parquet")

## write_flight/3

Consume the pipeline and upload every batch to a Flight server.  `opts` are
forwarded to `ExArrow.Flight.Client.do_put/4`.

    ExArrow.Stream.from_parquet("/data/events.parquet")
    |> ExArrow.Pipeline.write_flight(client, descriptor: {:cmd, "events"})

## write_dataframe/1

Consume the pipeline and convert it into an `Explorer.DataFrame`.  Requires
the optional `{:explorer, "~> 0.11"}` dependency.

    {:ok, df} =
      ExArrow.Stream.from_parquet("/data/events.parquet")
      |> ExArrow.Pipeline.write_dataframe()

## Composing stages

Stages compose with `|>/2` because every function accepts the previous
stage's `{:ok, pipeline}` result:

    ExArrow.Stream.from_flight_sql(client, "SELECT * FROM events")
    |> ExArrow.Pipeline.map_batches(&ExArrow.Batch.select(&1, ["id", "score"]))
    |> ExArrow.Pipeline.each_batch(&log_batch/1)
    |> ExArrow.Pipeline.write_parquet("/data/slim.parquet")

## Error propagation

If a constructor or stage returns `{:error, reason}`, every subsequent stage
passes the error through unchanged:

    {:error, "no connection"}
    |> ExArrow.Pipeline.map_batches(& &1)        # => {:error, "no connection"}
    |> ExArrow.Pipeline.write_parquet("/x.parquet")  # => {:error, "no connection"}

Use a `with` chain if you prefer explicit error handling:

    with {:ok, stream} <- ExArrow.Stream.from_parquet("/data/events.parquet"),
         {:ok, _} <- ExArrow.Pipeline.write_parquet({:ok, stream}, "/data/copy.parquet") do
      :ok
    end

## When to use Pipeline vs Flow vs GenStage

| Pipeline                       | Flow                          | GenStage                   |
|--------------------------------|-------------------------------|----------------------------|
| Thin, stable abstraction       | Explicit parallelism control  | Demand-driven backpressure |
| Map-only transformations       | Partition/reduce semantics    | Long-running pipelines     |
| Telemetry wired in             | One-shot batch jobs           | Custom consumer wiring     |
| Single-process (lazy Stream)   | Multi-process                 | Multi-process              |

`ExArrow.Pipeline` is the right starting point.  Reach for `ExArrow.Flow`
when you need parallelism beyond a single process, and for `ExArrow.GenStage`
when you need explicit demand/backpressure or a long-running pipeline.

## Schema handling

The pipeline captures the stream's schema at wrap time (via
`ExArrow.Stream.schema/1`).  When a transformation changes the schema (e.g.
`ExArrow.Batch.select/2`), the sink derives the schema from the first emitted
batch so the output file carries the correct schema.  If the pipeline
produces zero batches, the captured schema is used as a fallback so an empty
Parquet file still has a schema.

## Telemetry summary

| Event                              | When                         |
|------------------------------------|------------------------------|
| `[:ex_arrow, :pipeline, :batch]`   | `map_batches`/`each_batch` per batch |
| `[:ex_arrow, :parquet, :write]`    | `write_parquet/2`            |
| `[:ex_arrow, :flight, :query]`     | `write_flight/3`             |

Attach a handler with `:telemetry.attach/4` (see the `ExArrow.Telemetry`
module docs for an example).

## Example: end-to-end Flight SQL to Parquet

    ExArrow.Stream.from_flight_sql(client, "SELECT * FROM events")
    |> ExArrow.Pipeline.map_batches(fn batch ->
      {:ok, slim} = ExArrow.Batch.select(batch, ["id", "ts", "amount"])
      slim
    end)
    |> ExArrow.Pipeline.write_parquet("/data/events_slim.parquet")
