# Arrow Streams

ExArrow v0.7.0 introduces first-class streaming as the primary mechanism for
working with large datasets.  This guide explains the streaming abstraction,
the available sources, and the tradeoffs of each.

## The unit of streaming: RecordBatch

ExArrow streams yield `ExArrow.RecordBatch` values, not row maps.  A
`RecordBatch` is an opaque handle to a native Arrow batch — a collection of
column arrays sharing a schema and row count.  The column buffers stay in
native (Rust) memory until you explicitly extract them; only the small handle
crosses the BEAM heap.

This is the central architectural principle of v0.7.0:

  Operate on Arrow RecordBatch values.
  Not `list(map())`.  Not `Explorer.DataFrame`.  Not `Nx.Tensor`.

Explorer and Nx remain downstream consumers.  ExArrow is the Arrow layer.

## The `ExArrow.Stream` constructors

Every common source has a `from_*/1` constructor on `ExArrow.Stream` that
returns `{:ok, stream} | {:error, reason}`:

| Constructor                          | Source                                       |
|--------------------------------------|----------------------------------------------|
| `ExArrow.Stream.from_parquet/1`      | Parquet file at `path`                       |
| `ExArrow.Stream.from_parquet_binary/1` | In-memory Parquet bytes                    |
| `ExArrow.Stream.from_ipc/1`          | Arrow IPC stream binary                      |
| `ExArrow.Stream.from_ipc_file/1`     | Arrow IPC file at `path`                     |
| `ExArrow.Stream.from_flight/2`       | Flight `do_get` ticket                       |
| `ExArrow.Stream.from_flight_sql/2`   | Flight SQL query                             |
| `ExArrow.Stream.from_adbc/1`         | Pre-built ADBC statement                     |
| `ExArrow.Stream.from_adbc/2`         | `{connection, sql}` one-shot ADBC query      |

Each constructor tags the stream with a `source` term (e.g. `{:parquet, path}`
or `{:flight_sql, sql}`) that is forwarded to telemetry events as `:source`
metadata.

## Consuming a stream

Three consumption patterns cover every use case.

### 1. Lazy iteration with `next/1`

`ExArrow.Stream.next/1` pulls one batch at a time and returns `nil` when the
stream is exhausted.  This is the lowest-level interface and the one that
gives you explicit control over error handling.

    {:ok, stream} = ExArrow.Stream.from_parquet("/data/events.parquet")

    stream
    |> Stream.iterate(&ExArrow.Stream.next/1)
    |> Enum.take_while(&(&1 != nil))
    |> Enum.each(fn batch ->
      IO.puts("rows: #{ExArrow.RecordBatch.num_rows(batch)}")
    end)

For recoverable error handling, pattern-match on the `{:error, reason}` return
of `next/1` rather than letting `Enum` raise.

### 2. Enumerable consumption

`ExArrow.Stream` implements `Enumerable`, so all `Enum` and `Stream` functions
work directly on a stream handle:

    {:ok, stream} = ExArrow.Stream.from_flight_sql(client, "SELECT * FROM t")

    batches = Enum.to_list(stream)
    row_counts = Enum.map(stream, &ExArrow.RecordBatch.num_rows/1)
    first_two = Enum.take(stream, 2)

`Enum.take/2` stops consuming early — the remaining batches are never fetched.
Enumeration raises on a transport or server error; for recoverable handling
use `next/1`.

### 3. Pipeline consumption

`ExArrow.Pipeline` (see the [Pipeline Patterns](10_arrow_pipeline_patterns.md)
guide) wraps a stream with composable `map_batches/2` and sink functions:

    ExArrow.Stream.from_parquet("/data/events.parquet")
    |> ExArrow.Pipeline.map_batches(fn batch ->
      {:ok, slim} = ExArrow.Batch.select(batch, ["id", "score"])
      slim
    end)
    |> ExArrow.Pipeline.write_parquet("/data/slim.parquet")

The pipeline is lazy: nothing runs until a sink consumes it.

## Schema preservation

Every stream carries its Arrow schema.  `ExArrow.Stream.schema/1` returns the
`ExArrow.Schema` handle without consuming any batches:

    {:ok, stream} = ExArrow.Stream.from_parquet("/data/events.parquet")
    {:ok, schema} = ExArrow.Stream.schema(stream)
    ExArrow.Schema.field_names(schema)  # => ["id", "name", "score", ...]

Field names, types, and nullability are preserved end-to-end through IPC,
Parquet, Flight, and ADBC.

## Backpressure and laziness

ExArrow streams are lazy by construction:

- **Parquet** — the footer is scanned once on open (making the schema
  available); row groups are decoded on demand by `next/1`.  Stopping early
  leaves the remaining row groups undecoded.
- **IPC** — batches are decoded one at a time as `next/1` is called.
- **Flight / Flight SQL** — the gRPC stream stays open until exhausted or
  garbage-collected; each `next/1` pulls one server-side batch.
- **ADBC** — the driver's result iterator is consumed on demand.

For GenStage-style demand-driven backpressure, use the
`ExArrow.GenStage.*Producer` modules (see the
[GenStage guide](08_arrow_and_genstage.md)).

## Telemetry

Every `from_*/1` constructor for Parquet, Flight, and Flight SQL emits a
source-level telemetry event (`[:ex_arrow, :parquet, :read]`,
`[:ex_arrow, :flight, :query]`, `[:ex_arrow, :flight_sql, :query]`).  Each
`next/1` that yields a batch emits `[:ex_arrow, :stream, :batch]` with `rows`,
`columns`, and `batch_count` measurements.

See the [Telemetry](#) module docs for the full event list and a handler
example.

## Choosing a source

| Need                                  | Use                                             |
|---------------------------------------|-------------------------------------------------|
| Read a columnar file from disk        | `from_parquet/1` or `from_ipc_file/1`           |
| Read bytes already in memory          | `from_parquet_binary/1` or `from_ipc/1`         |
| Query a Flight / Flight SQL server    | `from_flight/2` or `from_flight_sql/2`          |
| Query via an ADBC driver              | `from_adbc/1` (prepared) or `from_adbc/2` (SQL) |
| Compose with Flow / GenStage / Broadway | `ExArrow.Flow`, `ExArrow.GenStage`, `ExArrow.Broadway` |

## Example: copy a Parquet file through Arrow

    {:ok, stream} = ExArrow.Stream.from_parquet("/data/events.parquet")
    {:ok, schema} = ExArrow.Stream.schema(stream)
    batches = ExArrow.Stream.to_list(stream)
    :ok = ExArrow.Parquet.Writer.to_file("/data/events_copy.parquet", schema, batches)

Or, using the Pipeline DSL:

    ExArrow.Stream.from_parquet("/data/events.parquet")
    |> ExArrow.Pipeline.write_parquet("/data/events_copy.parquet")
