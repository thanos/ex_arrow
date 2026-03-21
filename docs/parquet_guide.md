# Parquet guide

ExArrow supports reading and writing Apache Parquet files via the Arrow Rust
`parquet` crate.  The API is intentionally symmetric with `ExArrow.IPC` — you
get the same `ExArrow.Stream` interface on the read side and the same
schema + batches pattern on the write side.

---

## Reading

### From a file path

```elixir
{:ok, stream}  = ExArrow.Parquet.Reader.from_file("/data/events.parquet")
{:ok, schema}  = ExArrow.Stream.schema(stream)
IO.inspect ExArrow.Schema.field_names(schema)
# ["timestamp", "user_id", "event_type", "score"]

batches = ExArrow.Stream.to_list(stream)
total_rows = Enum.sum(Enum.map(batches, &ExArrow.RecordBatch.num_rows/1))
```

### From an in-memory binary

Useful when the Parquet data has been downloaded from S3, received over HTTP,
or produced in-process:

```elixir
parquet_bytes = File.read!("/data/events.parquet")
# or: HTTPoison.get!(url).body

{:ok, stream} = ExArrow.Parquet.Reader.from_binary(parquet_bytes)
batch = ExArrow.Stream.next(stream)
```

### Schema introspection

`ExArrow.Stream.schema/1` never fails for Parquet streams (the schema is
always available after a successful open):

```elixir
{:ok, stream} = ExArrow.Parquet.Reader.from_file("/data/trades.parquet")
{:ok, schema} = ExArrow.Stream.schema(stream)
fields = ExArrow.Schema.fields(schema)
# [%ExArrow.Field{name: "ts", type: :timestamp}, ...]
```

---

## Writing

You need an `ExArrow.Schema` handle and a list of `ExArrow.RecordBatch` handles.
These come from any ExArrow source: IPC readers, ADBC execute, Flight do_get,
or compute kernels.

### To a file

```elixir
:ok = ExArrow.Parquet.Writer.to_file("/out/result.parquet", schema, batches)
```

### To an in-memory binary

```elixir
{:ok, parquet_bytes} = ExArrow.Parquet.Writer.to_binary(schema, batches)
byte_size(parquet_bytes)  # ready to upload
```

### Schema from a batch

When you have batches but not a separate schema handle:

```elixir
schema = ExArrow.RecordBatch.schema(hd(batches))
:ok = ExArrow.Parquet.Writer.to_file("/out/result.parquet", schema, batches)
```

### Schema from a stream before consuming it

```elixir
{:ok, stream} = ExArrow.IPC.Reader.from_file("/data/source.arrow")
{:ok, schema} = ExArrow.Stream.schema(stream)
batches       = ExArrow.Stream.to_list(stream)
:ok = ExArrow.Parquet.Writer.to_file("/out/copy.parquet", schema, batches)
```

---

## End-to-end examples

### ADBC query → Parquet file

```elixir
{:ok, db}   = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_postgresql",
                uri: "postgresql://user:pass@localhost/mydb")
{:ok, conn} = ExArrow.ADBC.Connection.open(db)
{:ok, stmt} = ExArrow.ADBC.Statement.new(conn, "SELECT * FROM sales WHERE year = 2024")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)
{:ok, schema} = ExArrow.Stream.schema(stream)
batches       = ExArrow.Stream.to_list(stream)

:ok = ExArrow.Parquet.Writer.to_file("/data/sales_2024.parquet", schema, batches)
```

### Parquet → Explorer DataFrame

```elixir
{:ok, stream} = ExArrow.Parquet.Reader.from_file("/data/report.parquet")
{:ok, df}     = ExArrow.Explorer.from_stream(stream)
Explorer.DataFrame.filter(df, score > 0.9)
```

### Parquet → Nx tensors for ML

```elixir
{:ok, stream}  = ExArrow.Parquet.Reader.from_file("/data/features.parquet")
batch          = ExArrow.Stream.next(stream)
{:ok, tensors} = ExArrow.Nx.to_tensors(batch)

# tensors is %{"feature1" => #Nx.Tensor<...>, "feature2" => #Nx.Tensor<...>}
inputs = Nx.stack(Map.values(tensors), axis: 1)
```

### Parquet → filter → write back

```elixir
{:ok, stream}   = ExArrow.Parquet.Reader.from_file("/data/all_users.parquet")
batch           = ExArrow.Stream.next(stream)

{:ok, mask}     = ExArrow.Compute.project(batch, ["is_active"])
{:ok, active}   = ExArrow.Compute.filter(batch, mask)

schema = ExArrow.RecordBatch.schema(active)
:ok = ExArrow.Parquet.Writer.to_file("/out/active_users.parquet", schema, [active])
```

---

## How Parquet is read

Parquet has a footer that is scanned once on `from_file/1` / `from_binary/1`
to extract the schema and locate row groups.  Row groups are then decoded
**lazily** — each call to `ExArrow.Stream.next/1` reads and decodes the next
row group on demand without touching the rest of the file.

```
from_file/1  →  footer scan only  (schema cached, reader open)
ExArrow.Stream.next/1  →  decode row-group 0
ExArrow.Stream.next/1  →  decode row-group 1
…
ExArrow.Stream.next/1  →  nil  (end of file)
```

Peak memory stays proportional to the largest single row group rather than the
full file.  If you only need the first *N* batches you can stop calling
`ExArrow.Stream.next/1` and the remaining row groups are never decoded.

For file-backed streams (`from_file/1`) the underlying OS file handle is kept
open until the stream resource is garbage-collected; for binary-backed streams
(`from_binary/1`) the bytes are held in native memory and released at the same
time.

**Implementation note:** each `ExArrow.Stream.next/1` call runs the native
`parquet_stream_next` step on a **dirty CPU** NIF scheduler so row-group decode
(and any file read inside that step) does not block normal BEAM scheduler
threads.

---

## Comparison with IPC

| | Arrow IPC (stream) | Parquet |
|---|---|---|
| Random access | No | Parquet footer only |
| Compression | No (raw) | Yes (Snappy, ZSTD, …) |
| Interop | Arrow ecosystem | Universal (Python, Spark, …) |
| Read API | `ExArrow.IPC.Reader` | `ExArrow.Parquet.Reader` |
| Write API | `ExArrow.IPC.Writer` | `ExArrow.Parquet.Writer` |
| Stream type | `:ipc` | `:parquet` |
| Stream interface | identical | identical |

Use IPC for high-throughput in-process pipelines and Flight transport.  Use
Parquet for on-disk storage, long-term archival, and interop with Python/R/Spark.
