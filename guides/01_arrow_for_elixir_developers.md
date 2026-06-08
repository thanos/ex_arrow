# Arrow for Elixir Developers

Apache Arrow is a cross-language, in-memory columnar data format designed for
efficient analytical processing.  This guide explains the core Arrow concepts
that ExArrow exposes and how they map to familiar Elixir ideas.

## The Arrow hierarchy

Arrow organises data in a strict, layered hierarchy:

```
Schema
  └─ Field (name, type, nullable)
       │
RecordBatch
  └─ Array (one per column, shared row count)
       │
Table
  └─ RecordBatch (one or more, shared schema)
       │
Stream
  └─ RecordBatch (lazy sequence, consumed one at a time)
```

**Array** is the leaf: a single typed column of contiguous values.  Arrays are
never created directly by users; they live inside a RecordBatch.

**RecordBatch** is the fundamental unit of exchange.  A batch groups N arrays
(one per column) with a shared row count and schema.  Most ExArrow functions
accept or return batches.

**Table** is a logical container for one or more batches that share a schema.
It is an Elixir-side aggregation — useful when you have already collected all
batches and want a convenient handle for `schema/1`, `num_rows/1`, and
`batches/1`.

**Stream** is a lazy sequence of batches.  It is the primary output type of
IPC readers, Flight/Flight SQL queries, ADBC statement execution, and Parquet
readers.  Streams implement the `Enumerable` protocol, so `Enum` and `Stream`
functions work directly.

**Schema** describes the column names, types, and nullability of a batch or
table.  **Field** is one column's metadata within a schema.

## Memory model

ExArrow keeps all Arrow data in native (Rust) memory.  Elixir holds only opaque
references.  This means:

- Zero-copy: reading a stream or batch does not copy column data into the BEAM
  heap.
- Explicit materialisation: when you need data in Elixir (e.g. for Explorer or
  Nx), you call a conversion function that performs one copy.
- Resource lifecycle: native resources are released when their Elixir handle is
  garbage-collected.

## Arrow types in ExArrow

The NIF layer reports column types as atoms:

| Atom        | Arrow type |
|-------------|------------|
| `:boolean`  | Boolean    |
| `:int8`     | Int8       |
| `:int16`    | Int16      |
| `:int32`    | Int32      |
| `:int64`    | Int64      |
| `:uint8`    | UInt8      |
| `:uint16`   | UInt16     |
| `:uint32`   | UInt32     |
| `:uint64`   | UInt64     |
| `:float32`  | Float32    |
| `:float64`  | Float64    |
| `:utf8`     | Utf8       |
| `:timestamp`| Timestamp  |

The `ExArrow.Schema.Mapper` module provides bidirectional mapping between these
Arrow types and external type systems (Explorer, Nx).  It is the single
authority for type conversion and is extensible for future targets such as
ExZarr and Dataset.

## When to use what

- **IPC** (`ExArrow.IPC`): Read/write Arrow data from files or binaries.  Good
  for local file exchange and testing.
- **Flight** (`ExArrow.Flight.Client`): Stream Arrow data over gRPC.  Good for
  inter-process and inter-service communication.
- **Flight SQL** (`ExArrow.FlightSQL.Client`): Execute SQL queries against
  Flight SQL servers and receive Arrow-native results.
- **ADBC** (`ExArrow.ADBC`): Connect to databases via ADBC drivers and execute
  SQL with Arrow-native result streams.
- **Parquet** (`ExArrow.Parquet`): Read/write Parquet files directly into Arrow
  batches.
- **Explorer bridge** (`ExArrow.from_dataframe/1`): Convert Explorer DataFrames
  to and from Arrow.
- **Nx bridge** (`ExArrow.from_nx/1`): Convert Nx tensors to and from Arrow.
