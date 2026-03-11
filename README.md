# ExArrow

[![CI](https://github.com/thanos/ex_arrow/actions/workflows/ci.yml/badge.svg)](https://github.com/thanos/ex_arrow/actions/workflows/ci.yml)
[![Hex version](https://img.shields.io/hexpm/v/ex_arrow.svg)](https://hex.pm/packages/ex_arrow)
[![Hex docs](https://img.shields.io/badge/docs-hexdocs.pm-blue)](https://hexdocs.pm/ex_arrow)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)


Native Apache Arrow for the BEAM: IPC streaming, Arrow Flight, and ADBC database bindings. Column data lives in Rust buffers; Elixir holds lightweight opaque handles. Precompiled NIFs for Linux, macOS, and Windows — no Rust required to use.

---

## Contents

- [Why ExArrow was built](#why-exarrow-was-built)
- [What it brings to the Elixir ecosystem](#what-it-brings-to-the-elixir-ecosystem)
- [How ExArrow differs from Explorer, Nx, ADBC, and ExZarr](#how-exarrow-differs-from-explorer-nx-adbc-and-exzarr)
- [Where ExArrow fits](#where-exarrow-fits)
- [What this enables](#what-this-enables)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Livebook tutorials](#livebook-tutorials)
- [IPC: stream and file](#ipc-stream-and-file)
- [Arrow Flight: client and server](#arrow-flight-client-and-server)
- [ADBC: database to Arrow streams](#adbc-database-to-arrow-streams)
- [Parquet: read and write](#parquet-read-and-write)
- [Arrow compute kernels](#arrow-compute-kernels)
- [Using ExArrow with Explorer](#using-exarrow-with-explorer)
- [Using ExArrow with Nx](#using-exarrow-with-nx)
- [Use case examples](#use-case-examples)
- [Benchmarks](#benchmarks)
- [Documentation](#documentation)
- [Development](#development)
- [Roadmap](#roadmap)
  - [Shipped (v0.2.0)](#shipped-v020)
  - [Shipped (v0.3.0)](#shipped-v030)
- [FAQ](#faq)
- [License](#license)

---

## Why ExArrow was built

The Arrow ecosystem has become the de-facto interchange standard for columnar
data. Python, R, Rust, Java, Go, and C++ all speak Arrow natively. Data
warehouses, query engines, stream processors, ML frameworks, and databases
expose Arrow Flight endpoints or ADBC interfaces. The BEAM had no first-class
way to participate in this ecosystem.

ExArrow was written to fill that gap. It gives Elixir and Erlang applications
the same low-level, zero-copy Arrow primitives that the rest of the ecosystem
already takes for granted — without requiring callers to understand NIF memory
management, dirty schedulers, or the Arrow C Data Interface.

The design goal is intentionally narrow: be the reliable Arrow transport and
interchange layer for the BEAM, and let other libraries (Explorer, Nx, etc.)
do the analysis on top of it.

---

## What it brings to the Elixir Ecosystem

Prior to ExArrow, an Elixir application that needed to exchange data with a
Flight server, query a database via ADBC, or read/write an Arrow IPC file had
three options: shell out to Python, implement the protocol manually in Elixir
(row-by-row, with all the copying that entails), or simply not do it.

ExArrow adds:

- **IPC reading and writing** — Arrow stream and file formats, from binary or
a file path, in both directions. Read a file produced by PyArrow, DuckDB, or
Pandas; write a file for the same consumers. No format conversion needed.
- **Arrow Flight client and server** — Connect to Dremio, InfluxDB IOx,
Snowflake Flight endpoints, or any custom Flight service. Run an in-process
echo server for testing. Transfer Arrow streams over gRPC with one API call.
- **ADBC database connectivity** — Execute SQL against any ADBC-compatible
database (SQLite, PostgreSQL, DuckDB, BigQuery, Snowflake, and more) and
receive the results as a lazy Arrow stream — never materialising rows into
BEAM terms unless you ask for them.
- **Zero-copy streaming** — Column buffers are allocated once in Rust and held
there until consumed. The BEAM scheduler is never stalled on large copies.
Dirty NIF schedulers are used for blocking I/O.
- **A uniform stream abstraction** — `ExArrow.Stream` works identically for
IPC, Flight, and ADBC results. Code that processes batches does not know or
care where the data came from.

---

## How ExArrow differs from Explorer, Nx, ADBC, and ExZarr

These libraries are complementary, not competing. Each has a distinct role.


| Library                 | Role                                                                                                    | Overlap with ExArrow                                                                                                                                             |
| ----------------------- | ------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Explorer**            | In-memory dataframe analysis (filter, group, sort, plot). Backed by Polars/Arrow internally.            | Explorer can load/dump Arrow IPC streams. ExArrow is the transport; Explorer is the analysis layer.                                                              |
| **Nx**                  | Numerical computing and tensor operations (multi-dimensional arrays, GPU support, ML).                  | `ExArrow.Nx` (v0.3+) converts Arrow numeric columns to `Nx.Tensor` values by sharing the raw byte buffer — no list materialisation. Add `{:nx, "~> 0.9"}` to enable. |
| **adbc** (livebook-dev) | Elixir wrapper around the ADBC C library for driver management — downloading and configuring drivers.   | ExArrow uses `adbc` optionally for driver download; `adbc`'s core purpose is driver lifecycle, not Arrow streaming or Flight.                                    |
| **ExZarr**              | Read/write Zarr v2/v3 chunked array format (used in climate science, genomics, cloud-native ND arrays). | Zarr and Arrow are complementary storage formats. ExZarr addresses ND chunk storage; ExArrow addresses columnar interchange and network transport.               |


**In short:** ExArrow is a transport and interchange library. It moves Arrow
data between processes, databases, services, and files as efficiently as
possible. It does not analyse, transform, or visualise data — that is the job
of Explorer, Nx, or your own application logic.

---

## Where ExArrow fits

```mermaid
flowchart TB
    App("Your Elixir Application")

    App --> Explorer("Explorer\ndataframes & analysis")
    App --> Nx("Nx\ntensors & ML")
    App --> ExArrow("ExArrow\nIPC · Flight · ADBC")
    App --> ExZarr("ExZarr\nZarr chunked arrays")

    ExArrow --> IPC("Arrow IPC\nstream & file")
    ExArrow --> FlightSvr("Arrow Flight\ngRPC server")
    ExArrow --> ADBCDrv("ADBC\ndriver")

    IPC -. "interop via IPC binary" .-> Explorer

    FlightSvr --> FlightSvcs("Dremio · InfluxDB IOx\nDuckDB · Snowflake")
    ADBCDrv   --> Databases("PostgreSQL · SQLite\nDuckDB · BigQuery")

    classDef app      fill:#1a1a2e,stroke:#4a90d9,color:#e0e0e0,rx:6
    classDef lib      fill:#16213e,stroke:#4a90d9,color:#e0e0e0,rx:6
    classDef proto    fill:#0f3460,stroke:#4a90d9,color:#e0e0e0,rx:6
    classDef external fill:#1a1a2e,stroke:#888,color:#aaa,rx:6,stroke-dasharray:4 4

    class App app
    class Explorer,Nx,ExArrow,ExZarr lib
    class IPC,FlightSvr,ADBCDrv proto
    class FlightSvcs,Databases external
```

ExArrow sits at the boundary between the BEAM and the Arrow ecosystem. It
speaks the protocols that data infrastructure uses — IPC, Flight, ADBC — and
surfaces them as idiomatic Elixir APIs. Explorer and Nx sit above it and
consume the data it delivers.

---

## What this enables

- **Elixir as a data pipeline node.** Read Arrow IPC from Kafka, HTTP, or a
socket; apply lightweight routing or filtering; forward via Flight or write
to file — without ever copying column data into BEAM terms.
- **Zero-copy query results.** Run SQL against PostgreSQL, DuckDB, SQLite, or
BigQuery via ADBC. The result stream is backed by native Arrow buffers. A
100-million-row result set uses minimal BEAM heap regardless of size.
- **Interop with the Python/R data world.** Read files produced by PyArrow,
Pandas, or Polars. Write files that DuckDB, R's arrow package, or any Arrow
consumer can read. No CSV conversion, no schema translation.
- **First-class Flight client.** Connect to Dremio, InfluxDB IOx, or any
service that exposes an Arrow Flight endpoint. List flights, fetch schemas,
stream data, or call custom actions — from a Phoenix controller, a GenServer,
or a Livebook cell.
- **Benchmarked, observable performance.** The included Benchee suite
quantifies the zero-copy advantage and publishes results per commit at
[thanos.github.io/ex_arrow/dev/bench](https://thanos.github.io/ex_arrow/dev/bench/).

---

## Requirements

- Elixir ~> 1.14 (OTP 25 / NIF 2.15 and OTP 26+ / NIF 2.16)

---

## Installation

Add the dependency:

```elixir
def deps do
  [{:ex_arrow, "~> 0.4.0"}]
end
```

**Using precompiled NIFs (default)**
After `mix deps.get` and `mix compile`, ExArrow downloads a prebuilt NIF for
your platform from the project's GitHub releases. No Rust or C toolchain is
required. Supported platforms: Linux x8664/aarch64, macOS x8664/arm64,
Windows x8664.

**Building from source**
If no precompiled NIF exists for your platform, or you are developing ExArrow
itself, set `EX_ARROW_BUILD=1` and have Rust installed:

```bash
EX_ARROW_BUILD=1 mix deps.get
EX_ARROW_BUILD=1 mix compile
```

The optional dependency `{:rustler, "~> 0.32.0", optional: true}` is required
for source builds and is already listed in ExArrow's own `mix.exs`.

For **path dependencies** (e.g. Livebook or `Mix.install`), add `rustler`
explicitly and have Rust available:

```elixir
Mix.install([
  {:ex_arrow, path: "/path/to/ex_arrow"},
  {:rustler, "~> 0.37.3", optional: true}
])
```

Alternatively, use the published Hex package so the precompiled NIF is used
and no Rust is needed: `Mix.install([{:ex_arrow, "~> 0.4.0"}])`.

---

## Quick start

**Read an Arrow IPC stream:**

```elixir
{:ok, stream} = ExArrow.IPC.Reader.from_file("/path/to/data.arrow")
{:ok, schema} = ExArrow.Stream.schema(stream)
fields = ExArrow.Schema.fields(schema)

case ExArrow.Stream.next(stream) do
  %ExArrow.RecordBatch{} = batch -> IO.inspect(ExArrow.RecordBatch.num_rows(batch))
  nil -> :done
  {:error, msg} -> IO.puts("Error: #{msg}")
end
```

**Connect to an Arrow Flight server:**

```elixir
{:ok, client} = ExArrow.Flight.Client.connect("localhost", 9999, [])
{:ok, stream} = ExArrow.Flight.Client.do_get(client, "my_ticket")
{:ok, schema} = ExArrow.Stream.schema(stream)
batch = ExArrow.Stream.next(stream)
```

**Query a database with ADBC:**

```elixir
{:ok, db} = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_sqlite", uri: ":memory:")
{:ok, conn} = ExArrow.ADBC.Connection.open(db)
{:ok, stmt} = ExArrow.ADBC.Statement.new(conn, "SELECT 1 AS n")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)
{:ok, schema} = ExArrow.Stream.schema(stream)
batch = ExArrow.Stream.next(stream)
```

---

## Livebook tutorials

Interactive notebooks (open in [Livebook](https://livebook.dev)):

- **[Quick start](livebook/00_quickstart.livemd)** — IPC, Flight, and ADBC in one notebook.
- **[01 IPC](livebook/01_ipc.livemd)** — Stream vs file format, read/write, schema, Explorer interop.
- **[02 Flight](livebook/02_flight.livemd)** — Echo server, client, listflights, getschema, actions.
- **[03 ADBC](livebook/03_adbc.livemd)** — Database, Connection, Statement, Stream, metadata APIs.

See [livebook/README.md](livebook/README.md) for run instructions.

---

## IPC: stream and file

**Stream (sequential)** — from binary or file path:

```elixir
{:ok, stream} = ExArrow.IPC.Reader.from_binary(ipc_bytes)
{:ok, stream} = ExArrow.IPC.Reader.from_file("/data/events.arrow")

{:ok, schema} = ExArrow.Stream.schema(stream)

Stream.repeatedly(fn -> ExArrow.Stream.next(stream) end)
|> Enum.take_while(&(&1 != nil and not match?({:error, _}, &1)))
```

**Write to binary or file:**

```elixir
{:ok, binary} = ExArrow.IPC.Writer.to_binary(schema, batches)
:ok = ExArrow.IPC.Writer.to_file("/out/result.arrow", schema, batches)
```

**File format (random access):**

```elixir
{:ok, file} = ExArrow.IPC.File.from_file("/data/large.arrow")
{:ok, schema} = ExArrow.IPC.File.schema(file)
n = ExArrow.IPC.File.batch_count(file)
{:ok, batch} = ExArrow.IPC.File.get_batch(file, 0)
```

---

## Arrow Flight: client and server

**Start the built-in echo server:**

```elixir
{:ok, server} = ExArrow.Flight.Server.start_link(9999)
{:ok, port} = ExArrow.Flight.Server.port(server)
:ok = ExArrow.Flight.Server.stop(server)
```

**Transfer data:**

```elixir
{:ok, client} = ExArrow.Flight.Client.connect("localhost", 9999, [])

# Put under a named ticket (multi-dataset routing, added in v0.2)
:ok = ExArrow.Flight.Client.do_put(client, schema, [batch1, batch2],
        descriptor: {:cmd, "sales_2024"})

# Default ticket "echo" when no descriptor is given
:ok = ExArrow.Flight.Client.do_put(client, schema, [batch1, batch2])

{:ok, stream} = ExArrow.Flight.Client.do_get(client, "sales_2024")
batch = ExArrow.Stream.next(stream)
```

**Metadata:**

```elixir
{:ok, flights} = ExArrow.Flight.Client.list_flights(client, <<>>)
{:ok, info}    = ExArrow.Flight.Client.get_flight_info(client, {:cmd, "echo"})
{:ok, schema}  = ExArrow.Flight.Client.get_schema(client, {:cmd, "echo"})
{:ok, actions} = ExArrow.Flight.Client.list_actions(client)
{:ok, ["pong"]} = ExArrow.Flight.Client.do_action(client, "ping", <<>>)
# List all stored datasets (v0.2+)
{:ok, tickets} = ExArrow.Flight.Client.do_action(client, "list_tickets", <<>>)
```

**TLS (v0.2+):**

```elixir
# One-way TLS — server presents a certificate
{:ok, server} = ExArrow.Flight.Server.start_link(9999,
  tls: [cert_pem: File.read!("server.crt"), key_pem: File.read!("server.key")])

# Mutual TLS — both sides present certificates
{:ok, server} = ExArrow.Flight.Server.start_link(9999,
  tls: [cert_pem: cert, key_pem: key, ca_cert_pem: File.read!("ca.crt")])

# Client with custom CA
{:ok, client} = ExArrow.Flight.Client.connect("host", 9999,
  tls: [ca_cert_pem: File.read!("ca.crt")])
```

Plaintext continues to work as before. Products that speak Arrow Flight
include Dremio, InfluxDB IOx, and custom analytics servers.

---

## ADBC: database to Arrow streams

**SQLite in-memory:**

```elixir
{:ok, db}   = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_sqlite", uri: ":memory:")
{:ok, conn} = ExArrow.ADBC.Connection.open(db)
{:ok, stmt} = ExArrow.ADBC.Statement.new(conn, "SELECT 1 AS n, 'hello' AS s")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)
batch = ExArrow.Stream.next(stream)
```

**PostgreSQL:**

```elixir
{:ok, db} = ExArrow.ADBC.Database.open(
  driver_name: "adbc_driver_postgresql",
  uri: "postgresql://user:pass@localhost:5432/mydb"
)
{:ok, conn}   = ExArrow.ADBC.Connection.open(db)
{:ok, stmt}   = ExArrow.ADBC.Statement.new(conn, "SELECT id, name FROM users")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)
```

**DuckDB:**

DuckDB's ADBC driver uses `path` (not `uri`) for the database location, and
requires an explicit `entrypoint`:

```elixir
{:ok, db} = ExArrow.ADBC.Database.open(
  driver_path: "/usr/local/lib/libduckdb.so",
  entrypoint: "duckdb_adbc_init",
  path: ":memory:"                             # or a file path
)
{:ok, conn}   = ExArrow.ADBC.Connection.open(db)
{:ok, stmt}   = ExArrow.ADBC.Statement.new(conn, "SELECT 42 AS answer")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)
batches = ExArrow.Stream.to_list(stream)
```

**Connection pool (v0.2+):**

`ExArrow.ADBC.ConnectionPool` is a NimblePool-backed pool that reuses open
connections. Start it under a supervisor with a named `DatabaseServer`:

```elixir
children = [
  {ExArrow.ADBC.DatabaseServer,
    name: :mydb,
    driver_name: "adbc_driver_postgresql",
    uri: "postgresql://localhost/mydb"},
  {ExArrow.ADBC.ConnectionPool,
    name: :mypool, database: :mydb, pool_size: 4}
]
Supervisor.start_link(children, strategy: :one_for_one)

# Query from anywhere in the application:
{:ok, stream} = ExArrow.ADBC.ConnectionPool.query(:mypool,
                  "SELECT * FROM events WHERE day = today()")

# Multi-statement checkout:
ExArrow.ADBC.ConnectionPool.with_connection(:mypool, fn conn ->
  {:ok, stmt} = ExArrow.ADBC.Statement.new(conn, "SELECT count(*) FROM users")
  ExArrow.ADBC.Statement.execute(stmt)
end)
```

Requires `{:nimble_pool, "~> 1.1"}` in your `mix.exs`.

**Metadata:**

```elixir
{:ok, types_stream} = ExArrow.ADBC.Connection.get_table_types(conn)
{:ok, schema}       = ExArrow.ADBC.Connection.get_table_schema(conn, nil, nil, "users")
{:ok, objs_stream}  = ExArrow.ADBC.Connection.get_objects(conn, depth: "tables")
```

**Optional driver download via the `adbc` package:**

```elixir
# Add {:adbc, "~> 0.9"} to deps, then:
Adbc.download_driver!(:sqlite)
{:ok, db} = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_sqlite", uri: ":memory:")
```

Or use the convenience helper which calls `Adbc.download_driver!/1` when the
package is available: `ExArrow.ADBC.DriverHelper.ensure_driver_and_open/2`.

---

## Parquet: read and write

**Read from file:**

```elixir
{:ok, stream}  = ExArrow.Parquet.Reader.from_file("/data/events.parquet")
{:ok, schema}  = ExArrow.Stream.schema(stream)
batches = ExArrow.Stream.to_list(stream)
```

**Read from binary (e.g. downloaded from S3):**

```elixir
{:ok, stream} = ExArrow.Parquet.Reader.from_binary(parquet_bytes)
batch = ExArrow.Stream.next(stream)
```

**Write to file:**

```elixir
:ok = ExArrow.Parquet.Writer.to_file("/out/result.parquet", schema, batches)
```

**Write to binary (e.g. upload to object storage):**

```elixir
{:ok, parquet_bytes} = ExArrow.Parquet.Writer.to_binary(schema, batches)
```

Parquet streams share the same `ExArrow.Stream` interface as IPC and ADBC
streams — `schema/1`, `next/1`, and `to_list/1` all work identically.

---

## Arrow compute kernels

All operations run entirely in native memory. Results are new
`ExArrow.RecordBatch` handles — no data is copied into BEAM terms.

**Filter rows** using a boolean column as a predicate:

```elixir
# Build a predicate batch where the first column is a boolean mask
{:ok, filtered} = ExArrow.Compute.filter(batch, predicate_batch)
```

**Project (select) columns:**

```elixir
{:ok, slim} = ExArrow.Compute.project(batch, ["id", "name", "score"])
```

**Sort by column:**

```elixir
{:ok, sorted_asc}  = ExArrow.Compute.sort(batch, "score")
{:ok, sorted_desc} = ExArrow.Compute.sort(batch, "score", ascending: false)
```

---

## Using ExArrow with Explorer

[Explorer](https://hex.pm/packages/explorer) handles in-memory analysis.
ExArrow handles streaming and transport. Add `{:explorer, "~> 0.8"}` to your
`mix.exs` to enable the bridge.

**ExArrow → Explorer** (one call, no boilerplate):

```elixir
{:ok, stream} = ExArrow.IPC.Reader.from_file("/data/source.arrow")
{:ok, df}     = ExArrow.Explorer.from_stream(stream)
Explorer.DataFrame.filter(df, score > 0.9)
```

**Single batch → DataFrame:**

```elixir
batch = ExArrow.Stream.next(stream)
{:ok, df} = ExArrow.Explorer.from_record_batch(batch)
```

**Explorer → ExArrow** (e.g. to write to Parquet or send via Flight):

```elixir
df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
{:ok, stream} = ExArrow.Explorer.to_stream(df)
:ok = ExArrow.Parquet.Writer.to_file("/out/result.parquet",
        ExArrow.Stream.schema(stream) |> elem(1),
        ExArrow.Stream.to_list(stream))
```

**Manual path** (IPC round-trip, still works):

```elixir
binary = Explorer.DataFrame.dump_ipc_stream!(df)
{:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
batch = ExArrow.Stream.next(stream)
```

---

## Using ExArrow with Nx

[Nx](https://hex.pm/packages/nx) provides numerical computing and tensor
operations.  ExArrow bridges Arrow columns to Nx tensors by sharing raw byte
buffers — no list materialisation occurs.

Add `{:nx, "~> 0.9"}` to your `mix.exs` to enable this module.

**Column to tensor:**

```elixir
{:ok, tensor} = ExArrow.Nx.column_to_tensor(batch, "price")
mean = tensor |> Nx.mean() |> Nx.to_number()
```

**All numeric columns to a map of tensors:**

```elixir
{:ok, tensors} = ExArrow.Nx.to_tensors(batch)
sorted_scores = tensors["score"] |> Nx.sort()
```

**Tensor back to a single-column RecordBatch:**

```elixir
weights = Nx.tensor([0.1, 0.2, 0.7], type: {:f, 64})
{:ok, batch} = ExArrow.Nx.from_tensor(weights, "weights")
```

Non-numeric columns (strings, booleans, timestamps) are silently skipped by
`to_tensors/1`. Unsupported Nx dtypes (e.g. `:bf16`) return `{:error, ...}`.

---

## Use case examples

### Ingest IPC from HTTP or Kafka and write to file

```elixir
ipc_bytes = get_arrow_stream_from_http_or_kafka()
{:ok, stream} = ExArrow.IPC.Reader.from_binary(ipc_bytes)
{:ok, schema} = ExArrow.Stream.schema(stream)
batches =
  Stream.repeatedly(fn -> ExArrow.Stream.next(stream) end)
  |> Enum.take_while(fn nil -> false; {:error, _} -> false; _ -> true end)
:ok = ExArrow.IPC.Writer.to_file("/data/ingested.arrow", schema, batches)
```

### Query a database and forward via Flight

```elixir
{:ok, db}     = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_sqlite", uri: "file:report.db")
{:ok, conn}   = ExArrow.ADBC.Connection.open(db)
{:ok, stmt}   = ExArrow.ADBC.Statement.new(conn, "SELECT * FROM sales WHERE year = 2024")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)
{:ok, schema} = ExArrow.Stream.schema(stream)
batches =
  Stream.repeatedly(fn -> ExArrow.Stream.next(stream) end)
  |> Enum.take_while(fn nil -> false; {:error, _} -> false; _ -> true end)

{:ok, client} = ExArrow.Flight.Client.connect("flight.example.com", 32010, [])
:ok = ExArrow.Flight.Client.do_put(client, schema, batches)
```

### Connect to Dremio, InfluxDB IOx, or a custom Flight service

```elixir
{:ok, client}  = ExArrow.Flight.Client.connect("dremio.example.com", 32010, connect_timeout_ms: 5_000)
{:ok, flights} = ExArrow.Flight.Client.list_flights(client, <<>>)
{:ok, stream}  = ExArrow.Flight.Client.do_get(client, ticket_from_service)
batch = ExArrow.Stream.next(stream)
```

### Interchange with Python or R

```elixir
# Read a file written by PyArrow or Pandas
{:ok, file}   = ExArrow.IPC.File.from_file("/data/from_python.arrow")
{:ok, schema} = ExArrow.IPC.File.schema(file)
n = ExArrow.IPC.File.batch_count(file)
for i <- 0..(n - 1) do
  {:ok, batch} = ExArrow.IPC.File.get_batch(file, i)
  # process batch
end

# Write for Python, R, or DuckDB
:ok = ExArrow.IPC.Writer.to_file("/data/for_python.arrow", schema, batches)
```

### End-to-end: ADBC to Flight

```elixir
{:ok, db}     = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_postgresql",
                  uri: "postgresql://localhost/mydb")
{:ok, conn}   = ExArrow.ADBC.Connection.open(db)
{:ok, stmt}   = ExArrow.ADBC.Statement.new(conn, "SELECT * FROM sensor_readings")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)
{:ok, schema} = ExArrow.Stream.schema(stream)
batches =
  Stream.repeatedly(fn -> ExArrow.Stream.next(stream) end)
  |> Enum.take_while(fn nil -> false; {:error, _} -> false; _ -> true end)

{:ok, client} = ExArrow.Flight.Client.connect("flight.internal", 32010, [])
:ok = ExArrow.Flight.Client.do_put(client, schema, batches)
```

---

## Benchmarks

ExArrow ships a Benchee-based benchmark suite in `bench/` that quantifies the
zero-copy streaming advantage over row-oriented alternatives.

### Running locally

Benchee is a `:dev`-only dependency; `MIX_ENV=dev` is required.

```bash
MIX_ENV=dev mix run bench/ipc_read_bench.exs   # single suite
MIX_ENV=dev mix run bench/run_all.exs           # all suites
MIX_ENV=dev mix bench                           # convenience alias
```

HTML reports are written to `bench/output/` (gitignored).

### Suites


| File                  | What it measures                                                               |
| --------------------- | ------------------------------------------------------------------------------ |
| `ipc_read_bench.exs`  | Stream handle vs materialise — BEAM memory saved by keeping data native        |
| `ipc_write_bench.exs` | IPC serialisation vs `:erlang.term_to_binary` — columnar vs row-oriented write |
| `flight_bench.exs`    | Flight doput / doget / roundtrip latency with in-process server                |
| `adbc_bench.exs`      | Stream handle vs schema peek vs full collect                                   |
| `pipeline_bench.exs`  | End-to-end: IPC file on disk to Flight doput without materialising in BEAM     |


### Published results

Results from every push to `main` are published at:
**[https://thanos.github.io/ex_arrow/dev/bench/](https://thanos.github.io/ex_arrow/dev/bench/)**

The CI workflow posts a PR alert comment when any scenario regresses more than
20% relative to the previous baseline.

---

## Documentation

- [Memory model](docs/memory_model.md) — handles, copying rules, NIF scheduling
- [IPC guide](docs/ipc_guide.md) — stream vs file, types, limitations
- [Parquet guide](docs/parquet_guide.md) — read/write Parquet, streaming, comparison with IPC
- [Compute guide](docs/compute_guide.md) — filter, project, sort, chaining kernels
- [Flight guide](docs/flight_guide.md) — server, client, timeouts, security
- [ADBC guide](docs/adbc_guide.md) — driver loading, metadata, binding
- [Benchmarks guide](docs/benchmarks.md) — suites, CI publishing, interpreting results

API reference: `mix docs` or [hexdocs.pm/ex_arrow](https://hexdocs.pm/ex_arrow).

---

## Development

```bash
mix deps.get
EX_ARROW_BUILD=1 mix compile    # build NIF from source
mix test                         # exclude :adbc / :adbc_package tags if no drivers installed
mix docs                         # generate ExDoc
MIX_ENV=dev mix bench            # run benchmark suite
```

Local CI script (runs format, credo, dialyzer, tests, coverage, docs):

```bash
script/ci
```

---

## Roadmap

The items below represent the planned direction for ExArrow. Contributions are
welcome for any of them.

### Shipped (v0.2.0)

- **TLS for Arrow Flight** — one-way and mutual TLS on `Server.start_link/2`.
- **Multi-dataset Flight server** — datasets stored by ticket; `do_put` accepts a `:descriptor` option.
- **ADBC connection pool** — `ConnectionPool` and `DatabaseServer` backed by NimblePool.
- **Broader integration test matrix** — PostgreSQL 14/15/16 and DuckDB 1.1.3/1.2.0 tested in CI.

### Shipped (v0.3.0)

- **Arrow compute kernels** — `ExArrow.Compute.filter/2`, `project/2`, `sort/3`: filter, select columns, and sort record batches entirely in native Arrow buffers without materialising data into BEAM terms.
- **Parquet support** — `ExArrow.Parquet.Reader` and `ExArrow.Parquet.Writer`: read and write Parquet files and in-memory binaries via the Arrow Rust `parquet` crate; streaming API compatible with IPC and ADBC streams.
- **Explorer bridge module** — `ExArrow.Explorer`: direct conversion between `ExArrow.Stream` / `ExArrow.RecordBatch` and `Explorer.DataFrame` without writing manual IPC code. Add `{:explorer, "~> 0.8"}` to enable.
- **Nx bridge module** — `ExArrow.Nx`: convert Arrow columns to `Nx.Tensor` values and back by sharing raw byte buffers. No list materialisation. Add `{:nx, "~> 0.9"}` to enable.

### Shipped (v0.4.0)

- **Arrow C Data Interface** — `ExArrow.CDI`: export a RecordBatch to
  `FFI_ArrowArray` + `FFI_ArrowSchema` C structs and import them back.
  Pointer addresses are exposed for interop with any CDI-compatible runtime
  (Polars, DuckDB, etc.) running in the same process.  Provides the foundation
  for a future zero-copy Explorer bridge that bypasses IPC entirely.
- **Nx bridge — multi-column batch from tensors** — `ExArrow.Nx.from_tensors/1`
  builds a multi-column RecordBatch from a `%{col_name => Nx.Tensor}` map in a
  single NIF call, complementing the existing per-column `from_tensor/2`.
- **Parquet row-group streaming** — `ExArrow.Parquet.Reader.from_file/1` and
  `from_binary/1` now decode row groups **lazily** via `Stream.next/1` instead
  of eagerly loading the entire file, dramatically reducing peak memory for
  large Parquet files.

### Near-term (v0.5)

### Longer-term

- **Flight SQL** — the Flight SQL protocol for databases that expose it
(DuckDB, CockroachDB, Dremio).
- **Streaming writes to Delta Lake** — sink for data pipeline nodes.
- **OTel / telemetry integration** — `:telemetry` events for IPC read/write
throughput, Flight request latency, and ADBC query duration.
- **Windows aarch64 precompiled NIF** — once GitHub-hosted Windows arm64
runners are generally available.

---

## FAQ

**When should I use ExArrow?**
Use ExArrow when you need to read or write Arrow IPC (stream or file), connect
to an Arrow Flight server (Dremio, InfluxDB IOx, custom), or run SQL via ADBC
and receive Arrow result streams. Good fit for data pipelines, ETL, and
interchange with systems that already speak Arrow.

**When should I not use ExArrow?**
Do not use it as a dataframe or query engine. For in-memory analysis, filtering,
grouping, and plotting, use Explorer. Do not use it as a replacement for Ecto
when you only need normal SQL results. For Parquet-only workflows with no
Flight/ADBC, consider Explorer's Parquet support first.

**Can I use ExArrow and Explorer together?**
Yes. Add `{:explorer, "~> 0.8"}` to your `mix.exs` and use `ExArrow.Explorer`
(v0.3+) for one-call conversion: `ExArrow.Explorer.from_stream/1`,
`from_record_batch/1`, `to_stream/1`. The bridge uses Arrow IPC internally;
you can also do the round-trip manually with `ExArrow.IPC.Writer.to_binary/2`
and `Explorer.DataFrame.load_ipc_stream!/1`.

**Why do I get a 404 or "couldn't fetch NIF" on compile?**
Precompiled NIFs are hosted on GitHub releases. If you are on an unsupported
platform or an unreleased version, the download fails. Set `EX_ARROW_BUILD=1`,
install Rust, and run `mix compile` to build from source.

**Is Arrow Flight over TLS supported?**
Yes, as of v0.2. Pass `tls: [cert_pem: ..., key_pem: ...]` to
`Server.start_link/2` for one-way TLS, or add `ca_cert_pem:` for mutual TLS.
The client automatically selects TLS for non-loopback hosts; use
`tls: [ca_cert_pem: pem]` for a custom CA.

**Which ADBC drivers are supported?**
Any ADBC driver that provides a shared library — for example
`adbc_driver_sqlite`, `adbc_driver_postgresql`, or the DuckDB ADBC driver. You
must install the driver and pass its path (`:driver_path`), or ensure the driver
manager can find it by name (`:driver_name`). Some drivers require extra options:
DuckDB expects `entrypoint: "duckdb_adbc_init"` and uses `path:` rather than
`uri:` for the database location. Metadata and binding support depend on the
individual driver.

---

## License

MIT. See [LICENSE](LICENSE) for details. Copyright (c) 2025 Thanos Vassilakis.
