# ExArrow

Apache Arrow support for the BEAM: IPC (stream and file), Arrow Flight (client and server), and ADBC bindings. Arrow data stays in native memory; Elixir holds opaque handles. Precompiled NIFs for Linux, macOS, and Windows (no Rust required).

**Author:** Thanos Vassilakis  
**Package:** [Hex](https://hex.pm/packages/ex_arrow) | **Source:** [GitHub](https://github.com/thanos/ex_arrow) | **Docs:** [hexdocs.pm/ex_arrow](https://hexdocs.pm/ex_arrow)

ExArrow provides a native core (Arrow in Rust buffers, Elixir handles), a stable API (Schema, RecordBatch, Table, Stream), and three pillars: IPC, Flight, and ADBC. It is not a full dataframe library (like Polars/Explorer) and not a replacement for Ecto; it focuses on interchange and streaming.

---

## Requirements

- Elixir ~> 1.14 (OTP 25 / NIF 2.15 and OTP 26+ / NIF 2.16)

## Installation

Add the dependency:

```elixir
def deps do
  [{:ex_arrow, "~> 0.1.0"}]
end
```

**Using precompiled NIFs (default)**  
After `mix deps.get` and `mix compile`, ExArrow downloads a prebuilt NIF for your platform from the project’s GitHub releases. No Rust or C toolchain is required. This is the recommended way to use ExArrow on supported platforms (Linux x86_64/aarch64, macOS x86_64/arm64, Windows x86_64).

**Building from source**  
If no precompiled NIF exists for your platform (e.g. FreeBSD, or an older OS), or you are developing ExArrow itself, set `EX_ARROW_BUILD=1` and have Rust installed. Then `mix compile` will build the NIF from the crate in `native/ex_arrow_native`. **The optional dependency `rustler` is required for this path:** RustlerPrecompiled needs it to trigger the build. In a normal Mix project, `ex_arrow` already lists `{:rustler, "~> 0.32.0", optional: true}` in its own `mix.exs`, so `mix deps.get` brings it in. If you use ExArrow as a **path dependency** (e.g. `{:ex_arrow, path: ".."}` in Livebook or `Mix.install`), the precompiled NIF may not be used (e.g. unreleased version or placeholder release URL), so the build-from-source path runs and **you must add `rustler` to your deps** and have Rust installed. For example in Livebook:

```elixir
Mix.install([
  {:ex_arrow, path: "/path/to/ex_arrow"},
  {:rustler, "~> 0.37.3", optional: true}
])
```

Then run the notebook with Rust available so the NIF can compile. Alternatively, use the published Hex package in Livebook so the precompiled NIF is downloaded and no Rust or rustler is needed: `Mix.install([{:ex_arrow, "~> 0.1.0"}])`.

In a normal Mix project when building from source:

```bash
EX_ARROW_BUILD=1 mix deps.get
EX_ARROW_BUILD=1 mix compile
```

---

## Quick start

Read an Arrow IPC stream from a file and consume batches:

```elixir
{:ok, stream} = ExArrow.IPC.Reader.from_file("/path/to/data.arrow")
{:ok, schema} = ExArrow.Stream.schema(stream)
fields = ExArrow.Schema.fields(schema)

# One batch at a time
case ExArrow.Stream.next(stream) do
  %ExArrow.RecordBatch{} = batch -> IO.inspect(ExArrow.RecordBatch.num_rows(batch))
  nil -> :done
  {:error, msg} -> IO.puts("Error: #{msg}")
end
```

Connect to an Arrow Flight server and fetch a stream:

```elixir
{:ok, client} = ExArrow.Flight.Client.connect("localhost", 9999, [])
{:ok, stream} = ExArrow.Flight.Client.do_get(client, "echo")
{:ok, schema} = ExArrow.Stream.schema(stream)
batch = ExArrow.Stream.next(stream)
```

Query a database with ADBC (e.g. SQLite) and get Arrow result batches:

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
- **[02 Flight](livebook/02_flight.livemd)** — Echo server, client, list_flights, get_schema, actions.
- **[03 ADBC](livebook/03_adbc.livemd)** — Database → Connection → Statement → Stream, metadata APIs.

See [livebook/README.md](livebook/README.md) for an index and run instructions.

---

## What ExArrow provides

| Area | Description |
|------|-------------|
| **IPC** | Read/write Arrow stream format (binary or file). Random-access file format (schema, batch count, get batch by index). |
| **Flight** | gRPC client and built-in echo server. do_put/do_get, list_flights, get_flight_info, get_schema, list_actions, do_action. Plaintext HTTP/2. |
| **ADBC** | Open database by driver path or name; execute SQL; get Arrow result stream. Metadata APIs (get_table_types, get_table_schema, get_objects) and Statement.bind where the driver supports them. |

Data lives in Rust/Arrow buffers. Elixir gets Schema, RecordBatch, Table, Stream handles. No BEAM heap copy unless you explicitly request data. Long-running NIF work uses dirty schedulers.

---

## IPC: stream and file

**Stream (sequential)** — from binary (e.g. socket, HTTP body) or file path:

```elixir
# From binary
{:ok, stream} = ExArrow.IPC.Reader.from_binary(ipc_bytes)

# From file
{:ok, stream} = ExArrow.IPC.Reader.from_file("/data/events.arrow")

# Schema without consuming
{:ok, schema} = ExArrow.Stream.schema(stream)
fields = ExArrow.Schema.fields(schema)

# Consume batches
Stream.repeatedly(fn -> ExArrow.Stream.next(stream) end)
|> Enum.take_while(&(&1 != nil and not match?({:error, _}, &1)))
```

**Write stream to binary or file** (schema + list of record batches):

```elixir
{:ok, binary} = ExArrow.IPC.Writer.to_binary(schema, batches)
:ok = ExArrow.IPC.Writer.to_file("/out/result.arrow", schema, batches)
```

**File format (random access)** — when you need batch count or access by index:

```elixir
{:ok, file} = ExArrow.IPC.File.from_file("/data/large.arrow")
{:ok, schema} = ExArrow.IPC.File.schema(file)
n = ExArrow.IPC.File.batch_count(file)
{:ok, batch} = ExArrow.IPC.File.get_batch(file, 0)
rows = ExArrow.RecordBatch.num_rows(batch)
```

---

## Arrow Flight: client and server

**Start the built-in echo server** (stores last do_put, serves it on do_get with ticket `"echo"`):

```elixir
{:ok, server} = ExArrow.Flight.Server.start_link(9999, [])
{:ok, port} = ExArrow.Flight.Server.port(server)
# ... later
:ok = ExArrow.Flight.Server.stop(server)
```

**Connect and transfer data**:

```elixir
{:ok, client} = ExArrow.Flight.Client.connect("localhost", 9999, [])

# Upload schema + batches
:ok = ExArrow.Flight.Client.do_put(client, schema, [batch1, batch2])

# Download by ticket
{:ok, stream} = ExArrow.Flight.Client.do_get(client, "echo")
{:ok, schema} = ExArrow.Stream.schema(stream)
batch = ExArrow.Stream.next(stream)
```

**List flights and metadata**:

```elixir
{:ok, flights} = ExArrow.Flight.Client.list_flights(client, <<>>)
{:ok, info} = ExArrow.Flight.Client.get_flight_info(client, {:cmd, "echo"})
{:ok, schema} = ExArrow.Flight.Client.get_schema(client, {:cmd, "echo"})
{:ok, actions} = ExArrow.Flight.Client.list_actions(client)
{:ok, ["pong"]} = ExArrow.Flight.Client.do_action(client, "ping", <<>>)
```

Flight is plaintext only in this release. Use on localhost or trusted networks. Products that speak Arrow Flight include Dremio, InfluxDB IOx, and custom analytics servers; connect to their host/port and use the same client API with the appropriate ticket or descriptor.

---

## ADBC: database to Arrow streams

Open by **driver path** or **driver name** (with optional URI). Then connection, statement, execute to a stream.

**SQLite (in-memory)**:

```elixir
{:ok, db} = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_sqlite", uri: ":memory:")
{:ok, conn} = ExArrow.ADBC.Connection.open(db)
{:ok, stmt} = ExArrow.ADBC.Statement.new(conn, "SELECT 1 AS n, 'hello' AS s")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)

{:ok, schema} = ExArrow.Stream.schema(stream)
batch = ExArrow.Stream.next(stream)
# ... consume until nil
```

**SQLite (file)** — pass path to the driver shared library, or use name + URI:

```elixir
# By path (e.g. Homebrew on macOS)
{:ok, db} = ExArrow.ADBC.Database.open("/usr/local/lib/libadbc_driver_sqlite.dylib")
# Then set URI via connection options if the driver requires it; or use driver_name + uri for ":file:path.db"

# By name + file URI (driver manager finds library via ADBC_DRIVER or system path)
{:ok, db} = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_sqlite", uri: "file:analytics.db")
{:ok, conn} = ExArrow.ADBC.Connection.open(db)
{:ok, stmt} = ExArrow.ADBC.Statement.new(conn, "SELECT * FROM events LIMIT 10000")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)
# Stream is the same ExArrow.Stream as IPC/Flight; use schema/1 and next/1
```

**PostgreSQL** — use the ADBC PostgreSQL driver (install and point to its shared library or set driver name/URI as required by the driver):

```elixir
# Example: driver by name with connection URI (driver-dependent)
{:ok, db} = ExArrow.ADBC.Database.open(
  driver_name: "adbc_driver_postgresql",
  uri: "postgresql://user:pass@localhost:5432/mydb"
)
{:ok, conn} = ExArrow.ADBC.Connection.open(db)
{:ok, stmt} = ExArrow.ADBC.Statement.new(conn, "SELECT id, name FROM users WHERE active = true")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)
# Process Arrow batches with ExArrow.Stream.schema/1 and next/1
```

**Metadata** (when the driver supports it):

```elixir
{:ok, table_types_stream} = ExArrow.ADBC.Connection.get_table_types(conn)
{:ok, schema} = ExArrow.ADBC.Connection.get_table_schema(conn, nil, nil, "users")
{:ok, objects_stream} = ExArrow.ADBC.Connection.get_objects(conn, depth: "tables")
```

Errors are `{:error, message}`. Use `ExArrow.ADBC.Error.from_message/1` for consistent handling.

### Driver setup and optional download (using the `adbc` package)

ExArrow does not manage or download ADBC drivers itself. It works with **any**
ADBC driver that exposes a shared library (for example
`adbc_driver_sqlite`, `adbc_driver_postgresql`) and is discoverable by the
ADBC driver manager.

If you want a higher-level way to **configure drivers and download them on
first use**, use the separate
[`adbc`](https://hex.pm/packages/adbc) package:

- Add it to your project (optional dependency alongside ExArrow):

  ```elixir
  {:adbc, "~> 0.7"}
  ```

- Use `Adbc.download_driver!/1` (or its configuration) to ensure drivers such as
  `:sqlite` or `:postgresql` are available.
- Then open the database with ExArrow as usual, either by path or by
  `driver_name` + `uri`.

For example, using `adbc` to download the SQLite driver and ExArrow to get
Arrow result streams:

```elixir
# Ensure the SQLite driver is available (no-op if already installed)
Adbc.download_driver!(:sqlite)

# Then use ExArrow's ADBC APIs for Arrow streams
{:ok, db} =
  ExArrow.ADBC.Database.open(driver_name: "adbc_driver_sqlite", uri: ":memory:")

{:ok, conn} = ExArrow.ADBC.Connection.open(db)
{:ok, stmt} = ExArrow.ADBC.Statement.new(conn, "SELECT 1 AS n")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)
```

If you prefer, you can also use `ExArrow.ADBC.DriverHelper.ensure_driver_and_open/2`,
which calls `Adbc.download_driver!/1` when the `:adbc` package is available and
then opens the database via `ExArrow.ADBC.Database.open/1`.

---

## Use case examples

### Ingest Arrow IPC from a pipeline and write to file

Consume Arrow IPC bytes from Kafka, HTTP, or another producer; write to a file or process batches in place.

```elixir
# Example: IPC bytes from HTTP or Kafka
ipc_bytes = get_arrow_stream_from_http_or_kafka()

{:ok, stream} = ExArrow.IPC.Reader.from_binary(ipc_bytes)
{:ok, schema} = ExArrow.Stream.schema(stream)
batches = Stream.repeatedly(fn -> ExArrow.Stream.next(stream) end)
         |> Enum.take_while(fn
              nil -> false
              {:error, _} -> false
              _ -> true
            end)

:ok = ExArrow.IPC.Writer.to_file("/data/ingested.arrow", schema, batches)
```

### Query a database with ADBC and stream into IPC or Flight

Use ADBC to run SQL and get Arrow result sets; optionally re-export as IPC file or send via Flight.

```elixir
{:ok, db} = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_sqlite", uri: "file:report.db")
{:ok, conn} = ExArrow.ADBC.Connection.open(db)
{:ok, stmt} = ExArrow.ADBC.Statement.new(conn, "SELECT * FROM sales WHERE year = 2024")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)

{:ok, schema} = ExArrow.Stream.schema(stream)
batches =
  Stream.repeatedly(fn -> ExArrow.Stream.next(stream) end)
  |> Enum.take_while(fn nil -> false; {:error, _} -> false; _ -> true end)

# Option A: write IPC file for downstream (e.g. Python, R, DuckDB)
:ok = ExArrow.IPC.Writer.to_file("/reports/sales_2024.arrow", schema, batches)

# Option B: send to a Flight server
{:ok, client} = ExArrow.Flight.Client.connect("flight.example.com", 32010, [])
:ok = ExArrow.Flight.Client.do_put(client, schema, batches)
```

### Connect to a Flight-compatible service (Dremio, InfluxDB, custom)

Arrow Flight is used by Dremio, InfluxDB IOx, Snowflake (for some APIs), and custom servers. Use the same client: connect, then list_flights / get_flight_info / do_get with the ticket or descriptor the service expects.

```elixir
# Example: connect to a Flight endpoint (host/port from your deployment)
{:ok, client} = ExArrow.Flight.Client.connect("dremio.example.com", 32010, connect_timeout_ms: 5_000)

# List available flights (service-specific)
{:ok, flights} = ExArrow.Flight.Client.list_flights(client, <<>>)

# Get schema for a given flight (descriptor is service-specific)
{:ok, schema} = ExArrow.Flight.Client.get_schema(client, {:path, ["my_dataset", "my_table"]})

# Stream data with do_get (ticket from get_flight_info or service docs)
{:ok, stream} = ExArrow.Flight.Client.do_get(client, ticket_from_service)
{:ok, schema} = ExArrow.Stream.schema(stream)
batch = ExArrow.Stream.next(stream)
# ... consume
```

### Interchange with Python or R (read/write Arrow files)

Read Arrow files produced by PyArrow, Pandas (`to_arrow()`), or R (arrow package). Write Arrow files for consumption by Python/R or by tools like DuckDB.

```elixir
# Read Arrow file produced by Python: pyarrow.ipc.open_file(...) or pandas + to_arrow
{:ok, file} = ExArrow.IPC.File.from_file("/data/from_python.arrow")
{:ok, schema} = ExArrow.IPC.File.schema(file)
n = ExArrow.IPC.File.batch_count(file)
for i <- 0..(n - 1) do
  {:ok, batch} = ExArrow.IPC.File.get_batch(file, i)
  # Process batch (e.g. filter, aggregate, or re-export)
end

# Write Arrow stream file for Python/R or DuckDB
{:ok, stream} = ExArrow.IPC.Reader.from_file("/data/elixir_processed.arrow")
{:ok, schema} = ExArrow.Stream.schema(stream)
batches =
  Stream.repeatedly(fn -> ExArrow.Stream.next(stream) end)
  |> Enum.take_while(fn nil -> false; {:error, _} -> false; _ -> true end)
:ok = ExArrow.IPC.Writer.to_file("/data/for_python.arrow", schema, batches)
```

### End-to-end: ADBC to Flight server

Run a query, stream Arrow batches from ADBC, and push them to a Flight server for other consumers.

```elixir
# 1. Query Postgres (or SQLite) via ADBC
{:ok, db} = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_postgresql", uri: "postgresql://localhost/mydb")
{:ok, conn} = ExArrow.ADBC.Connection.open(db)
{:ok, stmt} = ExArrow.ADBC.Statement.new(conn, "SELECT * FROM sensor_readings WHERE ts > NOW() - INTERVAL '1 day'")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)

{:ok, schema} = ExArrow.Stream.schema(stream)
batches =
  Stream.repeatedly(fn -> ExArrow.Stream.next(stream) end)
  |> Enum.take_while(fn nil -> false; {:error, _} -> false; _ -> true end)

# 2. Push to Flight server (e.g. for dashboards or other services)
{:ok, client} = ExArrow.Flight.Client.connect("flight.internal", 32010, [])
:ok = ExArrow.Flight.Client.do_put(client, schema, batches)
```

---

## Using ExArrow with Explorer

[Explorer](https://hex.pm/packages/explorer) is a dataframe library for Elixir. You can move data between ExArrow and Explorer via Arrow IPC (binary or file). ExArrow handles streaming and low-level IPC/Flight/ADBC; Explorer handles in-memory analysis and transformations.

**ExArrow to Explorer** — Read with ExArrow (file or stream), collect batches, write IPC binary, then load into Explorer:

```elixir
# From IPC file
{:ok, stream} = ExArrow.IPC.Reader.from_file("/data/source.arrow")
{:ok, schema} = ExArrow.Stream.schema(stream)
batches = Stream.repeatedly(fn -> ExArrow.Stream.next(stream) end)
          |> Enum.take_while(fn nil -> false; {:error, _} -> false; _ -> true end)
{:ok, binary} = ExArrow.IPC.Writer.to_binary(schema, batches)
df = Explorer.DataFrame.load_ipc_stream!(binary)
```

Or write to a file and use Explorer’s file API: `ExArrow.IPC.Writer.to_file(path, schema, batches)` then `Explorer.DataFrame.read_ipc_stream!(path)` (Writer produces stream format).

**Explorer to ExArrow** — Dump a dataframe to IPC stream binary and read it with ExArrow (use `dump_ipc_stream!`; `Reader.from_binary` expects stream format):

```elixir
df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
binary = Explorer.DataFrame.dump_ipc_stream!(df)
{:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
{:ok, schema} = ExArrow.Stream.schema(stream)
batch = ExArrow.Stream.next(stream)
```

Typical workflow: fetch or stream Arrow data with ExArrow (ADBC, Flight, IPC), optionally write to a temporary IPC file, load into Explorer for analysis, then dump back to IPC if you need to send results via Flight or write to file.

---

## Documentation

- [Memory model](docs/memory_model.md) — handles, copying rules, NIF scheduling
- [IPC guide](docs/ipc_guide.md) — stream vs file, types, limitations
- [Flight guide](docs/flight_guide.md) — server, client, timeouts, security
- [ADBC guide](docs/adbc_guide.md) — driver loading, metadata, binding

API reference: `mix docs` or [Hex Docs](https://hexdocs.pm/ex_arrow).

## Development

Until a release with precompiled NIFs exists, set `EX_ARROW_BUILD=1` and have Rust installed so `mix compile` builds the NIF from source.

- `mix compile` — precompiled NIF or local build if `EX_ARROW_BUILD=1`
- `mix test` — test suite (use `mix test --exclude adbc` when no ADBC driver)
- `mix docs` — generate ExDoc
- `mix run examples/ipc_roundtrip.exs` — IPC roundtrip example
- Flight: `mix run examples/flight_echo/server.exs` and `examples/flight_echo/client.exs` in two terminals

---

## FAQ

**When should I use ExArrow?**  
Use ExArrow when you need to read or write Arrow IPC (stream or file), talk to an Arrow Flight server (e.g. Dremio, InfluxDB IOx, or a custom service), or run SQL via ADBC and get Arrow result streams. It is a good fit for data pipelines, ETL, and interchange with systems that speak Arrow.

**When should I not use ExArrow?**  
Do not use ExArrow as a general-purpose dataframe or query engine. For in-memory analysis, filtering, grouping, and plotting, use Explorer or similar. Do not use it as a replacement for Ecto or DB drivers when you only need normal SQL results (use Ecto/Postgrex instead). For Parquet-only workflows with no Flight/ADBC, consider Explorer’s Parquet support first.

**Can I use ExArrow and Explorer together?**  
Yes. ExArrow handles streaming and protocol layers (IPC, Flight, ADBC). Use `ExArrow.IPC.Writer.to_binary/2` (or `to_file/3`) to produce IPC stream from ExArrow, then `Explorer.DataFrame.load_ipc_stream!/1` to get a dataframe. Use `Explorer.DataFrame.dump_ipc_stream!/1` to get IPC stream binary and `ExArrow.IPC.Reader.from_binary/1` to read it back.

**Why do I get a 404 or “couldn’t fetch NIF” on compile?**  
Precompiled NIFs are hosted on GitHub releases. If you are on an unsupported platform or using a version that has no build yet, the download fails. Set `EX_ARROW_BUILD=1`, install Rust, and run `mix compile` to build from source.

**Is Arrow Flight over TLS supported?**  
Not yet. Flight in this release is plaintext only. Use on localhost or trusted networks. TLS is planned for a later release.

**Which ADBC drivers are supported?**  
ExArrow works with any ADBC driver that provides a shared library (e.g. `adbc_driver_sqlite`, `adbc_driver_postgresql`). You must install the driver and pass its path or ensure the driver manager can find it (e.g. via `ADBC_DRIVER` or system path). Metadata and binding support depend on the driver.

## License

Apache 2.0
