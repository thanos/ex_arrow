# ExArrow 0.5.0 — Release Notes

**Released:** 2026-04-16

ExArrow 0.5.0 adds a production-grade Arrow Flight SQL client, making Elixir
a first-class participant in the Flight SQL ecosystem alongside DuckDB,
DataFusion, Dremio, and InfluxDB v3.  The release also delivers lazy streaming
with `Enumerable` support for all stream types, a Mox-compatible behaviour for
unit testing, and structured error types with gRPC status codes.

All changes are backward compatible; upgrading from 0.4.0 requires only a
version bump.

---

## What is new

### Arrow Flight SQL client

Arrow Flight SQL layers SQL query semantics on top of Arrow Flight (gRPC +
Arrow IPC).  Queries are dispatched to the server, which executes them and
streams results back as columnar `RecordBatch` data — the same Arrow format
used everywhere in ExArrow.

**Quick start:**

```elixir
{:ok, client} = ExArrow.FlightSQL.Client.connect("localhost:32010")

# Materialised query — all batches collected before returning
{:ok, result} = ExArrow.FlightSQL.Client.query(client, "SELECT id, name FROM users")
result.num_rows  #=> 42
result.schema    #=> %ExArrow.Schema{...}

# Lazy query — stream one batch at a time
{:ok, stream} = ExArrow.FlightSQL.Client.stream_query(client, "SELECT * FROM big_table")
Enum.each(stream, fn batch -> process(batch) end)

# DML
{:ok, 3}        = ExArrow.FlightSQL.Client.execute(client, "DELETE FROM t WHERE id < 4")
{:ok, :unknown} = ExArrow.FlightSQL.Client.execute(client, "CREATE TABLE t (id INT)")
```

**TLS connections** — plaintext is used automatically for loopback addresses;
remote hosts use the OS trust store; a custom CA certificate can be provided:

```elixir
# TLS with OS trust store (automatic for remote hosts)
{:ok, client} = ExArrow.FlightSQL.Client.connect("dremio.example.com:32010")

# Custom CA
pem = File.read!("priv/ca.pem")
{:ok, client} = ExArrow.FlightSQL.Client.connect("secure.server:32010",
  tls: [ca_cert_pem: pem])
```

**Bearer-token authentication:**

```elixir
{:ok, client} = ExArrow.FlightSQL.Client.connect("dremio.example.com:32010",
  tls: true,
  headers: [{"authorization", "Bearer my-pat-token"}]
)
```

---

### Lazy streaming with `Enumerable`

`ExArrow.Stream` now implements the `Enumerable` protocol, so all `Enum.*`
and `Stream.*` functions work directly on any stream handle — IPC, Parquet,
ADBC, and Flight SQL alike.

```elixir
{:ok, stream} = ExArrow.FlightSQL.Client.stream_query(client, "SELECT * FROM events")

# Collect all batches
batches = Enum.to_list(stream)

# Map, filter, reduce — standard Elixir idioms
row_counts = Enum.map(stream, &ExArrow.RecordBatch.num_rows/1)

# Take only the first N batches — the rest are never fetched
first_two = Enum.take(stream, 2)

# Comprehension syntax
for batch <- stream, do: process_batch(batch)
```

Early termination (e.g. `Enum.take/2`) is safe — the underlying gRPC channel
is released when the stream variable goes out of scope.

---

### Prepared statements

Server-side prepared statements allow the server to parse and plan a query
once and then execute it one or more times:

```elixir
{:ok, stmt} = ExArrow.FlightSQL.Client.prepare(client,
  "SELECT * FROM events WHERE ts > '2024-01-01'")

# Execute as a streaming query
{:ok, stream} = ExArrow.FlightSQL.Statement.execute(stmt)
batches = Enum.to_list(stream)

# Re-execute the same statement (reuses the server plan)
{:ok, stream2} = ExArrow.FlightSQL.Statement.execute(stmt)

# Or execute as DML
{:ok, dml_stmt} = ExArrow.FlightSQL.Client.prepare(client,
  "DELETE FROM logs WHERE ts < '2020-01-01'")
{:ok, 1042} = ExArrow.FlightSQL.Statement.execute_update(dml_stmt)
```

Servers that do not support prepared statements return
`{:error, %Error{code: :unimplemented}}`.

---

### Metadata discovery

```elixir
{:ok, stream} = ExArrow.FlightSQL.Client.get_tables(client,
  db_schema_filter: "public", table_types: ["TABLE", "VIEW"])
batches = Enum.to_list(stream)

{:ok, stream} = ExArrow.FlightSQL.Client.get_db_schemas(client)
{:ok, stream} = ExArrow.FlightSQL.Client.get_sql_info(client)
```

---

### Explorer and Nx integration

```elixir
# Query → Explorer DataFrame
{:ok, result} = ExArrow.FlightSQL.Client.query(client, "SELECT * FROM sales")
{:ok, df}     = ExArrow.FlightSQL.Result.to_dataframe(result)

# Query → Nx tensor (first batch only)
{:ok, result} = ExArrow.FlightSQL.Client.query(client, "SELECT price FROM quotes")
{:ok, tensor} = ExArrow.FlightSQL.Result.to_tensor(result, "price")

# Lazy stream → Explorer DataFrame (large result sets)
{:ok, stream} = ExArrow.FlightSQL.Client.stream_query(client, "SELECT * FROM big_table")
{:ok, df}     = ExArrow.Explorer.from_stream(stream)
```

`to_dataframe/1` requires `{:explorer, "~> 0.11"}`.  `to_tensor/2` requires
`{:nx, "~> 0.9"}`.  Both return
`{:error, %ExArrow.FlightSQL.Error{code: :conversion_error}}` when the
optional dependency is absent.

---

### Mox-compatible behaviour for unit testing

Swap the real implementation for a mock without a live server:

```elixir
# test/test_helper.exs
Mox.defmock(MyApp.FlightSQLMock, for: ExArrow.FlightSQL.ClientBehaviour)

# In your test
Application.put_env(:ex_arrow, :flight_sql_client_impl, MyApp.FlightSQLMock)

Mox.expect(MyApp.FlightSQLMock, :query, fn _client, "SELECT 1", [] ->
  {:ok, %ExArrow.FlightSQL.Result{schema: schema, batches: [], num_rows: 0}}
end)
```

---

### Structured errors

All non-bang functions return `{:ok, value}` or
`{:error, %ExArrow.FlightSQL.Error{}}`:

```elixir
case ExArrow.FlightSQL.Client.query(client, sql) do
  {:ok, result}                                          -> handle(result)
  {:error, %Error{code: :unauthenticated}}               -> reauthenticate()
  {:error, %Error{code: :not_found, message: msg}}       -> Logger.warn(msg)
  {:error, err}                                          -> raise err
end
```

Error codes: `:transport_error`, `:server_error`, `:unimplemented`,
`:unauthenticated`, `:permission_denied`, `:not_found`, `:invalid_argument`,
`:protocol_error`, `:multi_endpoint`, `:invalid_option`, `:conversion_error`.

---

## New public API

| Module | Function | Description |
|---|---|---|
| `ExArrow.FlightSQL.Client` | `connect/1,2` | Connect to a Flight SQL server |
| `ExArrow.FlightSQL.Client` | `query/2`, `query!/2` | Materialised SQL query |
| `ExArrow.FlightSQL.Client` | `stream_query/2` | Lazy SQL query returning `ExArrow.Stream` |
| `ExArrow.FlightSQL.Client` | `execute/2` | DML/DDL with affected-row count |
| `ExArrow.FlightSQL.Client` | `prepare/2` | Server-side prepared statement |
| `ExArrow.FlightSQL.Client` | `get_tables/1,2` | List tables visible to the connected user |
| `ExArrow.FlightSQL.Client` | `get_db_schemas/1,2` | List database schemas |
| `ExArrow.FlightSQL.Client` | `get_sql_info/1` | Server capability flags |
| `ExArrow.FlightSQL.Statement` | `execute/1` | Execute a prepared statement as a lazy stream |
| `ExArrow.FlightSQL.Statement` | `execute_update/1` | Execute a prepared DML statement |
| `ExArrow.FlightSQL.Result` | `from_stream/1` | Materialise a stream into a `Result` struct |
| `ExArrow.FlightSQL.Result` | `to_dataframe/1` | Convert result to `Explorer.DataFrame` |
| `ExArrow.FlightSQL.Result` | `to_tensor/2` | Extract a numeric column as `Nx.Tensor` |
| `ExArrow.Stream` | — | Now implements `Enumerable` |

---

## Changed behaviour

**`ExArrow.Stream` is now `Enumerable`** — `Enum.to_list/1`, `Enum.map/2`,
`Enum.take/2`, and all other `Enum.*` / `Stream.*` functions work directly on
stream handles.  Existing code using `Stream.next/1` and `Stream.to_list/1`
continues to work unchanged.

---

## Bug fixes

**Elixir 1.17+ typing warning in `Adbc.Result.from_py/1`** — the
`{:ok, stream_ref, capsule}` match was unreachable when `Pythonx` is not
loaded.  Both `from_py/1` and `from_py!/1` are now guarded with
`Code.ensure_loaded?(Pythonx)`, eliminating the "clause will never match"
compiler warning.

---

## Dependencies

No new required dependencies.  Optional dependencies for new features:

```elixir
# Required only for TLS with a custom CA (Flight SQL connect option)
# Uses OTP :ssl and :public_key — no new Hex packages needed.

# Optional (unchanged from 0.4.0 — enable for ecosystem bridges)
{:explorer, "~> 0.11", optional: true}   # Result.to_dataframe/1
{:nx, "~> 0.9", optional: true}          # Result.to_tensor/2
```

---

## Upgrade guide

No breaking changes.  Update your version pin:

```elixir
# Before
{:ex_arrow, "~> 0.4.0"}

# After
{:ex_arrow, "~> 0.5.0"}
```

Then run `mix deps.get` and `mix compile`.

---

## Full changelog

See [CHANGELOG.md](CHANGELOG.md) for the complete list of changes including
internal fixes and documentation updates.
