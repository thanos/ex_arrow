# Arrow Flight SQL

Arrow Flight SQL layers SQL query semantics on top of Arrow Flight (gRPC + Arrow IPC).
Queries are dispatched to the server, which executes them and streams results back as
columnar `RecordBatch` data — the same Arrow format used everywhere in ExArrow.

## When to use Flight SQL vs ADBC

| Scenario | Recommended |
|----------|-------------|
| Remote query server (DuckDB over the network, DataFusion, Dremio, InfluxDB v3) | **Flight SQL** |
| In-process database (DuckDB local, SQLite, PostgreSQL via driver) | **ADBC** |
| Receiving Arrow data from an existing Flight server (non-SQL) | **`ExArrow.Flight.Client`** |

---

## Module overview

| Module | Purpose |
|--------|---------|
| `ExArrow.FlightSQL.Client` | Connect, query, and execute DML |
| `ExArrow.FlightSQL.Result` | Materialised result (schema + batches + row count) |
| `ExArrow.FlightSQL.Error` | Structured error type for all Flight SQL failures |

All operations go through `ExArrow.FlightSQL.Client`.

---

## Quick start

```elixir
{:ok, client} = ExArrow.FlightSQL.Client.connect("localhost:32010")

# Materialised query — collects all batches before returning
{:ok, result} = ExArrow.FlightSQL.Client.query(client, "SELECT id, name FROM users")
result.num_rows   #=> 42
result.schema     #=> %ExArrow.Schema{...}

# Lazy query — streams batches one at a time
{:ok, stream} = ExArrow.FlightSQL.Client.stream_query(client, "SELECT * FROM big_table")

# DML
{:ok, 3}        = ExArrow.FlightSQL.Client.execute(client, "DELETE FROM t WHERE id < 4")
{:ok, :unknown} = ExArrow.FlightSQL.Client.execute(client, "CREATE TABLE t (id INT)")
```

---

## Connection

`connect/1` accepts a `"host:port"` string.  `connect/2` accepts the same string plus
a keyword options list.

```elixir
{:ok, client} = ExArrow.FlightSQL.Client.connect("localhost:32010")
{:ok, client} = ExArrow.FlightSQL.Client.connect("dremio.example.com:32010", tls: true)
```

IPv6 addresses must use the bracketed URI form:

```elixir
{:ok, client} = ExArrow.FlightSQL.Client.connect("[::1]:32010")
```

### TLS

| `:tls` value | Behaviour |
|---|---|
| not set, loopback host (`localhost`, `127.0.0.1`, `::1`) | plaintext (auto) |
| not set, remote host | TLS with native OS certificate store (auto) |
| `false` | plaintext regardless of host |
| `true` | TLS with native OS certificate store |
| `[ca_cert_pem: pem]` | TLS with a custom PEM-encoded CA certificate |

```elixir
# Custom CA (mutual TLS or private PKI)
pem = File.read!("priv/ca.pem")
{:ok, client} = ExArrow.FlightSQL.Client.connect("secure.server:32010", tls: [ca_cert_pem: pem])
```

### Authentication

Pass credentials as gRPC metadata via the `:headers` option.  Bearer tokens are the
most common pattern:

```elixir
{:ok, client} = ExArrow.FlightSQL.Client.connect("dremio.example.com:32010",
  tls: true,
  headers: [{"authorization", "Bearer my-pat-token"}]
)
```

Any number of `{name, value}` string tuples are accepted; they are sent as gRPC
metadata on every request.

---

## Queries

### Materialised — `query/2`

Collects all record batches from the server before returning.  Returns an
`ExArrow.FlightSQL.Result` struct.

```elixir
{:ok, result} = ExArrow.FlightSQL.Client.query(client, "SELECT * FROM orders LIMIT 1000")
result.num_rows   #=> 1000
result.schema     #=> %ExArrow.Schema{...}
result.batches    #=> [%ExArrow.RecordBatch{...}, ...]
```

Use `query!/2` to raise `ExArrow.FlightSQL.Error` on failure instead of returning
`{:error, ...}`:

```elixir
result = ExArrow.FlightSQL.Client.query!(client, "SELECT count(*) FROM orders")
```

### Lazy — `stream_query/2`

Returns an `ExArrow.Stream` that is consumed one batch at a time.  The gRPC
connection stays open until the stream is exhausted or the resource is
garbage-collected.

Prefer this over `query/2` for large result sets that should not be fully buffered
in memory.

#### Using `Enum` functions (recommended)

`ExArrow.Stream` implements the `Enumerable` protocol, so all `Enum` and `Stream`
functions work directly on the stream handle.  Each element is an
`ExArrow.RecordBatch.t()`.

```elixir
{:ok, stream} = ExArrow.FlightSQL.Client.stream_query(client, "SELECT * FROM events")

# Collect all batches into a list
batches = Enum.to_list(stream)

# Map over every batch
row_counts = Enum.map(stream, &ExArrow.RecordBatch.num_rows/1)

# Take only the first N batches — the rest are never fetched
first_two = Enum.take(stream, 2)

# Comprehension syntax
for batch <- stream, do: process_batch(batch)
```

Enumeration raises `RuntimeError` on a transport or server error.  For
recoverable error handling, iterate manually with `ExArrow.Stream.next/1`:

```elixir
case ExArrow.Stream.next(stream) do
  nil            -> :done
  {:error, msg}  -> {:error, msg}
  batch          -> process(batch)
end
```

#### Manual iteration

```elixir
{:ok, stream} = ExArrow.FlightSQL.Client.stream_query(client, "SELECT * FROM events")

{:ok, schema} = ExArrow.Stream.schema(stream)

# Collect everything (raises on error)
batches = ExArrow.Stream.to_list(stream)
```

#### Resource lifecycle

The underlying gRPC channel and batch buffer are held in a native (Rust) resource.
The resource is released when the stream handle is garbage-collected.  Stopping
enumeration early (e.g. `Enum.take/2`) is safe — the resource is released once the
stream variable goes out of scope.

> #### Concurrency {: .warning}
> Concurrent calls on the **same** client handle serialise internally — the gRPC
> client requires exclusive access per call.  Create separate handles with
> `connect/2` for parallel query workloads.

### DML and DDL — `execute/2`

Runs INSERT, UPDATE, DELETE, CREATE TABLE, and similar statements.  Returns the
affected row count or `:unknown` when the server does not report one.

```elixir
{:ok, 5}        = ExArrow.FlightSQL.Client.execute(client, "DELETE FROM logs WHERE ts < now() - interval '7 days'")
{:ok, :unknown} = ExArrow.FlightSQL.Client.execute(client, "CREATE TABLE staging AS SELECT * FROM raw")
```

---

## Error handling

All non-bang functions return `{:ok, value}` or `{:error, %ExArrow.FlightSQL.Error{}}`.

```elixir
case ExArrow.FlightSQL.Client.query(client, sql) do
  {:ok, result}  -> handle(result)
  {:error, %ExArrow.FlightSQL.Error{code: :unauthenticated}} -> reauthenticate()
  {:error, err}  -> Logger.error(ExArrow.FlightSQL.Error.message(err))
end
```

### Error codes

| Code | Meaning |
|------|---------|
| `:transport_error` | TCP/TLS channel failure; also `Cancelled`, `Unavailable`, `DeadlineExceeded` |
| `:server_error` | gRPC `INTERNAL`, `ResourceExhausted`, `Aborted`, `DataLoss` |
| `:unimplemented` | Server does not support the operation |
| `:unauthenticated` | Missing or rejected credentials |
| `:permission_denied` | Insufficient privileges |
| `:not_found` | Table or object does not exist |
| `:invalid_argument` | Bad SQL syntax, wrong parameter types, `OutOfRange` |
| `:protocol_error` | Malformed or unexpected Flight SQL response |
| `:multi_endpoint` | `FlightInfo` returned more than one endpoint (not supported in v0.5.0) |
| `:invalid_option` | Invalid connect or query option at the Elixir layer |
| `:conversion_error` | Arrow → Explorer or Arrow → Nx conversion failure |

The `:grpc_status` field on the error struct holds the raw gRPC integer code when the
failure came from the server; it is `nil` for local (transport or option) errors.

---

## Explorer and Nx integration

### Convert to an Explorer DataFrame

Requires the optional `:explorer` dependency (`{:explorer, "~> 0.11"}`):

```elixir
{:ok, result} = ExArrow.FlightSQL.Client.query(client, "SELECT * FROM sales")
{:ok, df}     = ExArrow.FlightSQL.Result.to_dataframe(result)
```

For large result sets, convert the stream directly:

```elixir
{:ok, stream} = ExArrow.FlightSQL.Client.stream_query(client, "SELECT * FROM big_table")
{:ok, df}     = ExArrow.Explorer.from_stream(stream)
```

### Convert a column to an Nx tensor

Requires the optional `:nx` dependency (`{:nx, "~> 0.9"}`).  Only the **first batch**
is converted — for multi-batch results iterate the stream and convert batch-by-batch:

```elixir
{:ok, result}  = ExArrow.FlightSQL.Client.query(client, "SELECT price FROM quotes")
{:ok, tensor}  = ExArrow.FlightSQL.Result.to_tensor(result, "price")
```

---

## Testing with Mox

Swap the real implementation for a mock in tests by setting the
`:flight_sql_client_impl` application environment key:

```elixir
# test/test_helper.exs
Mox.defmock(MyApp.FlightSQLMock, for: ExArrow.FlightSQL.ClientBehaviour)

# In your test
Application.put_env(:ex_arrow, :flight_sql_client_impl, MyApp.FlightSQLMock)

Mox.expect(MyApp.FlightSQLMock, :query, fn _client, "SELECT 1", [] ->
  {:ok, %ExArrow.Stream{resource: make_ref(), backend: :flight_sql}}
end)
```

---

## Metadata discovery

### List tables — `get_tables/2`

Returns a lazy stream of record batches describing the tables visible to the connected user.

```elixir
{:ok, stream} = ExArrow.FlightSQL.Client.get_tables(client)
batches = Enum.to_list(stream)

# With filters
{:ok, stream} = ExArrow.FlightSQL.Client.get_tables(client,
  db_schema_filter: "public",
  table_types: ["TABLE", "VIEW"]
)
```

Result columns (per the Arrow Flight SQL specification):
- `catalog_name` — utf8 (nullable)
- `db_schema_name` — utf8 (nullable)
- `table_name` — utf8
- `table_type` — utf8

Options:
- `:catalog` — exact catalog name filter
- `:db_schema_filter` — SQL `LIKE` pattern for schema names
- `:table_name_filter` — SQL `LIKE` pattern for table names
- `:table_types` — list of type strings, e.g. `["TABLE", "VIEW"]`
- `:include_schema` — `true` adds an IPC-encoded schema column per table (default: `false`)

### List schemas — `get_db_schemas/2`

Returns a lazy stream of record batches describing the database schemas.

```elixir
{:ok, stream} = ExArrow.FlightSQL.Client.get_db_schemas(client)
batches = Enum.to_list(stream)

{:ok, stream} = ExArrow.FlightSQL.Client.get_db_schemas(client, catalog: "main")
```

Result columns: `catalog_name` (nullable), `db_schema_name`.

### Server capabilities — `get_sql_info/1`

Returns a lazy stream of record batches encoding server capability flags and SQL dialect information as defined by the Flight SQL specification.

```elixir
{:ok, stream} = ExArrow.FlightSQL.Client.get_sql_info(client)
batches = Enum.to_list(stream)
```

Each row has two columns:
- `info_name` — uint32 (the numeric `SqlInfo` code)
- `value` — dense_union (the value; type depends on the code)

### Server compatibility note

Metadata support is optional in the Flight SQL specification.  Servers that
do not implement a particular metadata command return
`{:error, %ExArrow.FlightSQL.Error{code: :unimplemented}}`.  Always pattern-match on
the error code rather than assuming all metadata APIs are available:

```elixir
case ExArrow.FlightSQL.Client.get_tables(client) do
  {:ok, stream}                               -> Enum.to_list(stream)
  {:error, %Error{code: :unimplemented}}      -> []        # server doesn't support it
  {:error, err}                               -> raise err
end
```

---

## v0.5.0 scope

**Supported:**

- Ad-hoc SQL query execution (`query/2`, `query!/2`, `stream_query/2`)
- DML execution with affected-row count (`execute/2`)
- Lazy streaming of large result sets
- TLS connections — plaintext, OS trust store, or custom CA certificate
- Bearer-token and arbitrary gRPC header injection
- Mox-compatible `ClientBehaviour` for unit testing without a server

**Not supported in v0.5.0 (deferred):**

- Prepared statements (v0.6.0)
- Bulk ingestion (`DoPut`)
- Transactions (`BEGIN`, `COMMIT`, `ROLLBACK`)
- Multi-endpoint distributed `FlightInfo` responses
- Filtering `get_sql_info` by specific info code (returns all codes)

---

## Integration tests

Integration tests require a running Flight SQL server and are excluded from
`mix test` by default:

```bash
mix test test/ex_arrow/flight_sql_integration_test.exs --include flight_sql_integration
```

DuckDB can be started as a Flight SQL server using the
[`duckdb_flight_sql`](https://duckdb.org/docs/api/flight_sql.html) extension.
