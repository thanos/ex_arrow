# ExArrow 0.2.0 — Release Notes

**Released:** 2026-03-09

ExArrow 0.2.0 delivers the four features promised on the v0.2 roadmap: TLS for
Arrow Flight, multi-dataset server routing, an ADBC connection pool, and a
broader integration test matrix.  All changes are backward compatible; no
existing code needs to change to upgrade from 0.1.0.

---

## What is new

### TLS for Arrow Flight

The built-in Flight server now supports encrypted connections.  Pass a `:tls`
option to `Server.start_link/2`:

```elixir
# One-way TLS (server presents a certificate)
cert = File.read!("server.crt")
key  = File.read!("server.key")
{:ok, server} = ExArrow.Flight.Server.start_link(9999,
  tls: [cert_pem: cert, key_pem: key])

# Mutual TLS (both sides present certificates)
ca = File.read!("ca.crt")
{:ok, server} = ExArrow.Flight.Server.start_link(9999,
  tls: [cert_pem: cert, key_pem: key, ca_cert_pem: ca])
```

The client already selected TLS automatically for non-loopback hosts (using the
OS certificate store).  For a custom or self-signed CA, pass
`tls: [ca_cert_pem: pem]` to `Client.connect/3`.

Plaintext (`tls: false`, or no `:tls` option on loopback) continues to work
exactly as before.

---

### Flight server routing — multiple named datasets

The built-in Flight server now stores datasets in a `HashMap` keyed by ticket,
rather than always overwriting a single `"echo"` slot.

Upload with a descriptor to store under a named ticket:

```elixir
:ok = ExArrow.Flight.Client.do_put(client, schema, batches,
        descriptor: {:cmd, "sales_2024"})

:ok = ExArrow.Flight.Client.do_put(client, schema, other_batches,
        descriptor: {:path, ["metrics", "daily"]})
```

Retrieve by ticket:

```elixir
{:ok, stream} = ExArrow.Flight.Client.do_get(client, "sales_2024")
{:ok, stream} = ExArrow.Flight.Client.do_get(client, "metrics/daily")
```

List all stored datasets:

```elixir
{:ok, flights} = ExArrow.Flight.Client.list_flights(client)
{:ok, tickets} = ExArrow.Flight.Client.do_action(client, "list_tickets", <<>>)
```

Calls that do not pass a `:descriptor` default to the `"echo"` ticket, so all
existing code continues to work without modification.

---

### ADBC connection pool

`ExArrow.ADBC.ConnectionPool` is a NimblePool-backed pool that reuses open
`Connection` handles across callers.

#### Supervised pool (recommended)

```elixir
children = [
  {ExArrow.ADBC.DatabaseServer,
    name: :mydb,
    driver_path: "/usr/local/lib/libadbc_driver_duckdb.so"},
  {ExArrow.ADBC.ConnectionPool,
    name: :mypool, database: :mydb, pool_size: 4}
]
Supervisor.start_link(children, strategy: :one_for_one)

# Anywhere in the application:
{:ok, stream} = ExArrow.ADBC.ConnectionPool.query(:mypool,
                  "SELECT * FROM events WHERE day = today()")
```

#### Ad-hoc pool

```elixir
{:ok, db}   = ExArrow.ADBC.Database.open(driver_path: "/path/to/driver.so")
{:ok, pool} = ExArrow.ADBC.ConnectionPool.start_link(database: db, pool_size: 4)
{:ok, stream} = ExArrow.ADBC.ConnectionPool.query(pool, "SELECT 42 AS n")
```

#### Multi-statement checkout

```elixir
ExArrow.ADBC.ConnectionPool.with_connection(pool, fn conn ->
  {:ok, stmt} = ExArrow.ADBC.Statement.new(conn)
  ExArrow.ADBC.Statement.set_sql(stmt, "BEGIN")
  ExArrow.ADBC.Statement.execute(stmt)
  # ... more statements ...
  {result, :ok}
end)
```

---

### Larger integration test matrix

A new `.github/workflows/integration.yml` workflow runs ADBC integration tests
against:

- **PostgreSQL** 14, 15, and 16 (via GitHub Actions service containers)
- **DuckDB** 1.1.3 and 1.2.0 (via downloaded ADBC driver binary)

The tests live in `test/ex_arrow/adbc_integration_test.exs` and are excluded
from the default test run.  To run them locally:

```bash
# Start PostgreSQL
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16

PG_HOST=localhost mix test --include adbc_integration \
  test/ex_arrow/adbc_integration_test.exs

# DuckDB
DUCKDB_DRIVER=/usr/local/lib/libduckdb_adbc.so \
  mix test --include adbc_integration \
  test/ex_arrow/adbc_integration_test.exs
```

---

### New public API additions

| Module | Function | Description |
|--------|----------|-------------|
| `ExArrow.Schema` | `field_names/1` | Returns field names as `[String.t()]` |
| `ExArrow.Stream` | `to_list/1` | Collects all batches into a list |
| `ExArrow.ADBC.Database` | `close/1` | Explicit handle release |
| `ExArrow.ADBC.Connection` | `close/1` | Explicit handle release |
| `ExArrow.ADBC.ConnectionPool` | `start_link/1`, `query/3`, `with_connection/3` | NimblePool pool |
| `ExArrow.ADBC.DatabaseServer` | `start_link/1`, `get/1` | Supervised database handle |

---

## Upgrade guide

No breaking changes.  The only API change that requires attention is if you
have a **Mox stub for `ClientBehaviour.do_put`**: the callback now takes four
arguments (`client, schema, batches, opts`).  Update your mock expectation:

```elixir
# Before (0.1.0)
Mox.expect(MyMock, :do_put, fn client, schema, batches -> :ok end)

# After (0.2.0)
Mox.expect(MyMock, :do_put, fn client, schema, batches, _opts -> :ok end)
```

---

## Dependencies

No new required dependencies.  The connection pool requires `nimble_pool`
(already optional in 0.1.0); add it to your `mix.exs` to use
`ExArrow.ADBC.ConnectionPool`:

```elixir
{:nimble_pool, "~> 1.1"}
```

---

## Full changelog

See [CHANGELOG.md](CHANGELOG.md) for the complete list of changes.
