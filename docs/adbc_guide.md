# ADBC (Arrow Database Connectivity)

ADBC exposes databases through the canonical adbc.h API. ExArrow binds to the ADBC driver manager and returns Arrow streams from SQL execution.

## Concepts

| Handle | Module | Purpose |
|--------|--------|---------|
| Database | `ExArrow.ADBC.Database` | Driver + init options (e.g. URI). |
| Connection | `ExArrow.ADBC.Connection` | Session from a database. |
| Statement | `ExArrow.ADBC.Statement` | SQL text, execute → stream of record batches. |

Flow: **Database.open** → **Connection.open** → **Statement.new** → **set_sql** → **execute** → **Stream** (same `ExArrow.Stream` as IPC/Flight; use `ExArrow.Stream.schema/1` and `ExArrow.Stream.next/1`).

## Driver loading

- **By path** — pass a string: the path to the driver shared library (e.g. `libadbc_driver_sqlite.so` or absolute path).
- **By name** — pass a keyword list with `driver_name` and optionally `uri`. The driver manager looks up the library by name (e.g. from `ADBC_DRIVER` or system search paths). If you pass `uri`, it is sent to the driver as the database URI (e.g. SQLite `uri: ":memory:"`). If you omit `uri`, no URI option is set; behavior is driver-dependent (some drivers require a URI and will fail at connection time).

If the driver cannot be loaded (wrong path, missing env), `Database.open/1` returns `{:error, message}`.

## Example

```elixir
# Path to the driver .so / .dylib
{:ok, db} = ExArrow.ADBC.Database.open("/path/to/libadbc_driver_sqlite.so")
# Or by name (uses env / system search)
{:ok, db} = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_sqlite", uri: ":memory:")

{:ok, conn} = ExArrow.ADBC.Connection.open(db)
{:ok, stmt} = ExArrow.ADBC.Statement.new(conn)
:ok = ExArrow.ADBC.Statement.set_sql(stmt, "SELECT 1 AS n")
{:ok, stream} = ExArrow.ADBC.Statement.execute(stmt)

{:ok, schema} = ExArrow.Stream.schema(stream)
batch = ExArrow.Stream.next(stream)
# Consume until nil
```

## When no driver is available

ExUnit does not support skipping a test dynamically from `setup`. The ADBC integration test therefore **fails with a clear message** when the driver cannot be opened (instead of passing), so that missing driver setup is visible when running `mix test --include adbc`. Use `mix test --exclude adbc` to omit it when no driver is installed.

In your own tests you can match on `{:error, _}` and raise an actionable message or exit:

```elixir
case ExArrow.ADBC.Database.open(opts) do
  {:error, reason} -> raise "ADBC driver not available: #{inspect(reason)}"
  {:ok, db}        -> run_query(db)
end
```

In scripts, match on `{:error, _}` to log and exit or skip the workflow.

## Errors

NIF errors (driver load failure, execute failure, invalid handle) are returned as `{:error, message}` where `message` is a string. Map these to `ExArrow.Error` at the call site if you need a structured exception.
