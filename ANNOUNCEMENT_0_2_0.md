# ExArrow 0.2.0 — TLS Flight, multi-dataset routing, ADBC connection pool

**[r/elixir | Elixir Forum post]**

---

ExArrow 0.2.0 is out. ExArrow is an Elixir library that brings Apache Arrow
IPC, Arrow Flight (gRPC), and ADBC (Arrow Database Connectivity) to the BEAM
with zero-copy semantics: data lives in native Rust/Arrow buffers and never
touches the BEAM heap unless you explicitly materialise it.

This release delivers the full v0.2 roadmap.

---

## What is new

### TLS for Arrow Flight

The built-in Flight server now accepts encrypted connections. Pass `tls:` to
`Server.start_link/2` for one-way or mutual TLS:

```elixir
cert = File.read!("server.crt")
key  = File.read!("server.key")

{:ok, server} = ExArrow.Flight.Server.start_link(9999,
  tls: [cert_pem: cert, key_pem: key])
```

Mutual TLS (mTLS) adds `ca_cert_pem:`. The client already auto-selected TLS for
remote hosts via the OS certificate store; nothing changes there.

---

### Multi-dataset routing in the Flight server

The server now stores multiple named datasets instead of a single echo slot.
Upload with a named descriptor, retrieve by ticket:

```elixir
:ok = ExArrow.Flight.Client.do_put(client, schema, batches,
        descriptor: {:cmd, "sales_2024"})

{:ok, stream} = ExArrow.Flight.Client.do_get(client, "sales_2024")
```

All existing code that relies on the default `"echo"` ticket continues to work
unchanged.

---

### ADBC connection pool

`ExArrow.ADBC.ConnectionPool` is a NimblePool-backed pool that recycles open
ADBC connections across callers. Drop it into your supervision tree:

```elixir
children = [
  {ExArrow.ADBC.DatabaseServer,
    name: :mydb,
    driver_path: "/usr/local/lib/libadbc_driver_duckdb.so"},
  {ExArrow.ADBC.ConnectionPool,
    name: :mypool, database: :mydb, pool_size: 4}
]
Supervisor.start_link(children, strategy: :one_for_one)

{:ok, stream} = ExArrow.ADBC.ConnectionPool.query(:mypool,
                  "SELECT * FROM events WHERE day = today()")
```

---

### Integration test matrix

A new CI workflow (`integration.yml`) runs live ADBC tests against PostgreSQL
14/15/16 and DuckDB 1.1.3/1.2.0 on every push to main and on pull requests.
The tests are in `test/ex_arrow/adbc_integration_test.exs` and are excluded
from the default `mix test` run.

---

## Links

- Hex: https://hex.pm/packages/ex_arrow
- Docs: https://hexdocs.pm/ex_arrow
- GitHub: https://github.com/thanos/ex_arrow
- Changelog: https://github.com/thanos/ex_arrow/blob/main/CHANGELOG.md

---

## What is ExArrow for?

ExArrow is transport and protocol glue, not a dataframe library:

- Read and write Arrow IPC (stream and file formats)
- Connect to Arrow Flight servers (Dremio, InfluxDB IOx, DuckDB, custom)
- Execute SQL via ADBC and receive Arrow result streams with minimal BEAM copying
- Build data pipelines where the Elixir node is an ingestion endpoint, forwarder,
  or thin query client

For in-memory analysis use Explorer. For normal Ecto queries use Ecto. ExArrow
sits at the layer below, where the Arrow wire format matters.

---

## Upgrade

No breaking changes for most users. If you have a Mox stub for the
`do_put` Flight callback, update it from 3 to 4 arguments:

```elixir
# Before
Mox.expect(MyMock, :do_put, fn client, schema, batches -> :ok end)
# After
Mox.expect(MyMock, :do_put, fn client, schema, batches, _opts -> :ok end)
```

Full upgrade guide: [RELEASE_NOTES_0_2_0.md](RELEASE_NOTES_0_2_0.md)

---

Feedback, issues, and pull requests welcome on GitHub. Next up (v0.3): Arrow
compute kernels, Parquet support, and Explorer/Nx bridge modules.
