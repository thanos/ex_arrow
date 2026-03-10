# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-03-09

### Added

- **TLS for Arrow Flight server** — `Server.start_link/2` now accepts a `:tls`
  option for one-way TLS (`[cert_pem: ..., key_pem: ...]`) and mutual TLS (add
  `ca_cert_pem:`). The Rust NIF uses tonic's `ServerTlsConfig` and `Identity`.
- **Flight server routing** — the built-in Flight server now stores multiple
  named datasets in a `HashMap`. Each `do_put` keys the dataset by the
  `FlightDescriptor` (cmd bytes or joined path segments), falling back to the
  legacy `"echo"` ticket when no descriptor is provided. New `"list_tickets"`
  action returns all stored ticket names.
- **`Client.do_put/4`** — optional `:descriptor` keyword argument
  (`{:cmd, binary()}` or `{:path, [String.t()]}`) routes the upload to a named
  ticket on the server.
- **ADBC connection pool** — `ExArrow.ADBC.ConnectionPool` is a first-class
  NimblePool-backed pool with `start_link/1`, `query/3`, and
  `with_connection/3`. Use it from a supervision tree via the companion
  `ExArrow.ADBC.DatabaseServer` GenServer.
- **`ExArrow.ADBC.DatabaseServer`** — supervised GenServer that holds an open
  `Database` handle and vends it by registered name to connection pools.
- **`Database.close/1` and `Connection.close/1`** — explicit close helpers
  (idiomatic no-ops backed by NIF GC; useful in pool `terminate_worker`).
- **`ExArrow.Schema.field_names/1`** — convenience accessor that returns just
  the field name strings without allocating `Field` structs.
- **`ExArrow.Stream.to_list/1`** — collects all remaining batches from a stream
  into an Elixir list.
- **Integration test matrix** — `test/ex_arrow/adbc_integration_test.exs` with
  PostgreSQL (pg 14/15/16) and DuckDB (1.1.3, 1.2.0) suites, gated behind the
  `:adbc_integration` tag and driven by a new
  `.github/workflows/integration.yml` CI workflow.

### Changed

- **`flight_server_start` NIF** now takes a third `server_tls` argument.
  Elixir callers use `Server.start_link/2` with the `:tls` option; the arity
  change is hidden behind the public API.
- **`flight_client_do_put` NIF** now takes a fourth `descriptor` argument.
  Existing calls pass `:none`; the default is wired in automatically by
  `Client.do_put/3` (3-arity form still works unchanged).
- **`ClientBehaviour.do_put/4`** — callback now includes the `opts` argument.
  Any existing Mox stubs must be updated to match 4 arguments.

### Fixed

- Dialyzer `call_without_opaque` warning in `ConnectionPool.init_worker/1` —
  replaced struct pattern match on opaque `Database.t()` with an `is_atom/1`
  guard, preserving the opaque boundary.
- Credo `alias must appear before module attribute` warning in integration test.
- Credo `length/1 is expensive` warnings replaced with `!= []` comparisons.
- `ExUnit.skip/1` compile warning in integration test replaced with
  `raise ExUnit.SkipError`.

## [0.1.0] - 2026-02-27

Initial release.

### Added

- **IPC**: Stream and file format. Read from binary or file; write to binary or file. Random-access file API (schema, batch count, get batch by index).
- **Arrow Flight**: Client and echo server. Connect, do_put/do_get, list_flights, get_flight_info, get_schema, list_actions, do_action. Plaintext HTTP/2 only.
- **ADBC**: Database, Connection, Statement. Open by driver path or name; execute SQL to Arrow stream. Metadata APIs (get_table_types, get_table_schema, get_objects) and Statement.bind where supported by driver.
- **Memory model**: Opaque handles (Schema, RecordBatch, Table, Stream). Data stays in native Arrow buffers; no BEAM heap copy by default. Dirty NIFs for long-running work.
- **Precompiled NIFs**: RustlerPrecompiled; prebuilt binaries for common targets (Linux, macOS, Windows) from GitHub releases. Optional local build via `EX_ARROW_BUILD=1`.

### Requirements

- Elixir ~> 1.14 (OTP 25/26, NIF 2.15 and 2.16)
- No Rust required for normal use (precompiled NIFs)
