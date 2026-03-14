# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-03-11

### Added

- **Arrow C Data Interface (`ExArrow.CDI`)** — zero-copy record batch transfer
  using the standardised Arrow CDI ABI.  `export/1` serialises a RecordBatch
  into heap-allocated `FFI_ArrowSchema` + `FFI_ArrowArray` C structs wrapped in
  a BEAM-managed resource handle.  `import/1` reconstructs a RecordBatch from
  the handle without any intermediate IPC bytes.  `pointers/1` exposes the raw
  C struct addresses as `{schema_ptr, array_ptr}` integers for interop with any
  CDI-compatible runtime (Polars, DuckDB, etc.) running in the same process.
  `mark_consumed/1` safely nulls the handle so the BEAM GC skips the Arrow
  release callback after an external consumer has taken ownership.
- **`ExArrow.Nx.from_tensors/1`** — builds a multi-column `RecordBatch` from a
  `%{col_name => Nx.Tensor}` map in a single NIF call (new
  `record_batch_from_column_binaries` NIF).  Column order follows
  `Map.to_list/1` (sorted by key).  All tensors must have the same number of
  elements; mismatched sizes return `{:error, "all tensors must have the same
  size…"}`.
- **Parquet lazy row-group streaming** — `ExArrow.Parquet.Reader.from_file/1`
  and `from_binary/1` now decode row groups lazily on demand via
  `ExArrow.Stream.next/1` instead of eagerly collecting all batches on open.
  Peak memory scales with the largest single row group rather than the full
  file.  The Elixir API is unchanged.
- **`docs/cdi_guide.md`** — new guide covering CDI concepts, round-trip usage,
  interop with external consumers, memory safety guarantees, and the roadmap
  for the zero-copy Explorer bridge.

### Changed

- **`ExArrow.Explorer` module documentation** updated to describe the current
  IPC path and the planned CDI zero-copy path for a future Explorer release.
- **`ExArrow.Nx` module documentation** updated with `from_tensors/1` examples.
- **Cargo.toml** — `arrow` crate updated to include the `ffi` feature;
  `ex_arrow_native` version bumped to `0.4.0`.

### Fixed

- `ExArrow.Parquet` stream lock now propagates `ArrowError` from lazy iterator
  (previously impossible with eager loading; now correctly returned as
  `{:error, msg}`).

---

## [0.3.0] - 2026-03-10

### Added

- **Arrow compute kernels** — `ExArrow.Compute` with three operations:
  `filter/2` (mask rows using the first column of a boolean-typed record
  batch), `project/2` (select and reorder columns by name), and `sort/3`
  (sort a batch by a named column, ascending or descending).  All operations
  run entirely in native Arrow memory via the `arrow-select` and `arrow-ord`
  Rust crates; results are new `ExArrow.RecordBatch` handles that can be
  piped directly into IPC writers, Flight `do_put`, or further compute calls.
- **Parquet support** — `ExArrow.Parquet.Reader` and
  `ExArrow.Parquet.Writer` for reading and writing the Parquet columnar
  storage format, backed by the `parquet` Rust crate.  Both file paths and
  in-memory binaries are supported.  Parquet streams share the same
  `ExArrow.Stream` interface as IPC and ADBC streams — `schema/1`, `next/1`,
  and `to_list/1` all work identically.
- **Explorer bridge module** — `ExArrow.Explorer` for direct conversion
  between `ExArrow.Stream` / `ExArrow.RecordBatch` and
  `Explorer.DataFrame` without writing manual IPC boilerplate.  Functions:
  `from_stream/1`, `from_record_batch/1`, `to_stream/1`,
  `to_record_batches/1`.  Requires `{:explorer, "~> 0.8"}` in your
  `mix.exs`; when Explorer is absent every function returns an informative
  `{:error, ...}` tuple.
- **Nx bridge module** — `ExArrow.Nx` for converting Arrow numeric columns
  to `Nx.Tensor` values (and back) by sharing the raw byte buffer — no
  list materialisation.  Functions: `column_to_tensor/2`, `to_tensors/1`,
  `from_tensor/2`.  Supports all integer and float Arrow types.
  Non-numeric columns return `{:error, ...}` and are silently skipped by
  `to_tensors/1`.  Requires `{:nx, "~> 0.7"}` in your `mix.exs`.
- **New optional dependency** — `{:nx, "~> 0.7", optional: true}` added to
  `mix.exs`.  No action required unless you use `ExArrow.Nx`.
- **`ExArrow.Stream` Parquet backend** — `Stream.schema/1` and
  `Stream.next/1` dispatch correctly for `:parquet`-backed streams returned
  by `ExArrow.Parquet.Reader`.
- **New Rust crate dependencies** — `arrow-select`, `arrow-ord`,
  `arrow-buffer`, `arrow-data` (compute kernels) and `parquet` (Parquet
  support) added to `native/ex_arrow_native/Cargo.toml`.
- **New guides** — `docs/parquet_guide.md` and `docs/compute_guide.md` with
  full API walkthroughs, examples, and performance notes.

### Fixed

- Dialyzer `call_without_opaque` warnings in `ExArrow.Explorer` and
  `ExArrow.Nx` — removed unnecessary struct pattern matches from function
  heads that were leaking the concrete struct type through the `@opaque`
  boundary of `ExArrow.Stream.t()` and `ExArrow.RecordBatch.t()`.
- Credo `alias must appear before module attribute` in `nx_test.exs` and
  `explorer_test.exs`.
- Credo `Pipe chain should start with a raw value` in `parquet_test.exs`.
- Credo `length/1 is expensive` in `explorer_test.exs`.
- `checksum-Elixir.ExArrow.Native.exs` regenerated for v0.3.0 across all
  supported platforms.

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
