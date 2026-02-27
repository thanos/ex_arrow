# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
