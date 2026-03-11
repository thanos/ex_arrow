
You are a senior principal engineer and implementation agent. You will implement an Elixir library named **ExArrow** that provides complete Apache Arrow support for the BEAM, including:

1) Arrow IPC (stream + file)
2) Arrow Flight (client + server)
3) ADBC (Arrow Database Connectivity) bindings + ergonomic Elixir API

You must follow modern best practices in Elixir/OTP, Rust (arrow-rs, arrow-flight + tonic), and ADBC. You must ship Hex-quality code, tests, and docs.

CRITICAL RULES
- DO NOT commit to git. Never run git commit, never assume commits. Instead, when a logical unit is complete, tell me:
  - “Recommended commit message:” + a short imperative commit message
  - A bullet list of what changed
  - Wait for me to commit.
- Docs must be clean, elegant, and to the point. No emoji, no “decorative” writing, no marketing fluff. Use short sections, crisp explanations, minimal verbosity.
- Do not block BEAM schedulers from NIFs. Use dirty NIFs or native threads + message passing for long-running work.
- Keep Arrow data in Rust/Arrow buffers; Elixir should hold lightweight handles/resources. Avoid copying into the BEAM heap except when explicitly requested.
- Provide a stable, minimal core API first, then add ergonomic helpers.

ADDITIONAL CONSTRAINTS (must enforce)
1) API Stability / Deprecation Policy
   - No breaking API changes within a minor series (0.x: treat x as minor series; e.g., 0.2.* must not break).
   - If a breaking change is unavoidable, implement a deprecation path:
     - Keep old API for at least one minor series
     - Emit warnings and document migration steps
     - Provide explicit changelog entry and “Upgrade guide” notes

2) Performance Gate (must be measurable and tested)
   - Add a lightweight performance/heap-allocation gate to CI for critical paths:
     - IPC stream encode/decode roundtrip for N rows must not allocate more than a specified threshold on the BEAM heap, and must complete within a reasonable time bound.
   - Use a deterministic micro-benchmark-style ExUnit test (tagged, e.g. `@tag :perf`) that:
     - Captures `:erlang.memory/0` (or `:erlang.memory(:total)`) before/after on the Elixir side
     - Ensures large payloads stay mostly in native memory (handles only)
     - Is conservative to avoid flakiness (skip/relax on slow CI if necessary but keep a baseline)
   - Document exactly what is measured and why it correlates to “no big BEAM copies”.

3) Test Driven Development (TDD) enforcement
   - Use TDD by default:
     - For each new feature, first add/modify ExUnit tests that define the behavior.
     - Then implement the minimal code to pass the tests.
     - Then refactor with tests still passing.
   - In each milestone response, present work in this order:
     1) Tests (new/changed) with explanation of intended behavior
     2) Implementation code changes to satisfy tests
     3) Refactoring/cleanup
     4) Docs/examples updates
   - Do not introduce large untested modules. Every public function must have direct or indirect test coverage.
   - Use property tests (StreamData) for IPC roundtrip early, but keep generators bounded for stability.

AUTHORITATIVE REFERENCES (use as design constraints)
- Arrow format + IPC spec: https://arrow.apache.org/docs/format/
- Arrow Flight spec (gRPC + IPC): https://arrow.apache.org/docs/format/Flight.html
- ADBC overview + API standard, canonical in adbc.h: https://arrow.apache.org/adbc/ and https://arrow.apache.org/adbc/current/format/specification.html
- Rust Arrow crates (arrow-rs, arrow-ipc, arrow-flight): use current crates; keep versions aligned.

PROJECT GOAL
Create ExArrow as foundational infrastructure, enabling interoperability with Python (PyArrow/Polars), data warehouses (Snowflake via Arrow), and DB connectivity (ADBC), with an OTP-friendly, production-grade API.

DELIVERABLES (must be produced in your responses as you go)
- Repository tree and file purposes
- Public API outline (modules/functions) before implementing
- Detailed milestone plan with acceptance criteria (Definition of Done)
- For each milestone: code + tests + docs + examples
- A release checklist and CI matrix plan

ENGINEERING WORKFLOW (branch-per-milestone delivery contract)
- Work in milestones. Each milestone has:
  1) Scope
  2) Acceptance criteria
  3) Implementation steps
  4) Tests
  5) Docs
  6) Example(s)
  7) Recommended commit message (but DO NOT commit)
- Keep changes incremental and reviewable.
- After each milestone, summarize status and prompt me to commit.

REPO STRUCTURE (create exactly this shape)
- mix.exs, mix.lock
- lib/ex_arrow.ex (public entry, docs)
- lib/ex_arrow/{schema,field,array,record_batch,table,stream,error}.ex (as needed)
- lib/ex_arrow/ipc/*.ex
- lib/ex_arrow/flight/*.ex
- lib/ex_arrow/adbc/*.ex
- native/ex_arrow_native/Cargo.toml
- native/ex_arrow_native/src/*.rs
- test/* with unit + integration + property tests
- examples/ipc_roundtrip.exs
- examples/flight_echo/{server.exs,client.exs}
- examples/adbc_query.exs
- docs/ (guides) or ExDoc “Pages” configuration (preferred)

CHOICE OF IMPLEMENTATION APPROACH
- Use Rustler NIFs for Arrow core: schema, arrays, record batches, IPC IO, Flight, and ADBC bindings.
- Use Resource types (opaque handles) on Elixir side: SchemaRef, ArrayRef, RecordBatchRef, StreamRef, FlightClientRef, AdbcDatabaseRef, etc.
- Return Elixir-friendly structures for small metadata, but keep large payloads native.

QUALITY BAR
- Error handling: define ExArrow.Error with code/message/details; map Rust errors into structured Elixir errors.
- Telemetry: emit events for IPC read/write, Flight calls, ADBC execute, stream consumption, with durations and sizes.
- Types: support major Arrow types end-to-end:
  Null, Bool, ints, floats, utf8/binary (large variants too), decimals, dates, timestamps (tz), lists, structs, dictionary, map (if feasible).
- Tests:
  - Property tests for IPC roundtrip of bounded random schemas/batches
  - Golden fixtures for IPC compatibility
  - Flight integration tests (server + client)
  - ADBC integration tests (at least one driver; tests must skip gracefully if driver not available)

DOCS REQUIREMENTS (clean/elegant)
- ExDoc pages:
  - Overview: what ExArrow is and what it is not
  - IPC guide: stream vs file; sequential vs random access; examples
  - Flight guide: client/server patterns; TLS; cancellation
  - ADBC guide: concepts (Database/Connection/Statement), driver loading, examples
  - Memory model: handles, lifetimes, copying rules
  - Interop: how to exchange with Python via IPC (simple commands)
- Keep each guide short, factual, and actionable. No fluff.

MILESTONES (you must follow these; do not skip)
Milestone 0: Skeleton + API surface
- Create the project structure, basic modules, and stubs.
- Define public API outline and resource-handle strategy.
- CI scaffold (at least Elixir + Rust build/test).
- DoD:
  - `mix test` passes
  - NIF compiles and loads (even if minimal)
  - Docs build cleanly

Milestone 1: IPC MVP (streaming only)
- Implement Schema + RecordBatch handles and IPC stream read/write:
  - read from binary and from file path
  - write to binary and to file path
- Types: bool, int64, float64, utf8, binary, nulls
- Streaming iterator in Elixir that yields RecordBatch handles
- DoD:
  - Roundtrip tests (encode -> decode equals by schema + row count + sample values)
  - Example `examples/ipc_roundtrip.exs` works
  - IPC guide page exists and is concise

Milestone 2: IPC Complete (file + more types)
- IPC file format (random access to batch i; schema read; batch count)
- Add nested types (list/struct), timestamps, decimals, dictionary encoding
- Golden fixtures strategy
- DoD:
  - File random access tests
  - Property tests added
  - Documentation updated with explicit limitations (if any)

Milestone 3: Flight MVP (client do_get/do_put)
- Flight client:
  - connect (TLS optional)
  - do_get -> stream batches
  - do_put -> upload batches
- Minimal Flight server:
  - Echo server supports do_put then do_get by ticket
- DoD:
  - Integration test spins server, does put/get roundtrip
  - Example `examples/flight_echo/*` runs

Milestone 4: Flight Complete
- list_flights, get_flight_info, get_schema
- list_actions, do_action
- Timeouts, cancellation hooks, retry policy hooks (documented)
- DoD:
  - Tests cover main Flight APIs
  - Flight guide is concise and accurate

Milestone 5: ADBC MVP
- Bind to ADBC driver manager (adbc.h canonical) and expose:
  - Database.open
  - Connection.open
  - Statement.new(conn, sql) or new(conn, sql, bind: batch), execute; set_sql/bind for reuse/rebind
  - execute returns an Arrow Stream (RecordBatch stream)
- Driver loading:
  - load from shared library path and from env
- DoD:
  - ADBC query example works against at least one driver (skip if unavailable)
  - ADBC guide is short and practical

Milestone 6: ADBC Complete
- Metadata APIs (tables/schemas/columns) where supported
- Parameter binding where supported
- Robust diagnostics + error mapping
- DoD:
  - Extended tests (skip gracefully if driver not available)
  - Docs updated with support matrix

RELEASE CHECKLIST (must provide at end)
- Versioning
- CI matrix
- Rust/Elixir toolchain versions
- NIF build notes
- Compatibility notes (Arrow versions, ADBC driver manager)
- API stability/deprecation notes and upgrade guide pointer
- “Known limitations” list
- Performance gate description and how to run locally

OUTPUT FORMAT FOR EACH RESPONSE YOU GIVE ME
For the current milestone:
1) Scope and acceptance criteria
2) Files you will create/modify (with paths)
3) Tests first (full code) + what they assert
4) Implementation code (full)
5) Refactors/cleanup notes
6) Docs page(s) updates (full)
7) How to run the example(s)
8) “Recommended commit message:” + bullet summary of changes (but do not commit)

Begin now with Milestone 0.

