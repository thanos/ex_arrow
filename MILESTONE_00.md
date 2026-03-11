Milestone 0 complete
1) Scope and acceptance criteria
Scope: Project skeleton, public API outline, resource-handle strategy, minimal NIF, CI scaffold.
DoD:
mix test passes
NIF compiles and loads (ExArrow.native_version/0 returns the crate version)
Docs build with mix docs
2) Files created/updated
Path	Purpose
mix.exs	Rustler deps, ex_doc, stream_data; docs config
lib/ex_arrow.ex	Public entry; API outline; native_version/0
lib/ex_arrow/error.ex	Structured error (code, message, details)
lib/ex_arrow/schema.ex, field.ex, array.ex, record_batch.ex, table.ex, stream.ex	Core handle stubs
lib/ex_arrow/ipc/reader.ex, ipc/writer.ex	IPC stubs
lib/ex_arrow/flight/client.ex, flight/server.ex	Flight stubs
lib/ex_arrow/adbc/database.ex, connection.ex, statement.ex	ADBC stubs
lib/ex_arrow/native.ex	Rustler NIF loader (ex_arrow_native)
native/ex_arrow_native/Cargo.toml, src/lib.rs	Minimal Rust NIF (nif_version)
native/ex_arrow_native/.cargo/config.toml	macOS link flags for NIF
test/ex_arrow_test.exs	Tests for NIF, Error, core/IPC/Flight/ADBC stubs
examples/ipc_roundtrip.exs, flight_echo/server.exs, flight_echo/client.exs, adbc_query.exs	Example stubs
docs/overview.md, docs/memory_model.md	ExDoc extras
.formatter.exs	Include examples/**/*.exs
.gitignore	/priv/native/
.github/workflows/ci.yml	Elixir + Rust build and test
README.md	Short project and dev instructions

3) Tests (what they assert)
Native NIF: ExArrow.native_version/0 returns a non-empty string.
ExArrow.Error: Exception with code/message; message-only creates :unknown code.
Core stubs: Schema.fields → []; RecordBatch.schema/num_rows → nil/0; Stream.next → nil.
IPC/Flight/ADBC: Reader from_binary, Writer to_binary, Client connect, Database open return {:error, :not_implemented}.

4) Implementation
All of the above files were added or updated as described. NIF is a single Rust function nif_version(); Elixir loads it via ExArrow.Native with Rustler (path native/ex_arrow_native, debug in dev/test).

5) Refactors/cleanup
Removed unused alias ExArrow.RecordBatch from IPC.Reader.
No further refactors in this milestone.

6) Docs
Overview: What ExArrow is (IPC/Flight/ADBC, native handles), what it is not, current status.
Memory model: Handles and lifetimes, copying rules, NIF scheduling note.
ExDoc configured with main: "overview" and these extras.

7) How to run the examples
```
mix run examples/ipc_roundtrip.exs
mix run examples/flight_echo/server.exs
mix run examples/flight_echo/client.exs
mix run examples/adbc_query.exs
```
Each prints the native version and the current stub result (e.g. :not_implemented).