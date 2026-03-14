
## Milestone 1 complete: IPC MVP (streaming only)

1) Scope and acceptance criteria
Scope: Schema and RecordBatch handles; IPC stream read from binary and file; IPC stream write to binary and file; streaming iterator in Elixir; types: bool, int64, float64, utf8, binary, nulls.
DoD:
Roundtrip tests: encode → decode matches on schema, row count, and fixture
examples/ipc_roundtrip.exs runs successfully
IPC guide exists and is short
2) Files created/updated
Path	Change
native/ex_arrow_native/Cargo.toml	Added arrow, arrow-ipc, arrow-schema, arrow-array (56.x)
native/ex_arrow_native/src/lib.rs	on_load + resource registration; NIF list extended
native/ex_arrow_native/src/resources.rs	New: ExArrowSchema, ExArrowRecordBatch, ExArrowIpcStream
native/ex_arrow_native/src/ipc.rs	New: IPC read/write NIFs, schema/batch accessors, test fixture
lib/ex_arrow/native.ex	NIF stubs for all new functions
lib/ex_arrow/schema.ex	fields/1 implemented via NIF
lib/ex_arrow/record_batch.ex	schema/1, num_rows/1 implemented via NIF
lib/ex_arrow/stream.ex	schema/1, next/1 implemented via NIF
lib/ex_arrow/ipc/reader.ex	from_binary/1, from_file/1 implemented
lib/ex_arrow/ipc/writer.ex	to_binary/2, to_file/3 implemented
test/ex_arrow_test.exs	IPC roundtrip tests, from_binary error test, stream iterator test
examples/ipc_roundtrip.exs	Full roundtrip: fixture → read → write → read
docs/ipc_guide.md	New: IPC guide (stream vs file, read, write, types, example)
mix.exs	docs/ipc_guide.md added to extras
3) Tests (what they assert)
Roundtrip: ipc_test_fixture_binary() returns binary; from_binary gives stream; stream schema has 2 fields (id int64, name utf8); first batch has 2 rows; roundtrip write then read gives same schema and row count.
from_binary empty: Returns {:error, _}.
Stream iterator: Collecting batches from the fixture yields one batch with 2 rows.
Schema.fields / RecordBatch from real IPC: Schema has 2 fields; batch has 2 rows and schema with 2 fields.
4) Implementation notes
Rust: Resource structs hold Arc<Schema>, RecordBatch, and Mutex<StreamReader<Cursor<Vec<u8>>>>. NIFs return {:ok, term} or {:error, msg} as encoded terms. Binary results use OwnedBinary so Elixir gets real binaries. Fixture NIF builds a small IPC stream (id int64, name utf8, 2 rows) for tests.
Elixir: Handles are %Schema{resource: ref} etc.; refs are passed to NIFs. Stream.next/1 returns %RecordBatch{}, nil, or {:error, msg}.
5) Refactors/cleanup
None beyond the above.
6) Docs
IPC guide (docs/ipc_guide.md): stream vs file, reading (from_binary, from_file, schema, next), writing (to_binary, to_file), supported types, link to examples/ipc_roundtrip.exs.
7) How to run the example
```
mix run examples/ipc_roundtrip.exs
```
Example output: fixture size, schema fields ["id", "name"], 1 batch and 2 rows, roundtrip write size, and “Roundtrip read OK”.


Summary of changes:
Rust: New resources and ipc modules; IPC stream read/write; resource types for schema, batch, stream; test fixture binary; all results returned as proper binaries where needed.
Elixir: Schema.fields, RecordBatch.schema/num_rows, Stream.schema/next implemented; IPC.Reader.from_binary/from_file, IPC.Writer.to_binary/to_file implemented; example and IPC guide added.
Tests: 4 new IPC tests (roundtrip, empty binary error, iterator, schema/batch accessors); Flight/ADBC stubs unchanged.
