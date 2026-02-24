# Memory model

## Handles and lifetimes

ExArrow uses **opaque handles** (Elixir structs wrapping a `resource` reference) for Schema, Array, RecordBatch, Table, and Stream. The actual Arrow data lives in native (Rust) memory. The BEAM only holds the handle; when the handle is garbage-collected or the resource is released, the native memory is freed.

## Copying rules

- **By default**: No copying. Functions return handles or small metadata (e.g. field names, row count). Large buffers stay in native memory.
- **Explicit copy**: When we add accessors that return Elixir lists or binaries (e.g. "give me column X as a list"), that will be documented as copying data onto the BEAM heap.
- **Streaming**: IPC and Flight streams yield RecordBatch handles one at a time. Consume and drop handles to allow native memory to be reclaimed; do not hold all batches in memory if you need low footprint.

## NIF scheduling

Long-running or blocking work in NIFs must not block BEAM schedulers. ExArrow will use dirty NIFs or native threads with message passing for such paths, as required by the project rules.
