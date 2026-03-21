# Memory model

## Handles and lifetimes

ExArrow uses **opaque handles** (Elixir structs wrapping a `resource` reference) for Schema, Array, RecordBatch, Table, and Stream. The actual Arrow data lives in native (Rust) memory. The BEAM only holds the handle; when the handle is garbage-collected or the resource is released, the native memory is freed.

## Copying rules

- **By default**: No copying. Functions return handles or small metadata (e.g. field names, row count). Large buffers stay in native memory.
- **Explicit copy**: Accessors that return Elixir lists or binaries (e.g. `ExArrow.Nx.column_to_tensor/2`) copy the buffer once from native Arrow memory onto the BEAM heap and are documented as such.
- **Streaming**: IPC, Flight, and Parquet streams yield `RecordBatch` handles one at a time. Consume and drop handles to allow native memory to be reclaimed; do not hold all batches in memory if you need a low footprint.
- **CDI transfer**: `ExArrow.CDI.export/1` exposes a `RecordBatch`'s `FFI_ArrowSchema` and `FFI_ArrowArray` C structs as raw pointer addresses, enabling a CDI-compatible runtime (Polars, DuckDB, etc.) running in the same OS process to import the data **without any copy or serialisation**. The BEAM resource holds the C structs alive until `import/1` or `mark_consumed/1` is called. See [CDI guide](cdi_guide.md).

## NIF scheduling

Long-running or blocking work in NIFs must not block BEAM schedulers. ExArrow will use dirty NIFs or native threads with message passing for such paths, as required by the project rules.
