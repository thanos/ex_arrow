# C Data Interface Guide

The [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
(CDI) is a standardised C ABI that lets Arrow implementations transfer data
**without serialisation** — no IPC bytes, no copies, just raw C struct pointers
shared between runtimes that both live in the same OS process.

ExArrow v0.4 introduces `ExArrow.CDI` which exposes the full CDI export/import
cycle from Elixir.

## How it works

```
ExArrow RecordBatch
        │
        ▼  cdi_export  (arrow-rs to_ffi)
FFI_ArrowSchema + FFI_ArrowArray  (heap-allocated C structs)
        │
        ▼  schema_ptr / array_ptr
   integer addresses (uintptr_t cast to u64)
        │
        ▼  external CDI consumer (future Explorer, Polars, DuckDB, …)
   zero-copy import into the consumer's Arrow runtime
```

When both ExArrow and the consuming library are loaded into the same BEAM
process, the C structs are valid shared memory — no network, no file, no
binary copy is needed.

## Within ExArrow (round-trip)

The simplest use is a within-ExArrow round-trip, which is also a useful
correctness test:

```elixir
{:ok, batch}  = ExArrow.IPC.Reader.from_file("trades.arrow") |> then(&ExArrow.Stream.next/1)

{:ok, handle} = ExArrow.CDI.export(batch)
{:ok, batch2} = ExArrow.CDI.import(handle)

ExArrow.RecordBatch.num_rows(batch2)  #=> same as batch
```

`export/1` allocates `FFI_ArrowArray` and `FFI_ArrowSchema` on the heap and
wraps them in a BEAM-managed resource handle.  `import/1` consumes the handle,
rebuilds the RecordBatch, and safely releases all native memory.

## With an external CDI consumer

Any CDI-compatible library loaded in the same BEAM process can import the raw
C struct pointers:

```elixir
{:ok, handle}           = ExArrow.CDI.export(batch)
{schema_ptr, array_ptr} = ExArrow.CDI.pointers(handle)

# Hand the integer addresses to the external consumer.
# Keep `handle` alive (in scope) until the consumer has finished importing!
SomeLib.import_arrow_cdi(schema_ptr, array_ptr)

# Tell ExArrow the consumer has taken ownership (called release internally).
:ok = ExArrow.CDI.mark_consumed(handle)
```

After `mark_consumed/1` the BEAM GC will drop the handle without calling the
Arrow release callback a second time, preventing a double-free.

## Memory safety guarantees

| Scenario | What happens |
|---|---|
| `import/1` called — ExArrow consumes the handle | Pointers atomically swapped to null; `Drop` is a no-op |
| `mark_consumed/1` called — external consumer took the data | Same as above |
| Handle GC'd without import or mark_consumed | `Drop` calls Arrow release callbacks; underlying data freed |
| External consumer already called `release` (null'd the callback) | `Drop` sees null release; no double-free |

## Explorer CDI path (roadmap)

`ExArrow.Explorer` currently uses an IPC binary round-trip.  The CDI module
lays the groundwork for a zero-copy path that will activate automatically once
Explorer exposes a CDI import API.  No code changes in user applications will
be required — the bridge will detect CDI availability at compile time and
choose the fastest available path.

## `ExArrow.Nx.from_tensors/1`

v0.4 also ships `from_tensors/1` which constructs a multi-column RecordBatch
from a map of tensors in one call — the CDI-adjacent pattern for Nx:

```elixir
tensors = %{
  "price" => Nx.tensor([1.5, 2.5, 3.5], type: {:f, 64}),
  "qty"   => Nx.tensor([100, 200, 300],  type: {:s, 32})
}

{:ok, batch} = ExArrow.Nx.from_tensors(tensors)
ExArrow.RecordBatch.num_rows(batch)  #=> 3
```

Column order follows `Map.to_list/1` (sorted by key).  All tensors must have
the same number of elements.

## Parquet lazy streaming

v0.4 also makes Parquet reading **lazy by default**.  Row groups are now
decoded on demand as you call `ExArrow.Stream.next/1`, rather than being
collected into memory all at once on `from_file/1`:

```elixir
{:ok, stream} = ExArrow.Parquet.Reader.from_file("large_dataset.parquet")

# Only the first row group is decoded here:
batch = ExArrow.Stream.next(stream)

# Subsequent row groups are decoded on demand:
batch2 = ExArrow.Stream.next(stream)
```

This significantly reduces peak memory usage for large files where you process
data incrementally (e.g. streaming into a Flight server or writing to a sink
one chunk at a time).
