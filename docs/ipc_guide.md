# IPC guide

ExArrow supports the Arrow IPC format: streaming (sequential) and file format (random access). You can read and write record batches as a stream of bytes or to a file.

## Stream vs file

- **Stream**: Sequential read/write. Use `Reader.from_binary/1` or `Reader.from_file/1` to get an `ExArrow.Stream` that yields record batches via `ExArrow.Stream.next/1`. Use `Writer.to_binary/2` or `Writer.to_file/3` to write batches. No random access.
- **File format**: Random access by batch index. Use `ExArrow.IPC.File.from_file/1` or `ExArrow.IPC.File.from_binary/1` to open; then `schema/1`, `batch_count/1`, and `get_batch/2` to read without consuming. Write file format with `ExArrow.Native.ipc_file_writer_to_file/3` (low-level; a higher-level API may be added later).

## Reading (stream)

Open a stream from binary or path:

```elixir
# From binary (e.g. from socket or buffer)
{:ok, stream} = ExArrow.IPC.Reader.from_binary(ipc_bytes)

# From file path
{:ok, stream} = ExArrow.IPC.Reader.from_file("/path/to/file.arrow")
```

Get the schema (without consuming the stream) and read batches:

```elixir
case ExArrow.Stream.schema(stream) do
  {:ok, schema} ->
    fields = ExArrow.Schema.fields(schema)
    # ...
  {:error, msg} -> # stream invalid (e.g. poisoned lock)
end

# Consume batches
case ExArrow.Stream.next(stream) do
  %ExArrow.RecordBatch{} = batch -> # use batch, then call next/1 again
  nil -> # stream finished
  {:error, msg} -> # read error
end
```

## Reading (file format, random access)

Open an IPC file (from path or in-memory binary) for random access:

```elixir
# From file path
{:ok, file} = ExArrow.IPC.File.from_file("/path/to/file.arrow")

# From binary (e.g. for tests or in-memory file format)
{:ok, file} = ExArrow.IPC.File.from_binary(ipc_file_bytes)
```

Then read schema, batch count, and any batch by index:

```elixir
schema = ExArrow.IPC.File.schema(file)
fields = ExArrow.Schema.fields(schema)

n = ExArrow.IPC.File.batch_count(file)

# Get batch at 0-based index
{:ok, batch} = ExArrow.IPC.File.get_batch(file, 0)
rows = ExArrow.RecordBatch.num_rows(batch)
```

File format uses an Arrow footer; stream format does not. Use file format when you need random access or to know the batch count without reading all batches.

## Writing

You need a schema and a list (or enumerable) of record batches. Typically the schema comes from the first batch or from a stream you read:

```elixir
# schema and batches from a stream you read
{:ok, binary} = ExArrow.IPC.Writer.to_binary(schema, batches)
# or
:ok = ExArrow.IPC.Writer.to_file("/path/to/out.arrow", schema, batches)
```

## Types

Schema field types are returned as atoms from `Schema.fields/1`. Supported type atoms:

- Primitives: `:null`, `:boolean`, `:int64`, `:float64`
- Strings/binary: `:utf8`, `:large_utf8`, `:binary`, `:large_binary`
- Nested: `:list`, `:large_list`, `:struct`
- Time: `:timestamp` (unit/timezone not exposed as atoms)
- Decimal: `:decimal128`, `:decimal256`
- Dictionary: `:dictionary`

Any type not explicitly mapped is returned as `:unknown`. Reading and writing data for these types may still work; only the schema reflection uses the atom.

## Limitations

- **Stream vs file**: Writer `to_file/3` produces stream format; for file format (footer, random access) use `ExArrow.Native.ipc_file_writer_to_file/3`.
- **Type reflection**: Only the atoms above are returned for field types; nested/child types (e.g. element type of a list) are not yet exposed.
- **No column/array access**: Record batches are opaque; copying data out to Elixir (e.g. column arrays) is planned for a later milestone.

## Example

See `examples/ipc_roundtrip.exs`: load a fixture binary, read stream, collect batches, write back to binary, read again. Run with:

```bash
mix run examples/ipc_roundtrip.exs
```
