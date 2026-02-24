# IPC guide

ExArrow supports the Arrow IPC (streaming) format: read and write record batches as a stream of bytes or to a file.

## Stream vs file

- **Stream**: Sequential read/write. Use `Reader.from_binary/1` or `Reader.from_file/1` to get an `ExArrow.Stream` that yields record batches via `ExArrow.Stream.next/1`. Use `Writer.to_binary/2` or `Writer.to_file/3` to write batches. No random access.
- **File format** (random access, batch index): Planned for Milestone 2.

## Reading

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

## Writing

You need a schema and a list (or enumerable) of record batches. Typically the schema comes from the first batch or from a stream you read:

```elixir
# schema and batches from a stream you read
{:ok, binary} = ExArrow.IPC.Writer.to_binary(schema, batches)
# or
:ok = ExArrow.IPC.Writer.to_file("/path/to/out.arrow", schema, batches)
```

## Types

Supported Arrow types in this release: `null`, `boolean`, `int64`, `float64`, `utf8`, `large_utf8`, `binary`, `large_binary`. Schema field types are returned as atoms (e.g. `:int64`, `:utf8`) from `Schema.fields/1`.

## Example

See `examples/ipc_roundtrip.exs`: load a fixture binary, read stream, collect batches, write back to binary, read again. Run with:

```bash
mix run examples/ipc_roundtrip.exs
```
