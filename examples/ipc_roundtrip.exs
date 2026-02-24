# IPC roundtrip example: read Arrow IPC from binary, then write back and read again.
# Usage: mix run examples/ipc_roundtrip.exs

alias ExArrow.IPC.Reader
alias ExArrow.IPC.Writer
alias ExArrow.Stream
alias ExArrow.Schema
alias ExArrow.RecordBatch
alias ExArrow.Native

IO.puts("ExArrow IPC roundtrip example")
IO.puts("Native NIF version: #{ExArrow.native_version()}")

# Get a small fixture binary (schema: id int64, name utf8; 2 rows)
{:ok, binary} = Native.ipc_test_fixture_binary()
IO.puts("Fixture size: #{byte_size(binary)} bytes")

# Read stream
case Reader.from_binary(binary) do
  {:ok, stream} ->
    schema = Stream.schema(stream)
    fields = Schema.fields(schema)
    IO.puts("Schema fields: #{inspect(Enum.map(fields, & &1.name))}")

    collect = fn collect, stream, acc ->
      case Stream.next(stream) do
        nil -> Enum.reverse(acc)
        {:error, _} -> Enum.reverse(acc)
        batch -> collect.(collect, stream, [batch | acc])
      end
    end

    batches = collect.(collect, stream, [])
    total_rows = Enum.reduce(batches, 0, fn b, acc -> acc + RecordBatch.num_rows(b) end)
    IO.puts("Batches: #{length(batches)}, total rows: #{total_rows}")

    # Roundtrip: write then read
    case Writer.to_binary(schema, batches) do
      {:ok, binary2} ->
        IO.puts("Roundtrip write: #{byte_size(binary2)} bytes")

        case Reader.from_binary(binary2) do
          {:ok, stream2} ->
            schema2 = Stream.schema(stream2)
            IO.puts("Roundtrip read OK, schema fields: #{length(Schema.fields(schema2))}")

          {:error, msg} ->
            IO.puts("Roundtrip read error: #{msg}")
        end

      {:error, msg} ->
        IO.puts("Roundtrip write error: #{msg}")
    end

  {:error, msg} ->
    IO.puts("Read error: #{msg}")
end
