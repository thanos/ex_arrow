# IPC roundtrip example (stub until Milestone 1).
# Usage: mix run examples/ipc_roundtrip.exs
#
# When implemented: read Arrow IPC from binary/file, then write back and verify.

IO.puts("ExArrow IPC roundtrip example (stub)")
IO.puts("Native NIF version: #{ExArrow.native_version()}")

case ExArrow.IPC.Reader.from_binary(<<>>) do
  {:ok, _stream} -> IO.puts("Read stream OK")
  {:error, reason} -> IO.puts("Read (expected stub): #{inspect(reason)}")
end
