# ADBC query example (stub until Milestone 5).
# Usage: mix run examples/adbc_query.exs
#
# When implemented: open database (driver), connection, statement, execute, stream batches.

IO.puts("ExArrow ADBC query example (stub)")
IO.puts("Native NIF version: #{ExArrow.native_version()}")

case ExArrow.ADBC.Database.open(System.get_env("ADBC_DRIVER") || "/path/to/driver") do
  {:ok, _db} -> IO.puts("Database opened")
  {:error, reason} -> IO.puts("Open (expected stub): #{inspect(reason)}")
end
