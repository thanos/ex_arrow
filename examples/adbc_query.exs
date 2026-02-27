# ADBC query example.
# Usage: mix run examples/adbc_query.exs
#
# Set ADBC_DRIVER to the path of an ADBC driver shared library (e.g. libadbc_driver_sqlite.so),
# or leave unset to try driver_name lookup (fails cleanly if no driver is installed).

alias ExArrow.ADBC.{Connection, Database, Statement}
alias ExArrow.Stream

opts =
  case System.get_env("ADBC_DRIVER") do
    path when is_binary(path) and path != "" -> path
    _ -> [driver_name: "adbc_driver_sqlite", uri: ":memory:"]
  end

case Database.open(opts) do
  {:error, reason} ->
    IO.puts("Database open failed (driver not available?): #{inspect(reason)}")
    IO.puts("Set ADBC_DRIVER to the path of your ADBC driver .so/.dylib to run a query.")

  {:ok, db} ->
    IO.puts("Database opened")
    {:ok, conn} = Connection.open(db)
    {:ok, stmt} = Statement.new(conn, "SELECT 1 AS n")
    {:ok, stream} = Statement.execute(stmt)
    {:ok, schema} = Stream.schema(stream)
    IO.puts("Columns: #{inspect(ExArrow.Schema.fields(schema) |> Enum.map(& &1.name))}")
    batch = Stream.next(stream)

    if batch do
      IO.puts("Rows: #{ExArrow.RecordBatch.num_rows(batch)}")
    else
      IO.puts("No rows")
    end
end
