defmodule ExArrow.FlightSQL do
  @moduledoc """
  Arrow Flight SQL client for ExArrow.

  Provides a production-grade Elixir interface for executing SQL queries against
  remote Flight SQL servers (DuckDB, DataFusion, Dremio, InfluxDB v3, and others)
  and consuming results as Arrow-native data.

  ## What Flight SQL is

  Arrow Flight SQL layers SQL query semantics on top of Arrow Flight (gRPC + Arrow IPC).
  Queries are dispatched to the server, which executes them and streams results back as
  columnar `RecordBatch` data — the same Arrow format used everywhere in ExArrow.

  Flight SQL is the correct choice when connecting to **remote query servers**.
  For in-process databases (DuckDB local, SQLite, PostgreSQL via driver), use
  `ExArrow.ADBC` instead.

  ## Entry point

  All operations go through `ExArrow.FlightSQL.Client`:

      {:ok, client} = ExArrow.FlightSQL.Client.connect("localhost:32010")
      {:ok, result} = ExArrow.FlightSQL.Client.query(client, "SELECT * FROM t")

  See `ExArrow.FlightSQL.Client` for the full API.

  ## v0.5.0 scope

  The following are supported:

  - Ad-hoc SQL query execution (`query/2`, `query!/2`, `stream_query/2`)
  - DML execution with affected-row count (`execute/2`)
  - Lazy streaming of large result sets (`stream_query/2`)
  - TLS connections (plaintext, OS trust store, custom CA)
  - Bearer-token and custom gRPC header injection

  The following are **not** supported in v0.5.0 and are deferred:

  - Prepared statements (v0.6.0)
  - Bulk ingestion (`DoPut`)
  - Transactions (`BEGIN`, `COMMIT`, `ROLLBACK`)
  - Multi-endpoint distributed query results
  - Flight SQL server-side implementation (ExArrow is a client library only)

  ## Ecosystem integration

      # Convert result to Explorer DataFrame
      {:ok, result} = ExArrow.FlightSQL.Client.query(client, "SELECT * FROM t")
      {:ok, df}     = ExArrow.FlightSQL.Result.to_dataframe(result)

      # Convert a numeric column to an Nx tensor
      {:ok, tensor} = ExArrow.FlightSQL.Result.to_tensor(result, "price")

      # Use lazy stream for large results
      {:ok, stream} = ExArrow.FlightSQL.Client.stream_query(client, "SELECT * FROM big_table")
      {:ok, df}     = ExArrow.Explorer.from_stream(stream)
  """
end
