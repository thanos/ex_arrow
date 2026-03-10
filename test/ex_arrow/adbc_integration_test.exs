defmodule ExArrow.ADBCIntegrationTest do
  @moduledoc """
  Integration tests for ADBC against real database drivers.

  These tests are **excluded by default** and require the `adbc_integration`
  ExUnit tag to run:

      mix test --include adbc_integration

  ## Environment variables

  Each suite reads connection details from environment variables so the same
  test module works in CI (where services are injected by GitHub Actions
  service containers) and locally (pointing to a developer database):

  ### PostgreSQL

  | Variable          | Default         | Description                          |
  |-------------------|-----------------|--------------------------------------|
  | `PG_HOST`         | `localhost`     | PostgreSQL host                      |
  | `PG_PORT`         | `5432`          | PostgreSQL port                      |
  | `PG_USER`         | `postgres`      | Username                             |
  | `PG_PASSWORD`     | `postgres`      | Password                             |
  | `PG_DATABASE`     | `postgres`      | Database name                        |
  | `PG_ADBC_DRIVER`  | *(optional)*    | Explicit path to ADBC PG driver `.so`|

  When `PG_ADBC_DRIVER` is not set the driver is looked up by name
  (`adbc_driver_postgresql`) via the system library search path.
  CI sets this variable by installing `adbc-driver-postgresql` via pip.

  ### DuckDB

  | Variable           | Default      | Description                         |
  |--------------------|--------------|-------------------------------------|
  | `DUCKDB_DRIVER`    | *(required)* | Path to `libduckdb.so`              |
  | `DUCKDB_DATABASE`  | `:memory:`   | Database path (`":memory:"` ok)     |

  ## Running locally

      # PostgreSQL (Docker)
      docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16

      # Then run
      mix test --include adbc_integration test/ex_arrow/adbc_integration_test.exs
  """

  use ExUnit.Case, async: false

  alias ExArrow.ADBC.{Connection, Database, Statement}

  @moduletag :adbc_integration

  # ── Helpers ──────────────────────────────────────────────────────────────────

  defp env(key, default \\ nil), do: System.get_env(key, default)

  # Open a connection and run `fun.(conn)`, closing everything afterwards.
  defp with_connection(db_opts, fun) do
    db_result = Database.open(db_opts)

    assert {:ok, db} = db_result,
           "Database.open failed with: #{inspect(db_result)}\n  opts: #{inspect(db_opts)}"

    conn_result = Connection.open(db)

    assert {:ok, conn} = conn_result,
           "Connection.open failed with: #{inspect(conn_result)}"

    result = fun.(conn)
    Connection.close(conn)
    Database.close(db)
    result
  end

  # ── PostgreSQL ────────────────────────────────────────────────────────────────

  describe "PostgreSQL ADBC" do
    # Evaluated at compile time: skip the whole describe block when PG_HOST is
    # not set.  Runtime skip helpers (raise/throw) are unreliable in setup
    # callbacks across ExUnit versions; @describetag is guaranteed to work.
    unless System.get_env("PG_HOST") do
      @describetag skip: "set PG_HOST env var to enable PostgreSQL integration tests"
    end

    defp pg_opts do
      uri =
        "postgresql://#{env("PG_USER", "postgres")}:#{env("PG_PASSWORD", "postgres")}" <>
          "@#{env("PG_HOST", "localhost")}:#{env("PG_PORT", "5432")}/#{env("PG_DATABASE", "postgres")}"

      # Prefer an explicit driver path (set by CI); fall back to driver_name so
      # the ADBC driver manager searches LD_LIBRARY_PATH / system paths.
      driver_opt =
        case env("PG_ADBC_DRIVER") do
          nil -> {:driver_name, "adbc_driver_postgresql"}
          path -> {:driver_path, path}
        end

      [driver_opt, uri: uri]
    end

    test "connect, create table, insert and query rows" do
      with_connection(pg_opts(), fn conn ->
        # Create a temporary table.
        assert {:ok, stmt} = Statement.new(conn)
        assert :ok = Statement.set_sql(stmt, "CREATE TEMP TABLE ex_arrow_test (id INT, val TEXT)")
        assert {:ok, _stream} = Statement.execute(stmt)

        # Insert rows.
        assert {:ok, stmt} = Statement.new(conn)

        assert :ok =
                 Statement.set_sql(
                   stmt,
                   "INSERT INTO ex_arrow_test VALUES (1, 'hello'), (2, 'world')"
                 )

        assert {:ok, _stream} = Statement.execute(stmt)

        # Query and stream batches.
        assert {:ok, stmt} = Statement.new(conn)
        assert :ok = Statement.set_sql(stmt, "SELECT id, val FROM ex_arrow_test ORDER BY id")
        assert {:ok, stream} = Statement.execute(stmt)
        batches = ExArrow.Stream.to_list(stream)
        assert batches != []
        total_rows = Enum.sum(Enum.map(batches, &ExArrow.RecordBatch.num_rows/1))
        assert total_rows == 2
      end)
    end

    test "schema contains expected field names" do
      with_connection(pg_opts(), fn conn ->
        assert {:ok, stmt} = Statement.new(conn)
        assert :ok = Statement.set_sql(stmt, "SELECT 1 AS one, 'a' AS letter")
        assert {:ok, stream} = Statement.execute(stmt)
        assert {:ok, schema} = ExArrow.Stream.schema(stream)
        field_names = ExArrow.Schema.field_names(schema)
        assert "one" in field_names
        assert "letter" in field_names
      end)
    end

    test "get_table_types returns a non-empty Arrow stream" do
      with_connection(pg_opts(), fn conn ->
        # get_table_types/1 returns {:ok, Stream.t()} per the ADBC spec.
        assert {:ok, stream} = Connection.get_table_types(conn)
        batches = ExArrow.Stream.to_list(stream)
        assert batches != []
        total_rows = Enum.sum(Enum.map(batches, &ExArrow.RecordBatch.num_rows/1))
        assert total_rows > 0
      end)
    end
  end

  # ── DuckDB ────────────────────────────────────────────────────────────────────

  describe "DuckDB ADBC" do
    unless System.get_env("DUCKDB_DRIVER") do
      @describetag skip: "set DUCKDB_DRIVER env var to enable DuckDB integration tests"
    end

    defp duckdb_opts do
      [
        driver_path: env("DUCKDB_DRIVER"),
        # DuckDB accepts "uri" as the option key for the database path.
        # ":memory:" creates an in-memory database (no file on disk).
        uri: env("DUCKDB_DATABASE", ":memory:")
      ]
    end

    test "connect and run a simple SELECT" do
      with_connection(duckdb_opts(), fn conn ->
        assert {:ok, stmt} = Statement.new(conn)
        assert :ok = Statement.set_sql(stmt, "SELECT 42 AS answer")
        assert {:ok, stream} = Statement.execute(stmt)
        batches = ExArrow.Stream.to_list(stream)
        assert batches != []
        total_rows = Enum.sum(Enum.map(batches, &ExArrow.RecordBatch.num_rows/1))
        assert total_rows == 1
      end)
    end

    test "create table, insert rows and query via Arrow stream" do
      with_connection(duckdb_opts(), fn conn ->
        assert {:ok, stmt} = Statement.new(conn)

        assert :ok =
                 Statement.set_sql(stmt, "CREATE TABLE ex_arrow_test (id INTEGER, name VARCHAR)")

        assert {:ok, _} = Statement.execute(stmt)

        assert {:ok, stmt} = Statement.new(conn)

        assert :ok =
                 Statement.set_sql(
                   stmt,
                   "INSERT INTO ex_arrow_test VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')"
                 )

        assert {:ok, _} = Statement.execute(stmt)

        assert {:ok, stmt} = Statement.new(conn)
        assert :ok = Statement.set_sql(stmt, "SELECT * FROM ex_arrow_test ORDER BY id")
        assert {:ok, stream} = Statement.execute(stmt)
        batches = ExArrow.Stream.to_list(stream)
        total_rows = Enum.sum(Enum.map(batches, &ExArrow.RecordBatch.num_rows/1))
        assert total_rows == 3
      end)
    end

    test "schema field names match query columns" do
      with_connection(duckdb_opts(), fn conn ->
        assert {:ok, stmt} = Statement.new(conn)
        assert :ok = Statement.set_sql(stmt, "SELECT 1.5 AS price, 'kg' AS unit")
        assert {:ok, stream} = Statement.execute(stmt)
        assert {:ok, schema} = ExArrow.Stream.schema(stream)
        names = ExArrow.Schema.field_names(schema)
        assert "price" in names
        assert "unit" in names
      end)
    end

    test "large result set streams in multiple batches" do
      with_connection(duckdb_opts(), fn conn ->
        assert {:ok, stmt} = Statement.new(conn)

        assert :ok =
                 Statement.set_sql(
                   stmt,
                   "SELECT range AS n FROM range(0, 100000)"
                 )

        assert {:ok, stream} = Statement.execute(stmt)
        batches = ExArrow.Stream.to_list(stream)
        total_rows = Enum.sum(Enum.map(batches, &ExArrow.RecordBatch.num_rows/1))
        assert total_rows == 100_000
      end)
    end
  end
end
