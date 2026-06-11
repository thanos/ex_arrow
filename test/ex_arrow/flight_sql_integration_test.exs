defmodule ExArrow.FlightSQL.IntegrationTest do
  @moduledoc """
  End-to-end integration tests for the Flight SQL client.

  These tests require a live Arrow Flight SQL server and are **excluded** from
  the default `mix test` run.  Enable them by passing `--include flight_sql_integration`:

      mix test test/ex_arrow/flight_sql_integration_test.exs --include flight_sql_integration

  ## DuckDB setup

  The easiest way to get a Flight SQL server running locally is via DuckDB's
  `flight_sql` extension.  Install DuckDB (>= 1.1), then:

      duckdb -c "INSTALL flight_sql; LOAD flight_sql; SELECT * FROM flight_sql_server_start();"

  The server listens on `localhost:32010` by default.

  ## Environment variables

  | Variable | Default | Description |
  |---|---|---|
  | `FLIGHT_SQL_HOST` | `localhost:32010` | Server URI (`host:port`) |
  | `FLIGHT_SQL_TOKEN` | — | Bearer token for authentication (optional) |
  """

  use ExUnit.Case
  alias ExArrow.FlightSQL.{Client, Error, Statement}
  alias ExArrow.RecordBatch

  @moduletag :flight_sql_integration

  @server_uri System.get_env("FLIGHT_SQL_HOST", "localhost:32010")

  defp connect do
    token = System.get_env("FLIGHT_SQL_TOKEN")

    opts =
      if token do
        [headers: [{"authorization", "Bearer #{token}"}]]
      else
        []
      end

    case Client.connect(@server_uri, opts) do
      {:ok, client} ->
        client

      {:error, err} ->
        ExUnit.Assertions.flunk(
          "cannot connect to Flight SQL server at #{@server_uri}: #{Error.message(err)}"
        )
    end
  end

  # ── Basic connectivity ────────────────────────────────────────────────────────

  describe "connect" do
    test "connects to the server" do
      client = connect()
      assert %Client{} = client
    end
  end

  # ── Query ────────────────────────────────────────────────────────────────────

  describe "query/2" do
    test "executes a SELECT and returns a Result" do
      client = connect()
      assert {:ok, result} = Client.query(client, "SELECT 1 AS n")
      assert result.num_rows == 1
      assert %ExArrow.Schema{} = result.schema
    end

    test "returns invalid_argument for bad SQL" do
      client = connect()

      assert {:error, %Error{code: code}} = Client.query(client, "SELECT FROM")
      assert code in [:invalid_argument, :server_error]
    end
  end

  # ── Stream query ──────────────────────────────────────────────────────────────

  describe "stream_query/2" do
    test "returns a lazy stream that is Enumerable" do
      client = connect()
      assert {:ok, stream} = Client.stream_query(client, "SELECT 1 AS n")
      batches = Enum.to_list(stream)
      assert length(batches) >= 1
    end
  end

  # ── DML ──────────────────────────────────────────────────────────────────────

  describe "execute/2" do
    test "creates a table and returns :unknown affected-row count" do
      client = connect()
      _ = Client.execute(client, "DROP TABLE IF EXISTS ex_arrow_integration_test")

      assert {:ok, count} =
               Client.execute(client, "CREATE TABLE ex_arrow_integration_test (id INT)")

      assert count in [0, :unknown]
      _ = Client.execute(client, "DROP TABLE IF EXISTS ex_arrow_integration_test")
    end
  end

  # ── Metadata ─────────────────────────────────────────────────────────────────

  describe "get_tables/2" do
    test "returns a stream of table metadata" do
      client = connect()

      case Client.get_tables(client) do
        {:ok, stream} ->
          batches = Enum.to_list(stream)
          assert is_list(batches)

        {:error, %Error{code: :unimplemented}} ->
          :ok
      end
    end
  end

  describe "get_db_schemas/2" do
    test "returns a stream of schema metadata or :unimplemented" do
      client = connect()

      case Client.get_db_schemas(client) do
        {:ok, stream} -> assert is_list(Enum.to_list(stream))
        {:error, %Error{code: :unimplemented}} -> :ok
      end
    end
  end

  describe "get_sql_info/1" do
    test "returns a stream of SQL capability info or :unimplemented" do
      client = connect()

      case Client.get_sql_info(client) do
        {:ok, stream} -> assert is_list(Enum.to_list(stream))
        {:error, %Error{code: :unimplemented}} -> :ok
      end
    end
  end

  # ── Prepared statements ───────────────────────────────────────────────────────

  # Skip the body when the server does not implement prepared statements.
  defp with_stmt(client, sql, fun) do
    case Client.prepare(client, sql) do
      {:ok, stmt} ->
        try do
          fun.(stmt)
        after
          # Best-effort cleanup; close errors here are not test failures.
          _ = Statement.close(stmt)
        end

      {:error, %Error{code: :unimplemented}} ->
        :ok
    end
  end

  describe "prepare/2 + Statement.execute/1" do
    test "prepares and executes a SELECT" do
      client = connect()

      with_stmt(client, "SELECT 1 AS n", fn stmt ->
        assert {:ok, stream} = Statement.execute(stmt)
        batches = Enum.to_list(stream)
        assert length(batches) >= 1
      end)
    end
  end

  # ── Statement.parameter_schema/1 ─────────────────────────────────────────────

  describe "Statement.parameter_schema/1" do
    test "returns a Schema for a statement with no parameters" do
      client = connect()

      with_stmt(client, "SELECT 1 AS n", fn stmt ->
        case Statement.parameter_schema(stmt) do
          {:ok, %ExArrow.Schema{} = schema} ->
            # Parameter-less queries should produce an empty parameter schema.
            assert ExArrow.Schema.fields(schema) == []

          {:error, %Error{code: code}} ->
            # Some servers don't expose a parameter schema for parameter-less
            # queries; accept :unimplemented or :invalid_argument.
            assert code in [:unimplemented, :invalid_argument]
        end
      end)
    end

    test "describes a single ? placeholder with one Field" do
      client = connect()

      with_stmt(client, "SELECT ? AS x", fn stmt ->
        case Statement.parameter_schema(stmt) do
          {:ok, %ExArrow.Schema{} = schema} ->
            fields = ExArrow.Schema.fields(schema)
            assert length(fields) == 1
            [field] = fields
            assert %ExArrow.Field{} = field
            assert is_binary(field.name)
            assert is_atom(field.type)

          {:error, %Error{code: code}} ->
            assert code in [:unimplemented, :invalid_argument]
        end
      end)
    end

    test "describes two ? placeholders with two Fields" do
      client = connect()

      with_stmt(client, "SELECT ? AS x, ? AS y", fn stmt ->
        case Statement.parameter_schema(stmt) do
          {:ok, %ExArrow.Schema{} = schema} ->
            assert length(ExArrow.Schema.fields(schema)) == 2

          {:error, %Error{code: code}} ->
            assert code in [:unimplemented, :invalid_argument]
        end
      end)
    end
  end

  # ── Statement.bind/2 ────────────────────────────────────────────────────────

  describe "Statement.bind/2 + Statement.execute/1" do
    test "binds an int64 parameter and executes" do
      client = connect()

      with_stmt(client, "SELECT ? AS x", fn stmt ->
        {:ok, params} =
          RecordBatch.from_columns(["x"], [<<42::little-signed-64>>], ["s64"], 1)

        case Statement.bind(stmt, params) do
          :ok ->
            assert {:ok, stream} = Statement.execute(stmt)
            batches = Enum.to_list(stream)
            assert length(batches) >= 1

          {:error, %Error{code: code}} ->
            # Servers that don't accept bind for ad-hoc placeholders
            # may return :unimplemented or :invalid_argument.
            assert code in [:unimplemented, :invalid_argument]
        end
      end)
    end

    test "rebinding replaces previous parameters" do
      client = connect()

      with_stmt(client, "SELECT ? AS x", fn stmt ->
        {:ok, p1} = RecordBatch.from_columns(["x"], [<<1::little-signed-64>>], ["s64"], 1)
        {:ok, p2} = RecordBatch.from_columns(["x"], [<<2::little-signed-64>>], ["s64"], 1)

        case {Statement.bind(stmt, p1), Statement.bind(stmt, p2)} do
          {:ok, :ok} ->
            assert {:ok, stream} = Statement.execute(stmt)
            assert length(Enum.to_list(stream)) >= 1

          {{:error, %Error{code: code}}, _} ->
            assert code in [:unimplemented, :invalid_argument]

          {_, {:error, %Error{code: code}}} ->
            assert code in [:unimplemented, :invalid_argument]
        end
      end)
    end

    test "binds a utf8 parameter built via from_columns/4 (length-prefixed wire format)" do
      client = connect()

      with_stmt(client, "SELECT ? AS s", fn stmt ->
        # Two strings, length-prefixed.
        utf8 = <<5::little-32, "hello", 5::little-32, "world">>
        {:ok, params} = RecordBatch.from_columns(["s"], [utf8], ["utf8"], 2)

        case Statement.bind(stmt, params) do
          :ok ->
            assert {:ok, stream} = Statement.execute(stmt)
            assert length(Enum.to_list(stream)) >= 1

          {:error, %Error{code: code}} ->
            assert code in [:unimplemented, :invalid_argument]
        end
      end)
    end
  end

  # ── Statement.close/1 lifecycle ─────────────────────────────────────────────

  describe "Statement.close/1" do
    test "explicit close after execute returns :ok" do
      client = connect()

      case Client.prepare(client, "SELECT 1 AS n") do
        {:ok, stmt} ->
          assert {:ok, _stream} = Statement.execute(stmt)
          assert :ok = Statement.close(stmt)

        {:error, %Error{code: :unimplemented}} ->
          :ok
      end
    end

    test "operations on a closed statement return :protocol_error" do
      client = connect()

      case Client.prepare(client, "SELECT 1 AS n") do
        {:ok, stmt} ->
          assert :ok = Statement.close(stmt)

          assert {:error, %Error{code: :protocol_error}} = Statement.execute(stmt)
          assert {:error, %Error{code: :protocol_error}} = Statement.execute_update(stmt)
          assert {:error, %Error{code: :protocol_error}} = Statement.parameter_schema(stmt)

          {:ok, params} =
            RecordBatch.from_columns(["x"], [<<1::little-signed-64>>], ["s64"], 1)

          assert {:error, %Error{code: :protocol_error}} = Statement.bind(stmt, params)

        {:error, %Error{code: :unimplemented}} ->
          :ok
      end
    end

    test "close is idempotent" do
      client = connect()

      case Client.prepare(client, "SELECT 1 AS n") do
        {:ok, stmt} ->
          assert :ok = Statement.close(stmt)
          assert :ok = Statement.close(stmt)
          assert :ok = Statement.close(stmt)

        {:error, %Error{code: :unimplemented}} ->
          :ok
      end
    end
  end
end
