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

  @moduletag :flight_sql_integration

  alias ExArrow.FlightSQL.{Client, Error}

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
      assert {:ok, count} = Client.execute(client, "CREATE TABLE ex_arrow_integration_test (id INT)")
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

  describe "prepare/2 + Statement.execute/1" do
    test "prepares and executes a SELECT" do
      client = connect()

      case Client.prepare(client, "SELECT 1 AS n") do
        {:ok, stmt} ->
          assert {:ok, stream} = ExArrow.FlightSQL.Statement.execute(stmt)
          batches = Enum.to_list(stream)
          assert length(batches) >= 1

        {:error, %Error{code: :unimplemented}} ->
          :ok
      end
    end
  end
end
