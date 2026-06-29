defmodule ExArrow.StreamTest do
  use ExUnit.Case, async: false

  alias ExArrow.Stream

  # ── Helpers ─────────────────────────────────────────────────────────────────

  defp stream(backend, resource \\ make_ref()) do
    %Stream{resource: resource, backend: backend}
  end

  setup do
    prev = Application.get_env(:ex_arrow, :stream_native)

    on_exit(fn ->
      case prev do
        nil -> Application.delete_env(:ex_arrow, :stream_native)
        val -> Application.put_env(:ex_arrow, :stream_native, val)
      end
    end)

    :ok
  end

  # ── schema/1 error paths ─────────────────────────────────────────────────────

  describe "schema/1 error branches" do
    setup do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeError)
      :ok
    end

    test "returns {:error, msg} for :adbc backend when native returns error" do
      assert {:error, "adbc stream schema error"} = Stream.schema(stream(:adbc))
    end

    test "returns {:error, msg} for :ipc backend when native returns error" do
      assert {:error, "ipc stream schema error"} = Stream.schema(stream(:ipc))
    end
  end

  # ── schema/1 :parquet backend (real NIF) ─────────────────────────────────────
  # Uses the IPC test-fixture binary to get a real stream resource. The parquet
  # schema path has no error branch — just calling it with a live stream covers
  # the 3 lines (function clause, parquet_stream_schema call, {:ok, ...} return).

  describe "schema/1 :parquet backend" do
    @tag :nif
    test "returns {:ok, schema} for a real parquet stream" do
      s = %Stream{resource: build_parquet_stream(), backend: :parquet}
      assert {:ok, %ExArrow.Schema{}} = Stream.schema(s)
    end
  end

  # ── next/1 error and done paths ──────────────────────────────────────────────

  describe "next/1 error branches" do
    setup do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeError)
      :ok
    end

    test "returns {:error, msg} for :adbc backend when native returns error" do
      assert {:error, "adbc stream next error"} = Stream.next(stream(:adbc))
    end

    test "returns {:error, msg} for :ipc backend when native returns error" do
      assert {:error, "ipc stream next error"} = Stream.next(stream(:ipc))
    end

    test "returns {:error, msg} for :parquet backend when native returns error" do
      assert {:error, "parquet stream next error"} = Stream.next(stream(:parquet))
    end
  end

  describe "next/1 :done branch" do
    setup do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeDone)
      :ok
    end

    test "returns nil when native returns :done for :adbc backend" do
      assert nil == Stream.next(stream(:adbc))
    end

    test "returns nil when native returns :done for :ipc backend" do
      assert nil == Stream.next(stream(:ipc))
    end

    test "returns nil when native returns :done for :parquet backend" do
      assert nil == Stream.next(stream(:parquet))
    end
  end

  describe "next/1 success paths (real NIF)" do
    @tag :nif
    test "returns a RecordBatch for :parquet backend" do
      s = %Stream{resource: build_parquet_stream(), backend: :parquet}
      assert %ExArrow.RecordBatch{} = Stream.next(s)
    end

    @tag :nif
    test "returns a RecordBatch for :ipc backend" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream_ref} = ExArrow.Native.ipc_reader_from_binary(ipc_bin)
      s = %Stream{resource: stream_ref, backend: :ipc}
      assert %ExArrow.RecordBatch{} = Stream.next(s)
    end
  end

  # ── :flight_sql backend ──────────────────────────────────────────────────────

  describe "schema/1 :flight_sql backend" do
    test "returns {:error, msg} when native returns error" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeError)
      assert {:error, "flight_sql stream schema error"} = Stream.schema(stream(:flight_sql))
    end
  end

  describe "next/1 :flight_sql backend" do
    test "returns nil when native returns :done" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeDone)
      assert nil == Stream.next(stream(:flight_sql))
    end

    test "returns {:error, msg} for plain string error" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeError)
      assert {:error, "flight_sql stream next error"} = Stream.next(stream(:flight_sql))
    end

    test "passes gRPC triple error through as a structured tuple" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeFlightSqlTriple)
      assert {:error, {:unavailable, 14, "server gone"}} = Stream.next(stream(:flight_sql))
    end
  end

  # ── to_list/1 error (do_collect raise) ───────────────────────────────────────

  describe "to_list/1" do
    test "raises when next/1 returns an error" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeError)

      assert_raise RuntimeError, ~r/ExArrow.Stream.to_list\/1 failed/, fn ->
        Stream.to_list(stream(:adbc))
      end
    end
  end

  # ── Enumerable ────────────────────────────────────────────────────────────────

  describe "Enumerable — empty stream (stub)" do
    test "Enum.to_list/1 returns [] for an empty stream" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeDone)
      assert [] == Enum.to_list(stream(:flight_sql))
    end

    test "Enum.count/1 returns 0 for an empty stream" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeDone)
      assert 0 == Enum.count(stream(:flight_sql))
    end
  end

  describe "Enumerable — error propagation (stub)" do
    test "Enum.to_list/1 raises on a plain string error" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeError)

      assert_raise RuntimeError, ~r/ExArrow.Stream enumeration error/, fn ->
        Enum.to_list(stream(:flight_sql))
      end
    end

    test "Enum.to_list/1 raises on a gRPC triple error with [code] prefix" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeFlightSqlTriple)

      assert_raise RuntimeError, ~r/\[unavailable\] server gone/, fn ->
        Enum.to_list(stream(:flight_sql))
      end
    end
  end

  describe "to_list/1 triple error" do
    test "raises with [code] prefix when next/1 returns a gRPC triple" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeFlightSqlTriple)

      assert_raise RuntimeError, ~r/\[unavailable\] server gone/, fn ->
        Stream.to_list(stream(:flight_sql))
      end
    end
  end

  describe "Enumerable — single batch (real NIF)" do
    @tag :nif
    test "Enum.to_list/1 collects batches from an IPC stream" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream_ref} = ExArrow.Native.ipc_reader_from_binary(ipc_bin)
      s = %Stream{resource: stream_ref, backend: :ipc}

      batches = Enum.to_list(s)
      assert length(batches) >= 1
      assert Enum.all?(batches, &match?(%ExArrow.RecordBatch{}, &1))
    end

    @tag :nif
    test "Enum.take/2 stops after N batches without consuming the rest" do
      # Build a 2-batch IPC stream by writing the same batch twice.
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, reader} = ExArrow.Native.ipc_reader_from_binary(ipc_bin)
      schema_ref = ExArrow.Native.ipc_stream_schema(reader)
      {:ok, batch_ref} = ExArrow.Native.ipc_stream_next(reader)

      {:ok, two_batch_bin} =
        ExArrow.Native.ipc_writer_to_binary(schema_ref, [batch_ref, batch_ref])

      {:ok, stream_ref} = ExArrow.Native.ipc_reader_from_binary(two_batch_bin)
      s = %Stream{resource: stream_ref, backend: :ipc}

      result = Enum.take(s, 1)
      assert length(result) == 1
      assert match?(%ExArrow.RecordBatch{}, hd(result))
    end

    @tag :nif
    test "Enum.map/2 transforms each batch" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream_ref} = ExArrow.Native.ipc_reader_from_binary(ipc_bin)
      s = %Stream{resource: stream_ref, backend: :ipc}

      row_counts = Enum.map(s, &ExArrow.RecordBatch.num_rows/1)
      assert Enum.all?(row_counts, fn n -> is_integer(n) and n > 0 end)
    end
  end

  describe "Enumerable — multiple batches (real NIF)" do
    @tag :nif
    test "Enum.to_list/1 collects all batches from a multi-batch IPC stream" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, reader} = ExArrow.Native.ipc_reader_from_binary(ipc_bin)
      schema_ref = ExArrow.Native.ipc_stream_schema(reader)
      {:ok, batch_ref} = ExArrow.Native.ipc_stream_next(reader)

      {:ok, two_batch_bin} =
        ExArrow.Native.ipc_writer_to_binary(schema_ref, [batch_ref, batch_ref])

      {:ok, stream_ref} = ExArrow.Native.ipc_reader_from_binary(two_batch_bin)
      s = %Stream{resource: stream_ref, backend: :ipc}

      batches = Enum.to_list(s)
      assert length(batches) == 2
      assert Enum.all?(batches, &match?(%ExArrow.RecordBatch{}, &1))
    end

    @tag :nif
    test "Enum.count/1 traverses all batches and returns the count" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, reader} = ExArrow.Native.ipc_reader_from_binary(ipc_bin)
      schema_ref = ExArrow.Native.ipc_stream_schema(reader)
      {:ok, batch_ref} = ExArrow.Native.ipc_stream_next(reader)

      {:ok, three_batch_bin} =
        ExArrow.Native.ipc_writer_to_binary(schema_ref, [batch_ref, batch_ref, batch_ref])

      {:ok, stream_ref} = ExArrow.Native.ipc_reader_from_binary(three_batch_bin)
      s = %Stream{resource: stream_ref, backend: :ipc}

      assert Enum.count(s) == 3
    end
  end

  # ── NIF fixture helpers ──────────────────────────────────────────────────────

  # Returns a Parquet binary built from the IPC fixture.
  defp parquet_fixture do
    {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, reader} = ExArrow.Native.ipc_reader_from_binary(ipc_bin)
    schema_ref = ExArrow.Native.ipc_stream_schema(reader)
    {:ok, batch_ref} = ExArrow.Native.ipc_stream_next(reader)
    {:ok, parquet_bin} = ExArrow.Native.parquet_writer_to_binary(schema_ref, [batch_ref])
    parquet_bin
  end

  # Returns a live parquet stream resource (already opened via the NIF).
  defp build_parquet_stream do
    {:ok, stream_ref} = ExArrow.Native.parquet_reader_from_binary(parquet_fixture())
    stream_ref
  end
end

defmodule ExArrow.StreamConstructorsTest do
  use ExUnit.Case, async: false

  alias ExArrow.Schema
  alias ExArrow.Stream

  # Build a small IPC binary once per test module.
  defp ipc_binary do
    {:ok, bin} = ExArrow.Native.ipc_test_fixture_binary()
    bin
  end

  defp parquet_binary do
    {:ok, reader} = ExArrow.Native.ipc_reader_from_binary(ipc_binary())
    schema_ref = ExArrow.Native.ipc_stream_schema(reader)
    {:ok, batch_ref} = ExArrow.Native.ipc_stream_next(reader)
    {:ok, parquet_bin} = ExArrow.Native.parquet_writer_to_binary(schema_ref, [batch_ref])
    parquet_bin
  end

  describe "from_ipc/1 and from_ipc_file/1" do
    @tag :nif
    test "from_ipc/1 returns a stream tagged with {:ipc, :binary}" do
      assert {:ok, %Stream{backend: :ipc} = stream} = Stream.from_ipc(ipc_binary())
      assert Stream.source(stream) == {:ipc, :binary}
      assert {:ok, %ExArrow.Schema{}} = Stream.schema(stream)
    end

    @tag :tmp_dir
    test "from_ipc_file/1 tags the stream with the path", %{tmp_dir: dir} do
      path = Path.join(dir, "ipc.arrows")
      File.write!(path, ipc_binary())

      assert {:ok, %Stream{backend: :ipc} = stream} = Stream.from_ipc_file(path)
      assert Stream.source(stream) == {:ipc, path}
    end

    test "from_ipc/1 returns error for non-IPC binary" do
      assert {:error, _} = Stream.from_ipc(<<"not ipc">>)
    end
  end

  describe "from_parquet/1 and from_parquet_binary/1" do
    @tag :nif
    test "from_parquet_binary/1 returns a stream tagged with {:parquet, :binary}" do
      assert {:ok, %Stream{backend: :parquet} = stream} =
               Stream.from_parquet_binary(parquet_binary())

      assert Stream.source(stream) == {:parquet, :binary}
      assert {:ok, %ExArrow.Schema{}} = Stream.schema(stream)
    end

    @tag :tmp_dir
    test "from_parquet/1 tags the stream with the path", %{tmp_dir: dir} do
      path = Path.join(dir, "events.parquet")
      File.write!(path, parquet_binary())

      assert {:ok, %Stream{backend: :parquet} = stream} = Stream.from_parquet(path)
      assert Stream.source(stream) == {:parquet, path}
      assert {:ok, %ExArrow.Schema{}} = Stream.schema(stream)
    end

    test "from_parquet/1 returns error for missing file" do
      assert {:error, _} =
               Stream.from_parquet(
                 "/tmp/ex_arrow_missing_#{:erlang.unique_integer([:positive])}.parquet"
               )
    end
  end

  describe "from_flight_sql/2 (delegation)" do
    setup context do
      prev = Application.get_env(:ex_arrow, :flight_sql_client_impl)
      Application.put_env(:ex_arrow, :flight_sql_client_impl, ExArrow.FlightSQL.ClientMock)
      Mox.set_mox_from_context(context)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_sql_client_impl) end)
      :ok
    end

    test "delegates to FlightSQL.Client.stream_query/2 and tags source" do
      ExArrow.FlightSQL.ClientMock
      |> Mox.expect(:query, fn _client, _sql, _opts ->
        {:ok, %Stream{resource: make_ref(), backend: :flight_sql}}
      end)

      client = %ExArrow.FlightSQL.Client{resource: make_ref()}

      assert {:ok, %Stream{backend: :flight_sql} = stream} =
               Stream.from_flight_sql(client, "SELECT 1")

      assert Stream.source(stream) == {:flight_sql, "SELECT 1"}
    end

    test "passes errors through unchanged" do
      ExArrow.FlightSQL.ClientMock
      |> Mox.expect(:query, fn _client, _sql, _opts ->
        {:error, %ExArrow.FlightSQL.Error{code: :unavailable}}
      end)

      client = %ExArrow.FlightSQL.Client{resource: make_ref()}

      assert {:error, %ExArrow.FlightSQL.Error{code: :unavailable}} =
               Stream.from_flight_sql(client, "SELECT 1")
    end
  end

  describe "from_flight/2 (delegation)" do
    setup context do
      prev = Application.get_env(:ex_arrow, :flight_client_impl)
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      Mox.set_mox_from_context(context)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)
      :ok
    end

    test "delegates to Flight.Client.do_get/2 and tags source" do
      ExArrow.Flight.ClientMock
      |> Mox.expect(:do_get, fn _client, _ticket ->
        {:ok, %Stream{resource: make_ref(), backend: :ipc}}
      end)

      client = %ExArrow.Flight.Client{resource: make_ref()}
      assert {:ok, %Stream{} = stream} = Stream.from_flight(client, "sales_2024")
      assert Stream.source(stream) == {:flight, "sales_2024"}
    end
  end

  describe "from_adbc/1 and from_adbc/2 (delegation)" do
    setup context do
      prev = Application.get_env(:ex_arrow, :adbc_statement_impl)
      Application.put_env(:ex_arrow, :adbc_statement_impl, ExArrow.ADBC.StatementMock)
      Mox.set_mox_from_context(context)
      on_exit(fn -> Application.delete_env(:ex_arrow, :adbc_statement_impl) end)
      :ok
    end

    test "from_adbc/1 executes a pre-built statement" do
      ExArrow.ADBC.StatementMock
      |> Mox.expect(:execute, fn _stmt -> {:ok, %Stream{resource: make_ref(), backend: :adbc}} end)

      stmt = %ExArrow.ADBC.Statement{resource: make_ref()}
      assert {:ok, %Stream{backend: :adbc} = stream} = Stream.from_adbc(stmt)
      assert Stream.source(stream) == {:adbc, :statement}
    end

    test "from_adbc/2 builds and executes a one-shot statement" do
      conn = %ExArrow.ADBC.Connection{resource: make_ref()}
      stmt = %ExArrow.ADBC.Statement{resource: make_ref()}

      ExArrow.ADBC.StatementMock
      |> Mox.expect(:new, fn ^conn -> {:ok, stmt} end)
      |> Mox.expect(:set_sql, fn ^stmt, "SELECT 1" -> :ok end)
      |> Mox.expect(:execute, fn ^stmt -> {:ok, %Stream{resource: make_ref(), backend: :adbc}} end)

      assert {:ok, %Stream{backend: :adbc} = stream} = Stream.from_adbc(conn, "SELECT 1")
      assert Stream.source(stream) == {:adbc, "SELECT 1"}
    end

    test "from_adbc/2 propagates statement-creation errors" do
      conn = %ExArrow.ADBC.Connection{resource: make_ref()}

      ExArrow.ADBC.StatementMock
      |> Mox.expect(:new, fn ^conn -> {:error, "no connection"} end)

      assert {:error, "no connection"} = Stream.from_adbc(conn, "SELECT 1")
    end
  end

  describe "telemetry on stream open" do
    @tag :nif
    test "from_parquet_binary/1 emits [:ex_arrow, :parquet, :read]" do
      ref = make_ref()

      :telemetry.attach(
        {:ex_arrow_parquet_read, ref},
        [:ex_arrow, :parquet, :read],
        fn _event, _measurements, metadata, config ->
          send(config[:pid], {:parquet_read, metadata})
        end,
        %{pid: self()}
      )

      Stream.from_parquet_binary(parquet_binary())

      assert_received {:parquet_read, %{source: :binary}}

      :telemetry.detach({:ex_arrow_parquet_read, ref})
    end
  end

  describe "schema preservation through constructors" do
    @tag :nif
    test "from_ipc/1 round-trips field names" do
      {:ok, orig_stream} = ExArrow.IPC.Reader.from_binary(ipc_binary())
      {:ok, orig_schema} = Stream.schema(orig_stream)
      expected_names = Schema.field_names(orig_schema)

      assert {:ok, stream} = Stream.from_ipc(ipc_binary())
      assert {:ok, schema} = Stream.schema(stream)
      assert Schema.field_names(schema) == expected_names
    end
  end
end
