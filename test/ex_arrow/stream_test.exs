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

    test "formats gRPC triple error as [code] message" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeFlightSqlTriple)
      assert {:error, "[unavailable] server gone"} = Stream.next(stream(:flight_sql))
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
    test "Enum.to_list/1 raises on a stream error" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeError)

      assert_raise RuntimeError, ~r/ExArrow.Stream enumeration error/, fn ->
        Enum.to_list(stream(:flight_sql))
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
