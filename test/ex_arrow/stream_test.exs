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

  # ── to_list/1 error (do_collect raise) ───────────────────────────────────────

  describe "to_list/1" do
    test "raises when next/1 returns an error" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeError)

      assert_raise RuntimeError, ~r/ExArrow.Stream.to_list\/1 failed/, fn ->
        Stream.to_list(stream(:adbc))
      end
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
