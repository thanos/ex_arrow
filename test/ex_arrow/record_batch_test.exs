defmodule ExArrow.RecordBatchTest do
  use ExUnit.Case, async: true

  alias ExArrow.IPC
  alias ExArrow.RecordBatch
  alias ExArrow.Stream

  @moduletag :ipc

  describe "schema/1" do
    test "returns the batch's schema" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
      batch = Stream.next(stream)
      schema = RecordBatch.schema(batch)
      assert %ExArrow.Schema{} = schema
    end
  end

  describe "num_rows/1" do
    test "returns the row count" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
      batch = Stream.next(stream)
      assert RecordBatch.num_rows(batch) == 2
    end
  end

  describe "num_columns/1" do
    test "returns the column count" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
      batch = Stream.next(stream)
      assert RecordBatch.num_columns(batch) == 2
    end
  end

  describe "column_names/1" do
    test "returns column names" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
      batch = Stream.next(stream)
      assert RecordBatch.column_names(batch) == ["id", "name"]
    end
  end

  describe "Native.record_batch_concat/1" do
    test "concatenates batches with the same schema, summing rows" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
      batch = Stream.next(stream)
      ref = RecordBatch.resource_ref(batch)

      assert {:ok, merged_ref} = ExArrow.Native.record_batch_concat([ref, ref, ref])
      merged = RecordBatch.from_ref(merged_ref)
      assert RecordBatch.num_rows(merged) == RecordBatch.num_rows(batch) * 3
      assert RecordBatch.column_names(merged) == RecordBatch.column_names(batch)
    end

    test "returns an error for an empty list" do
      assert {:error, msg} = ExArrow.Native.record_batch_concat([])
      assert msg =~ "empty"
    end
  end

  describe "from_columns/4 — happy path" do
    test "single s64 column, one row" do
      assert {:ok, %RecordBatch{} = batch} =
               RecordBatch.from_columns(["id"], [<<42::little-signed-64>>], ["s64"], 1)

      assert RecordBatch.num_rows(batch) == 1
      assert RecordBatch.num_columns(batch) == 1
      assert RecordBatch.column_names(batch) == ["id"]
    end

    test "multi-row s64 column" do
      bin =
        <<1::little-signed-64, 2::little-signed-64, 3::little-signed-64, 4::little-signed-64>>

      assert {:ok, %RecordBatch{} = batch} =
               RecordBatch.from_columns(["x"], [bin], ["s64"], 4)

      assert RecordBatch.num_rows(batch) == 4
    end

    test "all signed integer dtypes" do
      cases = [
        {"s8", <<7::signed-8>>},
        {"s16", <<7::little-signed-16>>},
        {"s32", <<7::little-signed-32>>},
        {"s64", <<7::little-signed-64>>}
      ]

      for {dtype, bin} <- cases do
        assert {:ok, %RecordBatch{} = batch} =
                 RecordBatch.from_columns(["x"], [bin], [dtype], 1),
               "expected dtype #{dtype} to succeed"

        assert RecordBatch.num_rows(batch) == 1
      end
    end

    test "all unsigned integer dtypes" do
      cases = [
        {"u8", <<7::unsigned-8>>},
        {"u16", <<7::little-unsigned-16>>},
        {"u32", <<7::little-unsigned-32>>},
        {"u64", <<7::little-unsigned-64>>}
      ]

      for {dtype, bin} <- cases do
        assert {:ok, %RecordBatch{} = batch} =
                 RecordBatch.from_columns(["x"], [bin], [dtype], 1),
               "expected dtype #{dtype} to succeed"

        assert RecordBatch.num_rows(batch) == 1
      end
    end

    test "float dtypes" do
      cases = [
        {"f32", <<1.5::little-float-32>>},
        {"f64", <<1.5::little-float-64>>}
      ]

      for {dtype, bin} <- cases do
        assert {:ok, %RecordBatch{} = batch} =
                 RecordBatch.from_columns(["x"], [bin], [dtype], 1),
               "expected dtype #{dtype} to succeed"

        assert RecordBatch.num_rows(batch) == 1
      end
    end

    test "bool dtype: one byte per element, non-zero is true" do
      assert {:ok, %RecordBatch{} = batch} =
               RecordBatch.from_columns(["b"], [<<0, 1, 0, 1>>], ["bool"], 4)

      assert RecordBatch.num_rows(batch) == 4
    end

    test "multiple columns of different dtypes" do
      assert {:ok, %RecordBatch{} = batch} =
               RecordBatch.from_columns(
                 ["id", "score"],
                 [<<1::little-signed-64>>, <<3.14::little-float-64>>],
                 ["s64", "f64"],
                 1
               )

      assert RecordBatch.num_rows(batch) == 1
      assert RecordBatch.num_columns(batch) == 2
      assert RecordBatch.column_names(batch) == ["id", "score"]
    end

    test "result is a real RecordBatch struct (resource is a reference)" do
      assert {:ok, %RecordBatch{resource: ref}} =
               RecordBatch.from_columns(["id"], [<<1::little-signed-64>>], ["s64"], 1)

      assert is_reference(ref)
    end
  end

  describe "from_columns/4 — input validation" do
    test "returns error when names/binaries/dtypes lengths differ" do
      assert {:error, msg} =
               RecordBatch.from_columns(
                 ["a", "b"],
                 [<<1::little-signed-64>>],
                 ["s64", "s64"],
                 1
               )

      assert msg =~ "same length"
    end

    test "returns error when no columns are provided" do
      assert {:error, msg} = RecordBatch.from_columns([], [], [], 0)
      assert msg =~ "at least one column"
    end

    test "returns error for unknown dtype" do
      assert {:error, msg} =
               RecordBatch.from_columns(["x"], [<<1::little-signed-64>>], ["int64"], 1)

      assert msg =~ "unknown dtype"
    end

    test "returns error when binary length doesn't match length × element_size" do
      # 8 bytes for s64 × length 2 expects 16 bytes
      assert {:error, msg} =
               RecordBatch.from_columns(["x"], [<<1::little-signed-64>>], ["s64"], 2)

      assert msg =~ "binary length mismatch"
    end

    test "returns error for bool when binary length differs from row count" do
      assert {:error, msg} =
               RecordBatch.from_columns(["b"], [<<1>>], ["bool"], 4)

      assert msg =~ "binary length mismatch"
    end
  end

  describe "from_columns/4 — date and time dtypes" do
    test "date32 (i32 days since epoch)" do
      bin = <<19_000::little-signed-32, 19_001::little-signed-32>>

      assert {:ok, %RecordBatch{} = batch} =
               RecordBatch.from_columns(["d"], [bin], ["date32"], 2)

      assert RecordBatch.num_rows(batch) == 2
    end

    test "date64 (i64 millis since epoch)" do
      bin = <<1_700_000_000_000::little-signed-64>>

      assert {:ok, %RecordBatch{} = batch} =
               RecordBatch.from_columns(["d"], [bin], ["date64"], 1)

      assert RecordBatch.num_rows(batch) == 1
    end

    test "timestamp dtypes (all four time units)" do
      ts_bin = <<1_700_000_000_000_000::little-signed-64>>

      for dtype <- ~w(timestamp_seconds timestamp_millis timestamp_micros timestamp_nanos) do
        assert {:ok, %RecordBatch{} = batch} =
                 RecordBatch.from_columns(["t"], [ts_bin], [dtype], 1),
               "expected dtype #{dtype} to succeed"

        assert RecordBatch.num_rows(batch) == 1
      end
    end

    test "duration dtypes (all four time units)" do
      d_bin = <<3_600::little-signed-64>>

      for dtype <- ~w(duration_seconds duration_millis duration_micros duration_nanos) do
        assert {:ok, %RecordBatch{} = batch} =
                 RecordBatch.from_columns(["d"], [d_bin], [dtype], 1),
               "expected dtype #{dtype} to succeed"

        assert RecordBatch.num_rows(batch) == 1
      end
    end

    test "date32 reports binary length mismatch" do
      assert {:error, msg} =
               RecordBatch.from_columns(["d"], [<<1::little-signed-32>>], ["date32"], 2)

      assert msg =~ "binary length mismatch"
    end
  end

  describe "from_columns/4 — variable-length string and binary dtypes" do
    # Wire format: <<elem_len::little-32, elem_bytes::binary-size(elem_len)>> × length
    defp varlen([]), do: <<>>

    defp varlen([head | tail]) when is_binary(head) do
      <<byte_size(head)::little-32, head::binary, varlen(tail)::binary>>
    end

    test "utf8 with multiple elements" do
      bin = varlen(["hello", "world", ""])

      assert {:ok, %RecordBatch{} = batch} =
               RecordBatch.from_columns(["s"], [bin], ["utf8"], 3)

      assert RecordBatch.num_rows(batch) == 3
    end

    test "large_utf8 accepts the same wire format" do
      bin = varlen(["a", "bb", "ccc"])

      assert {:ok, %RecordBatch{} = batch} =
               RecordBatch.from_columns(["s"], [bin], ["large_utf8"], 3)

      assert RecordBatch.num_rows(batch) == 3
    end

    test "binary accepts arbitrary bytes" do
      bin = varlen([<<0, 1, 2>>, <<255, 254>>])

      assert {:ok, %RecordBatch{} = batch} =
               RecordBatch.from_columns(["b"], [bin], ["binary"], 2)

      assert RecordBatch.num_rows(batch) == 2
    end

    test "large_binary accepts arbitrary bytes" do
      bin = varlen([<<0xFF, 0xFE>>])

      assert {:ok, %RecordBatch{} = batch} =
               RecordBatch.from_columns(["b"], [bin], ["large_binary"], 1)

      assert RecordBatch.num_rows(batch) == 1
    end

    test "empty utf8 column" do
      assert {:ok, %RecordBatch{} = batch} =
               RecordBatch.from_columns(["s"], [<<>>], ["utf8"], 0)

      assert RecordBatch.num_rows(batch) == 0
    end

    test "utf8 rejects invalid utf-8 bytes" do
      bin = varlen([<<0xFF, 0xFE>>])

      assert {:error, msg} = RecordBatch.from_columns(["s"], [bin], ["utf8"], 1)
      assert msg =~ "invalid utf-8"
    end

    test "utf8 rejects truncated length prefix" do
      # Declares a 4-byte length but provides only 2 bytes of header data.
      bin = <<10::little-32, "ab">>

      assert {:error, msg} = RecordBatch.from_columns(["s"], [bin], ["utf8"], 1)
      assert msg =~ "truncated"
    end

    test "utf8 rejects trailing bytes after the declared element count" do
      bin = varlen(["a"]) <> <<0::little-32>>

      assert {:error, msg} = RecordBatch.from_columns(["s"], [bin], ["utf8"], 1)
      assert msg =~ "trailing bytes"
    end

    test "binary rejects truncated payload" do
      # Declares 5 bytes for the first element but provides only 3.
      bin = <<5::little-32, "abc">>

      assert {:error, msg} = RecordBatch.from_columns(["b"], [bin], ["binary"], 1)
      assert msg =~ "truncated"
    end
  end
end
