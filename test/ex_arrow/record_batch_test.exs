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
end
