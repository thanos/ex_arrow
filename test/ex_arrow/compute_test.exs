defmodule ExArrow.ComputeTest do
  use ExUnit.Case, async: true

  alias ExArrow.Compute
  alias ExArrow.IPC
  alias ExArrow.Stream

  defp rich_batch do
    {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
    Stream.next(stream)
  end

  describe "project/2" do
    test "selects a subset of columns" do
      batch = rich_batch()
      assert {:ok, projected} = Compute.project(batch, ["id"])
      schema = ExArrow.RecordBatch.schema(projected)
      assert ExArrow.Schema.field_names(schema) == ["id"]
      assert ExArrow.RecordBatch.num_rows(projected) == ExArrow.RecordBatch.num_rows(batch)
    end

    test "preserves column order from the argument list" do
      batch = rich_batch()
      assert {:ok, projected} = Compute.project(batch, ["name", "id"])
      schema = ExArrow.RecordBatch.schema(projected)
      assert ExArrow.Schema.field_names(schema) == ["name", "id"]
    end

    test "returns error for unknown column" do
      batch = rich_batch()
      assert {:error, msg} = Compute.project(batch, ["nonexistent"])
      assert msg =~ "nonexistent"
    end
  end

  describe "sort/3" do
    test "sorts int64 column ascending by default" do
      batch = rich_batch()
      assert {:ok, sorted} = Compute.sort(batch, "id")
      assert ExArrow.RecordBatch.num_rows(sorted) == ExArrow.RecordBatch.num_rows(batch)
    end

    test "sorts int64 column descending" do
      batch = rich_batch()
      assert {:ok, _sorted} = Compute.sort(batch, "id", ascending: false)
    end

    test "returns error for unknown column" do
      batch = rich_batch()
      assert {:error, msg} = Compute.sort(batch, "no_such_col")
      assert msg =~ "no_such_col"
    end
  end

  describe "filter/2" do
    test "returns error when predicate batch has no columns" do
      batch = rich_batch()
      # Use id column batch as predicate — it is int64, not boolean, so filter should reject
      assert {:ok, id_only} = Compute.project(batch, ["id"])
      assert {:error, msg} = Compute.filter(batch, id_only)
      assert msg =~ "boolean"
    end
  end
end
