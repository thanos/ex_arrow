defmodule ExArrow.CdiTest do
  use ExUnit.Case, async: true

  alias ExArrow.CDI
  alias ExArrow.IPC
  alias ExArrow.RecordBatch
  alias ExArrow.Schema
  alias ExArrow.Stream

  defp fixture_batch do
    {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
    Stream.next(stream)
  end

  describe "export/1" do
    test "returns {:ok, handle}" do
      batch = fixture_batch()
      assert {:ok, _handle} = CDI.export(batch)
    end
  end

  describe "import/1" do
    test "produces an equivalent RecordBatch" do
      batch = fixture_batch()
      assert {:ok, handle} = CDI.export(batch)
      assert {:ok, batch2} = CDI.import(handle)

      assert RecordBatch.num_rows(batch2) == RecordBatch.num_rows(batch)

      names_orig = batch |> RecordBatch.schema() |> Schema.field_names()
      names_new = batch2 |> RecordBatch.schema() |> Schema.field_names()
      assert names_orig == names_new
    end

    test "returns error when handle is already consumed" do
      batch = fixture_batch()
      assert {:ok, handle} = CDI.export(batch)
      assert {:ok, _} = CDI.import(handle)
      assert {:error, msg} = CDI.import(handle)
      assert msg =~ "already consumed"
    end
  end

  describe "pointers/1" do
    test "returns non-zero integer addresses" do
      batch = fixture_batch()
      assert {:ok, handle} = CDI.export(batch)
      {schema_ptr, array_ptr} = CDI.pointers(handle)
      assert schema_ptr > 0
      assert array_ptr > 0
    end
  end

  describe "mark_consumed/1" do
    test "prevents double-release when GC collects the handle" do
      batch = fixture_batch()
      assert {:ok, handle} = CDI.export(batch)
      assert :ok = CDI.mark_consumed(handle)
      # After marking consumed, import should fail (pointers are null)
      assert {:error, _} = CDI.import(handle)
    end
  end

  describe "round-trip fidelity" do
    test "int64 column values are preserved" do
      batch = fixture_batch()
      assert {:ok, handle} = CDI.export(batch)
      assert {:ok, batch2} = CDI.import(handle)

      assert {:ok, stream1} = ExArrow.IPC.Writer.to_binary(RecordBatch.schema(batch), [batch])
      assert {:ok, stream2} = ExArrow.IPC.Writer.to_binary(RecordBatch.schema(batch2), [batch2])
      assert stream1 == stream2
    end

    test "multiple exports are independent" do
      batch = fixture_batch()
      assert {:ok, h1} = CDI.export(batch)
      assert {:ok, h2} = CDI.export(batch)

      assert {:ok, b1} = CDI.import(h1)
      assert {:ok, b2} = CDI.import(h2)

      assert RecordBatch.num_rows(b1) == RecordBatch.num_rows(b2)
    end
  end
end
