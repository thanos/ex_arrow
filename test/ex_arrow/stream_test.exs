defmodule ExArrow.StreamTest do
  use ExUnit.Case, async: true

  describe "next/1" do
    @tag :ipc
    test "returns nil when stream is exhausted" do
      {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
      assert %ExArrow.RecordBatch{} = ExArrow.Stream.next(stream)
      assert ExArrow.Stream.next(stream) == nil
    end
  end

  describe "schema/1" do
    @tag :ipc
    test "returns {:ok, schema} for valid stream" do
      {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
      assert {:ok, %ExArrow.Schema{}} = ExArrow.Stream.schema(stream)
    end
  end
end
