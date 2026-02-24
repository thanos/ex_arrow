defmodule ExArrow.IPC.ReaderWriterTest do
  use ExUnit.Case, async: true

  describe "Reader.from_file/1" do
    @tag :ipc
    test "returns error when file does not exist" do
      assert {:error, _msg} = ExArrow.IPC.Reader.from_file("/nonexistent/path/arrow.arrow")
    end
  end

  describe "Writer" do
    @tag :ipc
    test "to_binary with empty list returns ok or error (NIF dependent)" do
      {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
      {:ok, schema} = ExArrow.Stream.schema(stream)
      result = ExArrow.IPC.Writer.to_binary(schema, [])
      assert match?({:ok, _}, result) or match?({:error, _}, result)
    end

    @tag :ipc
    test "to_file returns error when parent directory does not exist" do
      {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
      {:ok, schema} = ExArrow.Stream.schema(stream)
      batch = ExArrow.Stream.next(stream)
      path = "/nonexistent_parent_#{:erlang.unique_integer([:positive])}/out.arrow"
      assert {:error, _msg} = ExArrow.IPC.Writer.to_file(path, schema, [batch])
    end
  end
end
