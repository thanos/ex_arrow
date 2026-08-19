defmodule ExArrow.IPC.FileTest do
  use ExUnit.Case, async: true

  describe "from_file/1" do
    @tag :ipc
    test "returns error when file does not exist" do
      assert {:error, _msg} = ExArrow.IPC.File.from_file("/nonexistent/file.arrow")
    end
  end

  describe "get_batch/2" do
    test "returns error for invalid index (negative)" do
      file = %ExArrow.IPC.File{resource: make_ref()}
      assert {:error, _msg} = ExArrow.IPC.File.get_batch(file, -1)
    end

    test "returns error for invalid index (non-integer)" do
      file = %ExArrow.IPC.File{resource: make_ref()}
      assert {:error, _msg} = ExArrow.IPC.File.get_batch(file, "0")
    end
  end

  describe "write/3" do
    @tag :ipc
    @tag :tmp_dir
    test "writes IPC file format and reads back", %{tmp_dir: dir} do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(ipc_bin)
      {:ok, schema} = ExArrow.Stream.schema(stream)
      batch = ExArrow.Stream.next(stream)
      path = Path.join(dir, "out.arrow")

      assert :ok = ExArrow.IPC.File.write(path, schema, [batch])
      assert {:ok, file} = ExArrow.IPC.File.from_file(path)
      assert ExArrow.IPC.File.batch_count(file) == 1
      assert {:ok, read} = ExArrow.IPC.File.get_batch(file, 0)
      assert ExArrow.RecordBatch.num_rows(read) == ExArrow.RecordBatch.num_rows(batch)
    end
  end
end
