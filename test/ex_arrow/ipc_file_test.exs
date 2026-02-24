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
end
