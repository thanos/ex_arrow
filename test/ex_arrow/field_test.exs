defmodule ExArrow.FieldTest do
  use ExUnit.Case, async: true

  describe "Field struct" do
    test "holds name and type" do
      field = %ExArrow.Field{name: "id", type: :int64}
      assert field.name == "id"
      assert field.type == :int64
      assert %ExArrow.Field{name: _, type: _} = field
    end

    test "new/2 returns struct with given name and type" do
      assert %ExArrow.Field{name: "x", type: :utf8} = ExArrow.Field.new("x", :utf8)
    end

    @tag :ipc
    test "matches structs returned from Schema.fields" do
      {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
      {:ok, schema} = ExArrow.Stream.schema(stream)
      fields = ExArrow.Schema.fields(schema)

      assert [%ExArrow.Field{name: "id", type: :int64}, %ExArrow.Field{name: "name", type: :utf8}] =
               fields
    end
  end
end
