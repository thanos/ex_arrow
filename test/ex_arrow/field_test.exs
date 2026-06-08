defmodule ExArrow.FieldTest do
  use ExUnit.Case, async: true

  describe "Field struct" do
    test "holds name, type, and nullable" do
      field = %ExArrow.Field{name: "id", type: :int64, nullable: false}
      assert field.name == "id"
      assert field.type == :int64
      assert field.nullable == false
    end

    test "nullable defaults to true" do
      field = %ExArrow.Field{name: "id", type: :int64}
      assert field.nullable == true
    end

    test "new/2 returns struct with given name and type, nullable defaults to true" do
      assert %ExArrow.Field{name: "x", type: :utf8, nullable: true} =
               ExArrow.Field.new("x", :utf8)
    end

    test "new/3 returns struct with explicit nullable" do
      assert %ExArrow.Field{name: "x", type: :int64, nullable: false} =
               ExArrow.Field.new("x", :int64, false)
    end

    @tag :ipc
    test "matches structs returned from Schema.fields with nullable flag" do
      {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
      {:ok, schema} = ExArrow.Stream.schema(stream)
      fields = ExArrow.Schema.fields(schema)

      assert [
               %ExArrow.Field{name: "id", type: :int64, nullable: false},
               %ExArrow.Field{name: "name", type: :utf8, nullable: false}
             ] = fields
    end
  end
end
