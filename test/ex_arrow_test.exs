defmodule ExArrowTest do
  use ExUnit.Case, async: true

  describe "native NIF" do
    @tag :nif
    test "nif_version returns a string" do
      assert is_binary(ExArrow.native_version())
      assert String.length(ExArrow.native_version()) > 0
    end
  end

  describe "ExArrow.Error" do
    test "exception with code and message" do
      err = ExArrow.Error.exception(code: :invalid_schema, message: "bad field")
      assert err.code == :invalid_schema
      assert err.message == "bad field"
      assert Exception.message(err) =~ "invalid_schema"
      assert Exception.message(err) =~ "bad field"
    end

    test "exception with message only" do
      err = ExArrow.Error.exception("something failed")
      assert err.code == :unknown
      assert err.message == "something failed"
    end
  end

  describe "core stubs" do
    test "Schema.fields returns empty list" do
      schema = %ExArrow.Schema{resource: make_ref()}
      assert ExArrow.Schema.fields(schema) == []
    end

    test "RecordBatch.schema/num_rows return nil and 0" do
      batch = %ExArrow.RecordBatch{resource: make_ref()}
      assert ExArrow.RecordBatch.schema(batch) == nil
      assert ExArrow.RecordBatch.num_rows(batch) == 0
    end

    test "Stream.next returns nil" do
      stream = %ExArrow.Stream{resource: make_ref()}
      assert ExArrow.Stream.next(stream) == nil
    end

    test "IPC.Reader.from_binary returns not_implemented" do
      assert ExArrow.IPC.Reader.from_binary(<<>>) == {:error, :not_implemented}
    end

    test "IPC.Writer.to_binary returns not_implemented" do
      schema = %ExArrow.Schema{resource: make_ref()}
      assert ExArrow.IPC.Writer.to_binary(schema, []) == {:error, :not_implemented}
    end

    test "Flight.Client.connect returns not_implemented" do
      assert ExArrow.Flight.Client.connect("localhost", 9999) == {:error, :not_implemented}
    end

    test "ADBC.Database.open returns not_implemented" do
      assert ExArrow.ADBC.Database.open("/path/to/driver") == {:error, :not_implemented}
    end
  end
end
