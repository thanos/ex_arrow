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

  describe "IPC roundtrip" do
    @tag :ipc
    test "encode then decode: schema and row count match" do
      # Get fixture binary from NIF (schema: id int64, name utf8; 2 rows)
      {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
      assert is_binary(binary) and byte_size(binary) > 0

      # Read stream
      assert {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
      schema = ExArrow.Stream.schema(stream)
      fields = ExArrow.Schema.fields(schema)
      assert length(fields) == 2
      assert Enum.any?(fields, fn f -> f.name == "id" and f.type == :int64 end)
      assert Enum.any?(fields, fn f -> f.name == "name" and f.type == :utf8 end)

      # First batch
      batch1 = ExArrow.Stream.next(stream)
      assert %ExArrow.RecordBatch{} = batch1
      assert ExArrow.RecordBatch.num_rows(batch1) == 2
      assert %ExArrow.Schema{} = ExArrow.RecordBatch.schema(batch1)
      batch1_schema_fields = ExArrow.Schema.fields(ExArrow.RecordBatch.schema(batch1))
      assert length(batch1_schema_fields) == 2

      # No more batches
      assert ExArrow.Stream.next(stream) == nil

      # Roundtrip: write then read again
      assert {:ok, binary2} = ExArrow.IPC.Writer.to_binary(schema, [batch1])
      assert {:ok, stream2} = ExArrow.IPC.Reader.from_binary(binary2)
      schema2 = ExArrow.Stream.schema(stream2)
      assert length(ExArrow.Schema.fields(schema2)) == 2
      batch2 = ExArrow.Stream.next(stream2)
      assert %ExArrow.RecordBatch{} = batch2
      assert ExArrow.RecordBatch.num_rows(batch2) == 2
      assert ExArrow.Stream.next(stream2) == nil
    end

    @tag :ipc
    test "from_binary with empty binary returns error" do
      assert {:error, _msg} = ExArrow.IPC.Reader.from_binary(<<>>)
    end

    @tag :ipc
    test "stream iterator yields batches until done" do
      {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
      assert {:ok, batches} = collect_batches(stream, [])
      assert length(batches) == 1
      assert ExArrow.RecordBatch.num_rows(hd(batches)) == 2
    end
  end

  describe "core with real IPC" do
    @tag :ipc
    test "Schema.fields returns field list from stream schema" do
      {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
      schema = ExArrow.Stream.schema(stream)
      assert length(ExArrow.Schema.fields(schema)) == 2
    end

    @tag :ipc
    test "RecordBatch.schema and num_rows from batch" do
      {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
      batch = ExArrow.Stream.next(stream)
      assert ExArrow.RecordBatch.num_rows(batch) == 2
      schema = ExArrow.RecordBatch.schema(batch)
      assert length(ExArrow.Schema.fields(schema)) == 2
    end
  end

  describe "stubs (Flight, ADBC)" do
    test "Flight.Client.connect returns not_implemented" do
      assert ExArrow.Flight.Client.connect("localhost", 9999) == {:error, :not_implemented}
    end

    test "ADBC.Database.open returns not_implemented" do
      assert ExArrow.ADBC.Database.open("/path/to/driver") == {:error, :not_implemented}
    end
  end

  defp collect_batches(stream, acc) do
    case ExArrow.Stream.next(stream) do
      nil -> {:ok, Enum.reverse(acc)}
      {:error, msg} -> {:error, msg}
      batch -> collect_batches(stream, [batch | acc])
    end
  end
end
