defmodule ExArrowTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

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
      assert {:ok, schema} = ExArrow.Stream.schema(stream)
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
      assert {:ok, schema2} = ExArrow.Stream.schema(stream2)
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
      assert {:ok, schema} = ExArrow.Stream.schema(stream)
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

  describe "IPC file format (random access)" do
    @tag :ipc
    test "from_binary: schema, batch_count, get_batch with file-format fixture" do
      {:ok, binary} = ExArrow.Native.ipc_test_fixture_file_binary()
      assert is_binary(binary) and byte_size(binary) > 0

      assert {:ok, file} = ExArrow.IPC.File.from_binary(binary)
      assert {:ok, schema} = ExArrow.IPC.File.schema(file)
      fields = ExArrow.Schema.fields(schema)
      assert length(fields) == 2
      assert Enum.any?(fields, fn f -> f.name == "id" and f.type == :int64 end)
      assert Enum.any?(fields, fn f -> f.name == "name" and f.type == :utf8 end)

      assert ExArrow.IPC.File.batch_count(file) == 1

      assert {:ok, batch} = ExArrow.IPC.File.get_batch(file, 0)
      assert ExArrow.RecordBatch.num_rows(batch) == 2
      assert {:error, _} = ExArrow.IPC.File.get_batch(file, 1)
    end

    @tag :ipc
    test "from_file: write file format to temp path then read random access" do
      path =
        Path.join(
          System.tmp_dir!(),
          "ex_arrow_file_format_#{:erlang.unique_integer([:positive])}.arrow"
        )

      try do
        {:ok, stream_binary} = ExArrow.Native.ipc_test_fixture_binary()
        {:ok, stream} = ExArrow.IPC.Reader.from_binary(stream_binary)
        {:ok, schema} = ExArrow.Stream.schema(stream)
        batch = ExArrow.Stream.next(stream)

        assert :ok =
                 ExArrow.Native.ipc_file_writer_to_file(path, schema.resource, [batch.resource])

        assert {:ok, file} = ExArrow.IPC.File.from_file(path)
        assert ExArrow.IPC.File.batch_count(file) == 1
        assert {:ok, read_batch} = ExArrow.IPC.File.get_batch(file, 0)
        assert ExArrow.RecordBatch.num_rows(read_batch) == 2
        assert {:ok, read_schema} = ExArrow.IPC.File.schema(file)
        assert length(ExArrow.Schema.fields(read_schema)) == 2
      after
        if File.exists?(path), do: File.rm(path)
      end
    end
  end

  describe "stubs (Flight, ADBC)" do
    test "Flight.Client.connect returns not_implemented" do
      assert ExArrow.Flight.Client.connect("localhost", 9999) == {:error, :not_implemented}
    end

    test "Flight.Server.start_link returns not_implemented" do
      assert ExArrow.Flight.Server.start_link(9090) == {:error, :not_implemented}
    end

    test "ADBC.Database.open returns not_implemented" do
      assert ExArrow.ADBC.Database.open("/path/to/driver") == {:error, :not_implemented}
    end
  end

  @tag :ipc
  test "IPC stream roundtrip preserves schema field count and total row count (property)" do
    check all(n <- integer(0..3)) do
      {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
      {field_count, row_count} = roundtrip_n(binary, n)
      assert field_count == 2, "after #{n} roundtrips schema should have 2 fields"
      assert row_count == 2, "after #{n} roundtrips total rows should be 2"
    end
  end

  defp roundtrip_n(binary, 0) do
    {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
    {:ok, schema} = ExArrow.Stream.schema(stream)
    batches = collect_batches!(stream)

    {length(ExArrow.Schema.fields(schema)),
     Enum.sum(Enum.map(batches, &ExArrow.RecordBatch.num_rows/1))}
  end

  defp roundtrip_n(binary, n) when n > 0 do
    {:ok, stream} = ExArrow.IPC.Reader.from_binary(binary)
    {:ok, schema} = ExArrow.Stream.schema(stream)
    batches = collect_batches!(stream)
    {:ok, out} = ExArrow.IPC.Writer.to_binary(schema, batches)
    roundtrip_n(out, n - 1)
  end

  defp collect_batches!(stream) do
    case collect_batches(stream, []) do
      {:ok, batches} -> batches
      {:error, msg} -> raise "collect_batches failed: #{msg}"
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
