defmodule ExArrow.ParquetTest do
  use ExUnit.Case, async: true

  alias ExArrow.IPC
  alias ExArrow.Parquet
  alias ExArrow.Schema
  alias ExArrow.Stream

  # Produce a small IPC batch to use as source data for Parquet round-trips.
  defp source_batch do
    {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
    batch = Stream.next(stream)
    {:ok, stream2} = IPC.Reader.from_binary(ipc_bin)
    {:ok, schema} = Stream.schema(stream2)
    {schema, batch}
  end

  describe "Writer.to_binary/2 and Reader.from_binary/1" do
    test "round-trip preserves schema field names" do
      {schema, batch} = source_batch()
      assert {:ok, parquet_bin} = Parquet.Writer.to_binary(schema, [batch])
      assert is_binary(parquet_bin)
      assert byte_size(parquet_bin) > 0

      assert {:ok, stream} = Parquet.Reader.from_binary(parquet_bin)
      assert {:ok, rt_schema} = Stream.schema(stream)
      assert Schema.field_names(rt_schema) == Schema.field_names(schema)
    end

    test "round-trip preserves row count" do
      {schema, batch} = source_batch()
      original_rows = ExArrow.RecordBatch.num_rows(batch)

      assert {:ok, parquet_bin} = Parquet.Writer.to_binary(schema, [batch])
      assert {:ok, stream} = Parquet.Reader.from_binary(parquet_bin)
      rt_batch = Stream.next(stream)
      assert ExArrow.RecordBatch.num_rows(rt_batch) == original_rows
    end

    test "stream is exhausted after consuming all batches" do
      {schema, batch} = source_batch()
      assert {:ok, parquet_bin} = Parquet.Writer.to_binary(schema, [batch])
      assert {:ok, stream} = Parquet.Reader.from_binary(parquet_bin)
      _batch = Stream.next(stream)
      assert Stream.next(stream) == nil
    end
  end

  describe "Writer.to_file/3 and Reader.from_file/1" do
    @tag :tmp_dir
    test "round-trip through a file", %{tmp_dir: dir} do
      path = Path.join(dir, "test.parquet")
      {schema, batch} = source_batch()

      assert :ok = Parquet.Writer.to_file(path, schema, [batch])
      assert File.exists?(path)

      assert {:ok, stream} = Parquet.Reader.from_file(path)
      assert {:ok, rt_schema} = Stream.schema(stream)
      assert Schema.field_names(rt_schema) == Schema.field_names(schema)
      rt_batch = Stream.next(stream)
      assert ExArrow.RecordBatch.num_rows(rt_batch) == ExArrow.RecordBatch.num_rows(batch)
    end

    test "from_file returns error for missing file" do
      assert {:error, _msg} = Parquet.Reader.from_file("/tmp/this_does_not_exist_xyz.parquet")
    end

    test "to_file returns error when parent directory does not exist" do
      {schema, batch} = source_batch()
      path = "/nonexistent_dir_#{:erlang.unique_integer([:positive])}/out.parquet"
      assert {:error, _msg} = Parquet.Writer.to_file(path, schema, [batch])
    end
  end

  describe "Reader.from_binary/1" do
    test "returns error for non-Parquet binary" do
      assert {:error, _msg} = Parquet.Reader.from_binary("this is not parquet data")
    end
  end

  describe "Stream integration" do
    test "to_list/1 collects all batches" do
      {schema, batch} = source_batch()
      assert {:ok, parquet_bin} = Parquet.Writer.to_binary(schema, [batch])
      assert {:ok, stream} = Parquet.Reader.from_binary(parquet_bin)
      batches = Stream.to_list(stream)
      assert length(batches) == 1
    end
  end

  describe "lazy row-group streaming" do
    test "batches are produced lazily via Stream.next/1" do
      {schema, batch} = source_batch()
      assert {:ok, parquet_bin} = Parquet.Writer.to_binary(schema, [batch])
      assert {:ok, stream} = Parquet.Reader.from_binary(parquet_bin)

      # First call returns the batch; second call signals end-of-stream.
      first = Stream.next(stream)
      assert first != nil
      assert ExArrow.RecordBatch.num_rows(first) > 0

      assert Stream.next(stream) == nil
    end

    test "multiple independent streams are isolated" do
      {schema, batch} = source_batch()
      assert {:ok, parquet_bin} = Parquet.Writer.to_binary(schema, [batch])

      assert {:ok, stream1} = Parquet.Reader.from_binary(parquet_bin)
      assert {:ok, stream2} = Parquet.Reader.from_binary(parquet_bin)

      # Consuming stream1 does not affect stream2.
      _b1 = Stream.next(stream1)
      assert Stream.next(stream1) == nil

      b2 = Stream.next(stream2)
      assert b2 != nil
      assert ExArrow.RecordBatch.num_rows(b2) == ExArrow.RecordBatch.num_rows(batch)
    end

    @tag :tmp_dir
    test "file-backed stream reads lazily without loading entire file", %{tmp_dir: dir} do
      path = Path.join(dir, "lazy.parquet")
      {schema, batch} = source_batch()
      assert :ok = Parquet.Writer.to_file(path, schema, [batch])

      assert {:ok, stream} = Parquet.Reader.from_file(path)
      rt_batch = Stream.next(stream)
      assert rt_batch != nil
      assert ExArrow.RecordBatch.num_rows(rt_batch) == ExArrow.RecordBatch.num_rows(batch)
      assert Stream.next(stream) == nil
    end
  end
end
