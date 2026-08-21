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

    # External DuckDB-generated ZSTD fixture (from @mindreframer / PR #243).
    test "reads a Zstandard-compressed file produced outside ExArrow" do
      path = Path.expand("../fixtures/parquet_zstd.parquet", __DIR__)

      assert {:ok, stream} = Parquet.Reader.from_file(path)
      assert {:ok, schema} = Stream.schema(stream)
      assert Schema.field_names(schema) == ["id", "name"]

      rows =
        stream
        |> Stream.to_list()
        |> Enum.map(&ExArrow.RecordBatch.num_rows/1)
        |> Enum.sum()

      assert rows == 3
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

  describe "write options" do
    test "zstd compression round-trips and records ZSTD codec in metadata" do
      {schema, batch} = source_batch()

      assert {:ok, bin} =
               Parquet.Writer.to_binary(schema, [batch], compression: :zstd)

      assert {:ok, meta} = Parquet.Metadata.from_binary(bin)
      cols = hd(meta.row_groups).columns
      assert cols != []
      assert Enum.all?(cols, &(&1.compression =~ ~r/ZSTD/i))

      assert {:ok, stream} = Parquet.Reader.from_binary(bin)

      assert ExArrow.RecordBatch.num_rows(Stream.next(stream)) ==
               ExArrow.RecordBatch.num_rows(batch)
    end

    test "snappy compression and dictionary option" do
      {schema, batch} = source_batch()

      assert {:ok, bin} =
               Parquet.Writer.to_binary(schema, [batch],
                 compression: :snappy,
                 dictionary: false,
                 row_group_size: 1
               )

      assert {:ok, meta} = Parquet.Metadata.from_binary(bin)
      assert meta.num_row_groups == ExArrow.RecordBatch.num_rows(batch)
      cols = hd(meta.row_groups).columns
      assert Enum.all?(cols, &(&1.compression =~ ~r/SNAPPY/i))

      # dictionary: false must not advertise dictionary encodings on chunks.
      assert Enum.all?(cols, fn col ->
               encodings = Enum.map(col.encodings, &String.upcase/1)
               not Enum.any?(encodings, &(&1 =~ "DICTIONARY"))
             end)

      assert {:ok, with_dict} =
               Parquet.Writer.to_binary(schema, [batch],
                 compression: :snappy,
                 dictionary: true
               )

      assert {:ok, meta_dict} = Parquet.Metadata.from_binary(with_dict)
      dict_cols = hd(meta_dict.row_groups).columns

      assert Enum.any?(dict_cols, fn col ->
               Enum.any?(col.encodings, &(String.upcase(&1) =~ "DICTIONARY"))
             end)
    end

    test "rejects invalid compression" do
      {schema, batch} = source_batch()

      assert {:error, msg} =
               Parquet.Writer.to_binary(schema, [batch], compression: :brotli)

      assert msg =~ "compression"
    end

    test "empty batch list writes a valid empty Parquet file" do
      {schema, _batch} = source_batch()
      assert {:ok, bin} = Parquet.Writer.to_binary(schema, [])
      assert is_binary(bin)
      assert {:ok, meta} = Parquet.Metadata.from_binary(bin)
      assert meta.num_rows == 0
    end
  end

  describe "read pushdown" do
    test "columns projection reduces schema" do
      {schema, batch} = source_batch()
      names = Schema.field_names(schema)
      assert length(names) >= 1
      col = hd(names)

      assert {:ok, bin} = Parquet.Writer.to_binary(schema, [batch])
      assert {:ok, stream} = Parquet.Reader.from_binary(bin, columns: [col])
      assert {:ok, projected} = Stream.schema(stream)
      assert Schema.field_names(projected) == [col]
    end

    test "filters predicate reduces rows to exact expected count" do
      n = 4
      ids = for i <- 1..n, into: <<>>, do: <<i::little-signed-64>>
      assert {:ok, batch} = ExArrow.RecordBatch.from_columns(["id"], [ids], ["s64"], n)
      schema = ExArrow.RecordBatch.schema(batch)

      assert {:ok, bin} = Parquet.Writer.to_binary(schema, [batch])

      assert {:ok, stream} =
               Parquet.Reader.from_binary(bin, filters: {:gt, "id", 2})

      filtered = Stream.to_list(stream)
      total = Enum.sum(Enum.map(filtered, &ExArrow.RecordBatch.num_rows/1))
      assert total == 2
    end

    test "filters prune row groups via statistics when ranges are disjoint" do
      n = 4
      ids = for i <- [1, 2, 100, 101], into: <<>>, do: <<i::little-signed-64>>
      assert {:ok, batch} = ExArrow.RecordBatch.from_columns(["id"], [ids], ["s64"], n)
      schema = ExArrow.RecordBatch.schema(batch)

      assert {:ok, bin} =
               Parquet.Writer.to_binary(schema, [batch], row_group_size: 2)

      assert {:ok, meta} = Parquet.Metadata.from_binary(bin)
      assert meta.num_row_groups == 2

      assert {:ok, stream} =
               Parquet.Reader.from_binary(bin, filters: {:gt, "id", 50})

      stats = Parquet.Reader.read_stats(stream)
      assert stats.row_groups_total == 2
      assert stats.row_groups_skipped >= 1
      assert stats.row_groups_selected == 2 - stats.row_groups_skipped

      total =
        stream
        |> Stream.to_list()
        |> Enum.map(&ExArrow.RecordBatch.num_rows/1)
        |> Enum.sum()

      assert total == 2
    end

    test "filters prune correctly when a struct column precedes the filtered column" do
      # PyArrow fixture: Arrow fields [meta(struct{a,b}), score]; Parquet leaves
      # [meta.a, meta.b, score]. Using Arrow field index for score would read
      # meta.b stats and wrongly skip the high-score row group.
      path = Path.expand("../fixtures/parquet_nested_struct_score.parquet", __DIR__)

      assert {:ok, stream} =
               Parquet.Reader.from_file(path, filters: {:gt, "score", 50})

      stats = Parquet.Reader.read_stats(stream)
      assert stats.row_groups_total == 2
      assert stats.row_groups_skipped == 1
      assert stats.row_groups_selected == 1

      total =
        stream
        |> Stream.to_list()
        |> Enum.map(&ExArrow.RecordBatch.num_rows/1)
        |> Enum.sum()

      assert total == 2
    end

    test "filter with out-of-range int value against Int32 column errors" do
      n = 2
      ids = for i <- 1..n, into: <<>>, do: <<i::little-signed-32>>
      assert {:ok, batch} = ExArrow.RecordBatch.from_columns(["id"], [ids], ["s32"], n)
      schema = ExArrow.RecordBatch.schema(batch)
      assert {:ok, bin} = Parquet.Writer.to_binary(schema, [batch])

      assert {:error, msg} =
               Parquet.Reader.from_binary(bin, filters: {:eq, "id", 5_000_000_000})

      assert msg =~ ~r/out of range|Int32/i
    end

    test "filter with non-representable float against Float32 column errors" do
      n = 2
      vals = for i <- 1..n, into: <<>>, do: <<i * 1.0::little-float-32>>
      assert {:ok, batch} = ExArrow.RecordBatch.from_columns(["x"], [vals], ["f32"], n)
      schema = ExArrow.RecordBatch.schema(batch)
      assert {:ok, bin} = Parquet.Writer.to_binary(schema, [batch])

      assert {:error, msg} =
               Parquet.Reader.from_binary(bin, filters: {:eq, "x", 1.0e40})

      assert msg =~ ~r/Float32|representable/i
    end

    test "row_groups selects subset" do
      {schema, batch} = source_batch()
      rows = ExArrow.RecordBatch.num_rows(batch)

      assert {:ok, bin} =
               Parquet.Writer.to_binary(schema, [batch], row_group_size: 1)

      assert {:ok, meta} = Parquet.Metadata.from_binary(bin)
      assert meta.num_row_groups == rows

      assert {:ok, stream} = Parquet.Reader.from_binary(bin, row_groups: [0])
      stats = Parquet.Reader.read_stats(stream)
      assert stats.row_groups_selected == 1
      assert stats.row_groups_total == meta.num_row_groups
    end

    test "invalid opts rejected in Elixir before NIF" do
      assert {:error, _} = Parquet.Reader.from_binary(<<>>, columns: [1])
      assert {:error, _} = Parquet.Reader.from_binary(<<>>, filters: {:nope, "x", 1})
    end
  end

  describe "Metadata" do
    test "from_binary returns row group info and column stats" do
      n = 3
      ids = for i <- [10, 20, 30], into: <<>>, do: <<i::little-signed-64>>
      assert {:ok, batch} = ExArrow.RecordBatch.from_columns(["id"], [ids], ["s64"], n)
      schema = ExArrow.RecordBatch.schema(batch)

      assert {:ok, bin} = Parquet.Writer.to_binary(schema, [batch], compression: :snappy)
      assert {:ok, meta} = Parquet.Metadata.from_binary(bin)
      assert meta.num_rows == n
      assert meta.num_row_groups >= 1
      assert is_list(meta.row_groups)

      col = hd(hd(meta.row_groups).columns)
      assert col.path == "id"
      assert col.compression =~ ~r/SNAPPY/i
      assert col.min == "10"
      assert col.max == "30"
    end

    @tag :tmp_dir
    test "from_file", %{tmp_dir: dir} do
      path = Path.join(dir, "meta.parquet")
      {schema, batch} = source_batch()
      assert :ok = Parquet.Writer.to_file(path, schema, [batch])
      assert {:ok, meta} = Parquet.Metadata.from_file(path)
      assert meta.num_row_groups >= 1
    end

    test "from_file returns an error for a missing path" do
      assert {:error, _} = Parquet.Metadata.from_file("/tmp/does_not_exist_xyz.parquet")
    end

    test "from_binary returns an error for a non-Parquet binary" do
      assert {:error, _} = Parquet.Metadata.from_binary("not parquet")
    end
  end

  describe "multi-file streams" do
    @tag :tmp_dir
    test "from_parquet_files concatenates lazily", %{tmp_dir: dir} do
      {schema, batch} = source_batch()
      p1 = Path.join(dir, "a.parquet")
      p2 = Path.join(dir, "b.parquet")
      assert :ok = Parquet.Writer.to_file(p1, schema, [batch])
      assert :ok = Parquet.Writer.to_file(p2, schema, [batch])

      assert {:ok, stream} = Stream.from_parquet_files([p1, p2])
      batches = Stream.to_list(stream)
      assert length(batches) == 2
      assert Stream.next(stream) == nil
      assert Stream.next(stream) == nil
    end

    @tag :tmp_dir
    test "Enum.take does not open later files", %{tmp_dir: dir} do
      {schema, batch} = source_batch()
      p1 = Path.join(dir, "a.parquet")
      p2 = Path.join(dir, "b.parquet")
      assert :ok = Parquet.Writer.to_file(p1, schema, [batch])
      assert :ok = Parquet.Writer.to_file(p2, schema, [batch])

      assert {:ok, stream} = Stream.from_parquet_files([p1, p2])
      _ = Enum.take(stream, 1)
      assert Stream.multi_opened_paths(stream) == [p1]
      assert :ok = Stream.close(stream)
      refute Process.alive?(stream.resource)
    end

    @tag :tmp_dir
    test "schema mismatch across files surfaces from next/1", %{tmp_dir: dir} do
      {schema_a, batch_a} = source_batch()
      p1 = Path.join(dir, "a.parquet")
      assert :ok = Parquet.Writer.to_file(p1, schema_a, [batch_a])

      n = 1
      ids = <<1::little-signed-64>>
      assert {:ok, batch_b} = ExArrow.RecordBatch.from_columns(["other"], [ids], ["s64"], n)
      schema_b = ExArrow.RecordBatch.schema(batch_b)
      p2 = Path.join(dir, "b.parquet")
      assert :ok = Parquet.Writer.to_file(p2, schema_b, [batch_b])

      assert {:ok, stream} = Stream.from_parquet_files([p1, p2])
      assert %ExArrow.RecordBatch{} = Stream.next(stream)
      assert {:error, msg} = Stream.next(stream)
      assert msg =~ "schema mismatch"
    end

    test "from_parquet_files/2 rejects non-string paths without raising" do
      assert {:error, msg} = Stream.from_parquet_files([:not_a_path])
      assert msg =~ "must be strings"

      assert {:error, msg} = Stream.from_parquet_files([%{}])
      assert msg =~ "must be strings"
    end

    @tag :tmp_dir
    test "from_parquet_dir", %{tmp_dir: dir} do
      {schema, batch} = source_batch()
      assert :ok = Parquet.Writer.to_file(Path.join(dir, "x.parquet"), schema, [batch])
      assert {:ok, stream} = Stream.from_parquet_dir(dir)
      assert length(Stream.to_list(stream)) == 1
    end
  end
end
