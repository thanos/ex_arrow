defmodule ExArrow.BatchTest do
  use ExUnit.Case, async: true

  alias ExArrow.Batch
  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  # Build a batch with two columns: id (s64) and label (utf8).
  defp two_col_batch(ids, labels) do
    n = length(ids)

    id_bin =
      ids
      |> Enum.map(&<<&1::little-signed-64>>)
      |> IO.iodata_to_binary()

    label_bin =
      labels
      |> Enum.map(fn s ->
        bytes = s |> String.to_charlist() |> IO.iodata_to_binary()
        <<byte_size(bytes)::little-32, bytes::binary>>
      end)
      |> IO.iodata_to_binary()

    {:ok, batch} =
      RecordBatch.from_columns(["id", "label"], [id_bin, label_bin], ["s64", "utf8"], n)

    batch
  end

  # Build a numeric-only batch with two s64 columns for rename tests.
  defp two_s64_batch(a, b) do
    n = length(a)

    bin_fn = fn list ->
      list |> Enum.map(&<<&1::little-signed-64>>) |> IO.iodata_to_binary()
    end

    {:ok, batch} =
      RecordBatch.from_columns(["a", "b"], [bin_fn.(a), bin_fn.(b)], ["s64", "s64"], n)

    batch
  end

  # Extract an s64 column as a list of integers.
  defp s64_column(batch, name) do
    ref = RecordBatch.resource_ref(batch)
    {:ok, {binary, "s64", _n}} = ExArrow.Native.record_batch_column_buffer(ref, name)

    for <<v::little-signed-64 <- binary>>, do: v
  end

  defp column_names(batch), do: RecordBatch.column_names(batch)

  describe "schema/1" do
    @tag :nif
    test "returns the batch schema" do
      batch = two_col_batch([1, 2, 3], ["a", "b", "c"])
      schema = Batch.schema(batch)
      assert Schema.field_names(schema) == ["id", "label"]
    end
  end

  describe "select/2" do
    @tag :nif
    test "selects columns in the requested order" do
      batch = two_col_batch([1, 2], ["a", "b"])
      assert {:ok, selected} = Batch.select(batch, ["label", "id"])
      assert column_names(selected) == ["label", "id"]
    end

    @tag :nif
    test "returns error for unknown column" do
      batch = two_col_batch([1], ["a"])
      assert {:error, msg} = Batch.select(batch, ["missing"])
      assert msg =~ "not found"
    end

    @tag :nif
    test "preserves row count" do
      batch = two_col_batch([10, 20, 30], ["x", "y", "z"])
      assert {:ok, selected} = Batch.select(batch, ["id"])
      assert RecordBatch.num_rows(selected) == 3
      assert s64_column(selected, "id") == [10, 20, 30]
    end
  end

  describe "drop/2" do
    @tag :nif
    test "removes the named columns and keeps the rest in order" do
      batch = two_col_batch([1, 2], ["a", "b"])
      assert {:ok, rest} = Batch.drop(batch, ["label"])
      assert column_names(rest) == ["id"]
      assert s64_column(rest, "id") == [1, 2]
    end

    @tag :nif
    test "dropping all columns yields an empty-schema batch" do
      batch = two_col_batch([1], ["a"])
      assert {:ok, rest} = Batch.drop(batch, ["id", "label"])
      assert RecordBatch.num_columns(rest) == 0
      assert RecordBatch.num_rows(rest) == 1
    end

    @tag :nif
    test "returns an error for an unknown column name" do
      batch = two_col_batch([1], ["a"])
      assert {:error, msg} = Batch.drop(batch, ["no_such_column"])
      assert msg =~ "no_such_column"
    end

    @tag :nif
    test "returns an error when one of several columns is unknown" do
      batch = two_col_batch([1], ["a"])
      assert {:error, msg} = Batch.drop(batch, ["id", "no_such_column"])
      assert msg =~ "no_such_column"
    end
  end

  describe "rename/2" do
    @tag :nif
    test "renames the supplied columns and preserves order and types" do
      batch = two_s64_batch([1, 2], [10, 20])
      assert {:ok, renamed} = Batch.rename(batch, %{"a" => "x", "b" => "y"})
      assert column_names(renamed) == ["x", "y"]
      assert s64_column(renamed, "x") == [1, 2]
      assert s64_column(renamed, "y") == [10, 20]
    end

    @tag :nif
    test "leaves un-mapped columns unchanged" do
      batch = two_s64_batch([1], [10])
      assert {:ok, renamed} = Batch.rename(batch, %{"a" => "x"})
      assert column_names(renamed) == ["x", "b"]
    end

    @tag :nif
    test "accepts a keyword list mapping" do
      batch = two_s64_batch([1], [10])
      assert {:ok, renamed} = Batch.rename(batch, a: "x")
      assert column_names(renamed) == ["x", "b"]
    end

    @tag :nif
    test "returns an error for an unknown source column" do
      batch = two_s64_batch([1], [10])
      assert {:error, msg} = Batch.rename(batch, %{"missing" => "x"})
      assert msg =~ "unknown column"
    end

    @tag :nif
    test "preserves schema types after rename" do
      batch = two_s64_batch([1, 2], [10, 20])
      assert {:ok, renamed} = Batch.rename(batch, %{"a" => "x"})
      fields = Schema.fields(RecordBatch.schema(renamed))
      assert Enum.map(fields, &{&1.name, &1.type}) == [{"x", :int64}, {"b", :int64}]
    end

    @tag :nif
    test "returns an error for unsupported (utf8) columns" do
      batch = two_col_batch([1, 2], ["a", "b"])
      assert {:error, msg} = Batch.rename(batch, %{"id" => "user_id"})
      assert msg =~ "unsupported column type"
    end
  end

  describe "take/2 — integer" do
    @tag :nif
    test "returns the first n rows" do
      batch = two_col_batch([10, 20, 30, 40], ["a", "b", "c", "d"])
      assert {:ok, taken} = Batch.take(batch, 2)
      assert RecordBatch.num_rows(taken) == 2
      assert s64_column(taken, "id") == [10, 20]
    end

    @tag :nif
    test "n larger than rows returns the batch unchanged" do
      batch = two_col_batch([1, 2], ["a", "b"])
      assert {:ok, taken} = Batch.take(batch, 10)
      assert RecordBatch.num_rows(taken) == 2
      assert s64_column(taken, "id") == [1, 2]
    end

    @tag :nif
    test "n = 0 returns an empty batch with the same columns" do
      batch = two_col_batch([1, 2], ["a", "b"])
      assert {:ok, taken} = Batch.take(batch, 0)
      assert RecordBatch.num_rows(taken) == 0
      assert column_names(taken) == ["id", "label"]
    end

    test "negative n returns an error" do
      batch = two_col_batch([1], ["a"])
      assert {:error, _} = Batch.take(batch, -1)
    end
  end

  describe "take/2 — indices" do
    @tag :nif
    test "selects the rows at the given indices (in original row order)" do
      batch = two_col_batch([10, 20, 30, 40], ["a", "b", "c", "d"])
      assert {:ok, taken} = Batch.take(batch, [2, 0, 3])
      assert RecordBatch.num_rows(taken) == 3
      # Rows 0, 2, 3 kept in original order: ids 10, 30, 40
      assert s64_column(taken, "id") == [10, 30, 40]
    end

    @tag :nif
    test "empty index list returns an empty batch" do
      batch = two_col_batch([1, 2], ["a", "b"])
      assert {:ok, taken} = Batch.take(batch, [])
      assert RecordBatch.num_rows(taken) == 0
    end

    @tag :nif
    test "out-of-range index is an error" do
      batch = two_col_batch([1, 2], ["a", "b"])
      assert {:error, msg} = Batch.take(batch, [0, 5])
      assert msg =~ "out of range"
    end
  end

  describe "filter/2" do
    @tag :nif
    test "delegates to Compute.filter/2 via a boolean predicate" do
      batch = two_col_batch([10, 20, 30, 40], ["a", "b", "c", "d"])
      # mask: keep rows 0 and 2 (id 10, 30)
      mask = <<1, 0, 1, 0>>
      {:ok, mask_batch} = RecordBatch.from_columns(["m"], [mask], ["bool"], 4)

      assert {:ok, filtered} = Batch.filter(batch, mask_batch)
      assert RecordBatch.num_rows(filtered) == 2
      assert s64_column(filtered, "id") == [10, 30]
    end
  end
end
