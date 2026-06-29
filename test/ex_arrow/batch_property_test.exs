defmodule ExArrow.BatchPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import ExArrow.TestFixtures
  alias ExArrow.Batch
  alias ExArrow.RecordBatch

  defp s64_column(batch, name) do
    ref = RecordBatch.resource_ref(batch)
    {:ok, {binary, "s64", _n}} = ExArrow.Native.record_batch_column_buffer(ref, name)

    for <<v::little-signed-64 <- binary>>, do: v
  end

  property "select/2 with all columns preserves row count and values" do
    check all(values <- list_of(integer(-1000..1000), min_length: 1, max_length: 50)) do
      batch = s64_batch(values)

      assert {:ok, selected} = Batch.select(batch, ["v"])
      assert RecordBatch.num_rows(selected) == length(values)
      assert s64_column(selected, "v") == values
    end
  end

  property "take/2 with n >= rows returns the batch unchanged" do
    check all(values <- list_of(integer(-1000..1000), min_length: 1, max_length: 50)) do
      batch = s64_batch(values)
      n = length(values)

      assert {:ok, taken} = Batch.take(batch, n)
      assert s64_column(taken, "v") == values

      assert {:ok, taken2} = Batch.take(batch, n + 10)
      assert s64_column(taken2, "v") == values
    end
  end

  property "take/2 with n returns the first n values" do
    check all(values <- list_of(integer(-1000..1000), min_length: 1, max_length: 50)) do
      batch = s64_batch(values)
      n = :rand.uniform(length(values))

      assert {:ok, taken} = Batch.take(batch, n)
      assert s64_column(taken, "v") == Enum.take(values, n)
    end
  end

  property "drop/2 of an empty list preserves the schema" do
    check all(values <- list_of(integer(-1000..1000), min_length: 1, max_length: 50)) do
      batch = s64_batch(values)

      assert {:ok, rest} = Batch.drop(batch, [])
      assert RecordBatch.column_names(rest) == ["v"]
      assert s64_column(rest, "v") == values
    end
  end

  property "rename/2 round-trips with the inverse mapping" do
    check all(values <- list_of(integer(-1000..1000), min_length: 1, max_length: 50)) do
      batch = s64_batch(values)

      assert {:ok, r1} = Batch.rename(batch, %{"v" => "w"})
      assert {:ok, r2} = Batch.rename(r1, %{"w" => "v"})
      assert RecordBatch.column_names(r2) == ["v"]
      assert s64_column(r2, "v") == values
    end
  end

  property "take/2 with indices selects exactly those rows (original order)" do
    check all(values <- list_of(integer(-1000..1000), min_length: 2, max_length: 50)) do
      batch = s64_batch(values)
      n = length(values)
      # pick a deterministic subset: even indices
      indices = for i <- 0..(n - 1), rem(i, 2) == 0, do: i

      assert {:ok, taken} = Batch.take(batch, indices)
      assert s64_column(taken, "v") == Enum.take_every(values, 2)
    end
  end
end
