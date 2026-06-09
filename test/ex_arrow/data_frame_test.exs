defmodule ExArrow.DataFrameTestSupport.ExplorerStubEmpty do
  @moduledoc false

  def to_record_batches(_df), do: {:ok, []}
end

defmodule ExArrow.DataFrameTest do
  use ExUnit.Case, async: true

  alias ExArrow.DataFrame
  alias ExArrow.Explorer, as: ExArrowExplorer
  alias ExArrow.RecordBatch

  @moduletag :explorer

  if Code.ensure_loaded?(Explorer.DataFrame) do
    describe "to_arrow/1" do
      test "converts a small dataframe to a single batch" do
        df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
        assert {:ok, batch} = DataFrame.to_arrow(df)
        assert RecordBatch.num_rows(batch) == 3
        assert RecordBatch.column_names(batch) == ["x", "y"]
      end

      # Regression for issue #200 (C1): a large dataframe that Explorer splits
      # into multiple IPC batches previously returned only the first batch,
      # silently dropping rows.  All batches must now be concatenated.
      test "preserves the full row count when Explorer splits into many batches" do
        n = 2_000_000
        df = Explorer.DataFrame.new(x: Enum.to_list(1..n))

        assert {:ok, batches} = ExArrowExplorer.to_record_batches(df)
        assert length(batches) > 1

        assert {:ok, batch} = DataFrame.to_arrow(df)
        assert RecordBatch.num_rows(batch) == n
      end

      test "preserves values across the concatenation boundary" do
        n = 2_000_000
        values = Enum.to_list(1..n)
        df = Explorer.DataFrame.new(x: values)

        assert {:ok, batch} = DataFrame.to_arrow(df)
        {:ok, df2} = DataFrame.from_arrow(batch)
        recovered = Explorer.Series.to_list(Explorer.DataFrame.pull(df2, "x"))
        assert recovered == values
      end

      test "returns an error when Explorer produces no batches" do
        stub = ExArrow.DataFrameTestSupport.ExplorerStubEmpty

        Process.put({DataFrame, :explorer_impl}, stub)
        on_exit(fn -> Process.delete({DataFrame, :explorer_impl}) end)

        df = Explorer.DataFrame.new(x: [1, 2, 3])
        assert {:error, msg} = DataFrame.to_arrow(df)
        assert msg =~ "no batches"
      end
    end

    describe "from_arrow/1 dispatches by struct type" do
      test "RecordBatch path: round-trips a dataframe" do
        df = Explorer.DataFrame.new(x: [1, 2, 3])
        {:ok, batch} = DataFrame.to_arrow(df)
        assert {:ok, df2} = DataFrame.from_arrow(batch)
        assert Explorer.DataFrame.n_rows(df2) == 3
      end

      test "Stream path: round-trips a dataframe" do
        df = Explorer.DataFrame.new(x: [1, 2, 3])
        {:ok, stream} = ExArrowExplorer.to_stream(df)
        assert {:ok, df2} = DataFrame.from_arrow(stream)
        assert Explorer.DataFrame.n_rows(df2) == 3
      end

      test "returns error for unsupported input type" do
        assert {:error, msg} = DataFrame.from_arrow("not a batch")
        assert msg =~ "RecordBatch" or msg =~ "Stream"
      end

      test "returns error for a bare map" do
        assert {:error, msg} = DataFrame.from_arrow(%{})
        assert msg =~ "RecordBatch" or msg =~ "Stream"
      end
    end
  else
    test "returns descriptive error when Explorer is not loaded" do
      assert {:error, msg} = DataFrame.to_arrow(:ignored)
      assert msg =~ "Explorer"
    end
  end
end
