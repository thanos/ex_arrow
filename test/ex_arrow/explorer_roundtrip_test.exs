defmodule ExArrow.ExplorerRoundtripTest do
  use ExUnit.Case, async: true

  alias ExArrow.Explorer, as: ExArrowExplorer

  @moduletag :explorer

  if Code.ensure_loaded?(Explorer.DataFrame) do
    describe "ExArrow.from_dataframe/1 and ExArrow.to_dataframe/1 round-trip" do
      test "preserves column names" do
        df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
        {:ok, batch} = ExArrow.from_dataframe(df)
        {:ok, df2} = ExArrow.to_dataframe(batch)
        assert Explorer.DataFrame.names(df2) == Explorer.DataFrame.names(df)
      end

      test "preserves row count" do
        df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
        {:ok, batch} = ExArrow.from_dataframe(df)
        {:ok, df2} = ExArrow.to_dataframe(batch)
        assert Explorer.DataFrame.n_rows(df2) == Explorer.DataFrame.n_rows(df)
      end

      test "preserves values" do
        df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
        {:ok, batch} = ExArrow.from_dataframe(df)
        {:ok, df2} = ExArrow.to_dataframe(batch)
        assert Explorer.DataFrame.to_rows(df2) == Explorer.DataFrame.to_rows(df)
      end

      test "preserves schema with integer and float columns" do
        df = Explorer.DataFrame.new(ints: [10, 20], floats: [1.5, 2.5])
        {:ok, batch} = ExArrow.from_dataframe(df)
        {:ok, df2} = ExArrow.to_dataframe(batch)
        assert Explorer.DataFrame.dtypes(df2) == Explorer.DataFrame.dtypes(df)
      end

      test "preserves boolean column" do
        df = Explorer.DataFrame.new(flags: [true, false, true], name: ["a", "b", "c"])
        {:ok, batch} = ExArrow.from_dataframe(df)
        {:ok, df2} = ExArrow.to_dataframe(batch)
        assert Explorer.Series.to_list(Explorer.DataFrame.pull(df2, "flags")) ==
                 Explorer.Series.to_list(Explorer.DataFrame.pull(df, "flags"))
      end

      test "preserves nullability" do
        df = Explorer.DataFrame.new(x: [1, 2, nil], y: ["a", nil, "c"])
        {:ok, batch} = ExArrow.from_dataframe(df)
        schema = ExArrow.RecordBatch.schema(batch)
        fields = ExArrow.Schema.fields(schema)
        assert Enum.all?(fields, & &1.nullable)
        {:ok, df2} = ExArrow.to_dataframe(batch)
        assert Explorer.DataFrame.n_rows(df2) == 3
      end

      test "nullable columns round-trip correctly" do
        df = Explorer.DataFrame.new(x: [1, 2, nil], y: ["a", nil, "c"])
        {:ok, batch} = ExArrow.from_dataframe(df)
        {:ok, df2} = ExArrow.to_dataframe(batch)
        assert Explorer.DataFrame.n_rows(df2) == 3
        x_vals = Explorer.Series.to_list(Explorer.DataFrame.pull(df2, "x"))
        assert Enum.at(x_vals, 0) == 1
        assert Enum.at(x_vals, 2) == nil
      end

      test "non-nullable source data round-trips correctly" do
        df = Explorer.DataFrame.new(x: [1, 2, 3])
        {:ok, batch} = ExArrow.from_dataframe(df)
        {:ok, df2} = ExArrow.to_dataframe(batch)
        x_vals = Explorer.Series.to_list(Explorer.DataFrame.pull(df2, "x"))
        assert x_vals == [1, 2, 3]
      end

      test "handles single-column dataframe" do
        df = Explorer.DataFrame.new(val: [42])
        {:ok, batch} = ExArrow.from_dataframe(df)
        assert ExArrow.RecordBatch.num_rows(batch) == 1
        {:ok, df2} = ExArrow.to_dataframe(batch)
        assert Explorer.DataFrame.n_rows(df2) == 1
      end

      test "handles empty dataframe" do
        df = Explorer.DataFrame.new(x: Explorer.Series.from_list([]))
        {:ok, batch} = ExArrow.from_dataframe(df)
        assert ExArrow.RecordBatch.num_rows(batch) == 0
      end
    end

    describe "ExArrow.DataFrame.from_arrow/1 and ExArrow.DataFrame.to_arrow/1 round-trip" do
      test "preserves column names" do
        df = Explorer.DataFrame.new(a: [10, 20], b: ["hello", "world"])
        {:ok, batch} = ExArrow.DataFrame.to_arrow(df)
        {:ok, df2} = ExArrow.DataFrame.from_arrow(batch)
        assert Explorer.DataFrame.names(df2) == Explorer.DataFrame.names(df)
      end

      test "preserves values" do
        df = Explorer.DataFrame.new(a: [10, 20], b: ["hello", "world"])
        {:ok, batch} = ExArrow.DataFrame.to_arrow(df)
        {:ok, df2} = ExArrow.DataFrame.from_arrow(batch)
        assert Explorer.DataFrame.to_rows(df2) == Explorer.DataFrame.to_rows(df)
      end

      test "accepts ExArrow.Stream for from_arrow" do
        df = Explorer.DataFrame.new(x: [1, 2, 3])
        {:ok, stream} = ExArrowExplorer.to_stream(df)
        {:ok, df2} = ExArrow.DataFrame.from_arrow(stream)
        assert Explorer.DataFrame.n_rows(df2) == 3
      end
    end

    describe "error cases" do
      test "from_dataframe returns error when Explorer not a dataframe" do
        assert {:error, msg} = ExArrow.from_dataframe(:not_a_df)
        assert is_binary(msg)
      end

      test "to_dataframe returns error for invalid batch" do
        bad_batch = %ExArrow.RecordBatch{resource: make_ref()}
        assert {:error, msg} = ExArrow.to_dataframe(bad_batch)
        assert is_binary(msg)
      end
    end
  else
    test "returns descriptive error when Explorer is not loaded" do
      assert {:error, msg} = ExArrow.from_dataframe(:ignored)
      assert msg =~ "Explorer"
    end
  end
end
