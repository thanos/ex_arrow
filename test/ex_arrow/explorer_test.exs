defmodule ExArrow.ExplorerTest do
  use ExUnit.Case, async: true

  @moduletag :explorer

  alias ExArrow.Explorer, as: ExArrowExplorer
  alias ExArrow.IPC
  alias ExArrow.Schema
  alias ExArrow.Stream

  defp source_stream do
    {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
    stream
  end

  defp source_batch do
    {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
    Stream.next(stream)
  end

  if Code.ensure_loaded?(Explorer.DataFrame) do
    describe "from_stream/1" do
      test "converts an ExArrow.Stream to an Explorer.DataFrame" do
        stream = source_stream()
        assert {:ok, df} = ExArrowExplorer.from_stream(stream)
        assert is_struct(df, Explorer.DataFrame)
        assert Explorer.DataFrame.n_rows(df) == 2
      end

      test "preserves column names" do
        stream = source_stream()
        {:ok, schema} = Stream.schema(stream)
        expected_cols = Schema.field_names(schema)

        stream2 = source_stream()
        assert {:ok, df} = ExArrowExplorer.from_stream(stream2)
        assert Enum.sort(Explorer.DataFrame.names(df)) == Enum.sort(expected_cols)
      end
    end

    describe "from_record_batch/1" do
      test "converts a single RecordBatch to an Explorer.DataFrame" do
        batch = source_batch()
        assert {:ok, df} = ExArrowExplorer.from_record_batch(batch)
        assert is_struct(df, Explorer.DataFrame)
        assert Explorer.DataFrame.n_rows(df) == 2
      end
    end

    describe "to_stream/1" do
      test "converts an Explorer.DataFrame back to an ExArrow.Stream" do
        df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
        assert {:ok, stream} = ExArrowExplorer.to_stream(df)
        assert is_struct(stream, ExArrow.Stream)
        batch = Stream.next(stream)
        assert ExArrow.RecordBatch.num_rows(batch) == 3
      end
    end

    describe "to_record_batches/1" do
      test "returns a list of RecordBatch handles" do
        df = Explorer.DataFrame.new(x: [10, 20], y: ["hello", "world"])
        assert {:ok, batches} = ExArrowExplorer.to_record_batches(df)
        assert is_list(batches)
        assert length(batches) >= 1
        total = Enum.sum(Enum.map(batches, &ExArrow.RecordBatch.num_rows/1))
        assert total == 2
      end
    end
  else
    test "returns descriptive error when Explorer is not loaded" do
      assert {:error, msg} = ExArrowExplorer.from_stream(:ignored)
      assert msg =~ "Explorer"
    end
  end
end
