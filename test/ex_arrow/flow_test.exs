defmodule ExArrow.FlowTest do
  use ExUnit.Case, async: true

  alias ExArrow.IPC
  # NOTE: do not alias ExArrow.Flow as `Flow` here — bare `Flow` must resolve to
  # the Flow library so Flow.map/Flow.partition/Flow.reduce/Flow.flat_map work.
  alias ExArrow.Stream

  # Build a multi-batch IPC stream so Flow has several batches to process.
  defp multi_batch_stream(num_batches) do
    {:ok, fixture} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, reader} = ExArrow.Native.ipc_reader_from_binary(fixture)
    schema_ref = ExArrow.Native.ipc_stream_schema(reader)
    {:ok, batch_ref} = ExArrow.Native.ipc_stream_next(reader)

    batch_refs = for _ <- 1..num_batches, do: batch_ref
    {:ok, bin} = ExArrow.Native.ipc_writer_to_binary(schema_ref, batch_refs)

    {:ok, stream} = IPC.Reader.from_binary(bin)
    stream
  end

  describe "from_batches/1" do
    @tag :nif
    test "builds a Flow from an ExArrow.Stream and yields every batch" do
      stream = multi_batch_stream(4)

      rows =
        stream
        |> ExArrow.Flow.from_batches()
        |> Flow.map(&ExArrow.RecordBatch.num_rows/1)
        |> Enum.to_list()

      assert length(rows) == 4
      assert Enum.all?(rows, &(&1 > 0))
    end

    @tag :nif
    test "accepts {:ok, stream} and unwraps it" do
      {:ok, stream} = ExArrow.IPC.Reader.from_binary(multi_batch_stream_binary(3))

      counts =
        {:ok, stream}
        |> ExArrow.Flow.from_batches()
        |> Flow.map(&ExArrow.RecordBatch.num_columns/1)
        |> Enum.to_list()

      assert length(counts) == 3
    end

    @tag :nif
    test "raises on {:error, _} input" do
      assert_raise RuntimeError, ~r/error result/, fn ->
        ExArrow.Flow.from_batches({:error, "boom"})
      end
    end

    @tag :nif
    test "supports Flow.partition/2 and Flow.reduce/3" do
      stream = multi_batch_stream(5)

      row_counts =
        stream
        |> ExArrow.Flow.from_batches()
        |> Flow.partition()
        |> Flow.reduce(fn -> [] end, fn batch, acc ->
          [ExArrow.RecordBatch.num_rows(batch) | acc]
        end)
        |> Enum.to_list()

      assert Enum.sum(row_counts) > 0
    end

    @tag :nif
    test "supports Flow.flat_map/2" do
      stream = multi_batch_stream(3)

      # Expand each batch into a one-element list, then collect.
      result =
        stream
        |> ExArrow.Flow.from_batches()
        |> Flow.flat_map(fn batch -> [ExArrow.RecordBatch.num_rows(batch)] end)
        |> Enum.to_list()

      assert length(result) == 3
    end
  end

  describe "map_batches/2" do
    @tag :nif
    test "maps over batches and returns transformed values" do
      stream = multi_batch_stream(3)

      rows =
        stream
        |> ExArrow.Flow.from_batches()
        |> ExArrow.Flow.map_batches(fn batch -> ExArrow.RecordBatch.num_rows(batch) end)
        |> Enum.to_list()

      assert length(rows) == 3
    end

    @tag :nif
    test "emits [:ex_arrow, :pipeline, :batch] telemetry per batch" do
      stream = multi_batch_stream(2)
      ref = make_ref()

      :telemetry.attach(
        {:ex_arrow_flow, ref},
        [:ex_arrow, :pipeline, :batch],
        fn _event, measurements, _meta, config ->
          send(config[:pid], {:batch_event, measurements[:rows]})
        end,
        %{pid: self()}
      )

      stream
      |> ExArrow.Flow.from_batches()
      |> ExArrow.Flow.map_batches(fn _batch -> :ok end)
      |> Enum.to_list()

      assert_received {:batch_event, _rows}
      assert_received {:batch_event, _rows}

      :telemetry.detach({:ex_arrow_flow, ref})
    end
  end

  describe "each_batch/2" do
    @tag :nif
    test "runs a side effect and preserves the batches in the flow" do
      stream = multi_batch_stream(2)
      pid = self()

      batches =
        stream
        |> ExArrow.Flow.from_batches()
        |> ExArrow.Flow.each_batch(fn _batch -> send(pid, :seen) end)
        |> Enum.to_list()

      assert length(batches) == 2
      assert Enum.all?(batches, &ExArrow.RecordBatch.record_batch?/1)
      assert_received :seen
      assert_received :seen
    end
  end

  # Build the IPC binary for a multi-batch stream.
  defp multi_batch_stream_binary(num_batches) do
    {:ok, fixture} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, reader} = ExArrow.Native.ipc_reader_from_binary(fixture)
    schema_ref = ExArrow.Native.ipc_stream_schema(reader)
    {:ok, batch_ref} = ExArrow.Native.ipc_stream_next(reader)
    batch_refs = for _ <- 1..num_batches, do: batch_ref
    {:ok, bin} = ExArrow.Native.ipc_writer_to_binary(schema_ref, batch_refs)
    bin
  end
end
