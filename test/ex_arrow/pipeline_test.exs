defmodule ExArrow.PipelineTest do
  use ExUnit.Case, async: false

  alias ExArrow.Batch
  alias ExArrow.Pipeline
  alias ExArrow.RecordBatch

  defp ipc_stream(num_batches) do
    {:ok, fixture} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, reader} = ExArrow.Native.ipc_reader_from_binary(fixture)
    schema_ref = ExArrow.Native.ipc_stream_schema(reader)
    {:ok, batch_ref} = ExArrow.Native.ipc_stream_next(reader)
    batch_refs = for _ <- 1..num_batches, do: batch_ref
    {:ok, ipc_bin} = ExArrow.Native.ipc_writer_to_binary(schema_ref, batch_refs)
    {:ok, stream} = ExArrow.IPC.Reader.from_binary(ipc_bin)
    stream
  end

  defp s64_batch(values, name \\ "v") do
    n = length(values)

    bin =
      values
      |> Enum.map(&<<&1::little-signed-64>>)
      |> IO.iodata_to_binary()

    {:ok, batch} = RecordBatch.from_columns([name], [bin], ["s64"], n)
    batch
  end

  describe "map_batches/2" do
    @tag :nif
    test "applies a transformation lazily and writes the result" do
      {:ok, stream} = ExArrow.Stream.from_ipc(build_ipc_bin(3))
      path = tmp_path("map.parquet")

      result =
        {:ok, stream}
        |> Pipeline.map_batches(fn batch ->
          {:ok, slim} = Batch.select(batch, ["id"])
          slim
        end)
        |> Pipeline.write_parquet(path)

      assert :ok = result
      assert File.exists?(path)

      {:ok, rt} = ExArrow.Stream.from_parquet(path)
      batches = ExArrow.Stream.to_list(rt)
      assert Enum.all?(batches, fn b -> RecordBatch.num_columns(b) == 1 end)

      File.rm(path)
    end

    @tag :nif
    test "threads errors from the stream constructor" do
      assert {:error, _} =
               {:error, "no connection"}
               |> Pipeline.map_batches(fn b -> b end)
    end

    @tag :nif
    test "emits [:ex_arrow, :pipeline, :batch] telemetry per batch" do
      {:ok, stream} = ExArrow.Stream.from_ipc(build_ipc_bin(2))
      path = tmp_path("tel.parquet")
      ref = make_ref()

      :telemetry.attach(
        {:ex_arrow_pl, ref},
        [:ex_arrow, :pipeline, :batch],
        fn _event, _measurements, metadata, config ->
          send(config[:pid], {:pl_batch, metadata})
        end,
        %{pid: self()}
      )

      {:ok, stream}
      |> Pipeline.map_batches(fn b -> b end)
      |> Pipeline.write_parquet(path)

      assert_received {:pl_batch, %{source: :pipeline}}, 200
      assert_received {:pl_batch, %{source: :pipeline}}, 200

      :telemetry.detach({:ex_arrow_pl, ref})
      File.rm(path)
    end
  end

  describe "each_batch/2" do
    @tag :nif
    test "runs a side effect and preserves batches" do
      {:ok, stream} = ExArrow.Stream.from_ipc(build_ipc_bin(2))
      path = tmp_path("each.parquet")
      pid = self()

      :ok =
        {:ok, stream}
        |> Pipeline.each_batch(fn _batch -> send(pid, :saw_batch) end)
        |> Pipeline.write_parquet(path)

      assert_received :saw_batch
      assert_received :saw_batch
      File.rm(path)
    end
  end

  describe "write_parquet/2" do
    @tag :nif
    test "writes a stream with no transformations" do
      {:ok, stream} = ExArrow.Stream.from_ipc(build_ipc_bin(2))
      path = tmp_path("raw.parquet")

      assert :ok = Pipeline.write_parquet({:ok, stream}, path)
      assert File.exists?(path)
      File.rm(path)
    end

    @tag :nif
    test "threads errors" do
      assert {:error, _} = Pipeline.write_parquet({:error, "boom"}, "/tmp/x.parquet")
    end

    @tag :nif
    test "emits [:ex_arrow, :parquet, :write] telemetry" do
      {:ok, stream} = ExArrow.Stream.from_ipc(build_ipc_bin(1))
      path = tmp_path("wt.parquet")
      ref = make_ref()

      :telemetry.attach(
        {:ex_arrow_pl_pq, ref},
        [:ex_arrow, :parquet, :write],
        fn _event, _measurements, metadata, config ->
          send(config[:pid], {:pl_pq, metadata})
        end,
        %{pid: self()}
      )

      Pipeline.write_parquet({:ok, stream}, path)

      assert_received {:pl_pq, %{destination: ^path, source: :pipeline}}
      :telemetry.detach({:ex_arrow_pl_pq, ref})
      File.rm(path)
    end
  end

  describe "write_flight/3" do
    setup context do
      prev = Application.get_env(:ex_arrow, :flight_client_impl)
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      Mox.set_mox_from_context(context)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)
      :ok
    end

    @tag :nif
    test "uploads the pipeline batches to Flight" do
      {:ok, stream} = ExArrow.Stream.from_ipc(build_ipc_bin(2))
      client = %ExArrow.Flight.Client{resource: make_ref()}

      ExArrow.Flight.ClientMock
      |> Mox.expect(:do_put, fn ^client, _schema, batches, _opts ->
        # batches may be transformed or raw; here they are raw
        send(self(), {:uploaded, length(batches)})
        :ok
      end)

      assert :ok = Pipeline.write_flight({:ok, stream}, client, descriptor: {:cmd, "x"})
      assert_received {:uploaded, 2}
    end

    @tag :nif
    test "threads errors" do
      client = %ExArrow.Flight.Client{resource: make_ref()}
      assert {:error, _} = Pipeline.write_flight({:error, "boom"}, client)
    end
  end

  describe "write_dataframe/1" do
    @tag :nif
    @tag :explorer
    test "converts a pipeline to an Explorer DataFrame" do
      batch = s64_batch([1, 2, 3])
      # Build a one-batch pipeline via a stream round-trip.
      schema = RecordBatch.schema(batch)
      {:ok, ipc_bin} = ExArrow.IPC.Writer.to_binary(schema, [batch])
      {:ok, stream} = ExArrow.Stream.from_ipc(ipc_bin)

      assert {:ok, df} = Pipeline.write_dataframe({:ok, stream})
      assert Explorer.DataFrame.n_rows(df) == 3
    end

    @tag :nif
    test "threads errors" do
      assert {:error, _} = Pipeline.write_dataframe({:error, "boom"})
    end
  end

  # ── helpers ──────────────────────────────────────────────────────────────────

  defp build_ipc_bin(num_batches) do
    {:ok, fixture} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, reader} = ExArrow.Native.ipc_reader_from_binary(fixture)
    schema_ref = ExArrow.Native.ipc_stream_schema(reader)
    {:ok, batch_ref} = ExArrow.Native.ipc_stream_next(reader)
    batch_refs = for _ <- 1..num_batches, do: batch_ref
    {:ok, bin} = ExArrow.Native.ipc_writer_to_binary(schema_ref, batch_refs)
    bin
  end

  defp tmp_path(name) do
    Path.join(
      System.tmp_dir!(),
      "ex_arrow_pipeline_#{System.unique_integer([:positive])}_#{name}"
    )
  end
end
