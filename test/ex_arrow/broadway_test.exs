defmodule ExArrow.BroadwayTest do
  use ExUnit.Case, async: false

  alias ExArrow.Broadway.BatchBuilder
  alias ExArrow.Broadway.FlightSink
  alias ExArrow.Broadway.ParquetSink
  alias ExArrow.RecordBatch

  # Build a real batch handle.
  defp s64_batch(values, name \\ "v") do
    n = length(values)

    bin =
      values
      |> Enum.map(&<<&1::little-signed-64>>)
      |> IO.iodata_to_binary()

    {:ok, batch} = RecordBatch.from_columns([name], [bin], ["s64"], n)
    batch
  end

  # A minimal fake Broadway message struct. Broadway.Message is a struct with a
  # :data field; we build one without starting a Broadway pipeline.
  defp msg(data), do: %{__struct__: Broadway.Message, data: data}

  describe "BatchBuilder.extract_batches/1" do
    @tag :nif
    test "extracts RecordBatch handles from message data" do
      b1 = s64_batch([1, 2])
      b2 = s64_batch([3, 4])

      assert {:ok, [^b1, ^b2]} = BatchBuilder.extract_batches([msg(b1), msg(b2)])
    end

    @tag :nif
    test "builds batches from {names, binaries, dtypes, length} data" do
      data = {["id"], [<<1::little-signed-64, 2::little-signed-64>>], ["s64"], 2}
      assert {:ok, [batch]} = BatchBuilder.extract_batches([msg(data)])
      assert RecordBatch.num_rows(batch) == 2
    end

    test "returns an error for unsupported message data" do
      assert {:error, msg_text} = BatchBuilder.extract_batches([msg(:nope)])
      assert msg_text =~ "unsupported Broadway message data"
    end

    test "returns an error for a non-message input" do
      assert {:error, msg_text} = BatchBuilder.extract_batches([:not_a_message])
      assert msg_text =~ "expected a Broadway.Message"
    end
  end

  describe "BatchBuilder.from_messages/1" do
    @tag :nif
    test "returns the shared schema and batch list" do
      b1 = s64_batch([1, 2])
      b2 = s64_batch([3, 4])

      assert {:ok, schema, [^b1, ^b2]} =
               BatchBuilder.from_messages([msg(b1), msg(b2)])

      assert ExArrow.Schema.field_names(schema) == ["v"]
    end

    test "returns an error for an empty message list" do
      assert {:error, msg_text} = BatchBuilder.from_messages([])
      assert msg_text =~ "no batches"
    end
  end

  describe "ParquetSink.write/3" do
    @tag :tmp_dir
    @tag :nif
    test "writes batches to a Parquet file", %{tmp_dir: dir} do
      path = Path.join(dir, "out.parquet")
      batch = s64_batch([10, 20, 30])

      schema = RecordBatch.schema(batch)
      assert :ok = ParquetSink.write(path, schema, [batch])
      assert File.exists?(path)

      # Round-trip: read it back.
      {:ok, stream} = ExArrow.Stream.from_parquet(path)
      rt = ExArrow.Stream.next(stream)
      assert RecordBatch.num_rows(rt) == 3
    end

    @tag :tmp_dir
    @tag :nif
    test "emits [:ex_arrow, :parquet, :write] telemetry", %{tmp_dir: dir} do
      path = Path.join(dir, "tel.parquet")
      batch = s64_batch([1, 2])
      schema = RecordBatch.schema(batch)

      ref = make_ref()

      :telemetry.attach(
        {:ex_arrow_bway_pq, ref},
        [:ex_arrow, :parquet, :write],
        fn _event, measurements, metadata, config ->
          send(config[:pid], {:pq_write, measurements, metadata})
        end,
        %{pid: self()}
      )

      ParquetSink.write(path, schema, [batch])

      # Filter for the event matching our destination path; other async tests
      # may emit on the same event name concurrently.
      assert_receive {:pq_write, measurements, %{destination: ^path} = metadata},
                     200

      assert measurements[:rows] == 2
      assert measurements[:batch_count] == 1
      assert metadata[:source] == :broadway

      :telemetry.detach({:ex_arrow_bway_pq, ref})
    end

    @tag :tmp_dir
    @tag :nif
    test "returns error for a bad path", %{tmp_dir: dir} do
      batch = s64_batch([1])
      schema = RecordBatch.schema(batch)
      bad_path = Path.join(dir, "no_such_dir/out.parquet")
      assert {:error, _} = ParquetSink.write(bad_path, schema, [batch])
    end
  end

  describe "FlightSink.write/4" do
    setup context do
      prev = Application.get_env(:ex_arrow, :flight_client_impl)
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      Mox.set_mox_from_context(context)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)
      :ok
    end

    @tag :nif
    test "calls Flight.Client.do_put/4 with schema and batches" do
      batch = s64_batch([1, 2])
      schema = RecordBatch.schema(batch)
      client = %ExArrow.Flight.Client{resource: make_ref()}

      ExArrow.Flight.ClientMock
      |> Mox.expect(:do_put, fn ^client, _schema, _batches, _opts -> :ok end)

      assert :ok = FlightSink.write(client, schema, [batch], descriptor: {:cmd, "x"})
    end

    test "no-ops on an empty batch list" do
      client = %ExArrow.Flight.Client{resource: make_ref()}
      schema = ExArrow.Schema.from_ref(make_ref())
      assert :ok = FlightSink.write(client, schema, [])
    end

    @tag :nif
    test "emits [:ex_arrow, :flight, :query] telemetry" do
      batch = s64_batch([1, 2, 3])
      schema = RecordBatch.schema(batch)
      client = %ExArrow.Flight.Client{resource: make_ref()}

      ExArrow.Flight.ClientMock
      |> Mox.expect(:do_put, fn _, _, _, _ -> :ok end)

      ref = make_ref()

      :telemetry.attach(
        {:ex_arrow_bway_flight, ref},
        [:ex_arrow, :flight, :query],
        fn _event, measurements, metadata, config ->
          send(config[:pid], {:flight_write, measurements, metadata})
        end,
        %{pid: self()}
      )

      FlightSink.write(client, schema, [batch], descriptor: {:cmd, "events"})

      assert_receive {:flight_write, measurements, %{destination: {:cmd, "events"}} = metadata},
                     200

      assert measurements[:rows] == 3
      assert metadata[:source] == :broadway

      :telemetry.detach({:ex_arrow_bway_flight, ref})
    end
  end
end
