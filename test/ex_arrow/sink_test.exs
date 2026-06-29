defmodule ExArrow.SinkTest do
  use ExUnit.Case, async: false

  import ExArrow.TestFixtures
  alias ExArrow.RecordBatch
  alias ExArrow.Sink.DataFrame
  alias ExArrow.Sink.Flight
  alias ExArrow.Sink.Parquet
  # NOTE: do not alias ExArrow.Sink.Nx as `Nx` — bare `Nx` must resolve to the
  # Nx library so Nx.shape/Nx.to_number work in assertions.

  describe "Parquet.write/2" do
    @tag :tmp_dir
    @tag :nif
    test "writes a stream to a Parquet file", %{tmp_dir: dir} do
      path = Path.join(dir, "s.parquet")
      stream = ipc_stream(2)

      assert :ok = Parquet.write(stream, path)
      assert File.exists?(path)

      {:ok, rt_stream} = ExArrow.Stream.from_parquet(path)
      batches = ExArrow.Stream.to_list(rt_stream)
      assert length(batches) >= 1
    end

    @tag :tmp_dir
    @tag :nif
    test "writes a {schema, batches} tuple", %{tmp_dir: dir} do
      path = Path.join(dir, "tuple.parquet")
      batch = s64_batch([1, 2, 3])
      schema = RecordBatch.schema(batch)

      assert :ok = Parquet.write({schema, [batch]}, path)
      assert File.exists?(path)
    end

    @tag :tmp_dir
    @tag :nif
    test "writes a bare batch list", %{tmp_dir: dir} do
      path = Path.join(dir, "list.parquet")
      batch = s64_batch([10, 20])

      assert :ok = Parquet.write([batch], path)
      assert File.exists?(path)
    end

    test "returns an error for an empty batch list" do
      assert {:error, msg} = Parquet.write([], "/tmp/x.parquet")
      assert msg =~ "empty batch list"
    end

    @tag :tmp_dir
    @tag :nif
    test "emits [:ex_arrow, :parquet, :write] telemetry", %{tmp_dir: dir} do
      path = Path.join(dir, "tel.parquet")
      batch = s64_batch([1, 2])
      ref = make_ref()

      :telemetry.attach(
        {:ex_arrow_sink_pq, ref},
        [:ex_arrow, :parquet, :write],
        fn _event, _measurements, metadata, config ->
          send(config[:pid], {:sink_pq, metadata})
        end,
        %{pid: self()}
      )

      Parquet.write([batch], path)

      assert_received {:sink_pq, %{destination: ^path, source: :sink}}
      :telemetry.detach({:ex_arrow_sink_pq, ref})
    end

    test "returns error for {schema, batches} where batches is not a list" do
      schema = ExArrow.Schema.from_ref(make_ref())
      assert {:error, msg} = Parquet.write({schema, :not_a_list}, "/tmp/x.parquet")
      assert msg =~ "expected {schema, [batches]}"
    end

    test "returns error for unsupported source type" do
      assert {:error, msg} = Parquet.write(:atom_source, "/tmp/x.parquet")
      assert msg =~ "unsupported source"
    end
  end

  describe "Flight.write/3" do
    setup context do
      prev = Application.get_env(:ex_arrow, :flight_client_impl)
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      Mox.set_mox_from_context(context)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)
      :ok
    end

    @tag :nif
    test "uploads a stream via Flight.Client.do_put/4" do
      stream = ipc_stream(2)
      client = %ExArrow.Flight.Client{resource: make_ref()}

      ExArrow.Flight.ClientMock
      |> Mox.expect(:do_put, fn ^client, _schema, _batches, _opts -> :ok end)

      assert :ok = Flight.write(stream, client, descriptor: {:cmd, "x"})
    end

    @tag :nif
    test "uploads a {schema, batches} tuple" do
      batch = s64_batch([1, 2])
      schema = RecordBatch.schema(batch)
      client = %ExArrow.Flight.Client{resource: make_ref()}

      ExArrow.Flight.ClientMock
      |> Mox.expect(:do_put, fn ^client, ^schema, [^batch], descriptor: {:cmd, "y"} -> :ok end)

      assert :ok = Flight.write({schema, [batch]}, client, descriptor: {:cmd, "y"})
    end

    test "no-ops on an empty batch list" do
      client = %ExArrow.Flight.Client{resource: make_ref()}
      assert :ok = Flight.write([], client)
    end
  end

  describe "DataFrame.write/1" do
    @tag :nif
    @tag :explorer
    test "converts a batch to an Explorer DataFrame" do
      batch = s64_batch([1, 2, 3])
      assert {:ok, df} = DataFrame.write(batch)
      assert Explorer.DataFrame.n_rows(df) == 3
    end
  end

  describe "Nx.write/1" do
    @tag :nif
    @tag :nx
    test "converts a batch to an Nx tensor" do
      batch = s64_batch([10, 20, 30])
      assert {:ok, tensor} = ExArrow.Sink.Nx.write(batch)
      assert Nx.shape(tensor) == {3}
    end
  end
end
