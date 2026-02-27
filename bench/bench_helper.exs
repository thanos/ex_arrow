defmodule Bench.DataGen do
  @moduledoc """
  Deterministic data generation for benchmarks.
  All data is built from ExArrow primitives so benchmarks run without external fixtures.
  """

  @doc """
  Returns a single IPC stream binary built by replicating the built-in test fixture
  `num_batches` times.
  """
  @spec ipc_binary(pos_integer()) :: binary()
  def ipc_binary(num_batches \\ 10) do
    {schema, batches} = schema_and_batches(num_batches)
    {:ok, binary} = ExArrow.IPC.Writer.to_binary(schema, batches)
    binary
  end

  @doc """
  Returns `{schema, [batch, ...]}` with `num_batches` identical record batches.
  """
  @spec schema_and_batches(pos_integer()) :: {ExArrow.Schema.t(), [ExArrow.RecordBatch.t()]}
  def schema_and_batches(num_batches \\ 10) do
    {:ok, fixture} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, stream} = ExArrow.IPC.Reader.from_binary(fixture)
    {:ok, schema} = ExArrow.Stream.schema(stream)
    batch = ExArrow.Stream.next(stream)

    batches = for _ <- 1..num_batches, do: batch
    {schema, batches}
  end

  @doc """
  Returns a large IPC binary (~50 batches) suitable for throughput benchmarks.
  """
  @spec large_ipc_binary() :: binary()
  def large_ipc_binary, do: ipc_binary(50)

  @doc """
  Writes an IPC binary to a temp file and returns the path.
  Caller is responsible for deleting the file.
  """
  @spec write_temp_ipc_file(binary()) :: Path.t()
  def write_temp_ipc_file(binary) do
    path =
      Path.join(
        System.tmp_dir!(),
        "ex_arrow_bench_#{System.unique_integer([:positive])}.arrows"
      )

    File.write!(path, binary)
    path
  end

  @doc """
  Drains an IPC stream into a list of record batches (materialises everything in BEAM).
  """
  @spec collect_stream(ExArrow.Stream.t()) :: [ExArrow.RecordBatch.t()]
  def collect_stream(stream), do: do_collect(stream, [])

  defp do_collect(stream, acc) do
    case ExArrow.Stream.next(stream) do
      nil -> Enum.reverse(acc)
      {:error, _} -> Enum.reverse(acc)
      batch -> do_collect(stream, [batch | acc])
    end
  end

  @doc """
  Returns the benchmark output directory (creates it if needed).
  """
  @spec output_dir() :: Path.t()
  def output_dir do
    dir = Path.join(File.cwd!(), "bench/output")
    File.mkdir_p!(dir)
    dir
  end
end
