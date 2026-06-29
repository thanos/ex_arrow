defmodule ExArrow.TestFixtures do
  @moduledoc false

  alias ExArrow.IPC.Reader, as: IPCReader

  # Shared test fixtures used across pipeline, gen_stage, sink, and flow tests.

  # Build a multi-batch IPC stream from the built-in NIF fixture.
  @spec ipc_stream(pos_integer()) :: ExArrow.Stream.t()
  def ipc_stream(num_batches) do
    {:ok, fixture} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, reader} = ExArrow.Native.ipc_reader_from_binary(fixture)
    schema_ref = ExArrow.Native.ipc_stream_schema(reader)
    {:ok, batch_ref} = ExArrow.Native.ipc_stream_next(reader)
    batch_refs = for _ <- 1..num_batches, do: batch_ref
    {:ok, ipc_bin} = ExArrow.Native.ipc_writer_to_binary(schema_ref, batch_refs)
    {:ok, stream} = IPCReader.from_binary(ipc_bin)
    stream
  end

  # Build a multi-batch IPC binary (for tests that need the raw bytes).
  @spec ipc_binary(pos_integer()) :: binary()
  def ipc_binary(num_batches) do
    {:ok, fixture} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, reader} = ExArrow.Native.ipc_reader_from_binary(fixture)
    schema_ref = ExArrow.Native.ipc_stream_schema(reader)
    {:ok, batch_ref} = ExArrow.Native.ipc_stream_next(reader)
    batch_refs = for _ <- 1..num_batches, do: batch_ref
    {:ok, bin} = ExArrow.Native.ipc_writer_to_binary(schema_ref, batch_refs)
    bin
  end

  # Build an s64 batch with `values` in a column named `name`.
  @spec s64_batch([integer()]) :: ExArrow.RecordBatch.t()
  def s64_batch(values, name \\ "v") do
    n = length(values)

    bin =
      values
      |> Enum.map(&<<&1::little-signed-64>>)
      |> IO.iodata_to_binary()

    {:ok, batch} = ExArrow.RecordBatch.from_columns([name], [bin], ["s64"], n)
    batch
  end
end
