defmodule ExArrow.StreamTest do
  use ExUnit.Case, async: true

  alias ExArrow.{IPC, Stream}

  # ── helpers ─────────────────────────────────────────────────────────────────

  defp fixture_stream do
    {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, stream} = IPC.Reader.from_binary(binary)
    stream
  end

  defp drain(stream, acc \\ []) do
    case Stream.next(stream) do
      nil -> Enum.reverse(acc)
      {:error, _} = err -> err
      batch -> drain(stream, [batch | acc])
    end
  end

  # ── schema/1 ────────────────────────────────────────────────────────────────

  describe "schema/1" do
    @tag :ipc
    test "returns {:ok, schema} for a valid stream" do
      assert {:ok, %ExArrow.Schema{}} = Stream.schema(fixture_stream())
    end

    @tag :ipc
    test "schema is callable multiple times without consuming the stream" do
      stream = fixture_stream()
      {:ok, schema1} = Stream.schema(stream)
      {:ok, schema2} = Stream.schema(stream)

      fields1 = ExArrow.Schema.fields(schema1)
      fields2 = ExArrow.Schema.fields(schema2)
      assert length(fields1) == length(fields2)

      for {f1, f2} <- Enum.zip(fields1, fields2) do
        assert f1.name == f2.name
        assert f1.type == f2.type
      end
    end

    @tag :ipc
    test "schema is still readable after batches are consumed" do
      stream = fixture_stream()
      {:ok, schema_before} = Stream.schema(stream)
      _batches = drain(stream)
      {:ok, schema_after} = Stream.schema(stream)

      assert ExArrow.Schema.fields(schema_before) ==
               ExArrow.Schema.fields(schema_after)
    end

    test "raises ArgumentError for an invalid (non-NIF) resource" do
      stream = %Stream{resource: make_ref()}
      assert_raise ArgumentError, fn -> Stream.schema(stream) end
    end
  end

  # ── next/1 ───────────────────────────────────────────────────────────────────

  describe "next/1" do
    @tag :ipc
    test "returns a RecordBatch then nil when the single-batch stream is exhausted" do
      stream = fixture_stream()
      assert %ExArrow.RecordBatch{} = Stream.next(stream)
      assert Stream.next(stream) == nil
    end

    @tag :ipc
    test "returns nil immediately on subsequent calls after exhaustion" do
      stream = fixture_stream()
      Stream.next(stream)
      assert Stream.next(stream) == nil
      assert Stream.next(stream) == nil
    end

    @tag :ipc
    test "yields batches in insertion order for a multi-batch stream" do
      # Build a 3-batch IPC binary using the test fixture batch, then read it back.
      src = fixture_stream()
      {:ok, schema} = Stream.schema(src)
      batch = Stream.next(src)

      {:ok, multi_binary} = ExArrow.IPC.Writer.to_binary(schema, [batch, batch, batch])
      {:ok, stream} = IPC.Reader.from_binary(multi_binary)

      batches = drain(stream)
      assert length(batches) == 3
      assert Enum.all?(batches, &match?(%ExArrow.RecordBatch{}, &1))
    end

    @tag :ipc
    test "each batch from a multi-batch stream has the expected row count" do
      src = fixture_stream()
      {:ok, schema} = Stream.schema(src)
      batch = Stream.next(src)
      expected_rows = ExArrow.RecordBatch.num_rows(batch)

      {:ok, multi_binary} = ExArrow.IPC.Writer.to_binary(schema, [batch, batch])
      {:ok, stream} = IPC.Reader.from_binary(multi_binary)

      for b <- drain(stream) do
        assert ExArrow.RecordBatch.num_rows(b) == expected_rows
      end
    end

    test "raises ArgumentError for an invalid (non-NIF) resource" do
      stream = %Stream{resource: make_ref()}
      assert_raise ArgumentError, fn -> Stream.next(stream) end
    end
  end

  # ── backend :adbc ───────────────────────────────────────────────────────────

  describe "schema/1 (backend :adbc)" do
    test "raises ArgumentError for invalid resource when backend is :adbc" do
      stream = %Stream{resource: make_ref(), backend: :adbc}
      assert_raise ArgumentError, fn -> Stream.schema(stream) end
    end
  end

  describe "next/1 (backend :adbc)" do
    test "raises ArgumentError for invalid resource when backend is :adbc" do
      stream = %Stream{resource: make_ref(), backend: :adbc}
      assert_raise ArgumentError, fn -> Stream.next(stream) end
    end
  end

  describe "struct" do
    test "default backend is :ipc" do
      stream = %Stream{resource: make_ref()}
      assert stream.backend == :ipc
    end

    test "backend can be set to :adbc" do
      stream = %Stream{resource: make_ref(), backend: :adbc}
      assert stream.backend == :adbc
    end
  end
end
