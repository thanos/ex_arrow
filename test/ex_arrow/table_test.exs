defmodule ExArrow.TableTest do
  use ExUnit.Case, async: true

  alias ExArrow.IPC
  alias ExArrow.Stream
  alias ExArrow.Table

  @moduletag :ipc

  describe "from_batches/1" do
    test "creates a table from a list of batches" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
      batches = Stream.to_list(stream)
      {:ok, table} = Table.from_batches(batches)
      assert %Table{} = table
    end

    test "returns error for empty list" do
      assert {:error, msg} = Table.from_batches([])
      assert msg =~ "empty"
    end
  end

  describe "schema/1" do
    test "returns the schema from the first batch" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
      batches = Stream.to_list(stream)
      {:ok, table} = Table.from_batches(batches)
      schema = Table.schema(table)
      assert %ExArrow.Schema{} = schema
      assert ExArrow.Schema.field_names(schema) == ["id", "name"]
    end
  end

  describe "num_rows/1" do
    test "returns total row count across all batches" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
      batches = Stream.to_list(stream)
      {:ok, table} = Table.from_batches(batches)
      assert Table.num_rows(table) == 2
    end
  end

  describe "batches/1" do
    test "returns the list of batches" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
      batches = Stream.to_list(stream)
      {:ok, table} = Table.from_batches(batches)
      assert Table.batches(table) == batches
    end
  end
end
