defmodule ExArrow.NxTest do
  use ExUnit.Case, async: true

  @moduletag :nx

  alias ExArrow.IPC
  alias ExArrow.Nx, as: ExArrowNx
  alias ExArrow.Stream

  defp float64_batch do
    # Use the standard IPC fixture (id: int64, name: utf8)
    {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
    {:ok, stream} = IPC.Reader.from_binary(ipc_bin)
    Stream.next(stream)
  end

  if Code.ensure_loaded?(Nx) do
    describe "column_to_tensor/2" do
      test "extracts an int64 column as an Nx tensor" do
        batch = float64_batch()
        assert {:ok, tensor} = ExArrowNx.column_to_tensor(batch, "id")
        assert Nx.type(tensor) == {:s, 64}
        assert Nx.size(tensor) == 2
      end

      test "returns error for non-numeric column" do
        batch = float64_batch()
        assert {:error, msg} = ExArrowNx.column_to_tensor(batch, "name")
        assert msg =~ "unsupported"
      end

      test "returns error for unknown column" do
        batch = float64_batch()
        assert {:error, msg} = ExArrowNx.column_to_tensor(batch, "no_such_col")
        assert msg =~ "no_such_col"
      end
    end

    describe "to_tensors/1" do
      test "returns a map with only numeric columns" do
        batch = float64_batch()
        assert {:ok, tensors} = ExArrowNx.to_tensors(batch)
        # 'id' is int64 (numeric), 'name' is utf8 (skipped)
        assert Map.has_key?(tensors, "id")
        refute Map.has_key?(tensors, "name")
      end

      test "tensor values match original data" do
        batch = float64_batch()
        assert {:ok, tensors} = ExArrowNx.to_tensors(batch)
        ids = tensors["id"] |> Nx.to_list()
        assert ids == [1, 2]
      end
    end

    describe "from_tensor/2" do
      test "creates a single-column RecordBatch from a float64 tensor" do
        tensor = Nx.tensor([1.0, 2.0, 3.0], type: {:f, 64})
        assert {:ok, batch} = ExArrowNx.from_tensor(tensor, "values")
        assert ExArrow.RecordBatch.num_rows(batch) == 3
        schema = ExArrow.RecordBatch.schema(batch)
        assert ExArrow.Schema.field_names(schema) == ["values"]
      end

      test "round-trip: tensor -> batch -> tensor preserves values" do
        original = Nx.tensor([10, 20, 30], type: {:s, 64})
        assert {:ok, batch} = ExArrowNx.from_tensor(original, "nums")
        assert {:ok, rt_tensor} = ExArrowNx.column_to_tensor(batch, "nums")
        assert Nx.to_list(rt_tensor) == Nx.to_list(original)
      end

      test "returns error for unsupported Nx dtype" do
        tensor = Nx.tensor([1, 2], type: {:bf, 16})
        assert {:error, msg} = ExArrowNx.from_tensor(tensor, "col")
        assert msg =~ "unsupported"
      end
    end
  else
    test "returns descriptive error when Nx is not loaded" do
      assert {:error, msg} = ExArrowNx.column_to_tensor(:ignored, "col")
      assert msg =~ "Nx"
    end
  end
end
