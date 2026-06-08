defmodule ExArrow.NxRoundtripTest do
  use ExUnit.Case, async: true

  @moduletag :nx

  if Code.ensure_loaded?(Nx) do
    describe "ExArrow.from_nx/1 and ExArrow.to_nx/1 — rank-1 round-trip" do
      for {dtype, label, values} <- [
            {{:u, 8}, "u8", [1, 0, 1, 255]},
            {{:s, 64}, "s64", [100, -200, 300, 0]},
            {{:f, 32}, "f32", [1.0, 2.0, 3.0]},
            {{:f, 64}, "f64", [1.5, 2.5, 3.5]},
            {{:u, 8}, "boolean_as_u8", [1, 0, 1, 0]}
          ] do
        test "round-trip preserves shape, dtype, values for #{label}" do
          tensor = Nx.tensor(unquote(values), type: unquote(dtype))
          {:ok, batch} = ExArrow.from_nx(tensor)
          {:ok, recovered} = ExArrow.to_nx(batch)
          assert Nx.shape(recovered) == Nx.shape(tensor)
          assert Nx.type(recovered) == unquote(dtype)
          assert Nx.to_list(recovered) == Nx.to_list(tensor)
        end
      end
    end

    describe "ExArrow.from_nx/1 — boolean tensor" do
      test "creates Arrow Boolean column with as: :boolean" do
        tensor = Nx.tensor([1, 0, 1, 0], type: {:u, 8})
        {:ok, batch} = ExArrow.from_nx(tensor, as: :boolean)
        schema = ExArrow.RecordBatch.schema(batch)
        [field] = ExArrow.Schema.fields(schema)
        assert field.type == :boolean
      end

      test "round-trip boolean column preserves values" do
        tensor = Nx.tensor([1, 0, 1, 0], type: {:u, 8})
        {:ok, batch} = ExArrow.from_nx(tensor, as: :boolean)
        {:ok, recovered} = ExArrow.to_nx(batch)
        assert Nx.type(recovered) == {:u, 8}
        assert Nx.to_list(recovered) == [1, 0, 1, 0]
      end

      test "as: :boolean fails for non-u8 tensor" do
        tensor = Nx.tensor([1, 2, 3], type: {:s, 64})
        assert {:error, msg} = ExArrow.from_nx(tensor, as: :boolean)
        assert msg =~ "as: :boolean"
      end
    end

    describe "ExArrow.from_nx/1 and ExArrow.to_nx/1 — rank-2 round-trip" do
      test "rank-2 tensor creates multi-column batch" do
        tensor = Nx.tensor([[1, 2, 3], [4, 5, 6]], type: {:s, 64})
        {:ok, batch} = ExArrow.from_nx(tensor)
        schema = ExArrow.RecordBatch.schema(batch)
        assert length(ExArrow.Schema.fields(schema)) == 3
        assert ExArrow.RecordBatch.num_rows(batch) == 2
      end

      test "rank-2 round-trip preserves shape, dtype, and values" do
        tensor = Nx.tensor([[1, 2, 3], [4, 5, 6]], type: {:s, 64})
        {:ok, batch} = ExArrow.from_nx(tensor)
        {:ok, recovered} = ExArrow.to_nx(batch)
        assert Nx.shape(recovered) == Nx.shape(tensor)
        assert Nx.type(recovered) == Nx.type(tensor)
        assert Nx.to_list(recovered) == Nx.to_list(tensor)
      end

      test "rank-2 f32 round-trip" do
        tensor = Nx.tensor([[1.0, 2.0], [3.0, 4.0]], type: {:f, 32})
        {:ok, batch} = ExArrow.from_nx(tensor)
        {:ok, recovered} = ExArrow.to_nx(batch)
        assert Nx.shape(recovered) == Nx.shape(tensor)
        assert Nx.type(recovered) == Nx.type(tensor)
        assert Nx.to_flat_list(recovered) |> Enum.map(&Nx.to_number/1) ==
                 Nx.to_flat_list(tensor) |> Enum.map(&Nx.to_number/1)
      end
    end

    describe "ExArrow.from_nx/1 — error cases" do
      test "returns error for unsupported dtype" do
        tensor = Nx.tensor([1, 2], type: {:bf, 16})
        assert {:error, msg} = ExArrow.from_nx(tensor)
        assert msg =~ "unsupported"
      end

      test "returns error for rank > 2" do
        tensor = Nx.tensor([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], type: {:s, 64})
        assert {:error, msg} = ExArrow.from_nx(tensor)
        assert msg =~ "rank"
      end
    end

    describe "ExArrow.to_nx/1 — multi-column batch to rank-2" do
      test "uniform numeric batch stacks into rank-2" do
        tensors = %{
          "a" => Nx.tensor([1, 2, 3], type: {:s, 64}),
          "b" => Nx.tensor([4, 5, 6], type: {:s, 64})
        }
        {:ok, batch} = ExArrow.Nx.from_tensors(tensors)
        {:ok, result} = ExArrow.to_nx(batch)
        assert tuple_size(Nx.shape(result)) == 2
      end

      test "mixed dtype batch returns error for to_nx" do
        tensors = %{
          "a" => Nx.tensor([1, 2, 3], type: {:s, 64}),
          "b" => Nx.tensor([1.0, 2.0, 3.0], type: {:f, 64})
        }
        {:ok, batch} = ExArrow.Nx.from_tensors(tensors)
        assert {:error, msg} = ExArrow.to_nx(batch)
        assert msg =~ "uniform"
      end

      test "single-column batch returns rank-1 tensor" do
        tensor = Nx.tensor([10, 20, 30], type: {:s, 64})
        {:ok, batch} = ExArrow.from_nx(tensor)
        {:ok, result} = ExArrow.to_nx(batch)
        assert Nx.shape(result) == {3}
      end
    end

    describe "ExArrow.to_nx/1 with non-numeric columns" do
      test "skips non-numeric columns and returns numeric ones" do
        {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
        {:ok, stream} = ExArrow.IPC.Reader.from_binary(ipc_bin)
        batch = ExArrow.Stream.next(stream)
        {:ok, tensor} = ExArrow.to_nx(batch)
        assert Nx.shape(tensor) == {2}
        assert Nx.type(tensor) == {:s, 64}
      end
    end
  else
    test "returns descriptive error when Nx is not loaded" do
      assert {:error, msg} = ExArrow.from_nx(:ignored)
      assert msg =~ "Nx"
    end
  end
end
