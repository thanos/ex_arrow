defmodule ExArrow.NxTest do
  use ExUnit.Case, async: true

  alias ExArrow.IPC
  alias ExArrow.Nx, as: ExArrowNx
  alias ExArrow.Stream

  @moduletag :nx

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

    describe "from_tensors/1" do
      test "creates a multi-column RecordBatch from a map of tensors" do
        tensors = %{
          "price" => Nx.tensor([1.5, 2.5, 3.5], type: {:f, 64}),
          "qty" => Nx.tensor([10, 20, 30], type: {:s, 32})
        }

        assert {:ok, batch} = ExArrowNx.from_tensors(tensors)
        assert ExArrow.RecordBatch.num_rows(batch) == 3
      end

      test "round-trips all numeric columns" do
        tensors = %{
          "a" => Nx.tensor([1, 2, 3], type: {:s, 64}),
          "b" => Nx.tensor([4.0, 5.0, 6.0], type: {:f, 32})
        }

        assert {:ok, batch} = ExArrowNx.from_tensors(tensors)
        assert {:ok, recovered} = ExArrowNx.to_tensors(batch)
        assert Nx.to_list(recovered["a"]) == [1, 2, 3]
        assert_in_delta Nx.to_number(recovered["b"][0]), 4.0, 0.001
      end

      test "respects column order (map key sort)" do
        tensors = %{
          "z" => Nx.tensor([9], type: {:u, 8}),
          "a" => Nx.tensor([1], type: {:u, 8})
        }

        assert {:ok, batch} = ExArrowNx.from_tensors(tensors)
        schema = ExArrow.RecordBatch.schema(batch)
        names = ExArrow.Schema.field_names(schema)
        # Map.to_list sorts by key
        assert names == ["a", "z"]
      end

      test "returns error when tensor sizes differ" do
        tensors = %{
          "x" => Nx.tensor([1, 2], type: {:s, 32}),
          "y" => Nx.tensor([1, 2, 3], type: {:s, 32})
        }

        assert {:error, msg} = ExArrowNx.from_tensors(tensors)
        assert msg =~ "same size"
      end

      test "returns error for unsupported dtype" do
        tensors = %{"x" => Nx.tensor([1], type: {:bf, 16})}
        assert {:error, msg} = ExArrowNx.from_tensors(tensors)
        assert msg =~ "unsupported"
      end

      test "returns error for empty map" do
        assert {:error, msg} = ExArrowNx.from_tensors(%{})
        assert msg =~ "at least one"
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

      # Round-trips for every remaining dtype — covers nx_dtype_to_arrow/1 AND
      # parse_nx_dtype/1 for each type in a single pass.
      for {nx_type, label, values} <- [
            {{:s, 8}, "s8", [1, 2, 3]},
            {{:s, 16}, "s16", [100, 200, 300]},
            {{:s, 32}, "s32", [1000, 2000, 3000]},
            {{:u, 8}, "u8", [1, 2, 255]},
            {{:u, 16}, "u16", [0, 1000, 65_535]},
            {{:u, 32}, "u32", [0, 1, 100_000]},
            {{:u, 64}, "u64", [0, 1, 999_999]},
            {{:f, 64}, "f64", [1.0, 2.0, 3.0]}
          ] do
        test "round-trip #{label}" do
          t = Nx.tensor(unquote(values), type: unquote(nx_type))
          assert {:ok, batch} = ExArrowNx.from_tensor(t, "col")
          assert {:ok, recovered} = ExArrowNx.column_to_tensor(batch, "col")
          assert Nx.type(recovered) == unquote(nx_type)
          assert Nx.size(recovered) == length(unquote(values))
        end
      end
    end

    describe "from_tensors/1 — edge cases" do
      test "returns error when a column name is not a string" do
        tensors = %{atom_key: Nx.tensor([1, 2], type: {:s, 32})}
        assert {:error, msg} = ExArrowNx.from_tensors(tensors)
        assert msg =~ "column name must be a string"
      end
    end
  else
    test "returns descriptive error when Nx is not loaded" do
      assert {:error, msg} = ExArrowNx.column_to_tensor(:ignored, "col")
      assert msg =~ "Nx"
    end
  end
end
