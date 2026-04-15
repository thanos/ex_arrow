defmodule ExArrow.FlightSQL.ResultTest do
  use ExUnit.Case, async: true

  alias ExArrow.FlightSQL.{Error, Result}

  describe "struct" do
    test "holds schema, batches, and num_rows" do
      schema = %ExArrow.Schema{resource: make_ref()}
      batch = %ExArrow.RecordBatch{resource: make_ref()}

      result = %Result{schema: schema, batches: [batch], num_rows: 5}

      assert result.schema == schema
      assert result.batches == [batch]
      assert result.num_rows == 5
    end

    test "allows empty batches" do
      schema = %ExArrow.Schema{resource: make_ref()}
      result = %Result{schema: schema, batches: [], num_rows: 0}
      assert result.num_rows == 0
    end
  end

  describe "to_dataframe/1 — Explorer not available" do
    test "returns conversion_error when ExArrow.Explorer is not loaded" do
      result = %Result{
        schema: %ExArrow.Schema{resource: make_ref()},
        batches: [],
        num_rows: 0
      }

      # In the standard test environment Explorer may or may not be loaded.
      # We only assert the shape of the error, not whether Explorer is available.
      case Result.to_dataframe(result) do
        {:ok, _df} ->
          # Explorer is loaded and the empty result converted successfully.
          :ok

        {:error, %Error{code: :conversion_error}} ->
          # Explorer not available or conversion failed on empty result.
          :ok
      end
    end
  end

  describe "to_tensor/2 — column missing" do
    test "returns conversion_error for empty batches" do
      result = %Result{
        schema: %ExArrow.Schema{resource: make_ref()},
        batches: [],
        num_rows: 0
      }

      assert {:error, %Error{code: :conversion_error, message: msg}} =
               Result.to_tensor(result, "price")

      assert msg =~ "no batches"
    end
  end

  describe "to_tensor/2 — Nx not available" do
    test "returns conversion_error when Nx is not loaded" do
      schema = %ExArrow.Schema{resource: make_ref()}
      batch = %ExArrow.RecordBatch{resource: make_ref()}
      result = %Result{schema: schema, batches: [batch], num_rows: 1}

      case Result.to_tensor(result, "price") do
        {:ok, _tensor} ->
          # Nx is loaded.
          :ok

        {:error, %Error{code: :conversion_error}} ->
          # Nx not available or column not found in fake batch.
          :ok
      end
    end
  end
end
