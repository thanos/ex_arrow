defmodule ExArrow.Compute do
  @moduledoc """
  Arrow compute kernels: filter, project (column selection), and sort.

  All operations run entirely in native memory.  Column buffers are never
  copied into BEAM terms — the result is a new `ExArrow.RecordBatch` handle
  that can be passed directly to IPC writers, Flight clients, or further
  compute operations.

  ## Examples

      # Keep only rows where `active` is true
      {:ok, mask}     = MyApp.build_bool_batch(active_flags)
      {:ok, filtered} = ExArrow.Compute.filter(batch, mask)

      # Select two columns
      {:ok, slim} = ExArrow.Compute.project(batch, ["id", "name"])

      # Sort by score descending
      {:ok, sorted} = ExArrow.Compute.sort(batch, "score", ascending: false)
  """

  alias ExArrow.Native
  alias ExArrow.RecordBatch

  @doc """
  Filter rows from `batch` using the first (boolean) column of `predicate_batch`.

  `predicate_batch` must have at least one column, and that column must be a
  boolean Arrow array.  Rows where the predicate is `true` are kept; rows where
  it is `false` or null are dropped.

  Returns `{:ok, filtered_batch}` or `{:error, message}`.
  """
  @spec filter(RecordBatch.t(), RecordBatch.t()) ::
          {:ok, RecordBatch.t()} | {:error, String.t()}
  def filter(%RecordBatch{resource: b}, %RecordBatch{resource: p}) do
    case Native.compute_filter(b, p) do
      {:ok, ref} -> {:ok, RecordBatch.from_ref(ref)}
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Project (select) a subset of columns from `batch` by name.

  Columns are returned in the order specified by `column_names`.  A name that
  does not exist in the batch returns `{:error, "column 'x' not found"}`.

  Returns `{:ok, projected_batch}` or `{:error, message}`.
  """
  @spec project(RecordBatch.t(), [String.t()]) ::
          {:ok, RecordBatch.t()} | {:error, String.t()}
  def project(%RecordBatch{resource: b}, column_names) when is_list(column_names) do
    case Native.compute_project(b, column_names) do
      {:ok, ref} -> {:ok, RecordBatch.from_ref(ref)}
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Sort `batch` by `column_name`.

  ## Options

    * `:ascending` — `true` (default) for ascending order, `false` for descending.

  Nulls are always placed first regardless of sort direction.

  Returns `{:ok, sorted_batch}` or `{:error, message}`.
  """
  @spec sort(RecordBatch.t(), String.t(), keyword()) ::
          {:ok, RecordBatch.t()} | {:error, String.t()}
  def sort(%RecordBatch{resource: b}, column_name, opts \\ []) when is_binary(column_name) do
    ascending = Keyword.get(opts, :ascending, true)

    case Native.compute_sort(b, column_name, ascending) do
      {:ok, ref} -> {:ok, RecordBatch.from_ref(ref)}
      {:error, msg} -> {:error, msg}
    end
  end
end
