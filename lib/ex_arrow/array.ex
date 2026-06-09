defmodule ExArrow.Array do
  @moduledoc """
  Arrow array handle (opaque reference to native array).

  An Array is the leaf node of the Arrow hierarchy — a single column of typed,
  contiguous values.  Arrays are grouped into `ExArrow.RecordBatch` instances
  (one array per column), and batches are streamed via `ExArrow.Stream`.

  Data lives in native (Rust) memory.  This module provides a stable handle;
  copying to the BEAM heap is done only when explicitly requested through
  bridge modules such as `ExArrow.Nx.column_to_tensor/2`.

  ## Position in the hierarchy

      Schema ── Field (metadata: name, type, nullable)
                  │
      RecordBatch ── Array (one per column, shared row count)
                        │
      Table ── RecordBatch (one or more batches, shared schema)
                        │
      Stream ── RecordBatch (lazy sequence)

  The Array handle itself is currently opaque on the Elixir side.  Inspection
  functions (length, data type, null count) will be added as the NIF layer
  gains the corresponding entry points.
  """
  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc false
  @spec new(reference()) :: t()
  def new(resource), do: %__MODULE__{resource: resource}
end
