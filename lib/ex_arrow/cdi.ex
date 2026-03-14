defmodule ExArrow.CDI do
  @moduledoc """
  Arrow C Data Interface (CDI) bridge for ExArrow.

  The [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
  is a standardised C ABI for transferring Arrow data between runtimes **without
  serialisation or copying**.  Instead of converting to/from IPC bytes, a producer
  exports `ArrowSchema` and `ArrowArray` C structs via raw memory pointers; the
  consumer reads those pointers directly.

  ## Within ExArrow (round-trip)

  You can export a batch to a CDI handle and import it back:

      {:ok, handle} = ExArrow.CDI.export(batch)
      {:ok, batch2} = ExArrow.CDI.import(handle)

  The round-trip exercises the full CDI path and produces an independent copy of
  the batch with no intermediate IPC binary.

  ## Interop with an external CDI consumer

  Any CDI-compatible library running in the same BEAM process can receive the
  raw C struct pointers:

      {:ok, handle}              = ExArrow.CDI.export(batch)
      {schema_ptr, array_ptr}    = ExArrow.CDI.pointers(handle)
      # hand schema_ptr / array_ptr (as integers) to the consumer
      :ok = SomeLib.import_arrow_cdi(schema_ptr, array_ptr)
      # tell ExArrow the consumer owns the data now
      :ok = ExArrow.CDI.mark_consumed(handle)

  Keeping `handle` alive (in a variable) during the consumer's import ensures
  the C structs are not garbage-collected while the consumer is reading them.
  Once `mark_consumed/1` is called, the BEAM GC releases the handle without
  calling the Arrow release callback again.

  ## Explorer integration

  ExArrow's CDI layer provides the foundation for a future zero-copy Explorer
  bridge.  When Explorer exposes a CDI import API (e.g. a `from_arrow_cdi/2` function on
  `Explorer.DataFrame`), `ExArrow.Explorer` will automatically use it instead of
  the current IPC round-trip.
  """

  alias ExArrow.Native
  alias ExArrow.RecordBatch

  @opaque handle :: reference()

  @doc """
  Export a `RecordBatch` as Arrow C Data Interface structs.

  Returns `{:ok, handle}` where `handle` is an opaque resource that keeps the
  CDI structs alive until the handle is garbage-collected or `mark_consumed/1`
  is called.

  ## Example

      {:ok, handle} = ExArrow.CDI.export(batch)
  """
  @spec export(RecordBatch.t()) :: {:ok, handle()} | {:error, String.t()}
  def export(batch) do
    ref = RecordBatch.resource_ref(batch)

    case Native.cdi_export(ref) do
      {:ok, handle_ref} -> {:ok, handle_ref}
      {:error, _} = err -> err
    end
  end

  @doc """
  Import a CDI handle back into an `ExArrow.RecordBatch`.

  Atomically consumes the handle — subsequent calls on the same handle return
  `{:error, "CDI handle already consumed"}`.

  ## Example

      {:ok, handle} = ExArrow.CDI.export(batch)
      {:ok, batch2} = ExArrow.CDI.import(handle)
  """
  @spec import(handle()) :: {:ok, RecordBatch.t()} | {:error, String.t()}
  def import(handle) do
    case Native.cdi_import(handle) do
      {:ok, ref} -> {:ok, RecordBatch.from_ref(ref)}
      {:error, _} = err -> err
    end
  end

  @doc """
  Return the raw CDI pointer addresses as `{schema_ptr, array_ptr}`.

  Both values are `non_neg_integer()` (C `uintptr_t` cast to 64-bit).  Pass
  them to an external CDI consumer.  Call `mark_consumed/1` afterwards.

  ## Example

      {:ok, handle}           = ExArrow.CDI.export(batch)
      {schema_ptr, array_ptr} = ExArrow.CDI.pointers(handle)
  """
  @spec pointers(handle()) :: {non_neg_integer(), non_neg_integer()}
  def pointers(handle) do
    Native.cdi_pointers(handle)
  end

  @doc """
  Free the CDI struct allocations after an external consumer has finished.

  Call this **after** the external CDI consumer has called the Arrow `release`
  callback on the exported array (which frees the underlying Arrow buffer data
  and nulls the release pointer per the CDI spec).

  What happens internally:
  - Both C-struct pointers are atomically swapped to null so the BEAM GC's
    `Drop` becomes a no-op.
  - The `Box<FFI_ArrowArray>` and `Box<FFI_ArrowSchema>` heap allocations
    created by `export/1` are freed.  Because the consumer has already nulled
    the `release` pointer, dropping the boxes reclaims only the struct memory
    — no second invocation of the Arrow release callback occurs.

  > #### Warning {: .warning}
  > Do **not** call this before the consumer has invoked `release`.  If the
  > release pointer is still non-null when the boxes are dropped, the callback
  > fires again, causing a double-release of the Arrow buffer data.

  Returns `:ok`.
  """
  @spec mark_consumed(handle()) :: :ok
  def mark_consumed(handle) do
    Native.cdi_mark_consumed(handle)
  end
end
