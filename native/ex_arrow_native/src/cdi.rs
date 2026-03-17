//! Arrow C Data Interface (CDI) NIFs.
//!
//! Provides zero-copy transfer of RecordBatches via the Arrow C Data Interface
//! (https://arrow.apache.org/docs/format/CDataInterface.html).  A `CdiHandle`
//! wraps the raw `FFI_ArrowArray` and `FFI_ArrowSchema` C structs behind a
//! Rustler resource so their lifetimes are managed safely by the BEAM GC.
//!
//! ## Typical usage within ExArrow (round-trip test)
//!
//!     {:ok, handle} = ExArrow.CDI.export(batch)
//!     {:ok, batch2} = ExArrow.CDI.import(handle)
//!
//! ## Interop with an external CDI consumer (e.g. future Explorer CDI support)
//!
//!     {:ok, handle}      = ExArrow.CDI.export(batch)
//!     {schema_ptr, arr_ptr} = ExArrow.CDI.pointers(handle)
//!     # pass schema_ptr / arr_ptr as integer addresses to the external consumer
//!     # call ExArrow.CDI.mark_consumed(handle) so the GC skips the release call

use std::alloc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, StructArray};
use rustler::ResourceArc;
use rustler::{Encoder, Env, Term};

use crate::resources::ExArrowRecordBatch;
use crate::util::{err_encode, ok_encode};

// ── Resource ────────────────────────────────────────────────────────────────

/// Holds heap-allocated CDI structs via raw pointers stored as `AtomicUsize`.
///
/// Using `AtomicUsize` allows `cdi_import` to atomically "consume" the pointers
/// (swap to 0) so the `Drop` impl never double-frees or double-releases.
///
/// # Safety invariant
/// - Both pointers are non-null from construction until consumed.
/// - Once a pointer is swapped to 0 by `cdi_import` or `Drop`, it must not be
///   accessed again.
pub struct ExArrowCdiHandle {
    pub schema_ptr: AtomicUsize, // *mut FFI_ArrowSchema
    pub array_ptr: AtomicUsize,  // *mut FFI_ArrowArray
}

// SAFETY: Both pointed-to C structs are heap-allocated, exclusively owned by
// this handle, and access is serialised by the atomic swap pattern.
unsafe impl Send for ExArrowCdiHandle {}
unsafe impl Sync for ExArrowCdiHandle {}

#[rustler::resource_impl]
impl rustler::Resource for ExArrowCdiHandle {}

impl Drop for ExArrowCdiHandle {
    fn drop(&mut self) {
        let schema_raw = self.schema_ptr.swap(0, Ordering::SeqCst) as *mut FFI_ArrowSchema;
        let array_raw = self.array_ptr.swap(0, Ordering::SeqCst) as *mut FFI_ArrowArray;
        // FFI_ArrowArray/Schema::Drop calls release() if non-null, then frees private_data.
        // This is safe whether or not an external consumer already called release
        // (CDI spec requires the consumer to null out the release pointer first).
        unsafe {
            if !array_raw.is_null() {
                drop(Box::from_raw(array_raw));
            }
            if !schema_raw.is_null() {
                drop(Box::from_raw(schema_raw));
            }
        }
    }
}

// ── NIFs ─────────────────────────────────────────────────────────────────────

/// Export a `RecordBatch` as Arrow C Data Interface structs wrapped in a handle.
///
/// The conversion goes through a `StructArray` so the entire batch (all columns)
/// is represented as a single CDI array with nested children.
///
/// Returns `{:ok, handle_ref}` or `{:error, msg}`.
#[rustler::nif(schedule = "DirtyCpu")]
pub fn cdi_export<'a>(env: Env<'a>, batch: ResourceArc<ExArrowRecordBatch>) -> Term<'a> {
    let struct_array: StructArray = batch.batch.clone().into();
    let array_data = struct_array.into_data();

    let (ffi_array, ffi_schema) = match to_ffi(&array_data) {
        Ok(pair) => pair,
        Err(e) => return err_encode(env, &e.to_string()),
    };

    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as usize;
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as usize;

    let handle = ExArrowCdiHandle {
        schema_ptr: AtomicUsize::new(schema_ptr),
        array_ptr: AtomicUsize::new(array_ptr),
    };
    ok_encode(env, ResourceArc::new(handle))
}

/// Import a previously exported CDI handle back into a `RecordBatch`.
///
/// Atomically consumes the handle (sets both pointers to null), so the BEAM GC
/// will not attempt a second release when the handle is garbage-collected.
///
/// Returns `{:ok, batch_ref}` or `{:error, msg}`.
#[rustler::nif(schedule = "DirtyCpu")]
pub fn cdi_import<'a>(env: Env<'a>, handle: ResourceArc<ExArrowCdiHandle>) -> Term<'a> {
    let schema_raw = handle.schema_ptr.swap(0, Ordering::SeqCst) as *mut FFI_ArrowSchema;
    let array_raw = handle.array_ptr.swap(0, Ordering::SeqCst) as *mut FFI_ArrowArray;

    if array_raw.is_null() || schema_raw.is_null() {
        return err_encode(env, "CDI handle already consumed");
    }

    // SAFETY:
    // - Both pointers are non-null (checked above) and were allocated by Box::new in cdi_export.
    // - The atomic swap ensures exclusive ownership: no other thread/call can access them now.
    // - `take_raw` performs a bitwise copy of the value and frees the heap allocation WITHOUT
    //   calling T::drop, transferring logical ownership to the caller (from_ffi / drop below).
    let array_data = unsafe {
        let ffi_array: FFI_ArrowArray = take_raw(array_raw);
        let result = from_ffi(ffi_array, &*schema_raw);
        // Free the schema struct; FFI_ArrowSchema::drop calls schema release if non-null.
        drop(Box::from_raw(schema_raw));
        match result {
            Ok(data) => data,
            Err(e) => return err_encode(env, &e.to_string()),
        }
    };

    let struct_array = StructArray::from(array_data);
    let batch = arrow::record_batch::RecordBatch::from(&struct_array);
    ok_encode(env, ResourceArc::new(ExArrowRecordBatch { batch }))
}

/// Return the raw CDI pointer addresses as `{schema_ptr, array_ptr}` integers.
///
/// These can be passed to any CDI-compatible library (e.g. a future version of
/// Explorer that exposes `Explorer.DataFrame.from_arrow_cdi/2`).
///
/// **Important**: keep the handle alive (hold a reference) until the external
/// consumer has finished importing and called the CDI release callback.  After
/// external consumption, call `cdi_mark_consumed/1` to prevent double-release.
#[rustler::nif]
pub fn cdi_pointers(handle: ResourceArc<ExArrowCdiHandle>) -> (u64, u64) {
    (
        handle.schema_ptr.load(Ordering::SeqCst) as u64,
        handle.array_ptr.load(Ordering::SeqCst) as u64,
    )
}

/// Mark the handle as consumed by an external CDI consumer and free the
/// C-struct heap allocations.
///
/// Call this after the external consumer has finished importing the data and
/// has invoked the CDI `release` callback (which frees the underlying Arrow
/// buffers and, per the CDI spec, nulls the `release` function pointer).
///
/// **What this does:**
/// - Atomically swaps both struct pointers to 0 so the `Drop` impl becomes a
///   no-op, preventing any attempt to free already-freed memory.
/// - Drops the `Box<FFI_ArrowArray>` and `Box<FFI_ArrowSchema>` heap
///   allocations created in `cdi_export`.  `FFI_ArrowArray/Schema::drop`
///   checks whether `release` is non-null before invoking it; a conformant
///   consumer will have already nulled it, so the drops only reclaim the
///   struct allocation itself — no second release of the Arrow buffers.
///
/// **Do not** call this before the consumer's `release` callback has been
/// invoked.  If `release` is still non-null when the boxes are dropped, the
/// callback will fire again, which is a double-release of the Arrow buffers.
#[rustler::nif]
pub fn cdi_mark_consumed<'a>(env: Env<'a>, handle: ResourceArc<ExArrowCdiHandle>) -> Term<'a> {
    let schema_raw = handle.schema_ptr.swap(0, Ordering::SeqCst) as *mut FFI_ArrowSchema;
    let array_raw = handle.array_ptr.swap(0, Ordering::SeqCst) as *mut FFI_ArrowArray;

    // SAFETY: both pointers were allocated by Box::new in cdi_export and are
    // exclusively owned by this handle (the atomic swap ensures they are taken
    // exactly once).  FFI_ArrowArray/Schema::drop checks release != null before
    // invoking it; if the consumer has already called release (nulling the
    // pointer per the CDI spec) these drops only free the struct heap allocation.
    unsafe {
        if !array_raw.is_null() {
            drop(Box::from_raw(array_raw));
        }
        if !schema_raw.is_null() {
            drop(Box::from_raw(schema_raw));
        }
    }

    rustler::types::atom::Atom::from_str(env, "ok").unwrap().encode(env)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Perform a bitwise copy of `*ptr` and free the heap allocation WITHOUT calling
/// `T::drop`.  This effectively "moves" the value out of a raw heap pointer,
/// transferring ownership to the caller.
///
/// # Safety
/// - `ptr` must be a non-null pointer to a valid heap allocation of `T` created
///   by `Box::into_raw`.
/// - After this call `ptr` is dangling; do not use it again.
unsafe fn take_raw<T>(ptr: *mut T) -> T {
    let val = std::ptr::read(ptr);
    alloc::dealloc(ptr as *mut u8, alloc::Layout::new::<T>());
    val
}
