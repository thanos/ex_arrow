//! Shared NIF encoding helpers and safe static wrappers used across all modules.

use rustler::{Encoder, Env, Term};
use rustler::resource::ResourceType;

/// A newtype that makes `ResourceType<T>` safe to store in a `static`.
///
/// # Safety
///
/// `ResourceType<T>` wraps `*const ErlNifResourceType`, a raw pointer that the
/// BEAM hands us during `on_load`.  The BEAM guarantees:
///
/// 1. The pointer is valid for the entire process lifetime.
/// 2. After `on_load` returns, the value is effectively read-only — we never
///    mutate it again.
///
/// Under those two invariants concurrent reads from any number of scheduler
/// threads are safe, satisfying the requirements of both `Send` and `Sync`.
pub(crate) struct SyncResourceType<T>(pub(crate) ResourceType<T>);

// SAFETY: see struct-level doc comment.
unsafe impl<T> Send for SyncResourceType<T> {}
unsafe impl<T> Sync for SyncResourceType<T> {}

/// Wraps a value in an Elixir `{:ok, value}` tuple.
pub(crate) fn ok_encode<'a, T: Encoder>(env: Env<'a>, t: T) -> Term<'a> {
    let ok = rustler::types::atom::Atom::from_str(env, "ok").unwrap();
    (ok, t).encode(env)
}

/// Wraps a message string in an Elixir `{:error, message}` tuple.
pub(crate) fn err_encode<'a>(env: Env<'a>, msg: &str) -> Term<'a> {
    let err = rustler::types::atom::Atom::from_str(env, "error").unwrap();
    (err, msg.to_string()).encode(env)
}
