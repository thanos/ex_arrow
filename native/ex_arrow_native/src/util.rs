//! Shared NIF encoding helpers used across all modules.

use rustler::{Encoder, Env, Term};

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
