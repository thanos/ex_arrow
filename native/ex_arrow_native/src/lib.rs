//! Minimal NIF crate for ExArrow. Arrow IPC/Flight/ADBC will be added in later milestones.

rustler::init!("Elixir.ExArrow.Native", [nif_version]);

#[rustler::nif]
fn nif_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
