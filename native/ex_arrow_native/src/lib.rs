//! ExArrow NIFs: IPC stream read/write, Schema and RecordBatch handles, Flight client/server,
//! ADBC database connectivity, Arrow compute kernels, Parquet read/write, Nx column buffers,
//! and the Arrow C Data Interface (CDI).

mod adbc;
mod cdi;
mod compute;
mod flight;
mod ipc;
mod parquet;
mod resources;
mod util;

#[rustler::nif]
fn nif_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

rustler::init!("Elixir.ExArrow.Native");
