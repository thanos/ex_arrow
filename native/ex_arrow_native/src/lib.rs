//! ExArrow NIFs: IPC stream read/write, Schema and RecordBatch handles.

mod ipc;
mod resources;

use rustler::Env;

use resources::{ExArrowIpcStream, ExArrowRecordBatch, ExArrowSchema};

#[rustler::nif]
fn nif_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

fn on_load(env: Env, _: rustler::Term) -> bool {
    rustler::resource!(ExArrowSchema, env);
    rustler::resource!(ExArrowRecordBatch, env);
    rustler::resource!(ExArrowIpcStream, env);
    true
}

rustler::init!(
    "Elixir.ExArrow.Native",
    [
        nif_version,
        ipc::ipc_test_fixture_binary,
        ipc::schema_fields,
        ipc::record_batch_schema,
        ipc::record_batch_num_rows,
        ipc::ipc_reader_from_binary,
        ipc::ipc_reader_from_file,
        ipc::ipc_stream_schema,
        ipc::ipc_stream_next,
        ipc::ipc_writer_to_binary,
        ipc::ipc_writer_to_file,
    ],
    load = on_load
);
