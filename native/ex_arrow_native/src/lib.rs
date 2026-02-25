//! ExArrow NIFs: IPC stream read/write, Schema and RecordBatch handles, Flight client/server, ADBC.

mod adbc;
mod flight;
mod ipc;
mod resources;
mod util;

use std::sync::OnceLock;

use rustler::resource::{open_struct_resource_type, ResourceTypeProvider, NIF_RESOURCE_FLAGS};
use rustler::Env;

use resources::{ExArrowIpcFile, ExArrowIpcStream, ExArrowRecordBatch, ExArrowSchema};
use util::SyncResourceType;

// Resource types written once during on_load (before any NIF can be called) and
// read-only from that point on.  OnceLock<SyncResourceType<T>> gives us safe,
// lock-free access — see util::SyncResourceType for the safety argument.
static EX_ARROW_SCHEMA_TYPE: OnceLock<SyncResourceType<ExArrowSchema>> = OnceLock::new();
static EX_ARROW_RECORD_BATCH_TYPE: OnceLock<SyncResourceType<ExArrowRecordBatch>> = OnceLock::new();
static EX_ARROW_IPC_STREAM_TYPE: OnceLock<SyncResourceType<ExArrowIpcStream>> = OnceLock::new();
static EX_ARROW_IPC_FILE_TYPE: OnceLock<SyncResourceType<ExArrowIpcFile>> = OnceLock::new();

impl ResourceTypeProvider for ExArrowSchema {
    fn get_type() -> &'static rustler::resource::ResourceType<Self> {
        &EX_ARROW_SCHEMA_TYPE.get().expect("ExArrowSchema resource not initialized (on_load not run?)").0
    }
}
impl ResourceTypeProvider for ExArrowRecordBatch {
    fn get_type() -> &'static rustler::resource::ResourceType<Self> {
        &EX_ARROW_RECORD_BATCH_TYPE.get().expect("ExArrowRecordBatch resource not initialized (on_load not run?)").0
    }
}
impl ResourceTypeProvider for ExArrowIpcStream {
    fn get_type() -> &'static rustler::resource::ResourceType<Self> {
        &EX_ARROW_IPC_STREAM_TYPE.get().expect("ExArrowIpcStream resource not initialized (on_load not run?)").0
    }
}
impl ResourceTypeProvider for ExArrowIpcFile {
    fn get_type() -> &'static rustler::resource::ResourceType<Self> {
        &EX_ARROW_IPC_FILE_TYPE.get().expect("ExArrowIpcFile resource not initialized (on_load not run?)").0
    }
}

#[rustler::nif]
fn nif_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

fn on_load(env: Env, _: rustler::Term) -> bool {
    let flags = NIF_RESOURCE_FLAGS::ERL_NIF_RT_CREATE;

    let Some(t) = open_struct_resource_type::<ExArrowSchema>(env, "ExArrowSchema\0", flags) else {
        return false;
    };
    let _ = EX_ARROW_SCHEMA_TYPE.set(SyncResourceType(t));

    let Some(t) = open_struct_resource_type::<ExArrowRecordBatch>(env, "ExArrowRecordBatch\0", flags) else {
        return false;
    };
    let _ = EX_ARROW_RECORD_BATCH_TYPE.set(SyncResourceType(t));

    let Some(t) = open_struct_resource_type::<ExArrowIpcStream>(env, "ExArrowIpcStream\0", flags) else {
        return false;
    };
    let _ = EX_ARROW_IPC_STREAM_TYPE.set(SyncResourceType(t));

    let Some(t) = open_struct_resource_type::<ExArrowIpcFile>(env, "ExArrowIpcFile\0", flags) else {
        return false;
    };
    let _ = EX_ARROW_IPC_FILE_TYPE.set(SyncResourceType(t));

    if !flight::flight_register_resources(env) {
        return false;
    }
    if !adbc::adbc_register_resources(env) {
        return false;
    }
    true
}

rustler::init!(
    "Elixir.ExArrow.Native",
    [
        nif_version,
        ipc::ipc_test_fixture_binary,
        ipc::ipc_test_fixture_file_binary,
        ipc::schema_fields,
        ipc::record_batch_schema,
        ipc::record_batch_num_rows,
        ipc::ipc_reader_from_binary,
        ipc::ipc_reader_from_file,
        ipc::ipc_stream_schema,
        ipc::ipc_stream_next,
        ipc::ipc_file_open,
        ipc::ipc_file_open_from_binary,
        ipc::ipc_file_schema,
        ipc::ipc_file_num_batches,
        ipc::ipc_file_get_batch,
        ipc::ipc_writer_to_binary,
        ipc::ipc_writer_to_file,
        ipc::ipc_file_writer_to_file,
        flight::flight_server_start,
        flight::flight_server_port,
        flight::flight_server_host,
        flight::flight_server_stop,
        flight::flight_client_connect,
        flight::flight_client_do_put,
        flight::flight_client_do_get,
        flight::flight_client_list_flights,
        flight::flight_client_get_flight_info,
        flight::flight_client_get_schema,
        flight::flight_client_list_actions,
        flight::flight_client_do_action,
        adbc::adbc_database_open,
        adbc::adbc_connection_open,
        adbc::adbc_statement_new,
        adbc::adbc_statement_set_sql,
        adbc::adbc_statement_execute,
        adbc::adbc_stream_schema,
        adbc::adbc_stream_next,
    ],
    load = on_load
);
