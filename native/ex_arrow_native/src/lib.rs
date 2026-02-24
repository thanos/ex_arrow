//! ExArrow NIFs: IPC stream read/write, Schema and RecordBatch handles.

mod ipc;
mod resources;

use rustler::resource::{open_struct_resource_type, ResourceType, ResourceTypeProvider, NIF_RESOURCE_FLAGS};
use rustler::Env;

use resources::{ExArrowIpcFile, ExArrowIpcStream, ExArrowRecordBatch, ExArrowSchema};

// Resource types: initialized once in on_load. Module-level impls avoid non-local impl warning from resource! macro.
static mut EX_ARROW_SCHEMA_TYPE: Option<ResourceType<ExArrowSchema>> = None;
static mut EX_ARROW_RECORD_BATCH_TYPE: Option<ResourceType<ExArrowRecordBatch>> = None;
static mut EX_ARROW_IPC_STREAM_TYPE: Option<ResourceType<ExArrowIpcStream>> = None;
static mut EX_ARROW_IPC_FILE_TYPE: Option<ResourceType<ExArrowIpcFile>> = None;

impl ResourceTypeProvider for ExArrowSchema {
    fn get_type() -> &'static ResourceType<Self> {
        #[allow(static_mut_refs)]
        unsafe { EX_ARROW_SCHEMA_TYPE.as_ref() }.expect("ExArrowSchema resource not initialized (on_load not run?)")
    }
}
impl ResourceTypeProvider for ExArrowRecordBatch {
    fn get_type() -> &'static ResourceType<Self> {
        #[allow(static_mut_refs)]
        unsafe { EX_ARROW_RECORD_BATCH_TYPE.as_ref() }.expect("ExArrowRecordBatch resource not initialized (on_load not run?)")
    }
}
impl ResourceTypeProvider for ExArrowIpcStream {
    fn get_type() -> &'static ResourceType<Self> {
        #[allow(static_mut_refs)]
        unsafe { EX_ARROW_IPC_STREAM_TYPE.as_ref() }.expect("ExArrowIpcStream resource not initialized (on_load not run?)")
    }
}
impl ResourceTypeProvider for ExArrowIpcFile {
    fn get_type() -> &'static ResourceType<Self> {
        #[allow(static_mut_refs)]
        unsafe { EX_ARROW_IPC_FILE_TYPE.as_ref() }.expect("ExArrowIpcFile resource not initialized (on_load not run?)")
    }
}

#[rustler::nif]
fn nif_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

fn on_load(env: Env, _: rustler::Term) -> bool {
    let flags = NIF_RESOURCE_FLAGS::ERL_NIF_RT_CREATE;

    if let Some(t) = open_struct_resource_type::<ExArrowSchema>(env, "ExArrowSchema\0", flags) {
        unsafe { EX_ARROW_SCHEMA_TYPE = Some(t); }
    } else {
        return false;
    }
    if let Some(t) = open_struct_resource_type::<ExArrowRecordBatch>(env, "ExArrowRecordBatch\0", flags) {
        unsafe { EX_ARROW_RECORD_BATCH_TYPE = Some(t); }
    } else {
        return false;
    }
    if let Some(t) = open_struct_resource_type::<ExArrowIpcStream>(env, "ExArrowIpcStream\0", flags) {
        unsafe { EX_ARROW_IPC_STREAM_TYPE = Some(t); }
    } else {
        return false;
    }
    if let Some(t) = open_struct_resource_type::<ExArrowIpcFile>(env, "ExArrowIpcFile\0", flags) {
        unsafe { EX_ARROW_IPC_FILE_TYPE = Some(t); }
    } else {
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
    ],
    load = on_load
);
