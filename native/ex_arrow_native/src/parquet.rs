//! Parquet NIFs: read and write Parquet files and in-memory binary blobs.
//!
//! Readers are **lazily** iterated: each call to `parquet_stream_next` reads
//! the next row-group from the underlying file/bytes without pre-loading the
//! entire file into memory.  This is ideal for large Parquet files where only a
//! subset of batches will be consumed.

use std::sync::{Mutex, OnceLock};

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use rustler::resource::{
    open_struct_resource_type, ResourceArc, ResourceTypeProvider, NIF_RESOURCE_FLAGS,
};
use rustler::{Encoder, Env, Term};

use crate::resources::{ExArrowRecordBatch, ExArrowSchema};
use crate::util::{err_encode, ok_encode, SyncResourceType};

// ── Resource ────────────────────────────────────────────────────────────────

/// Holds a lazy Parquet reader iterator; schema cached separately for
/// zero-cost `parquet_stream_schema` calls.
pub struct ExArrowParquetStream {
    pub schema: SchemaRef,
    pub reader: Mutex<Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>> + Send>>,
}

static EX_ARROW_PARQUET_STREAM_TYPE: OnceLock<SyncResourceType<ExArrowParquetStream>> =
    OnceLock::new();

impl ResourceTypeProvider for ExArrowParquetStream {
    fn get_type() -> &'static rustler::resource::ResourceType<Self> {
        &EX_ARROW_PARQUET_STREAM_TYPE
            .get()
            .expect("ExArrowParquetStream resource not initialised (on_load not run?)")
            .0
    }
}

pub fn parquet_register_resources(env: Env) -> bool {
    let flags = NIF_RESOURCE_FLAGS::ERL_NIF_RT_CREATE;
    let Some(t) =
        open_struct_resource_type::<ExArrowParquetStream>(env, "ExArrowParquetStream\0", flags)
    else {
        return false;
    };
    let _ = EX_ARROW_PARQUET_STREAM_TYPE.set(SyncResourceType(t));
    true
}

// ── Readers ─────────────────────────────────────────────────────────────────

/// Open a Parquet file for lazy row-group streaming.
/// Returns `{:ok, stream_ref}` or `{:error, msg}`.
#[rustler::nif]
pub fn parquet_reader_from_file<'a>(env: Env<'a>, path: String) -> Term<'a> {
    let file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    build_stream(env, builder)
}

/// Read Parquet data from an in-memory binary (lazy row-group streaming).
/// Returns `{:ok, stream_ref}` or `{:error, msg}`.
#[rustler::nif]
pub fn parquet_reader_from_binary<'a>(env: Env<'a>, binary: rustler::Binary) -> Term<'a> {
    let bytes = Bytes::copy_from_slice(binary.as_slice());
    let builder = match ParquetRecordBatchReaderBuilder::try_new(bytes) {
        Ok(b) => b,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    build_stream(env, builder)
}

fn build_stream<'a, T>(
    env: Env<'a>,
    builder: ParquetRecordBatchReaderBuilder<T>,
) -> Term<'a>
where
    T: parquet::file::reader::ChunkReader + 'static,
{
    let schema = builder.schema().clone();
    let reader = match builder.build() {
        Ok(r) => r,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    // Store the reader as a boxed trait object — row groups are read lazily on demand.
    let stream = ExArrowParquetStream {
        schema,
        reader: Mutex::new(Box::new(reader)),
    };
    ok_encode(env, ResourceArc::new(stream))
}

// ── Stream accessors ─────────────────────────────────────────────────────────

/// Return the schema of a Parquet stream (never errors; schema is always cached).
#[rustler::nif]
pub fn parquet_stream_schema<'a>(
    env: Env<'a>,
    stream: ResourceArc<ExArrowParquetStream>,
) -> Term<'a> {
    let handle = ExArrowSchema {
        schema: stream.schema.clone(),
    };
    ResourceArc::new(handle).encode(env)
}

/// Get the next record batch from the lazy reader.
/// Returns `:done`, `{:ok, batch_ref}`, or `{:error, msg}`.
#[rustler::nif]
pub fn parquet_stream_next<'a>(
    env: Env<'a>,
    stream: ResourceArc<ExArrowParquetStream>,
) -> Term<'a> {
    let mut guard = match stream.reader.lock() {
        Ok(g) => g,
        Err(_) => return err_encode(env, "parquet stream lock poisoned"),
    };
    match guard.next() {
        None => rustler::types::atom::Atom::from_str(env, "done")
            .unwrap()
            .encode(env),
        Some(Err(e)) => err_encode(env, &e.to_string()),
        Some(Ok(batch)) => ok_encode(env, ResourceArc::new(ExArrowRecordBatch { batch })),
    }
}

// ── Writers ──────────────────────────────────────────────────────────────────

/// Write `schema` and `batches` to a Parquet file at `path`.
/// Returns `:ok` or `{:error, msg}`.
#[rustler::nif]
pub fn parquet_writer_to_file<'a>(
    env: Env<'a>,
    path: String,
    schema: ResourceArc<ExArrowSchema>,
    batches: Vec<ResourceArc<ExArrowRecordBatch>>,
) -> Term<'a> {
    let file = match std::fs::File::create(&path) {
        Ok(f) => f,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let props = WriterProperties::builder().build();
    let mut writer = match ArrowWriter::try_new(file, schema.schema.clone(), Some(props)) {
        Ok(w) => w,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    for batch_ref in &batches {
        if let Err(e) = writer.write(&batch_ref.batch) {
            return err_encode(env, &e.to_string());
        }
    }
    match writer.close() {
        Ok(_) => rustler::types::atom::Atom::from_str(env, "ok")
            .unwrap()
            .encode(env),
        Err(e) => err_encode(env, &e.to_string()),
    }
}

/// Serialise `schema` and `batches` to a Parquet binary in memory.
/// Returns `{:ok, binary}` or `{:error, msg}`.
#[rustler::nif]
pub fn parquet_writer_to_binary<'a>(
    env: Env<'a>,
    schema: ResourceArc<ExArrowSchema>,
    batches: Vec<ResourceArc<ExArrowRecordBatch>>,
) -> Term<'a> {
    let mut buf: Vec<u8> = Vec::new();
    let props = WriterProperties::builder().build();
    {
        let mut writer = match ArrowWriter::try_new(&mut buf, schema.schema.clone(), Some(props)) {
            Ok(w) => w,
            Err(e) => return err_encode(env, &e.to_string()),
        };
        for batch_ref in &batches {
            if let Err(e) = writer.write(&batch_ref.batch) {
                return err_encode(env, &e.to_string());
            }
        }
        if let Err(e) = writer.close() {
            return err_encode(env, &e.to_string());
        }
    }
    let mut owned = match rustler::OwnedBinary::new(buf.len()) {
        Some(b) => b,
        None => return err_encode(env, "binary alloc"),
    };
    owned.as_mut_slice().copy_from_slice(&buf);
    let binary = rustler::Binary::from_owned(owned, env);
    ok_encode(env, binary)
}
