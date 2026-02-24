//! IPC stream read/write NIFs.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use rustler::resource::ResourceArc;
use rustler::{Encoder, Env, Term};

use crate::resources::{ExArrowIpcStream, ExArrowRecordBatch, ExArrowSchema};

fn ok_encode<'a, T: Encoder>(env: Env<'a>, t: T) -> Term<'a> {
    let ok = rustler::types::atom::Atom::from_str(env, "ok").unwrap();
    (ok, t).encode(env)
}

fn err_encode<'a>(env: Env<'a>, msg: &str) -> Term<'a> {
    let err = rustler::types::atom::Atom::from_str(env, "error").unwrap();
    (err, msg.to_string()).encode(env)
}

/// Builds a small IPC stream fixture (schema: id int64, name utf8; 2 rows) for tests.
#[rustler::nif]
fn ipc_test_fixture_binary<'a>(env: Env<'a>) -> Term<'a> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let id_array = Arc::new(Int64Array::from(vec![1_i64, 2]));
    let name_array = Arc::new(StringArray::from(vec!["a", "b"]));
    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, name_array]).unwrap();
    let mut buf = Vec::new();
    let mut writer = match StreamWriter::try_new(&mut buf, schema.as_ref()) {
        Ok(w) => w,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    if let Err(e) = writer.write(&batch) {
        return err_encode(env, &e.to_string());
    }
    if let Err(e) = writer.finish() {
        return err_encode(env, &e.to_string());
    }
    let mut owned = match rustler::OwnedBinary::new(buf.len()) {
        Some(b) => b,
        None => return err_encode(env, "binary alloc"),
    };
    owned.as_mut_slice().copy_from_slice(&buf);
    let binary = rustler::Binary::from_owned(owned, env);
    ok_encode(env, binary)
}

/// Read IPC stream from binary. Returns {:ok, stream_ref} or {:error, msg}.
#[rustler::nif]
fn ipc_reader_from_binary<'a>(env: Env<'a>, data: rustler::Binary) -> Term<'a> {
    let bytes = data.as_slice().to_vec();
    let cursor = Cursor::new(bytes);
    match StreamReader::try_new(cursor, None) {
        Ok(reader) => {
            let stream = ExArrowIpcStream {
                reader: std::sync::Mutex::new(reader),
            };
            ok_encode(env, ResourceArc::new(stream))
        }
        Err(e) => err_encode(env, &e.to_string()),
    }
}

/// Read IPC stream from file path. Returns {:ok, stream_ref} or {:error, msg}.
#[rustler::nif]
fn ipc_reader_from_file<'a>(env: Env<'a>, path: String) -> Term<'a> {
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let cursor = Cursor::new(bytes);
    let reader = match StreamReader::try_new(cursor, None) {
        Ok(r) => r,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let stream = ExArrowIpcStream {
        reader: std::sync::Mutex::new(reader),
    };
    ok_encode(env, ResourceArc::new(stream))
}

/// Return list of fields for a schema: [{name, type_atom}, ...]. type_atom is :int64, :float64, :utf8, :binary, :boolean, :null, etc.
#[rustler::nif]
fn schema_fields<'a>(env: Env<'a>, schema: ResourceArc<ExArrowSchema>) -> Term<'a> {
    let fields: Vec<Term> = schema
        .schema
        .fields()
        .iter()
        .map(|f| {
            (
                f.name().encode(env),
                data_type_to_atom(env, f.data_type()).encode(env),
            )
                .encode(env)
        })
        .collect();
    fields.encode(env)
}

fn data_type_to_atom(env: Env, dt: &arrow_schema::DataType) -> rustler::types::atom::Atom {
    let s = match dt {
        arrow_schema::DataType::Null => "null",
        arrow_schema::DataType::Boolean => "boolean",
        arrow_schema::DataType::Int64 => "int64",
        arrow_schema::DataType::Float64 => "float64",
        arrow_schema::DataType::Utf8 => "utf8",
        arrow_schema::DataType::LargeUtf8 => "large_utf8",
        arrow_schema::DataType::Binary => "binary",
        arrow_schema::DataType::LargeBinary => "large_binary",
        _ => "unknown",
    };
    rustler::types::atom::Atom::from_str(env, s).unwrap()
}

/// Return the schema ref of a record batch.
#[rustler::nif]
fn record_batch_schema(batch: ResourceArc<ExArrowRecordBatch>) -> rustler::resource::ResourceArc<ExArrowSchema> {
    let schema_handle = ExArrowSchema {
        schema: batch.batch.schema().clone(),
    };
    ResourceArc::new(schema_handle)
}

/// Return the number of rows in a record batch.
#[rustler::nif]
fn record_batch_num_rows(batch: ResourceArc<ExArrowRecordBatch>) -> i64 {
    batch.batch.num_rows() as i64
}

/// Return the schema of an IPC stream (without consuming it).
#[rustler::nif]
fn ipc_stream_schema<'a>(env: Env<'a>, stream: ResourceArc<ExArrowIpcStream>) -> Term<'a> {
    let guard = match stream.reader.lock() {
        Ok(g) => g,
        Err(_) => return err_encode(env, "stream lock"),
    };
    let schema_ref = guard.schema();
    let schema_handle = ExArrowSchema {
        schema: schema_ref,
    };
    ResourceArc::new(schema_handle).encode(env)
}

/// Read the next record batch from the stream. Returns {:ok, batch_ref} or :done or {:error, msg}.
#[rustler::nif]
fn ipc_stream_next<'a>(env: Env<'a>, stream: ResourceArc<ExArrowIpcStream>) -> Term<'a> {
    let mut guard = match stream.reader.lock() {
        Ok(g) => g,
        Err(_) => return err_encode(env, "stream lock"),
    };
    match guard.next() {
        None => rustler::types::atom::Atom::from_str(env, "done").unwrap().encode(env),
        Some(Err(e)) => err_encode(env, &e.to_string()),
        Some(Ok(batch)) => {
            let handle = ExArrowRecordBatch { batch };
            ok_encode(env, ResourceArc::new(handle))
        }
    }
}

/// Write schema and record batches to binary. Returns {:ok, binary} or {:error, msg}.
#[rustler::nif]
fn ipc_writer_to_binary<'a>(
    env: Env<'a>,
    schema: ResourceArc<ExArrowSchema>,
    batches: Vec<ResourceArc<ExArrowRecordBatch>>,
) -> Term<'a> {
    let mut buf = Vec::new();
    let mut writer = match StreamWriter::try_new(&mut buf, schema.schema.as_ref()) {
        Ok(w) => w,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    for batch_ref in &batches {
        if let Err(e) = writer.write(&batch_ref.batch) {
            return err_encode(env, &e.to_string());
        }
    }
    if let Err(e) = writer.finish() {
        return err_encode(env, &e.to_string());
    }
    let mut owned = match rustler::OwnedBinary::new(buf.len()) {
        Some(b) => b,
        None => return err_encode(env, "binary alloc"),
    };
    owned.as_mut_slice().copy_from_slice(&buf);
    let binary = rustler::Binary::from_owned(owned, env);
    ok_encode(env, binary)
}

/// Write schema and record batches to file. Returns :ok or {:error, msg}.
#[rustler::nif]
fn ipc_writer_to_file<'a>(
    env: Env<'a>,
    path: String,
    schema: ResourceArc<ExArrowSchema>,
    batches: Vec<ResourceArc<ExArrowRecordBatch>>,
) -> Term<'a> {
    let file = match std::fs::File::create(&path) {
        Ok(f) => f,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let mut writer = match StreamWriter::try_new(file, schema.schema.as_ref()) {
        Ok(w) => w,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    for batch_ref in &batches {
        if let Err(e) = writer.write(&batch_ref.batch) {
            return err_encode(env, &e.to_string());
        }
    }
    if let Err(e) = writer.finish() {
        return err_encode(env, &e.to_string());
    }
    rustler::types::atom::Atom::from_str(env, "ok").unwrap().encode(env)
}
