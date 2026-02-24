//! Resource types for ExArrow: Schema, RecordBatch, IPC stream.

use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;

use arrow_ipc::reader::StreamReader;

/// Opaque handle for an Arrow schema (held in native memory).
pub struct ExArrowSchema {
    pub schema: Arc<Schema>,
}

/// Opaque handle for an Arrow record batch.
pub struct ExArrowRecordBatch {
    pub batch: RecordBatch,
}

/// IPC stream reader: holds the StreamReader so we can call next() from Elixir.
pub struct ExArrowIpcStream {
    pub reader: Mutex<StreamReader<Cursor<Vec<u8>>>>,
}
