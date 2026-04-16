//! Flight SQL NIFs: connect, query, DML execute, metadata, and lazy stream iteration.
//!
//! All public NIF functions are prefixed `flight_sql_*` and scheduled on the
//! dirty-IO thread pool so they never block the BEAM scheduler.

use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};

use arrow::datatypes::Schema;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{CommandGetDbSchemas, CommandGetTables};
use arrow_flight::{FlightInfo, IpcMessage};
use arrow_schema::SchemaRef;
use futures::StreamExt;
use rustler::{Encoder, Env, ResourceArc, Term};
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

use crate::resources::{ExArrowRecordBatch, ExArrowSchema};
use crate::util::{err_encode, ok_encode};

// ── Atoms ─────────────────────────────────────────────────────────────────────

// All atoms used in this module declared in a single block to avoid conflicts.
rustler::atoms! {
    // Standard result atoms
    ok,
    error,
    // Error codes
    transport_error,
    server_error,
    unimplemented,
    unauthenticated,
    permission_denied,
    not_found,
    invalid_argument,
    protocol_error,
    multi_endpoint,
    unknown,
    done,
    // TLS mode atoms (same values as flight.rs — atoms are global BEAM atoms)
    plaintext,
    system_certs,
    custom_ca,
}

// ── Resource types ────────────────────────────────────────────────────────────

/// Opaque handle for an active Flight SQL connection.
pub struct FlightSqlClientHandle {
    pub rt: Arc<tokio::runtime::Runtime>,
    pub client: Mutex<FlightSqlServiceClient<Channel>>,
}

#[rustler::resource_impl]
impl rustler::Resource for FlightSqlClientHandle {}

/// Lazy record-batch stream returned by `flight_sql_query`.
///
/// The stream is pinned on the heap so it can be driven from multiple
/// NIF calls without requiring a stable stack address.
pub struct FlightSqlStreamResource {
    pub schema: SchemaRef,
    pub stream: Mutex<Pin<Box<FlightRecordBatchStream>>>,
}

#[rustler::resource_impl]
impl rustler::Resource for FlightSqlStreamResource {}

// ── Tokio runtime ─────────────────────────────────────────────────────────────

/// Shared Tokio runtime for all Flight SQL async operations.
///
/// A dedicated runtime keeps SQL traffic isolated from the existing
/// `CLIENT_RUNTIME` in `flight.rs`, avoiding head-of-line blocking when
/// both are used concurrently.
static FLIGHT_SQL_RUNTIME: OnceLock<Arc<tokio::runtime::Runtime>> = OnceLock::new();

fn sql_runtime() -> Arc<tokio::runtime::Runtime> {
    FLIGHT_SQL_RUNTIME
        .get_or_init(|| {
            Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("flight_sql tokio runtime"),
            )
        })
        .clone()
}

// ── TLS mode ──────────────────────────────────────────────────────────────────

enum TlsMode {
    Plaintext,
    SystemCerts,
    CustomCa(Vec<u8>),
}

fn parse_tls_mode<'a>(term: Term<'a>) -> Result<TlsMode, String> {
    if let Ok(atom) = term.decode::<rustler::Atom>() {
        if atom == plaintext() {
            return Ok(TlsMode::Plaintext);
        }
        if atom == system_certs() {
            return Ok(TlsMode::SystemCerts);
        }
        return Err(
            "unknown tls_mode atom; expected :plaintext or :system_certs".to_string(),
        );
    }

    let tuple = rustler::types::tuple::get_tuple(term).map_err(|_| {
        "tls_mode must be :plaintext, :system_certs, or {:custom_ca, pem_binary()}".to_string()
    })?;

    if tuple.len() == 2 {
        if let Ok(tag) = tuple[0].decode::<rustler::Atom>() {
            if tag == custom_ca() {
                let pem: rustler::Binary = tuple[1]
                    .decode()
                    .map_err(|_| "custom_ca payload must be a binary".to_string())?;
                return Ok(TlsMode::CustomCa(pem.as_slice().to_vec()));
            }
        }
    }

    Err(
        "invalid tls_mode; expected :plaintext, :system_certs, or {:custom_ca, binary()}"
            .to_string(),
    )
}

// ── Error encoding ────────────────────────────────────────────────────────────

/// Encode a Flight SQL error as `{:error, {code_atom, grpc_status_integer, message}}`.
///
/// `grpc_status` is 0 when the error is not a gRPC-level status error.
fn encode_sql_error<'a>(
    env: Env<'a>,
    code: rustler::Atom,
    grpc_status: i32,
    msg: &str,
) -> Term<'a> {
    (error(), (code, grpc_status, msg)).encode(env)
}

fn flight_error_to_term<'a>(env: Env<'a>, err: FlightError) -> Term<'a> {
    use tonic::Code;

    match &err {
        FlightError::Tonic(status) => {
            let (code, grpc_code) = match status.code() {
                // Transport / availability errors
                Code::Cancelled => (transport_error(), 1i32),
                Code::Unavailable => (transport_error(), 14i32),
                Code::DeadlineExceeded => (transport_error(), 4i32),
                // Auth errors
                Code::Unauthenticated => (unauthenticated(), 16i32),
                Code::PermissionDenied => (permission_denied(), 7i32),
                // Lookup / argument errors
                Code::NotFound => (not_found(), 5i32),
                Code::InvalidArgument => (invalid_argument(), 3i32),
                Code::OutOfRange => (invalid_argument(), 11i32),
                // Feature availability
                Code::Unimplemented => (unimplemented(), 12i32),
                // Server-side errors
                Code::Internal => (server_error(), 13i32),
                Code::Unknown => (server_error(), 2i32),
                Code::ResourceExhausted => (server_error(), 8i32),
                Code::FailedPrecondition => (server_error(), 9i32),
                Code::Aborted => (server_error(), 10i32),
                Code::AlreadyExists => (server_error(), 6i32),
                Code::DataLoss => (server_error(), 15i32),
                other => (server_error(), other as i32),
            };
            encode_sql_error(env, code, grpc_code, status.message())
        }
        FlightError::Arrow(inner) => {
            encode_sql_error(env, transport_error(), 0, &inner.to_string())
        }
        FlightError::DecodeError(msg) => {
            encode_sql_error(env, protocol_error(), 0, msg)
        }
        FlightError::ProtocolError(msg) => {
            encode_sql_error(env, protocol_error(), 0, msg)
        }
        other => encode_sql_error(env, transport_error(), 0, &other.to_string()),
    }
}

// ── Schema decode helper ──────────────────────────────────────────────────────

/// Decode the IPC-encoded schema bytes from a `FlightInfo` response.
///
/// Returns an empty schema when `bytes` is empty — some servers omit the
/// schema in `FlightInfo` and send it as the first `FlightData` message
/// instead.  The `FlightRecordBatchStream` decoder handles that path
/// transparently.
fn decode_flight_schema(bytes: bytes::Bytes) -> Result<SchemaRef, String> {
    if bytes.is_empty() {
        return Ok(Arc::new(Schema::empty()));
    }
    Schema::try_from(IpcMessage(bytes))
        .map(Arc::new)
        .map_err(|e| format!("schema decode: {e}"))
}

// ── NIFs ──────────────────────────────────────────────────────────────────────

/// Connect to a Flight SQL server.
///
/// Returns `{:ok, client_ref}` on success, `{:error, msg}` on failure.
///
/// `headers` is a list of `{name, value}` string pairs sent as gRPC metadata
/// on every request (use for bearer-token auth, custom headers, etc.).
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_sql_connect<'a>(
    env: Env<'a>,
    host: String,
    port: u16,
    tls_mode_term: Term<'a>,
    headers: Vec<(String, String)>,
) -> Term<'a> {
    let mode = match parse_tls_mode(tls_mode_term) {
        Ok(m) => m,
        Err(e) => return err_encode(env, &e),
    };

    let (scheme, tls_cfg) = match &mode {
        TlsMode::Plaintext => ("http", None),
        TlsMode::SystemCerts => ("https", Some(ClientTlsConfig::new().with_native_roots())),
        TlsMode::CustomCa(pem) => {
            let cert = Certificate::from_pem(pem);
            ("https", Some(ClientTlsConfig::new().ca_certificate(cert)))
        }
    };

    let uri = format!("{}://{}:{}", scheme, host, port);
    let endpoint = match Channel::from_shared(uri) {
        Ok(e) => e,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let endpoint = match tls_cfg {
        Some(cfg) => match endpoint.tls_config(cfg) {
            Ok(e) => e,
            Err(e) => return err_encode(env, &e.to_string()),
        },
        None => endpoint,
    };

    let rt = sql_runtime();
    let channel = match rt.block_on(endpoint.connect()) {
        Ok(ch) => ch,
        Err(e) => return err_encode(env, &e.to_string()),
    };

    let mut client = FlightSqlServiceClient::new(channel);
    for (k, v) in &headers {
        client.set_header(k.as_str(), v.as_str());
    }

    let handle = FlightSqlClientHandle {
        rt,
        client: Mutex::new(client),
    };
    ok_encode(env, ResourceArc::new(handle))
}

/// Execute a SQL query and return a lazy record-batch stream.
///
/// Internally performs `GetFlightInfo` (CommandStatementQuery) followed by
/// `DoGet` on the single returned endpoint.
///
/// Returns `{:ok, stream_ref}` or `{:error, {code, grpc_status, message}}`.
///
/// Returns `{:error, {multi_endpoint, 0, message}}` when `FlightInfo` contains
/// more than one endpoint — multi-endpoint distribution is not supported in v0.5.0.
///
/// **Concurrency note**: concurrent calls on the same client handle are serialised
/// by an internal Mutex.  `FlightSqlServiceClient::execute` requires exclusive
/// (`&mut self`) access, so queries cannot be pipelined through one handle.
/// Create separate client handles for concurrent query workloads.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_sql_query<'a>(
    env: Env<'a>,
    client_ref: ResourceArc<FlightSqlClientHandle>,
    sql: String,
) -> Term<'a> {
    let rt = client_ref.rt.clone();

    // Step 1: GetFlightInfo → FlightInfo
    let flight_info = {
        let mut guard = match client_ref.client.lock() {
            Ok(g) => g,
            Err(_) => return err_encode(env, "client lock poisoned"),
        };
        match rt.block_on(guard.execute(sql, None)) {
            Ok(info) => info,
            Err(e) => return flight_error_to_term(env, FlightError::Arrow(e)),
        }
    };

    // Step 2: Enforce single-endpoint constraint
    let endpoint_count = flight_info.endpoint.len();
    if endpoint_count != 1 {
        let msg = format!(
            "expected exactly 1 endpoint, got {}; \
             multi-endpoint distribution is not supported in v0.5.0",
            endpoint_count
        );
        return encode_sql_error(env, multi_endpoint(), 0, &msg);
    }

    // Step 3: Extract ticket
    let ticket = match flight_info.endpoint[0].ticket.clone() {
        Some(t) => t,
        None => {
            return encode_sql_error(
                env,
                protocol_error(),
                0,
                "FlightEndpoint has no ticket",
            )
        }
    };

    // Step 4: Decode schema from FlightInfo schema bytes
    let schema = match decode_flight_schema(flight_info.schema) {
        Ok(s) => s,
        Err(e) => return encode_sql_error(env, protocol_error(), 0, &e),
    };

    // Step 5: DoGet → FlightRecordBatchStream
    let stream = {
        let mut guard = match client_ref.client.lock() {
            Ok(g) => g,
            Err(_) => return err_encode(env, "client lock poisoned"),
        };
        match rt.block_on(guard.do_get(ticket)) {
            Ok(s) => s,
            Err(e) => return flight_error_to_term(env, FlightError::Arrow(e)),
        }
    };

    let resource = FlightSqlStreamResource {
        schema,
        stream: Mutex::new(Box::pin(stream)),
    };
    ok_encode(env, ResourceArc::new(resource))
}

/// Execute a DML statement (INSERT, UPDATE, DELETE, CREATE TABLE, etc.).
///
/// Returns `{:ok, n}` where `n` is the number of affected rows, or
/// `{:ok, :unknown}` when the server does not report a row count.
/// Returns `{:error, {code, grpc_status, message}}` on failure.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_sql_execute<'a>(
    env: Env<'a>,
    client_ref: ResourceArc<FlightSqlClientHandle>,
    sql: String,
) -> Term<'a> {
    let rt = client_ref.rt.clone();
    let mut guard = match client_ref.client.lock() {
        Ok(g) => g,
        Err(_) => return err_encode(env, "client lock poisoned"),
    };

    match rt.block_on(guard.execute_update(sql, None)) {
        Ok(n) if n < 0 => (ok(), unknown()).encode(env),
        Ok(n) => (ok(), n as u64).encode(env),
        Err(e) => flight_error_to_term(env, FlightError::Arrow(e)),
    }
}

/// Return the Arrow schema for an open Flight SQL stream.
///
/// The schema is decoded from the `FlightInfo` response at query time and
/// does not require reading any batches first.
///
/// Returns `{:ok, schema_ref}` or `{:error, msg}`.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_sql_stream_schema<'a>(
    env: Env<'a>,
    stream_ref: ResourceArc<FlightSqlStreamResource>,
) -> Term<'a> {
    let schema = ExArrowSchema {
        schema: stream_ref.schema.clone(),
    };
    ok_encode(env, ResourceArc::new(schema))
}

/// Read the next record batch from a Flight SQL stream.
///
/// Returns:
/// - `{:ok, batch_ref}` — the next batch (data stays in native memory).
/// - `:done` — the stream is exhausted.
/// - `{:error, {code, grpc_status, message}}` — a transport or server error.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_sql_stream_next<'a>(
    env: Env<'a>,
    stream_ref: ResourceArc<FlightSqlStreamResource>,
) -> Term<'a> {
    let rt = sql_runtime();
    let mut guard = match stream_ref.stream.lock() {
        Ok(g) => g,
        Err(_) => return err_encode(env, "stream lock poisoned"),
    };

    match rt.block_on(guard.next()) {
        None => done().encode(env),
        Some(Ok(batch)) => {
            let batch_ref = ExArrowRecordBatch { batch };
            ok_encode(env, ResourceArc::new(batch_ref))
        }
        Some(Err(e)) => flight_error_to_term(env, e),
    }
}

// ── Metadata helpers ──────────────────────────────────────────────────────────

/// Shared post-processing for any `FlightInfo`-returning metadata call.
///
/// Validates the single-endpoint constraint, extracts the ticket, decodes
/// the schema, issues `DoGet`, and wraps the result in a
/// `FlightSqlStreamResource`.  Returns an encoded Elixir term on both
/// success and failure so callers can return it directly.
fn flight_info_to_stream<'a>(
    env: Env<'a>,
    rt: &Arc<tokio::runtime::Runtime>,
    client_ref: &FlightSqlClientHandle,
    flight_info: FlightInfo,
) -> Term<'a> {
    // Enforce single-endpoint constraint (same as flight_sql_query).
    let endpoint_count = flight_info.endpoint.len();
    if endpoint_count != 1 {
        let msg = format!(
            "expected exactly 1 endpoint, got {}; \
             multi-endpoint distribution is not supported in v0.5.0",
            endpoint_count
        );
        return encode_sql_error(env, multi_endpoint(), 0, &msg);
    }

    let ticket = match flight_info.endpoint[0].ticket.clone() {
        Some(t) => t,
        None => {
            return encode_sql_error(
                env,
                protocol_error(),
                0,
                "FlightEndpoint has no ticket",
            )
        }
    };

    let schema = match decode_flight_schema(flight_info.schema) {
        Ok(s) => s,
        Err(e) => return encode_sql_error(env, protocol_error(), 0, &e),
    };

    let stream = {
        let mut guard = match client_ref.client.lock() {
            Ok(g) => g,
            Err(_) => return err_encode(env, "client lock poisoned"),
        };
        match rt.block_on(guard.do_get(ticket)) {
            Ok(s) => s,
            Err(e) => return flight_error_to_term(env, FlightError::Arrow(e)),
        }
    };

    let resource = FlightSqlStreamResource {
        schema,
        stream: Mutex::new(Box::pin(stream)),
    };
    ok_encode(env, ResourceArc::new(resource))
}

// ── Metadata NIFs ─────────────────────────────────────────────────────────────

/// List tables visible to the connected user.
///
/// All filter parameters are optional (`nil` means no filter).  `table_types`
/// is a list of type name strings, e.g. `["TABLE", "VIEW"]`; an empty list
/// means no type filter.  `include_schema` controls whether the IPC-encoded
/// column schema is embedded in the result batches.
///
/// Returns `{:ok, stream_ref}` or `{:error, {code, grpc_status, message}}`.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_sql_get_tables<'a>(
    env: Env<'a>,
    client_ref: ResourceArc<FlightSqlClientHandle>,
    catalog: Option<String>,
    db_schema_filter: Option<String>,
    table_name_filter: Option<String>,
    table_types: Vec<String>,
    include_schema: bool,
) -> Term<'a> {
    let rt = client_ref.rt.clone();

    let cmd = CommandGetTables {
        catalog,
        db_schema_filter_pattern: db_schema_filter,
        table_name_filter_pattern: table_name_filter,
        table_types,
        include_schema,
    };

    let flight_info = {
        let mut guard = match client_ref.client.lock() {
            Ok(g) => g,
            Err(_) => return err_encode(env, "client lock poisoned"),
        };
        match rt.block_on(guard.get_tables(cmd)) {
            Ok(info) => info,
            Err(e) => return flight_error_to_term(env, FlightError::Arrow(e)),
        }
    };

    flight_info_to_stream(env, &rt, &client_ref, flight_info)
}

/// List database schemas visible to the connected user.
///
/// Both filter parameters are optional (`nil` means no filter).
///
/// Returns `{:ok, stream_ref}` or `{:error, {code, grpc_status, message}}`.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_sql_get_db_schemas<'a>(
    env: Env<'a>,
    client_ref: ResourceArc<FlightSqlClientHandle>,
    catalog: Option<String>,
    db_schema_filter: Option<String>,
) -> Term<'a> {
    let rt = client_ref.rt.clone();

    let cmd = CommandGetDbSchemas {
        catalog,
        db_schema_filter_pattern: db_schema_filter,
    };

    let flight_info = {
        let mut guard = match client_ref.client.lock() {
            Ok(g) => g,
            Err(_) => return err_encode(env, "client lock poisoned"),
        };
        match rt.block_on(guard.get_db_schemas(cmd)) {
            Ok(info) => info,
            Err(e) => return flight_error_to_term(env, FlightError::Arrow(e)),
        }
    };

    flight_info_to_stream(env, &rt, &client_ref, flight_info)
}

/// Retrieve server capability and dialect information.
///
/// Returns all available `SqlInfo` entries.  The result is a lazy
/// record-batch stream; each batch encodes info codes and their values in
/// Arrow format as defined by the Flight SQL specification.
///
/// Returns `{:ok, stream_ref}` or `{:error, {code, grpc_status, message}}`.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_sql_get_sql_info<'a>(
    env: Env<'a>,
    client_ref: ResourceArc<FlightSqlClientHandle>,
) -> Term<'a> {
    let rt = client_ref.rt.clone();

    // Passing an empty Vec requests all available SQL info from the server.
    let flight_info = {
        let mut guard = match client_ref.client.lock() {
            Ok(g) => g,
            Err(_) => return err_encode(env, "client lock poisoned"),
        };
        match rt.block_on(guard.get_sql_info(vec![])) {
            Ok(info) => info,
            Err(e) => return flight_error_to_term(env, FlightError::Arrow(e)),
        }
    };

    flight_info_to_stream(env, &rt, &client_ref, flight_info)
}
