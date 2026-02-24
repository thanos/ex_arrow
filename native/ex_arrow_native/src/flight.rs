//! Arrow Flight NIFs: echo server (full Flight API) and client (do_put / do_get / Milestone 4 RPCs).

use std::io::Cursor;
use std::sync::{Arc, Mutex, OnceLock};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType as ArrowActionType, Criteria, Empty, FlightData, FlightDescriptor,
    FlightEndpoint, FlightInfo, PutResult, SchemaResult, Ticket,
};
use arrow_flight::{IpcMessage, SchemaAsIpc};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use futures::TryStreamExt;
use rustler::resource::ResourceArc;
use rustler::{Encoder, Env, Term};
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status, Streaming};
use tokio_stream::wrappers::TcpListenerStream;

use crate::util::{err_encode, ok_encode};
use crate::resources::{ExArrowIpcStream, ExArrowRecordBatch, ExArrowSchema, IpcStreamBacking};

// Pre-registered atoms used when encoding/decoding descriptor tuples.
rustler::atoms! {
    cmd,
    path,
}

/// Encode a `&[u8]` slice as an Elixir **binary** (not a charlist).
///
/// Rustler encodes `Vec<u8>` as a list of integers, which the BEAM may display
/// as a charlist when all bytes are printable ASCII.  Using `OwnedBinary` forces
/// the result to be a proper binary regardless of content.
///
/// Returns `None` if the BEAM allocator cannot fulfil the allocation.  Callers
/// must propagate the failure rather than panicking; on OOM the BEAM is already
/// in a bad state and returning `{:error, ...}` is the least harmful option.
fn try_encode_binary<'a>(env: Env<'a>, data: &[u8]) -> Option<Term<'a>> {
    let mut owned = rustler::OwnedBinary::new(data.len())?;
    owned.as_mut_slice().copy_from_slice(data);
    Some(owned.release(env).encode(env))
}

const ECHO_TICKET: &[u8] = b"echo";
const DESCRIPTOR_CMD: i32 = 2;
const DESCRIPTOR_PATH: i32 = 1;

// ── IPC schema encoding helper ───────────────────────────────────────────────

/// Serialise `schema` to the IPC FlatBuffer schema message format used in
/// `FlightInfo.schema` and `SchemaResult.schema`.
fn schema_ipc_bytes(schema: &SchemaRef) -> Result<Bytes, Status> {
    let msg: IpcMessage = SchemaAsIpc::new(schema, &IpcWriteOptions::default())
        .try_into()
        .map_err(|e| Status::internal(format!("schema IPC encode: {e}")))?;
    Ok(Bytes::from(msg.0))
}

// ── Descriptor codec helpers ─────────────────────────────────────────────────

/// Decode an Elixir descriptor term (`{:cmd, binary()}` or `{:path, [String.t()]}`)
/// into a `FlightDescriptor`.
fn decode_descriptor<'a>(term: Term<'a>) -> Result<FlightDescriptor, String> {
    let tuple = rustler::types::tuple::get_tuple(term)
        .map_err(|_| "descriptor must be a 2-tuple {:cmd, binary()} or {:path, [string]}".to_string())?;

    if tuple.len() != 2 {
        return Err("descriptor tuple must have exactly 2 elements".to_string());
    }

    let tag: rustler::Atom = tuple[0]
        .decode()
        .map_err(|_| "descriptor first element must be an atom".to_string())?;

    if tag == cmd() {
        let bin: rustler::Binary = tuple[1]
            .decode()
            .map_err(|_| "cmd descriptor: second element must be a binary".to_string())?;
        Ok(FlightDescriptor {
            r#type: DESCRIPTOR_CMD,
            cmd: Bytes::from(bin.as_slice().to_vec()),
            ..Default::default()
        })
    } else if tag == path() {
        let segments: Vec<String> = tuple[1]
            .decode()
            .map_err(|_| "path descriptor: second element must be a list of strings".to_string())?;
        Ok(FlightDescriptor {
            r#type: DESCRIPTOR_PATH,
            path: segments,
            ..Default::default()
        })
    } else {
        Err("descriptor tag must be :cmd or :path".to_string())
    }
}

/// Encode a `FlightDescriptor` as an Elixir term.
///
/// | Wire type | Elixir result             |
/// |-----------|---------------------------|
/// | CMD  (2)  | `{:cmd, binary()}`        |
/// | PATH (1)  | `{:path, [String.t()]}`   |
/// | None      | `:nil`                    |
/// | unknown   | `:nil` (not silently mis-encoded as PATH) |
///
/// Returns `None` if binary allocation fails for a CMD payload.
fn encode_descriptor<'a>(env: Env<'a>, desc: Option<&FlightDescriptor>) -> Option<Term<'a>> {
    let nil_term = || {
        rustler::types::atom::Atom::from_str(env, "nil")
            .unwrap()
            .encode(env)
    };
    match desc {
        None => Some(nil_term()),
        Some(d) if d.r#type == DESCRIPTOR_CMD => {
            let cmd_term = try_encode_binary(env, &d.cmd)?;
            Some(rustler::types::tuple::make_tuple(
                env,
                &[cmd().encode(env), cmd_term],
            ))
        }
        Some(d) if d.r#type == DESCRIPTOR_PATH => {
            Some((path(), d.path.clone()).encode(env))
        }
        Some(_) => {
            // Unknown/unsupported descriptor type returned by a non-echo server.
            // Emit :nil rather than silently mis-encoding the value as a PATH.
            Some(nil_term())
        }
    }
}

/// Encode a `FlightInfo` as the 5-tuple expected by `ExArrow.Flight.FlightInfo.from_native/1`.
///
/// Returns `None` if any BEAM binary allocation fails.  Callers must surface
/// the failure as `{:error, ...}` rather than propagating a partial term.
fn encode_flight_info<'a>(env: Env<'a>, info: &FlightInfo) -> Option<Term<'a>> {
    let schema_bytes = try_encode_binary(env, &info.schema)?;
    let descriptor = encode_descriptor(env, info.flight_descriptor.as_ref())?;
    let endpoints: Option<Vec<Term<'a>>> = info
        .endpoint
        .iter()
        .map(|ep| {
            let ticket_bytes: &[u8] = ep
                .ticket
                .as_ref()
                .map(|t| t.ticket.as_ref())
                .unwrap_or_default();
            let ticket_term = try_encode_binary(env, ticket_bytes)?;
            let locations: Vec<String> = ep.location.iter().map(|l| l.uri.clone()).collect();
            Some(rustler::types::tuple::make_tuple(
                env,
                &[ticket_term, locations.encode(env)],
            ))
        })
        .collect();
    let endpoints = endpoints?;
    Some(rustler::types::tuple::make_tuple(
        env,
        &[
            schema_bytes,
            descriptor,
            endpoints.encode(env),
            info.total_records.encode(env),
            info.total_bytes.encode(env),
        ],
    ))
}

// ── Echo server state and service ────────────────────────────────────────────

/// Shared state for the echo server: the last `do_put` (schema + batches).
struct EchoState {
    data: Option<(SchemaRef, Vec<RecordBatch>)>,
}

/// Echo Flight service: `do_put` stores data, `do_get` retrieves it.
#[derive(Clone)]
struct EchoFlightService {
    state: Arc<Mutex<EchoState>>,
}

#[tonic::async_trait]
impl FlightService for EchoFlightService {
    type HandshakeStream = BoxStream<'static, Result<arrow_flight::HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ArrowActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let guard = self.state.lock().map_err(|_| Status::internal("lock"))?;
        let info = match &guard.data {
            None => {
                let empty: Vec<Result<FlightInfo, Status>> = vec![];
                return Ok(Response::new(Box::pin(futures::stream::iter(empty))));
            }
            Some((schema, batches)) => {
                let schema_bytes = schema_ipc_bytes(schema)?;
                let total_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
                FlightInfo {
                    schema: schema_bytes,
                    flight_descriptor: Some(FlightDescriptor {
                        r#type: DESCRIPTOR_CMD,
                        cmd: Bytes::from(ECHO_TICKET),
                        ..Default::default()
                    }),
                    endpoint: vec![FlightEndpoint {
                        ticket: Some(Ticket {
                            ticket: Bytes::from(ECHO_TICKET),
                        }),
                        ..Default::default()
                    }],
                    total_records: total_rows,
                    total_bytes: -1,
                    ordered: false,
                    app_metadata: Bytes::new(),
                }
            }
        };
        Ok(Response::new(Box::pin(futures::stream::iter([Ok(info)]))))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let desc = request.into_inner();
        if desc.r#type != DESCRIPTOR_CMD || desc.cmd.as_ref() != ECHO_TICKET {
            return Err(Status::not_found("unknown descriptor"));
        }
        let guard = self.state.lock().map_err(|_| Status::internal("lock"))?;
        match &guard.data {
            None => Err(Status::not_found("no data: do_put first")),
            Some((schema, batches)) => {
                let schema_bytes = schema_ipc_bytes(schema)?;
                let total_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
                Ok(Response::new(FlightInfo {
                    schema: schema_bytes,
                    flight_descriptor: Some(FlightDescriptor {
                        r#type: DESCRIPTOR_CMD,
                        cmd: Bytes::from(ECHO_TICKET),
                        ..Default::default()
                    }),
                    endpoint: vec![FlightEndpoint {
                        ticket: Some(Ticket {
                            ticket: Bytes::from(ECHO_TICKET),
                        }),
                        ..Default::default()
                    }],
                    total_records: total_rows,
                    total_bytes: -1,
                    ordered: false,
                    app_metadata: Bytes::new(),
                }))
            }
        }
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let desc = request.into_inner();
        if desc.r#type != DESCRIPTOR_CMD || desc.cmd.as_ref() != ECHO_TICKET {
            return Err(Status::not_found("unknown descriptor"));
        }
        let guard = self.state.lock().map_err(|_| Status::internal("lock"))?;
        match &guard.data {
            None => Err(Status::not_found("no data: do_put first")),
            Some((schema, _)) => Ok(Response::new(SchemaResult {
                schema: schema_ipc_bytes(schema)?,
            })),
        }
    }

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner().ticket;
        if ticket.as_ref() != ECHO_TICKET {
            return Err(Status::not_found("unknown ticket"));
        }
        let (schema, batches) = {
            let guard = self.state.lock().map_err(|_| Status::internal("lock"))?;
            match &guard.data {
                Some((s, b)) => (s.clone(), b.clone()),
                None => return Err(Status::not_found("no data: do_put first")),
            }
        };
        let stream = futures::stream::iter(
            batches
                .into_iter()
                .map(Ok::<_, arrow_flight::error::FlightError>),
        );
        let flight_data = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream);
        let mapped = flight_data.map(|r| r.map_err(|e| Status::internal(e.to_string())));
        Ok(Response::new(Box::pin(mapped)))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let stream = request.into_inner();
        let batch_stream = FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|e| arrow_flight::error::FlightError::from(e)),
        );
        let batches: Vec<RecordBatch> = batch_stream
            .try_collect()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        // Flight do_put must carry at least one batch; an empty stream has no schema
        // and nothing useful to store, so we reject it here at the protocol boundary.
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| {
                Status::invalid_argument(
                    "do_put rejected: stream contained no record batches (schema cannot be inferred)",
                )
            })?;
        {
            let mut guard = self.state.lock().map_err(|_| Status::internal("lock"))?;
            guard.data = Some((schema, batches));
        }
        let result = PutResult::default();
        Ok(Response::new(Box::pin(futures::stream::iter([Ok(result)]))))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            "ping" => {
                let result = arrow_flight::Result {
                    body: Bytes::from("pong"),
                };
                Ok(Response::new(Box::pin(futures::stream::iter([Ok(result)]))))
            }
            "clear" => {
                let mut guard = self.state.lock().map_err(|_| Status::internal("lock"))?;
                guard.data = None;
                let empty: Vec<Result<arrow_flight::Result, Status>> = vec![];
                Ok(Response::new(Box::pin(futures::stream::iter(empty))))
            }
            other => Err(Status::not_found(format!("unknown action: {other}"))),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            Ok(ArrowActionType {
                r#type: "clear".to_string(),
                description: "Clear the stored echo data.".to_string(),
            }),
            Ok(ArrowActionType {
                r#type: "ping".to_string(),
                description: "Responds with 'pong'. Used to verify the server is alive."
                    .to_string(),
            }),
        ];
        Ok(Response::new(Box::pin(futures::stream::iter(actions))))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

// ── Server NIF handles ────────────────────────────────────────────────────────

/// Handle for the running server (join handle + port + shutdown sender).
pub struct FlightServerHandle {
    join: Mutex<Option<std::thread::JoinHandle<()>>>,
    host: String,
    port: u16,
    shutdown: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

/// Start Flight echo server on the given port (0 = any). Returns handle and actual port.
///
/// # Threading model
///
/// Each call to this function spawns **one dedicated OS thread** that owns a
/// single-server Tokio runtime for the lifetime of the returned handle. This
/// keeps the BEAM scheduler unblocked (the NIF is `DirtyIo`) and gives each
/// server complete isolation — a panic or slow handler in one server cannot
/// stall another.
///
/// The trade-off is that N concurrent servers consume N OS threads and N Tokio
/// thread-pools (each pool defaults to one worker thread per logical CPU). For
/// typical use — one server per node — this is negligible. If you need to run
/// many servers in the same OS process, consider sharing a single
/// `Arc<tokio::runtime::Runtime>` across handles (a future milestone). Until
/// then, keep the number of simultaneously live `FlightServerHandle` instances
/// small (single digits) to avoid thread exhaustion.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_server_start<'a>(env: Env<'a>, host: String, port: u16) -> Term<'a> {
    let state = Arc::new(Mutex::new(EchoState { data: None }));
    let service = EchoFlightService { state };
    let (tx, rx) = std::sync::mpsc::channel::<Result<(String, u16, tokio::sync::oneshot::Sender<()>), String>>();
    let join = std::thread::spawn(move || {
        let rt = match tokio::runtime::Runtime::new() {
            Ok(r) => r,
            Err(e) => {
                let _ = tx.send(Err(e.to_string()));
                return;
            }
        };
        rt.block_on(async move {
            let addr_str = format!("{}:{}", host, port);
            let addr: std::net::SocketAddr = match addr_str.parse() {
                Ok(a) => a,
                Err(e) => {
                    let _ = tx.send(Err(e.to_string()));
                    return;
                }
            };
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    let _ = tx.send(Err(e.to_string()));
                    return;
                }
            };
            let local = listener.local_addr().unwrap_or_else(|_| addr);
            let actual_port = local.port();
            let actual_host = local.ip().to_string();
            let incoming = TcpListenerStream::new(listener);
            let svc = FlightServiceServer::new(service);
            // Wire the shutdown signal into tonic's accept loop so that when
            // flight_server_stop fires shutdown_tx, the server stops accepting
            // new connections and waits for all in-flight RPCs to complete
            // before the future resolves.
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let server_fut = Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                });
            let server_handle = tokio::spawn(server_fut);
            // Probe the bound port until the server is actually accepting connections
            // rather than sleeping for a fixed interval (which is fragile on slow systems
            // and wasteful on fast ones). We poll up to 80 times with 25 ms back-off,
            // giving a maximum wait of 2 s before reporting a timeout.
            let probe_ip = if actual_host == "0.0.0.0" || actual_host == "::" {
                "127.0.0.1".to_string()
            } else {
                actual_host.clone()
            };
            let probe_addr = format!("{}:{}", probe_ip, actual_port);
            let mut ready = false;
            for _ in 0..80 {
                if tokio::net::TcpStream::connect(&probe_addr).await.is_ok() {
                    ready = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            if !ready {
                let _ = tx.send(Err(format!(
                    "server on {}:{} did not become ready within 2s",
                    actual_host, actual_port
                )));
                return;
            }
            let _ = tx.send(Ok((actual_host, actual_port, shutdown_tx)));
            let _ = server_handle.await;
        })
    });
    match rx.recv() {
        Ok(Ok((actual_host, actual_port, shutdown_tx))) => {
            let handle = FlightServerHandle {
                join: Mutex::new(Some(join)),
                host: actual_host,
                port: actual_port,
                shutdown: Mutex::new(Some(shutdown_tx)),
            };
            ok_encode(env, ResourceArc::new(handle))
        }
        Ok(Err(msg)) => err_encode(env, &msg),
        Err(_) => err_encode(env, "server failed to bind"),
    }
}

/// Return the port the server is listening on.
#[rustler::nif]
pub fn flight_server_port(handle: ResourceArc<FlightServerHandle>) -> u16 {
    handle.port
}

/// Return the host address the server is bound to (e.g. `"127.0.0.1"` or `"0.0.0.0"`).
#[rustler::nif]
pub fn flight_server_host(handle: ResourceArc<FlightServerHandle>) -> String {
    handle.host.clone()
}

/// Stop the Flight server (signals shutdown, then joins the server thread).
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_server_stop<'a>(env: Env<'a>, handle: ResourceArc<FlightServerHandle>) -> Term<'a> {
    if let Ok(mut shutdown_guard) = handle.shutdown.lock() {
        if let Some(shutdown_tx) = shutdown_guard.take() {
            let _ = shutdown_tx.send(());
        }
    }
    if let Ok(mut join_guard) = handle.join.lock() {
        if let Some(join) = join_guard.take() {
            let _ = join.join();
        }
    }
    rustler::types::atom::Atom::from_str(env, "ok").unwrap().encode(env)
}

// ── Shared client Tokio runtime ───────────────────────────────────────────────

/// A single multi-threaded Tokio runtime shared by **all** Flight client handles.
///
/// # Why shared?
///
/// `tokio::runtime::Runtime::new()` spawns one worker thread per logical CPU.
/// Creating a runtime per client would therefore multiply OS-thread usage by the
/// number of live clients, which can exhaust system resources when many clients
/// are created (e.g. in a connection pool or test suite).
///
/// A single shared runtime avoids that: all client calls across every client
/// handle are scheduled onto the same fixed-size thread pool.
/// The runtime lives for the process lifetime — it is never shut down — which
/// is intentional: dropping a `Runtime` while tasks are still running would
/// block until they complete, which is undesirable for a background executor.
static CLIENT_RUNTIME: OnceLock<Arc<tokio::runtime::Runtime>> = OnceLock::new();

fn client_runtime() -> Arc<tokio::runtime::Runtime> {
    CLIENT_RUNTIME
        .get_or_init(|| {
            Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .thread_name("ex-arrow-flight-client")
                    .enable_all()
                    .build()
                    .expect("failed to build Flight client Tokio runtime"),
            )
        })
        .clone()
}

/// Client handle wrapping Arrow Flight client and the runtime that owns the connection.
pub struct FlightClientHandle {
    rt: Arc<tokio::runtime::Runtime>,
    client: Mutex<arrow_flight::client::FlightClient>,
}

// ── Client NIFs ───────────────────────────────────────────────────────────────

/// Connect to a Flight server. Returns `{:ok, client_ref}` or `{:error, msg}`.
///
/// # Timeout
///
/// `connect_timeout_ms` sets the TCP connection timeout (0 = no limit).
///
/// # Security
///
/// Connections are **always plaintext HTTP/2** (no TLS). Only use this for
/// loopback / localhost endpoints or on a trusted private network.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_client_connect<'a>(
    env: Env<'a>,
    host: String,
    port: u16,
    connect_timeout_ms: u64,
) -> Term<'a> {
    // PLAINTEXT_ONLY: change this scheme to "https" and add TLS config when
    // TLS is implemented (requires tonic TLS feature + certificate handling).
    let endpoint_uri = format!("http://{}:{}", host, port);
    let endpoint = match Channel::from_shared(endpoint_uri) {
        Ok(c) => c,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let endpoint = if connect_timeout_ms > 0 {
        endpoint.connect_timeout(std::time::Duration::from_millis(connect_timeout_ms))
    } else {
        endpoint
    };
    let rt = client_runtime();
    let channel = match rt.block_on(endpoint.connect()) {
        Ok(ch) => ch,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let client = arrow_flight::client::FlightClient::new(channel);
    let handle = FlightClientHandle {
        rt,
        client: Mutex::new(client),
    };
    ok_encode(env, ResourceArc::new(handle))
}

/// do_put: upload schema and batches. Returns `:ok` or `{:error, msg}`.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_client_do_put<'a>(
    env: Env<'a>,
    client: ResourceArc<FlightClientHandle>,
    schema: ResourceArc<ExArrowSchema>,
    batches: Vec<ResourceArc<ExArrowRecordBatch>>,
) -> Term<'a> {
    let batches_owned: Vec<RecordBatch> = batches.iter().map(|b| b.batch.clone()).collect();
    let stream = futures::stream::iter(
        batches_owned
            .into_iter()
            .map(Ok::<_, arrow_flight::error::FlightError>),
    );
    let flight_data = FlightDataEncoderBuilder::new()
        .with_schema(schema.schema.clone())
        .build(stream);
    let mut guard = match client.client.lock() {
        Ok(g) => g,
        Err(_) => return err_encode(env, "client lock"),
    };
    match client.rt.block_on(guard.do_put(flight_data)) {
        Ok(mut result_stream) => {
            while client.rt.block_on(result_stream.next()).is_some() {}
            rustler::types::atom::Atom::from_str(env, "ok").unwrap().encode(env)
        }
        Err(e) => err_encode(env, &e.to_string()),
    }
}

/// do_get: fetch stream by ticket. Returns `{:ok, stream_ref}` or `{:error, msg}`.
///
/// # Memory model
///
/// Batches are received and written to the IPC buffer **one at a time**: each
/// decoded `RecordBatch` is dropped immediately after being serialised, so only
/// one batch and the growing IPC bytes need to be live simultaneously.
/// Peak memory is therefore `O(largest_batch + total_ipc_bytes)` rather than
/// `O(total_decoded_bytes + total_ipc_bytes)` as with a full `try_collect`.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_client_do_get<'a>(
    env: Env<'a>,
    client: ResourceArc<FlightClientHandle>,
    ticket_bytes: rustler::Binary,
) -> Term<'a> {
    let ticket = Ticket {
        ticket: Bytes::from(ticket_bytes.as_slice().to_vec()),
    };
    // Acquire the client lock only long enough to initiate the RPC; the
    // returned FlightRecordBatchStream is self-contained and can be driven
    // after the lock is released.
    let mut flight_stream = {
        let mut guard = match client.client.lock() {
            Ok(g) => g,
            Err(_) => return err_encode(env, "client lock"),
        };
        match client.rt.block_on(guard.do_get(ticket)) {
            Ok(s) => s,
            Err(e) => return err_encode(env, &e.to_string()),
        }
    };
    let first_batch = match client.rt.block_on(flight_stream.next()) {
        Some(Ok(b)) => b,
        Some(Err(e)) => return err_encode(env, &e.to_string()),
        None => {
            return err_encode(
                env,
                "do_get: server returned empty batch stream \
                 (internal invariant violated: do_put should have rejected this)",
            )
        }
    };
    let schema = first_batch.schema();
    let mut buf = Vec::new();
    let mut writer = match StreamWriter::try_new(&mut buf, schema.as_ref()) {
        Ok(w) => w,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    if let Err(e) = writer.write(&first_batch) {
        return err_encode(env, &e.to_string());
    }
    drop(first_batch);
    loop {
        match client.rt.block_on(flight_stream.next()) {
            Some(Ok(batch)) => {
                if let Err(e) = writer.write(&batch) {
                    return err_encode(env, &e.to_string());
                }
            }
            Some(Err(e)) => return err_encode(env, &e.to_string()),
            None => break,
        }
    }
    if let Err(e) = writer.finish() {
        return err_encode(env, &e.to_string());
    }
    let cursor = Cursor::new(buf);
    let reader = match StreamReader::try_new(cursor, None) {
        Ok(r) => r,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let ipc_stream = ExArrowIpcStream {
        reader: IpcStreamBacking::Binary(std::sync::Mutex::new(reader)),
    };
    ok_encode(env, ResourceArc::new(ipc_stream))
}

/// list_flights: enumerate available flights (filtered by `criteria_bytes`).
/// Returns `{:ok, [flight_info_tuple, ...]}` or `{:error, msg}`.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_client_list_flights<'a>(
    env: Env<'a>,
    client: ResourceArc<FlightClientHandle>,
    criteria_binary: rustler::Binary,
    timeout_ms: u64,
) -> Term<'a> {
    let criteria_bytes = criteria_binary.as_slice().to_vec();
    let flights = {
        let mut guard = match client.client.lock() {
            Ok(g) => g,
            Err(_) => return err_encode(env, "client lock poisoned"),
        };
        let collect_fut = async {
            let stream = guard
                .list_flights(criteria_bytes)
                .await
                .map_err(|e: arrow_flight::error::FlightError| e.to_string())?;
            stream
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e: arrow_flight::error::FlightError| e.to_string())
        };
        if timeout_ms > 0 {
            match client.rt.block_on(tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                collect_fut,
            )) {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => return err_encode(env, &e),
                Err(_) => return err_encode(env, "list_flights: request timed out"),
            }
        } else {
            match client.rt.block_on(collect_fut) {
                Ok(v) => v,
                Err(e) => return err_encode(env, &e),
            }
        }
    };
    let terms: Option<Vec<Term<'a>>> = flights
        .iter()
        .map(|f| encode_flight_info(env, f))
        .collect();
    match terms {
        Some(t) => ok_encode(env, t),
        None => err_encode(env, "binary allocation failed while encoding FlightInfo"),
    }
}

/// get_flight_info: metadata for a specific flight descriptor.
/// Returns `{:ok, flight_info_tuple}` or `{:error, msg}`.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_client_get_flight_info<'a>(
    env: Env<'a>,
    client: ResourceArc<FlightClientHandle>,
    descriptor: Term<'a>,
    timeout_ms: u64,
) -> Term<'a> {
    let flight_desc = match decode_descriptor(descriptor) {
        Ok(d) => d,
        Err(msg) => return err_encode(env, &msg),
    };
    let info = {
        let mut guard = match client.client.lock() {
            Ok(g) => g,
            Err(_) => return err_encode(env, "client lock poisoned"),
        };
        let fut = guard.get_flight_info(flight_desc);
        if timeout_ms > 0 {
            match client.rt.block_on(tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                fut,
            )) {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => return err_encode(env, &e.to_string()),
                Err(_) => return err_encode(env, "get_flight_info: request timed out"),
            }
        } else {
            match client.rt.block_on(fut) {
                Ok(v) => v,
                Err(e) => return err_encode(env, &e.to_string()),
            }
        }
    };
    match encode_flight_info(env, &info) {
        Some(t) => ok_encode(env, t),
        None => err_encode(env, "binary allocation failed while encoding FlightInfo"),
    }
}

/// get_schema: Arrow schema for the flight identified by `descriptor`.
/// Returns `{:ok, schema_ref}` or `{:error, msg}`.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_client_get_schema<'a>(
    env: Env<'a>,
    client: ResourceArc<FlightClientHandle>,
    descriptor: Term<'a>,
    timeout_ms: u64,
) -> Term<'a> {
    let flight_desc = match decode_descriptor(descriptor) {
        Ok(d) => d,
        Err(msg) => return err_encode(env, &msg),
    };
    let schema_ref = {
        let mut guard = match client.client.lock() {
            Ok(g) => g,
            Err(_) => return err_encode(env, "client lock poisoned"),
        };
        let fut = guard.get_schema(flight_desc);
        if timeout_ms > 0 {
            match client.rt.block_on(tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                fut,
            )) {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => return err_encode(env, &e.to_string()),
                Err(_) => return err_encode(env, "get_schema: request timed out"),
            }
        } else {
            match client.rt.block_on(fut) {
                Ok(v) => v,
                Err(e) => return err_encode(env, &e.to_string()),
            }
        }
    };
    ok_encode(env, ResourceArc::new(ExArrowSchema { schema: schema_ref.into() }))
}

/// list_actions: enumerate the action types supported by the server.
/// Returns `{:ok, [{type_string, description_string}, ...]}` or `{:error, msg}`.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_client_list_actions<'a>(
    env: Env<'a>,
    client: ResourceArc<FlightClientHandle>,
    timeout_ms: u64,
) -> Term<'a> {
    let action_types = {
        let mut guard = match client.client.lock() {
            Ok(g) => g,
            Err(_) => return err_encode(env, "client lock poisoned"),
        };
        let collect_fut = async {
            let stream = guard
                .list_actions()
                .await
                .map_err(|e: arrow_flight::error::FlightError| e.to_string())?;
            stream
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e: arrow_flight::error::FlightError| e.to_string())
        };
        if timeout_ms > 0 {
            match client.rt.block_on(tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                collect_fut,
            )) {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => return err_encode(env, &e),
                Err(_) => return err_encode(env, "list_actions: request timed out"),
            }
        } else {
            match client.rt.block_on(collect_fut) {
                Ok(v) => v,
                Err(e) => return err_encode(env, &e),
            }
        }
    };
    let terms: Vec<Term<'a>> = action_types
        .iter()
        .map(|at| (at.r#type.clone(), at.description.clone()).encode(env))
        .collect();
    ok_encode(env, terms)
}

/// do_action: execute a named action on the server with optional body.
/// Returns `{:ok, [result_binary, ...]}` or `{:error, msg}`.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_client_do_action<'a>(
    env: Env<'a>,
    client: ResourceArc<FlightClientHandle>,
    action_type: String,
    action_body: rustler::Binary,
    timeout_ms: u64,
) -> Term<'a> {
    let body_bytes = action_body.as_slice().to_vec();
    let action = Action {
        r#type: action_type,
        body: Bytes::from(body_bytes),
    };
    let results = {
        let mut guard = match client.client.lock() {
            Ok(g) => g,
            Err(_) => return err_encode(env, "client lock poisoned"),
        };
        // FlightDoActionStream yields Bytes (the body of each Result) directly.
        let collect_fut = async {
            let stream = guard
                .do_action(action)
                .await
                .map_err(|e: arrow_flight::error::FlightError| e.to_string())?;
            stream
                .try_collect::<Vec<Bytes>>()
                .await
                .map_err(|e: arrow_flight::error::FlightError| e.to_string())
        };
        if timeout_ms > 0 {
            match client.rt.block_on(tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                collect_fut,
            )) {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => return err_encode(env, &e),
                Err(_) => return err_encode(env, "do_action: request timed out"),
            }
        } else {
            match client.rt.block_on(collect_fut) {
                Ok(v) => v,
                Err(e) => return err_encode(env, &e),
            }
        }
    };
    let body_terms: Option<Vec<Term<'a>>> = results.iter().map(|r| try_encode_binary(env, r)).collect();
    match body_terms {
        Some(t) => ok_encode(env, t),
        None => err_encode(env, "binary allocation failed while encoding action results"),
    }
}

// ── Resource type registration ────────────────────────────────────────────────

use std::sync::OnceLock as ResourceOnceLock;
use rustler::resource::{open_struct_resource_type, ResourceType, ResourceTypeProvider, NIF_RESOURCE_FLAGS};
use crate::util::SyncResourceType;

static FLIGHT_SERVER_HANDLE_TYPE: ResourceOnceLock<SyncResourceType<FlightServerHandle>> =
    ResourceOnceLock::new();
static FLIGHT_CLIENT_HANDLE_TYPE: ResourceOnceLock<SyncResourceType<FlightClientHandle>> =
    ResourceOnceLock::new();

impl ResourceTypeProvider for FlightServerHandle {
    fn get_type() -> &'static ResourceType<Self> {
        &FLIGHT_SERVER_HANDLE_TYPE
            .get()
            .expect("FlightServerHandle not initialized")
            .0
    }
}

impl ResourceTypeProvider for FlightClientHandle {
    fn get_type() -> &'static ResourceType<Self> {
        &FLIGHT_CLIENT_HANDLE_TYPE
            .get()
            .expect("FlightClientHandle not initialized")
            .0
    }
}

pub fn flight_register_resources(env: rustler::Env) -> bool {
    let flags = NIF_RESOURCE_FLAGS::ERL_NIF_RT_CREATE;

    let Some(t) =
        open_struct_resource_type::<FlightServerHandle>(env, "ExArrowFlightServerHandle\0", flags)
    else {
        return false;
    };
    let _ = FLIGHT_SERVER_HANDLE_TYPE.set(SyncResourceType(t));

    let Some(t) =
        open_struct_resource_type::<FlightClientHandle>(env, "ExArrowFlightClientHandle\0", flags)
    else {
        return false;
    };
    let _ = FLIGHT_CLIENT_HANDLE_TYPE.set(SyncResourceType(t));

    true
}
