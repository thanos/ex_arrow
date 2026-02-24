//! Arrow Flight NIFs: minimal echo server and client (do_put / do_get).

use std::io::Cursor;
use std::sync::{Arc, Mutex, OnceLock};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{Empty, FlightData, PutResult, Ticket};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
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

const ECHO_TICKET: &[u8] = b"echo";

/// Shared state for echo server: last do_put (schema + batches).
struct EchoState {
    data: Option<(SchemaRef, Vec<RecordBatch>)>,
}

/// Echo Flight service: do_put stores under "echo", do_get returns it.
#[derive(Clone)]
struct EchoFlightService {
    state: Arc<Mutex<EchoState>>,
}

#[tonic::async_trait]
impl FlightService for EchoFlightService {
    type HandshakeStream = BoxStream<'static, Result<arrow_flight::HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<arrow_flight::FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<arrow_flight::ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<arrow_flight::Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<arrow_flight::FlightDescriptor>,
    ) -> Result<Response<arrow_flight::FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<arrow_flight::FlightDescriptor>,
    ) -> Result<Response<arrow_flight::SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
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
            .ok_or_else(|| Status::invalid_argument(
                "do_put rejected: stream contained no record batches (schema cannot be inferred)",
            ))?;
        {
            let mut guard = self.state.lock().map_err(|_| Status::internal("lock"))?;
            guard.data = Some((schema, batches));
        }
        let result = PutResult::default();
        Ok(Response::new(Box::pin(futures::stream::iter([Ok(result)]))))
    }

    async fn do_action(
        &self,
        _request: Request<arrow_flight::Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<arrow_flight::FlightDescriptor>,
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
            // before the future resolves.  Using a plain serve_with_incoming +
            // tokio::spawn would orphan the server task and cause abrupt
            // cancellation when the OS thread exits.
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
            //
            // Always probe via 127.0.0.1 regardless of the bind address: a server
            // bound to 0.0.0.0 also listens on loopback, and we can't connect to
            // 0.0.0.0 directly.  For specific non-loopback IPs the caller is
            // responsible for ensuring the address is reachable from this host.
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
            // Wait for the server task to finish (i.e. until shutdown_tx fires
            // and all in-flight requests drain).  This keeps the OS thread —
            // and therefore the Tokio runtime — alive for the server's lifetime.
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

/// Stop the Flight server (signals shutdown, then joins the server thread). Returns :ok or {:error, msg}.
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

/// A single multi-threaded Tokio runtime shared by **all** Flight client handles.
///
/// # Why shared?
///
/// `tokio::runtime::Runtime::new()` spawns one worker thread per logical CPU.
/// Creating a runtime per client would therefore multiply OS-thread usage by the
/// number of live clients, which can exhaust system resources when many clients
/// are created (e.g. in a connection pool or test suite).
///
/// A single shared runtime avoids that: all `do_put` / `do_get` calls across
/// every client handle are scheduled onto the same fixed-size thread pool.
/// The runtime lives for the process lifetime — it is never shut down — which
/// is intentional: dropping a `Runtime` while tasks are still running would
/// block until they complete, which is undesirable for a background executor.
///
/// Server handles are **not** eligible for runtime sharing because each server
/// runs an infinite `serve_with_incoming` loop that must be independently
/// cancellable; clients only need short-lived `block_on` calls.
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
/// The runtime must stay alive for the channel to remain connected.
pub struct FlightClientHandle {
    rt: Arc<tokio::runtime::Runtime>,
    client: Mutex<arrow_flight::client::FlightClient>,
}

/// Connect to a Flight server. Returns `{:ok, client_ref}` or `{:error, msg}`.
///
/// # Runtime
///
/// All client handles share a single global Tokio runtime (see `CLIENT_RUNTIME`).
/// No new thread-pool is created per connection.
///
/// # Security
///
/// Connections are **always plaintext HTTP/2** (no TLS). All Arrow schemas and
/// record batches travel unencrypted over the wire. Only use this for
/// loopback / localhost endpoints or on a trusted private network.
/// TLS support is deferred to a later milestone; the Elixir wrapper already
/// rejects `tls: true` with `{:error, :tls_not_supported}` to prevent callers
/// from inadvertently believing the connection is encrypted.
#[rustler::nif(schedule = "DirtyIo")]
pub fn flight_client_connect<'a>(env: Env<'a>, host: String, port: u16) -> Term<'a> {
    // PLAINTEXT_ONLY: change this scheme to "https" and add TLS config when
    // TLS is implemented (requires tonic TLS feature + certificate handling).
    const SCHEME: &str = "http";
    let endpoint = format!("{}://{}:{}", SCHEME, host, port);
    let channel = match Channel::from_shared(endpoint) {
        Ok(c) => c,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let rt = client_runtime();
    let channel = match rt.block_on(channel.connect()) {
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

/// do_put: upload schema and batches. Returns :ok or {:error, msg}.
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
            let ok_atom = rustler::types::atom::Atom::from_str(env, "ok").unwrap();
            ok_atom.encode(env)
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
///
/// The IPC buffer still accumulates the entire result before the returned stream
/// handle is readable.  True incremental streaming (returning a handle that pulls
/// from the network on demand, without buffering) is deferred to a later milestone.
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
    // Pull the first batch to obtain the schema for the IPC writer.
    // do_put rejects empty streams, so an empty response is an internal
    // invariant violation rather than a normal client error.
    let first_batch = match client.rt.block_on(flight_stream.next()) {
        Some(Ok(b)) => b,
        Some(Err(e)) => return err_encode(env, &e.to_string()),
        None => return err_encode(env, "do_get: server returned empty batch stream \
                                        (internal invariant violated: do_put should \
                                        have rejected this)"),
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
    // Drop the first batch before pulling the next — only the IPC bytes persist.
    drop(first_batch);
    // Stream remaining batches one at a time: each RecordBatch is written then
    // dropped before the next is fetched, keeping peak memory low.
    loop {
        match client.rt.block_on(flight_stream.next()) {
            Some(Ok(batch)) => {
                if let Err(e) = writer.write(&batch) {
                    return err_encode(env, &e.to_string());
                }
                // batch dropped here; only its IPC encoding remains in buf
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

// Resource type registration for Rustler (initialized in lib.rs on_load).
use std::sync::OnceLock as ResourceOnceLock;
use rustler::resource::{open_struct_resource_type, ResourceType, ResourceTypeProvider, NIF_RESOURCE_FLAGS};
use crate::util::SyncResourceType;

static FLIGHT_SERVER_HANDLE_TYPE: ResourceOnceLock<SyncResourceType<FlightServerHandle>> = ResourceOnceLock::new();
static FLIGHT_CLIENT_HANDLE_TYPE: ResourceOnceLock<SyncResourceType<FlightClientHandle>> = ResourceOnceLock::new();

impl ResourceTypeProvider for FlightServerHandle {
    fn get_type() -> &'static ResourceType<Self> {
        &FLIGHT_SERVER_HANDLE_TYPE.get().expect("FlightServerHandle not initialized").0
    }
}

impl ResourceTypeProvider for FlightClientHandle {
    fn get_type() -> &'static ResourceType<Self> {
        &FLIGHT_CLIENT_HANDLE_TYPE.get().expect("FlightClientHandle not initialized").0
    }
}

pub fn flight_register_resources(env: rustler::Env) -> bool {
    let flags = NIF_RESOURCE_FLAGS::ERL_NIF_RT_CREATE;

    let Some(t) = open_struct_resource_type::<FlightServerHandle>(env, "ExArrowFlightServerHandle\0", flags) else {
        return false;
    };
    let _ = FLIGHT_SERVER_HANDLE_TYPE.set(SyncResourceType(t));

    let Some(t) = open_struct_resource_type::<FlightClientHandle>(env, "ExArrowFlightClientHandle\0", flags) else {
        return false;
    };
    let _ = FLIGHT_CLIENT_HANDLE_TYPE.set(SyncResourceType(t));

    true
}
