//! ADBC NIFs: bind to adbc.h via adbc_driver_manager (Database, Connection, Statement, execute -> stream).

use std::sync::{Mutex, OnceLock};

use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_manager::{ManagedConnection, ManagedDatabase, ManagedDriver, ManagedStatement};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use rustler::resource::{open_struct_resource_type, ResourceType, ResourceTypeProvider, NIF_RESOURCE_FLAGS};
use rustler::resource::ResourceArc;
use rustler::{Encoder, Env, Term};

use crate::resources::{ExArrowRecordBatch, ExArrowSchema};
use crate::util::{err_encode, ok_encode, SyncResourceType};

rustler::atoms! {
    driver_path,
    driver_name,
    uri,
}

// ── Resources ───────────────────────────────────────────────────────────────

/// ADBC Database handle (owns driver + database; driver must outlive database).
pub struct AdbcDatabase {
    #[allow(dead_code)]
    driver: ManagedDriver,
    pub database: ManagedDatabase,
}

/// ADBC Connection handle (Mutex for new_statement(&mut self)).
pub struct AdbcConnection {
    pub connection: Mutex<ManagedConnection>,
}

/// ADBC Statement handle (Mutex for execute(&mut self)).
pub struct AdbcStatement {
    pub statement: Mutex<ManagedStatement>,
}

/// Result stream from Statement::execute: pre-collected batches + schema for schema/next.
pub struct AdbcResultStream {
    pub schema: SchemaRef,
    pub batches: Mutex<Vec<RecordBatch>>,
    pub index: Mutex<usize>,
}

// ── Database open: decode driver path or opts from Elixir ─────────────────────

fn decode_driver_spec<'a>(term: Term<'a>) -> Result<DriverSpec, String> {
    if let Ok(path) = term.decode::<String>() {
        return Ok(DriverSpec::Path(path));
    }
    let list: rustler::types::list::ListIterator = term.decode().map_err(|_| "opts must be a list")?;
    let mut driver_path_val = None;
    let mut driver_name_val = None;
    let mut uri_val = None;
    for item in list {
        let tuple = rustler::types::tuple::get_tuple(item).map_err(|_| "opt must be {key, value}")?;
        if tuple.len() != 2 {
            continue;
        }
        let key: rustler::Atom = tuple[0].decode().map_err(|_| "opt key must be atom")?;
        if key == driver_path() {
            driver_path_val = Some(tuple[1].decode::<String>().map_err(|_| "driver_path must be string")?);
        } else if key == driver_name() {
            driver_name_val = Some(tuple[1].decode::<String>().map_err(|_| "driver_name must be string")?);
        } else if key == uri() {
            uri_val = Some(tuple[1].decode::<String>().map_err(|_| "uri must be string")?);
        }
    }
    if let Some(p) = driver_path_val {
        Ok(DriverSpec::Path(p))
    } else if let Some(n) = driver_name_val {
        Ok(DriverSpec::Name {
            name: n,
            uri: uri_val,
        })
    } else {
        Err("opts must include driver_path or driver_name".to_string())
    }
}

enum DriverSpec {
    Path(String),
    /// name: driver library name; uri: only set when caller provides :uri (no default).
    Name {
        name: String,
        uri: Option<String>,
    },
}

/// Open a database: load driver from path or by name (env), init database.
/// Returns {:ok, database_ref} or {:error, msg}.
#[rustler::nif(schedule = "DirtyIo")]
pub fn adbc_database_open<'a>(env: Env<'a>, driver_path_or_opts: Term<'a>) -> Term<'a> {
    let spec = match decode_driver_spec(driver_path_or_opts) {
        Ok(s) => s,
        Err(e) => return err_encode(env, &e),
    };
    let version = AdbcVersion::V100;
    let (driver, database) = match spec {
        DriverSpec::Path(path) => {
            let mut d = match ManagedDriver::load_dynamic_from_filename(path, None, version) {
                Ok(d) => d,
                Err(e) => return err_encode(env, &e.to_string()),
            };
            let db = match d.new_database() {
                Ok(db) => db,
                Err(e) => return err_encode(env, &e.to_string()),
            };
            (d, db)
        }
        DriverSpec::Name { name, uri } => {
            let mut d = match ManagedDriver::load_dynamic_from_name(name.as_str(), None, version) {
                Ok(d) => d,
                Err(e) => return err_encode(env, &e.to_string()),
            };
            // Only set OptionDatabase::Uri when the caller provided :uri. We do not default to
            // ":memory:" or any other value, since that is driver-specific (e.g. SQLite) and
            // can cause surprising errors for other drivers.
            let db = match uri {
                Some(u) => {
                    let opts: Vec<(OptionDatabase, OptionValue)> =
                        vec![(OptionDatabase::Uri, OptionValue::String(u))];
                    match d.new_database_with_opts(opts) {
                        Ok(db) => db,
                        Err(e) => return err_encode(env, &e.to_string()),
                    }
                }
                None => match d.new_database() {
                    Ok(db) => db,
                    Err(e) => return err_encode(env, &e.to_string()),
                },
            };
            (d, db)
        }
    };
    let handle = AdbcDatabase { driver, database };
    ok_encode(env, ResourceArc::new(handle))
}

/// Open a connection from a database. Returns {:ok, connection_ref} or {:error, msg}.
#[rustler::nif(schedule = "DirtyIo")]
pub fn adbc_connection_open<'a>(
    env: Env<'a>,
    database: ResourceArc<AdbcDatabase>,
) -> Term<'a> {
    let conn = match database.database.new_connection() {
        Ok(c) => c,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let handle = AdbcConnection {
        connection: Mutex::new(conn),
    };
    ok_encode(env, ResourceArc::new(handle))
}

/// Create a new statement from a connection. Returns {:ok, statement_ref} or {:error, msg}.
#[rustler::nif(schedule = "DirtyIo")]
pub fn adbc_statement_new<'a>(
    env: Env<'a>,
    connection: ResourceArc<AdbcConnection>,
) -> Term<'a> {
    let mut guard = match connection.connection.lock() {
        Ok(g) => g,
        Err(_) => return err_encode(env, "connection lock"),
    };
    let stmt = match guard.new_statement() {
        Ok(s) => s,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let handle = AdbcStatement {
        statement: Mutex::new(stmt),
    };
    ok_encode(env, ResourceArc::new(handle))
}

/// Set the SQL query on a statement. Returns :ok or {:error, msg}.
#[rustler::nif(schedule = "DirtyIo")]
pub fn adbc_statement_set_sql<'a>(
    env: Env<'a>,
    statement: ResourceArc<AdbcStatement>,
    sql: String,
) -> Term<'a> {
    let mut guard = match statement.statement.lock() {
        Ok(g) => g,
        Err(_) => return err_encode(env, "statement lock"),
    };
    match guard.set_sql_query(sql.as_str()) {
        Ok(()) => rustler::types::atom::Atom::from_str(env, "ok").unwrap().encode(env),
        Err(e) => err_encode(env, &e.to_string()),
    }
}

/// Execute the statement and return a stream of record batches. Returns {:ok, stream_ref} or {:error, msg}.
#[rustler::nif(schedule = "DirtyIo")]
pub fn adbc_statement_execute<'a>(
    env: Env<'a>,
    statement: ResourceArc<AdbcStatement>,
) -> Term<'a> {
    let mut guard = match statement.statement.lock() {
        Ok(g) => g,
        Err(_) => return err_encode(env, "statement lock"),
    };
    let reader = match guard.execute() {
        Ok(r) => r,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = match reader.collect() {
        Ok(b) => b,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let stream = AdbcResultStream {
        schema,
        batches: Mutex::new(batches),
        index: Mutex::new(0),
    };
    ok_encode(env, ResourceArc::new(stream))
}

/// Return the schema of an ADBC result stream.
#[rustler::nif]
pub fn adbc_stream_schema<'a>(env: Env<'a>, stream: ResourceArc<AdbcResultStream>) -> Term<'a> {
    let schema_handle = ExArrowSchema {
        schema: stream.schema.clone(),
    };
    ResourceArc::new(schema_handle).encode(env)
}

/// Return the next record batch from an ADBC result stream. Returns {:ok, batch_ref}, :done, or {:error, msg}.
#[rustler::nif]
pub fn adbc_stream_next<'a>(env: Env<'a>, stream: ResourceArc<AdbcResultStream>) -> Term<'a> {
    let batches_guard = match stream.batches.lock() {
        Ok(g) => g,
        Err(_) => return err_encode(env, "stream lock"),
    };
    let len = batches_guard.len();
    drop(batches_guard);
    let mut index_guard = match stream.index.lock() {
        Ok(g) => g,
        Err(_) => return err_encode(env, "stream index lock"),
    };
    if *index_guard >= len {
        return rustler::types::atom::Atom::from_str(env, "done").unwrap().encode(env);
    }
    let i = *index_guard;
    *index_guard += 1;
    drop(index_guard);
    let batches_guard = match stream.batches.lock() {
        Ok(g) => g,
        Err(_) => return err_encode(env, "stream lock"),
    };
    let batch = batches_guard[i].clone();
    drop(batches_guard);
    let handle = ExArrowRecordBatch { batch };
    ok_encode(env, ResourceArc::new(handle))
}

// ── Resource type registration ────────────────────────────────────────────────

static ADBC_DATABASE_TYPE: OnceLock<SyncResourceType<AdbcDatabase>> = OnceLock::new();
static ADBC_CONNECTION_TYPE: OnceLock<SyncResourceType<AdbcConnection>> = OnceLock::new();
static ADBC_STATEMENT_TYPE: OnceLock<SyncResourceType<AdbcStatement>> = OnceLock::new();
static ADBC_RESULT_STREAM_TYPE: OnceLock<SyncResourceType<AdbcResultStream>> = OnceLock::new();

impl ResourceTypeProvider for AdbcDatabase {
    fn get_type() -> &'static ResourceType<Self> {
        &ADBC_DATABASE_TYPE.get().expect("AdbcDatabase not initialized").0
    }
}
impl ResourceTypeProvider for AdbcConnection {
    fn get_type() -> &'static ResourceType<Self> {
        &ADBC_CONNECTION_TYPE.get().expect("AdbcConnection not initialized").0
    }
}
impl ResourceTypeProvider for AdbcStatement {
    fn get_type() -> &'static ResourceType<Self> {
        &ADBC_STATEMENT_TYPE.get().expect("AdbcStatement not initialized").0
    }
}
impl ResourceTypeProvider for AdbcResultStream {
    fn get_type() -> &'static ResourceType<Self> {
        &ADBC_RESULT_STREAM_TYPE.get().expect("AdbcResultStream not initialized").0
    }
}

pub fn adbc_register_resources(env: rustler::Env) -> bool {
    let flags = NIF_RESOURCE_FLAGS::ERL_NIF_RT_CREATE;
    let Some(t) = open_struct_resource_type::<AdbcDatabase>(env, "AdbcDatabase\0", flags) else {
        return false;
    };
    let _ = ADBC_DATABASE_TYPE.set(SyncResourceType(t));
    let Some(t) = open_struct_resource_type::<AdbcConnection>(env, "AdbcConnection\0", flags) else {
        return false;
    };
    let _ = ADBC_CONNECTION_TYPE.set(SyncResourceType(t));
    let Some(t) = open_struct_resource_type::<AdbcStatement>(env, "AdbcStatement\0", flags) else {
        return false;
    };
    let _ = ADBC_STATEMENT_TYPE.set(SyncResourceType(t));
    let Some(t) = open_struct_resource_type::<AdbcResultStream>(env, "AdbcResultStream\0", flags) else {
        return false;
    };
    let _ = ADBC_RESULT_STREAM_TYPE.set(SyncResourceType(t));
    true
}
