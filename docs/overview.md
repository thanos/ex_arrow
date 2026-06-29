# Overview

The main overview, installation, quick start, and usage examples live in the
[README on GitHub](https://github.com/thanos/ex_arrow/blob/main/README.md).

## What's changed in v0.7.0

v0.7.0 adds Arrow-native streaming and pipeline infrastructure.  The unit of
execution is the Arrow `RecordBatch` — not `list(map())`, not
`Explorer.DataFrame`, not `Nx.Tensor`.  Explorer and Nx remain downstream
consumers; ExArrow is the Arrow layer.

New modules: `ExArrow.Stream` (constructors), `ExArrow.Batch` (transforms),
`ExArrow.Pipeline` (DSL), `ExArrow.Flow`, `ExArrow.GenStage.*Producer`,
`ExArrow.Broadway` (`BatchBuilder`, `ParquetSink`, `FlightSink`),
`ExArrow.Sink.*`, and `ExArrow.Telemetry`.  Optional deps added:
`:telemetry`, `:flow`, `:gen_stage`, `:broadway`.

New guides: [06 Arrow streams](06_arrow_streams.md),
[07 Arrow and Flow](07_arrow_and_flow.md),
[08 Arrow and GenStage](08_arrow_and_genstage.md),
[09 Arrow and Broadway](09_arrow_and_broadway.md),
[10 Arrow pipeline patterns](10_arrow_pipeline_patterns.md).

## Doc set

| Topic | Guide |
|-------|--------|
| Handles, copying, NIF scheduling | [Memory model](memory_model.md) |
| IPC stream and file read/write | [IPC guide](ipc_guide.md) |
| Arrow Flight client and server | [Flight guide](flight_guide.md) |
| Arrow Flight SQL remote query client | [Flight SQL guide](flight_sql_guide.md) |
| ADBC database connectivity | [ADBC guide](adbc_guide.md) |
| Parquet read and write | [Parquet guide](parquet_guide.md) |
| Compute kernels (filter, project, sort) | [Compute guide](compute_guide.md) |
| C Data Interface (CDI) | [CDI guide](cdi_guide.md) |
| Nx tensor bridge | [Nx guide](nx_guide.md) |
| Benchmarks and CI publishing | [Benchmarks guide](benchmarks.md) |

## Optional integrations

| Module | Requires | What it does |
|--------|----------|--------------|
| `ExArrow.FlightSQL.Client` | (none — built-in) | Connect to Arrow Flight SQL servers; query, stream, DML, prepared statements, metadata |
| `ExArrow.FlightSQL.Result` | (none — built-in) | Materialised Flight SQL result with `to_dataframe/1` and `to_tensor/2` |
| `ExArrow.Explorer` | `{:explorer, "~> 0.11"}` | Convert between `ExArrow.Stream`/`RecordBatch` and `Explorer.DataFrame` |
| `ExArrow.Nx` | `{:nx, "~> 0.9"}` | Convert numeric Arrow columns to/from `Nx.Tensor`; build multi-column batches from a tensor map |
| `ExArrow.CDI` | (none — built-in) | Zero-copy Arrow C Data Interface: export/import record batches as raw C struct pointers |
| `ExArrow.ADBC.ConnectionPool` | `{:nimble_pool, "~> 1.1"}` | NimblePool-backed connection pool for ADBC databases |
| `:adbc_package` backend | `{:adbc, "~> 0.9"}` + `{:explorer, "~> 0.11"}` | Supervised pure-Elixir ADBC backend; `Database.open(:adbc_package)` |
| ADBC driver download | `{:adbc, "~> 0.9"}` | `ExArrow.ADBC.DriverHelper.ensure_driver_and_open/2` |
| `ExArrow.Telemetry` | `{:telemetry, "~> 1.0"}` | Emit and observe telemetry events for every transport and pipeline operation |
| `ExArrow.Flow` | `{:flow, "~> 1.2"}` | Arrow-native Flow execution: `from_batches/1`, `map_batches/2`, `each_batch/2` |
| `ExArrow.GenStage.*Producer` | `{:gen_stage, "~> 1.2"}` | Demand-driven producers: `ParquetProducer`, `FlightProducer`, `ADBCProducer` |
| `ExArrow.Broadway` | `{:broadway, "~> 1.0"}` | Ingestion pipelines: `BatchBuilder`, `ParquetSink`, `FlightSink` |

API reference: `mix docs` or [Hex Docs](https://hexdocs.pm/ex_arrow).
