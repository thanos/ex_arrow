# The Arrow Ecosystem in Elixir

ExArrow is one piece of a larger Arrow-based data ecosystem.  This guide
explains how the pieces fit together and when to use each one.

## The pieces

| Component       | Role                                              |
|-----------------|---------------------------------------------------|
| **ExArrow**     | Arrow core: IPC, Schema, RecordBatch, Stream      |
| **Explorer**    | DataFrames with columnar operations               |
| **Nx**          | Numerical computing and machine learning tensors   |
| **ADBC**        | Database connectivity via Arrow-native drivers     |
| **Flight**      | Arrow data transport over gRPC                    |
| **Flight SQL**  | SQL query execution over the Flight protocol      |
| **Parquet**     | Columnar file format built on Arrow               |
| **ExZarr**      | Zarr/n-dimensional array storage (future)         |

## Data flow

```
                    ┌──────────┐
                    │  Parquet │
                    └────┬─────┘
                         │ read/write
                    ┌────▼─────┐
              ┌─────│  ExArrow │─────┐
              │     └────┬─────┘     │
              │          │           │
    from_dataframe     from_nx      │
    to_dataframe       to_nx        │
              │          │           │
        ┌─────▼──┐  ┌───▼────┐      │
        │Explorer│  │  Nx    │      │
        └────────┘  └────────┘      │
                         │          │
              ┌──────────┼─────────┤
              │          │         │
         ┌────▼───┐ ┌───▼────┐ ┌──▼───┐
         │ Flight │ │ADBC    │ │ IPC  │
         │ Client │ │Database│ │File  │
         └────────┘ └────────┘ └──────┘
              │
         ┌────▼───────┐
         │ Flight SQL │
         │  Client    │
         └────────────┘
```

## ExArrow as the interchange layer

ExArrow sits at the centre.  Arrow data arrives via IPC files, Flight,
Flight SQL, ADBC, or Parquet.  It is consumed by Explorer DataFrames or Nx
tensors.  The same Arrow-native data can be sent to Flight servers, written to
Parquet files, or passed to other Arrow-compatible systems without copying into
the BEAM heap.

## Explorer vs Nx

- Use **Explorer** for tabular data: filtering, grouping, joining, and
  aggregations on structured datasets with mixed column types.
- Use **Nx** for homogeneous numeric data: linear algebra, machine learning,
  and tensor operations on uniform arrays of numbers.
- Use **Arrow-native** (RecordBatch/Stream) when you need to move data between
  systems without materialising it in Elixir.

## ADBC

ADBC (Arrow Database Connectivity) provides a standard API for connecting to
databases and receiving Arrow-native result streams.  ExArrow's ADBC module
supports PostgreSQL, DuckDB, SQLite, and other databases via pluggable drivers.

Results from ADBC queries are `ExArrow.Stream` handles — the same type returned
by IPC and Flight readers — so the same downstream conversion functions work
uniformly.

## Flight

Arrow Flight is a gRPC-based protocol for streaming Arrow data between
processes and services.  ExArrow's Flight client supports `do_get`, `do_put`,
`list_flights`, and `get_flight_info`.  It is suitable for high-throughput
data transfer between services.

## Flight SQL

Flight SQL extends Flight with SQL query execution.  ExArrow's Flight SQL
client connects to any Flight SQL server (DuckDB, DataFusion, Dremio, InfluxDB
v3), executes SQL queries, and returns Arrow-native results.  It supports
streaming queries, DML, prepared statements, and metadata discovery.

## Parquet

Parquet is a columnar file format built on Arrow's type system.  ExArrow can
read and write Parquet files directly, producing `ExArrow.Stream` handles on
read and accepting `ExArrow.RecordBatch` lists on write.

## ExZarr (future)

ExZarr will provide Zarr-compatible n-dimensional array storage.  When it
arrives, `ExArrow.Schema.Mapper` will be extended with ExZarr <-> Arrow type
mappings, and ExArrow will serve as the interchange layer between Zarr arrays
and the rest of the Elixir Arrow ecosystem.
