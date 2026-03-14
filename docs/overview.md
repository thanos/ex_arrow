# Overview

The main overview, installation, quick start, and usage examples live in the
[README on GitHub](https://github.com/thanos/ex_arrow/blob/main/README.md).

This doc set covers:

| Topic | Guide |
|-------|--------|
| Handles, copying, NIF scheduling | [Memory model](memory_model.md) |
| IPC stream and file read/write | [IPC guide](ipc_guide.md) |
| Arrow Flight client and server | [Flight guide](flight_guide.md) |
| ADBC database connectivity | [ADBC guide](adbc_guide.md) |
| Parquet read and write | [Parquet guide](parquet_guide.md) |
| Compute kernels (filter, project, sort) | [Compute guide](compute_guide.md) |
| Benchmarks and CI publishing | [Benchmarks guide](benchmarks.md) |

## Optional integrations

| Module | Requires | What it does |
|--------|----------|--------------|
| `ExArrow.Explorer` | `{:explorer, "~> 0.8"}` | Convert between `ExArrow.Stream`/`RecordBatch` and `Explorer.DataFrame` |
| `ExArrow.Nx` | `{:nx, "~> 0.9"}` | Convert numeric Arrow columns to `Nx.Tensor` and back |
| `ExArrow.ADBC.ConnectionPool` | `{:nimble_pool, "~> 1.1"}` | NimblePool-backed connection pool for ADBC databases |
| ADBC driver download | `{:adbc, "~> 0.7"}` | `ExArrow.ADBC.DriverHelper.ensure_driver_and_open/2` |

API reference: `mix docs` or [Hex Docs](https://hexdocs.pm/ex_arrow).
