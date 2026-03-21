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
| C Data Interface (CDI) | [CDI guide](cdi_guide.md) |
| Nx tensor bridge | [Nx guide](nx_guide.md) |
| Benchmarks and CI publishing | [Benchmarks guide](benchmarks.md) |

## Optional integrations

| Module | Requires | What it does |
|--------|----------|--------------|
| `ExArrow.Explorer` | `{:explorer, "~> 0.11"}` | Convert between `ExArrow.Stream`/`RecordBatch` and `Explorer.DataFrame` |
| `ExArrow.Nx` | `{:nx, "~> 0.9"}` | Convert numeric Arrow columns to/from `Nx.Tensor`; build multi-column batches from a tensor map |
| `ExArrow.CDI` | (none — built-in) | Zero-copy Arrow C Data Interface: export/import record batches as raw C struct pointers |
| `ExArrow.ADBC.ConnectionPool` | `{:nimble_pool, "~> 1.1"}` | NimblePool-backed connection pool for ADBC databases |
| `:adbc_package` backend | `{:adbc, "~> 0.9"}` + `{:explorer, "~> 0.11"}` | Supervised pure-Elixir ADBC backend; `Database.open(:adbc_package)` |
| ADBC driver download | `{:adbc, "~> 0.9"}` | `ExArrow.ADBC.DriverHelper.ensure_driver_and_open/2` |

API reference: `mix docs` or [Hex Docs](https://hexdocs.pm/ex_arrow).
