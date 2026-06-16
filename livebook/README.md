# ExArrow Livebooks

Tutorial notebooks for the **ex_arrow** library, suitable for an introductory Medium article or self-paced learning.

## Notebooks

| File | Purpose |
|------|---------|
| **00_quickstart.livemd** | Get something running in minutes: IPC read/write, Flight echo client/server, ADBC SQLite query. |
| **01_ipc.livemd** | IPC deep dive: stream vs file format, reading from binary/file, writing, schema and types, optional Explorer interop. |
| **02_flight.livemd** | Arrow Flight: echo server, client, do_put/do_get, list_flights, get_flight_info, get_schema, actions. |
| **03_adbc.livemd** | ADBC: driver setup (optional `adbc` package), Database → Connection → Statement → Stream, metadata APIs, optional Explorer roundtrip. |
| **04_adbc_integration.livemd** | **adbc_package** backend: single Setup cell (Mix.install + config + manager), SQLite in-memory queries, then connection pooling with NimblePool. |

Together they demonstrate all ExArrow functionality: IPC (stream + file), Flight (client + server), and ADBC (Database/Connection/Statement and Arrow result streams).

## How to run

1. In a project that has `{:ex_arrow, "~> 0.1.0"}` in `deps`, start Livebook (e.g. `mix livebook.server` or from the Livebook app).
2. Open the notebooks in order: quickstart first, then 01 → 02 → 03.
3. For **ADBC** (00 and 03), you need a native ADBC driver (e.g. SQLite). See **[Installing an ADBC driver](INSTALL_ADBC_DRIVER.md)** for step-by-step options: the [`adbc`](https://hex.pm/packages/adbc) Hex package (precompiled artifacts; see [livebook-dev/adbc](https://github.com/livebook-dev/adbc) and their [CI](https://github.com/livebook-dev/adbc/blob/main/.github/workflows/ci.yml)), or building/installing from [Apache Arrow ADBC](https://github.com/apache/arrow-adbc). Without a driver, the ADBC cells will report an error and point you to that guide.

**Running from the repo (path dependency)**  
Livebook often autosaves notebooks under `~/Library/Application Support/livebook/autosaved/`, so `Path.join(__DIR__, "..")` does **not** point at this repo. To build local source in Livebook, set **`EX_ARROW_PATH`** to your clone before running the setup cell (e.g. `export EX_ARROW_PATH=/path/to/ex_arrow`), and ensure **Rust** is installed. The setup cell auto-detects a local checkout (via `EX_ARROW_PATH` or when the notebook is opened directly from `livebook/` in the repo) and adds `rustler` + `force_build`; otherwise it uses the Hex release with precompiled NIFs.
