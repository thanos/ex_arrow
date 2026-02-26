# ExArrow Livebooks

Tutorial notebooks for the **ex_arrow** library, suitable for an introductory Medium article or self-paced learning.

## Notebooks

| File | Purpose |
|------|---------|
| **00_quickstart.livemd** | Get something running in minutes: IPC read/write, Flight echo client/server, ADBC SQLite query. |
| **01_ipc.livemd** | IPC deep dive: stream vs file format, reading from binary/file, writing, schema and types, optional Explorer interop. |
| **02_flight.livemd** | Arrow Flight: echo server, client, do_put/do_get, list_flights, get_flight_info, get_schema, actions. |
| **03_adbc.livemd** | ADBC: driver setup (optional `adbc` package), Database → Connection → Statement → Stream, metadata APIs, optional Explorer roundtrip. |

Together they demonstrate all ExArrow functionality: IPC (stream + file), Flight (client + server), and ADBC (Database/Connection/Statement and Arrow result streams).

## How to run

1. In a project that has `{:ex_arrow, "~> 0.1.0"}` in `deps`, start Livebook (e.g. `mix livebook.server` or from the Livebook app).
2. Open the notebooks in order: quickstart first, then 01 → 02 → 03.
3. For **ADBC** (00 and 03), the SQLite examples work out of the box if you add `{:adbc, "~> 0.7"}` so `DriverHelper.ensure_driver_and_open/2` can fetch the driver. Without it, open by driver path/name if you have an ADBC driver installed.

## For the Medium article

- Use **00_quickstart** as the “try it in 5 minutes” section with copy-paste or screenshots.
- Use **01_ipc**, **02_flight**, and **03_adbc** as the tutorial sequence, with key code snippets and explanations drawn from each notebook.
