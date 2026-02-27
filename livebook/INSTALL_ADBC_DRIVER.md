# Installing an ADBC driver for ExArrow

ExArrow’s ADBC support needs a **native** (C/C++) driver: a **loadable shared library** (e.g. `libadbc_driver_sqlite.so`, `.dylib`, or `.dll`) that the **ADBC C driver manager** can load. That is different from the [adbc](https://hex.pm/packages/adbc) Hex package, which has its own process-based `Adbc.Database` / `Adbc.Connection` and native stack—so `Adbc.download_driver/1` and the adbc package’s precompiled artifacts do **not** necessarily provide a library ExArrow can use. This guide gives options that work with ExArrow in Livebook and in Mix projects.

---

## What ExArrow needs

- **ADBC driver manager** — built into ExArrow’s NIF.
- **A driver shared library** — e.g. the SQLite driver from [Apache Arrow ADBC](https://github.com/apache/arrow-adbc). The driver must be on your system and either:
  - discoverable by name (e.g. `driver_name: "adbc_driver_sqlite"` with `ADBC_DRIVER` or system path set), or
  - passed by path: `ExArrow.ADBC.Database.open(driver_path: "/path/to/libadbc_driver_sqlite.dylib", ...)`.

---

## Option 1: Use the [`adbc`](https://hex.pm/packages/adbc) Hex package (Elixir)

The [livebook-dev/adbc](https://github.com/livebook-dev/adbc) project provides Elixir bindings and **ships precompiled artifacts** for supported platforms. ExArrow can use it only to **obtain** the driver; ExArrow then opens the database with its own ADBC stack.

### In a Mix project

1. Add both deps:

   ```elixir
   def deps do
     [
       {:ex_arrow, "~> 0.1.0"},
       {:adbc, "~> 0.7", optional: true}
     ]
   end
   ```

2. Configure the driver(s) you want (see [Adbc docs](https://hexdocs.pm/adbc/Adbc.html)):

   ```elixir
   # config/config.exs
   config :adbc, :drivers, [:sqlite]
   ```

3. Run `mix deps.get`. The `adbc` package uses precompiled artifacts by default ([README](https://github.com/livebook-dev/adbc/tree/main)); to force a local build you need `cmake` and can set `ADBC_BUILD=1` or `config :elixir_make, :force_build, adbc: true`.

4. In code, either use ExArrow’s helper (which calls `Adbc.download_driver/1` when `adbc` is present) or open by driver name after the driver is available:

   ```elixir
   {:ok, db} = ExArrow.ADBC.DriverHelper.ensure_driver_and_open(:sqlite, ":memory:")
   # or, if the driver is already on path:
   {:ok, db} = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_sqlite", uri: ":memory:")
   ```

### In Livebook (Mix.install)

1. Install both packages:

   ```elixir
   Mix.install([
     {:ex_arrow, "~> 0.1.0"},
     {:adbc, "~> 0.7"}
   ])
   ```

2. Optionally configure drivers (e.g. in a setup cell):

   ```elixir
   Application.put_env(:adbc, :drivers, [:sqlite])
   ```

3. Use `ExArrow.ADBC.DriverHelper.ensure_driver_and_open(:sqlite, ":memory:")` or `Adbc.download_driver(:sqlite)` then `ExArrow.ADBC.Database.open(driver_name: "adbc_driver_sqlite", uri: ":memory:")`.

**Note:** If you see an error about a `.whl` (Python wheel) or a failed download, the `adbc` package’s download manifest may not be providing a native driver for your setup. The adbc package’s drivers are for its own stack, not for ExArrow’s C driver manager. Use Option 2 and point ExArrow at a standalone ADBC C driver you build or install yourself.

---

## Option 2: Build or install the native driver yourself

ExArrow talks to the same C/C++ drivers as [Apache Arrow ADBC](https://github.com/apache/arrow-adbc). You can build them from source or use a system package if available.

### Apache Arrow ADBC (source)

- **Repo:** [github.com/apache/arrow-adbc](https://github.com/apache/arrow-adbc)
- **Releases:** [Releases](https://github.com/apache/arrow-adbc/releases) (may include source or platform-specific builds; check the release assets for C/C++ drivers).
- Build the driver(s) you need (e.g. SQLite) with CMake as per the project’s docs, then set `ADBC_DRIVER` to the directory containing the shared library, or pass the full path to ExArrow:

  ```elixir
  ExArrow.ADBC.Database.open(
    driver_path: "/path/to/libadbc_driver_sqlite.dylib",
    uri: ":memory:"
  )
  ```

  (ExArrow’s API may expect `driver_path` or a single path argument; see [ADBC guide](../docs/adbc_guide.md).)

### How the `adbc` Elixir package is built and tested

The [livebook-dev/adbc](https://github.com/livebook-dev/adbc) repo builds native code (C++/CMake) and ships precompiled artifacts. Their [CI workflow](https://github.com/livebook-dev/adbc/blob/main/.github/workflows/ci.yml) runs `mix test` on Linux, Windows, and macOS; tests assume those precompiled artifacts (or a successful build with `ADBC_BUILD=1`). So in that project, “having the driver” means having the artifact the package builds or downloads. ExArrow does not use that process or those binaries directly; it loads the ADBC driver manager and a driver shared library. If the `adbc` package installs a compatible shared library somewhere on your system or in `ADBC_DRIVER`, ExArrow can use it via `driver_name` or `driver_path`.

---

## Quick reference

| Goal                         | Action |
|-----------------------------|--------|
| Use ExArrow in Livebook     | Prefer `Mix.install([{:ex_arrow, "~> 0.1.0"}])`; add `{:adbc, "~> 0.7"}` if you want the adbc package to try to provide a driver. |
| Driver from adbc package    | Add `{:adbc, "~> 0.7"}`, set `config :adbc, :drivers, [:sqlite]` (or use `Adbc.download_driver/1` in Livebook), then open with `DriverHelper.ensure_driver_and_open/2` or `Database.open(driver_name: "adbc_driver_sqlite", uri: "...")`. |
| Use your own driver binary  | Build or install the C/C++ driver (e.g. from [Arrow ADBC](https://github.com/apache/arrow-adbc)), then `Database.open(driver_path: "/path/to/libadbc_driver_sqlite.dylib", ...)` or set `ADBC_DRIVER` and use `driver_name`. |
| Download fails / wrong file | Rely on Option 2: build or install the native driver and use `driver_path` or `ADBC_DRIVER` + `driver_name`. |

See also: [ExArrow ADBC guide](../docs/adbc_guide.md), [adbc on Hex](https://hex.pm/packages/adbc), [livebook-dev/adbc](https://github.com/livebook-dev/adbc).
