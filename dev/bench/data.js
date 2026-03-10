window.BENCHMARK_DATA = {
  "lastUpdate": 1773152195737,
  "repoUrl": "https://github.com/thanos/ex_arrow",
  "entries": {
    "ExArrow Benchmark Suite": [
      {
        "commit": {
          "author": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "committer": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "distinct": true,
          "id": "fe3408ffab2765c1a46d0764a8125d4f4ed14b4c",
          "message": "added test for gh_pages",
          "timestamp": "2026-03-01T16:36:48-05:00",
          "tree_id": "fb743bf5a5439d304018d7d86c15e902a35fe81b",
          "url": "https://github.com/thanos/ex_arrow/commit/fe3408ffab2765c1a46d0764a8125d4f4ed14b4c"
        },
        "date": 1772401411942,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "[adbc] Enum.map (comparable row-oriented)",
            "value": 455,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] open ipc stream (20 batches)",
            "value": 2269,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream schema peek (20 batches)",
            "value": 2400,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream collect all batches (20 batches)",
            "value": 37640,
            "unit": "ns/op"
          },
          {
            "name": "[flight] list_flights",
            "value": 194845,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_put (10 batches)",
            "value": 379383,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get stream_handle only (10 batches)",
            "value": 40661234,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get + collect (10 batches)",
            "value": 40995632,
            "unit": "ns/op"
          },
          {
            "name": "[flight] roundtrip put→get (10 batches)",
            "value": 41376035,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (10 batches)",
            "value": 1878,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (50 batches)",
            "value": 3451,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file handle (50 batches)",
            "value": 6098,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (10 batches)",
            "value": 19047,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (50 batches)",
            "value": 90884,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file + materialise (50 batches)",
            "value": 100624,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] term_to_binary (100 rows, 3 fields)",
            "value": 9775,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (10 batches)",
            "value": 15930,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (50 batches)",
            "value": 72404,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_file (50 batches)",
            "value": 703692,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] binary → Flight (20 batches)",
            "value": 1485504,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] file → Flight (zero-copy, 20 batches)",
            "value": 1488802,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] materialise → Flight (20 batches)",
            "value": 1550668,
            "unit": "ns/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "committer": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "distinct": true,
          "id": "2255ad5800fa43a707cbfe94ece2abc660c0b3b9",
          "message": "Wait — YAML does not allow duplicate mapping keys. The second push: would silently overwrite the first, which would break branch-push builds. Let me revert that:\n\nprecompiled_nifs.yml\n+2\n-3\non:\n  # Tag push: full pipeline — build all NIFs, create GitHub release, publish to Hex.pm.\n  push:\n    tags:\n      - \"v*\"\n  # Branch push: build NIFs only (no release/publish) when native code changes.\n  push:\n    branches:\non:  # Tag push: full pipeline — build all NIFs, create GitHub release, publish to Hex.pm.  push:\non:\n  push:\n    # Tag push  → full pipeline: build NIFs, create GitHub release, publish to Hex.pm.\n    # Branch push → build NIFs only (release/publish jobs are skipped; no tag ref).\n    tags:\n      - \"v*\"\n    branches:\n  # Tag push: full pipeline — build all NIFs, create GitHub release, publish to Hex.pm.on:  push:    # Tag push  → full pipeline: build NIFs, create GitHub release, publish to Hex.pm.    # Branch push → build NIFs only (release/publish jobs are skipped; no tag ref).\nect YAML — one push: block, tags and branches+paths as siblings. GitHub ignores paths on tag pushes, so the two concerns stay cleanly separated in practice even though they share the same mapping key. The comments now make that explicit.\n\nSummary: Yes, git push origin v0.2.0 will trigger the full three-stage pipeline. No changes to the logic were needed — just added comments to make the intent clear.",
          "timestamp": "2026-03-09T20:07:18-04:00",
          "tree_id": "725e080cf915e2eba0880bdad7ae4f7d89aedfc1",
          "url": "https://github.com/thanos/ex_arrow/commit/2255ad5800fa43a707cbfe94ece2abc660c0b3b9"
        },
        "date": 1773101969565,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "[adbc] Enum.map (comparable row-oriented)",
            "value": 475,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream schema peek (20 batches)",
            "value": 2456,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] open ipc stream (20 batches)",
            "value": 2463,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream collect all batches (20 batches)",
            "value": 38269,
            "unit": "ns/op"
          },
          {
            "name": "[flight] list_flights",
            "value": 208044,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_put (10 batches)",
            "value": 692977,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get stream_handle only (10 batches)",
            "value": 40673749,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get + collect (10 batches)",
            "value": 40990147,
            "unit": "ns/op"
          },
          {
            "name": "[flight] roundtrip put→get (10 batches)",
            "value": 41347049,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (10 batches)",
            "value": 1910,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (50 batches)",
            "value": 3979,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file handle (50 batches)",
            "value": 6591,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (10 batches)",
            "value": 19801,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (50 batches)",
            "value": 89274,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file + materialise (50 batches)",
            "value": 102263,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] term_to_binary (100 rows, 3 fields)",
            "value": 9829,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (10 batches)",
            "value": 16269,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (50 batches)",
            "value": 74134,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_file (50 batches)",
            "value": 659181,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] binary → Flight (20 batches)",
            "value": 1404337,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] materialise → Flight (20 batches)",
            "value": 1546229,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] file → Flight (zero-copy, 20 batches)",
            "value": 1636960,
            "unit": "ns/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "committer": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "distinct": true,
          "id": "c4d5c3b659216c1607fd479a1e227056288c1cb6",
          "message": "fixed ci",
          "timestamp": "2026-03-09T20:23:19-04:00",
          "tree_id": "b905a3b0ce37dd6e0dcb44d8ae64a684f9f47d15",
          "url": "https://github.com/thanos/ex_arrow/commit/c4d5c3b659216c1607fd479a1e227056288c1cb6"
        },
        "date": 1773102507817,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "[adbc] Enum.map (comparable row-oriented)",
            "value": 457,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] open ipc stream (20 batches)",
            "value": 2199,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream schema peek (20 batches)",
            "value": 2396,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream collect all batches (20 batches)",
            "value": 36837,
            "unit": "ns/op"
          },
          {
            "name": "[flight] list_flights",
            "value": 207239,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_put (10 batches)",
            "value": 668632,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get stream_handle only (10 batches)",
            "value": 40676293,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get + collect (10 batches)",
            "value": 40995297,
            "unit": "ns/op"
          },
          {
            "name": "[flight] roundtrip put→get (10 batches)",
            "value": 41215405,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (10 batches)",
            "value": 1847,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (50 batches)",
            "value": 2987,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file handle (50 batches)",
            "value": 6147,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (10 batches)",
            "value": 19249,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (50 batches)",
            "value": 93270,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file + materialise (50 batches)",
            "value": 101923,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] term_to_binary (100 rows, 3 fields)",
            "value": 9781,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (10 batches)",
            "value": 16561,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (50 batches)",
            "value": 66278,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_file (50 batches)",
            "value": 649864,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] file → Flight (zero-copy, 20 batches)",
            "value": 1489894,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] materialise → Flight (20 batches)",
            "value": 1556444,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] binary → Flight (20 batches)",
            "value": 1572400,
            "unit": "ns/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "committer": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "distinct": true,
          "id": "3651af43e0817f7b4d643bbe732c8fa0d25986a5",
          "message": "closed #97\n\nRoot cause: The duckdb/duckdb-adbc GitHub repository no longer exists. Since DuckDB v1.0, the ADBC driver (duckdb_adbc_init) is built directly into libduckdb.so in the main DuckDB releases — no separate repo or .tar.gz needed.\n\nChanges to integration.yml:\n\nNew URL: https://github.com/duckdb/duckdb/releases/download/v{VERSION}/libduckdb-linux-amd64.zip (main DuckDB repo, .zip format)\ncurl -fsSL: The -f flag makes curl fail immediately with a non-zero exit code on HTTP errors (4xx/5xx), so a 404 won't silently produce an HTML page that then fails mysteriously at tar\nunzip instead of tar: The DuckDB release archive is a zip, not a gzip tarball\nLooks for libduckdb.so instead of libduckdb_adbc.so — the main library exports duckdb_adbc_init directly\nfile sanity check before extraction to get a clear error if the download was wrong\nAdded Install unzip step since it may not always be present on the ubuntu runner",
          "timestamp": "2026-03-10T08:07:53-04:00",
          "tree_id": "43174f644d4afa2e3ab453741a5b2e996ba8c631",
          "url": "https://github.com/thanos/ex_arrow/commit/3651af43e0817f7b4d643bbe732c8fa0d25986a5"
        },
        "date": 1773144792631,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "[adbc] Enum.map (comparable row-oriented)",
            "value": 455,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] open ipc stream (20 batches)",
            "value": 2682,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream schema peek (20 batches)",
            "value": 2825,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream collect all batches (20 batches)",
            "value": 37982,
            "unit": "ns/op"
          },
          {
            "name": "[flight] list_flights",
            "value": 203928,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_put (10 batches)",
            "value": 389664,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get stream_handle only (10 batches)",
            "value": 40663153,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get + collect (10 batches)",
            "value": 40989953,
            "unit": "ns/op"
          },
          {
            "name": "[flight] roundtrip put→get (10 batches)",
            "value": 42548547,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (10 batches)",
            "value": 2062,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (50 batches)",
            "value": 3572,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file handle (50 batches)",
            "value": 6519,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (10 batches)",
            "value": 20000,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (50 batches)",
            "value": 96051,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file + materialise (50 batches)",
            "value": 105552,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] term_to_binary (100 rows, 3 fields)",
            "value": 9776,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (10 batches)",
            "value": 16744,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (50 batches)",
            "value": 73829,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_file (50 batches)",
            "value": 715427,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] materialise → Flight (20 batches)",
            "value": 1518866,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] binary → Flight (20 batches)",
            "value": 1542377,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] file → Flight (zero-copy, 20 batches)",
            "value": 1554499,
            "unit": "ns/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "committer": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "distinct": true,
          "id": "d0749b68be5fa6ada0509f7cbd3b9c6318e1a6d7",
          "message": "adbc_integration_test.exs\n+13\n-5\n    defp pg_opts do\n      [\n        driver_name: \"adbc_driver_postgresql\",\n        uri:\n          \"postgresql://#{env(\"PG_USER\", \"postgres\")}:#{env(\"PG_PASSWORD\", \"postgres\")}@#{env(\"PG_HOST\", \"localhost\")}:#{env(\"PG_PORT\", \"5432\")}/#{env(\"PG_DATABASE\", \"postgres\")}\"\n      ]\n    end\n    defp pg_opts do      [        driver_name: \"adbc_driver_postgresql\",        uri:          \"postgresql://#{env(\"PG_USER\", \"postgres\")}:#{env(\"PG_PASSWORD\", \"postgres\")}@#{env(\"PG_HOST\", \"localhost\")}:#{env(\"PG_PORT\", \"5432\")}/#{env(\"PG_DATABASE\", \"postgres\")}\"\n    defp pg_opts do\n      uri =\n        \"postgresql://#{env(\"PG_USER\", \"postgres\")}:#{env(\"PG_PASSWORD\", \"postgres\")}\" <>\n          \"@#{env(\"PG_HOST\", \"localhost\")}:#{env(\"PG_PORT\", \"5432\")}/#{env(\"PG_DATABASE\", \"postgres\")}\"\n\n      # Prefer an explicit driver path (set by CI); fall back to driver_name so\n      # the ADBC driver manager searches LD_LIBRARY_PATH / system paths.\n      driver_opt =\n        case env(\"PG_ADBC_DRIVER\") do\n          nil -> {:driver_name, \"adbc_driver_postgresql\"}\n          path -> {:driver_path, path}\n        end\n\n      [driver_opt, uri: uri]\n    end\n      [        driver_name: \"adbc_driver_postgresql\",        uri:          \"postgresql://#{env(\"PG_USER\", \"postgres\")}:#{env(\"PG_PASSWORD\", \"postgres\")}@#{env(\"PG_HOST\", \"localhost\")}:#{env(\"PG_PORT\", \"5432\")}/#{env(\"PG_DATABASE\", \"postgres\")}\"      ]    defp pg_opts do\nAlso update the module doc to reflect the new env var:\n\nadbc_integration_test.exs\n+16\n-11\n\n  | Variable       | Default         | Description            |\n  |----------------|-----------------|------------------------|\n  | `PG_HOST`      | `localhost`     | PostgreSQL host        |\n  | `PG_PORT`      | `5432`          | PostgreSQL port        |\n  | `PG_USER`      | `postgres`      | Username               |\n  | `PG_PASSWORD`  | `postgres`      | Password               |\n  | `PG_DATABASE`  | `postgres`      | Database name          |\n\n  ### DuckDB\n\n  | Variable           | Default      | Description                    |\n  |--------------------|--------------|--------------------------------|\n  | `DUCKDB_DRIVER`    | *(required)* | Path to `libduckdb_adbc.so`    |\n  | `DUCKDB_DATABASE`  | `:memory:`   | Database path (`\":memory:\"` ok)|\n\n  | Variable       | Default         | Description            |  |----------------|-----------------|------------------------|  | `PG_HOST`      | `localhost`     | PostgreSQL host        |  | `PG_PORT`      | `5432`          | PostgreSQL port        |\n\n  | Variable          | Default         | Description                          |\n  |-------------------|-----------------|--------------------------------------|\n  | `PG_HOST`         | `localhost`     | PostgreSQL host                      |\n  | `PG_PORT`         | `5432`          | PostgreSQL port                      |\n  | `PG_USER`         | `postgres`      | Username                             |\n  | `PG_PASSWORD`     | `postgres`      | Password                             |\n  | `PG_DATABASE`     | `postgres`      | Database name                        |\n  | `PG_ADBC_DRIVER`  | *(optional)*    | Explicit path to ADBC PG driver `.so`|\n\n  When `PG_ADBC_DRIVER` is not set the driver is looked up by name\n  (`adbc_driver_postgresql`) via the system library search path.\n  CI sets this variable by installing `adbc-driver-postgresql` via pip.\n\n  ### DuckDB\n\n  | Variable           | Default      | Description                         |\n  |--------------------|--------------|-------------------------------------|\n  | `DUCKDB_DRIVER`    | *(required)* | Path to `libduckdb.so`              |\n  | `DUCKDB_DATABASE`  | `:memory:`   | Database path (`\":memory:\"` ok)     |\n\n  | Variable       | Default         | Description            |  |----------------|-----------------|------------------------|  | `PG_HOST`      | `localhost`     | PostgreSQL host        |  | `PG_PORT`      | `5432`          | PostgreSQL port        |  | `PG_USER`      | `postgres`      | Username               |  | `PG_PASSWORD`  | `postgres`      | Password               |  | `PG_DATABASE`  | `postgres`      | Database name          |\nNow rewrite the PostgreSQL driver download step in the CI workflow:\n\nintegration.yml\n+16\n-12\n\n      # The ADBC PostgreSQL driver is provided by the adbc Hex package (livebook-dev/adbc).\n      # We download the driver binary at test time via ADBC package manager.\n      - name: Download ADBC PostgreSQL driver\n        run: |\n          mix run -e '\n            {:ok, _} = Application.ensure_all_started(:adbc)\n            {:ok, path} = Adbc.download_driver(:postgresql)\n            IO.puts(\"PG driver: #{path}\")\n            System.put_env(\"PG_ADBC_DRIVER\", path)\n          ' || true\n          # Fallback: try system libadbc_driver_postgresql\n          if [ -z \"$PG_ADBC_DRIVER\" ]; then\n            echo \"PG_ADBC_DRIVER not set from adbc package; using driver_name fallback\"\n          fi\n\n      # The ADBC PostgreSQL driver is provided by the adbc Hex package (livebook-dev/adbc).      # We download the driver binary at test time via ADBC package manager.\n\n      # Install the official Apache ADBC PostgreSQL driver via pip.\n      # The Python wheel ships a precompiled libadbc_driver_postgresql.so that\n      # we can pass directly as driver_path — no system-wide install needed.\n      - name: Download ADBC PostgreSQL driver\n        run: |\n          pip3 install --quiet \"adbc-driver-postgresql\"\n          DRIVER_PATH=$(python3 - <<'PYEOF'\nimport adbc_driver_postgresql, pathlib, sys\npkg = pathlib.Path(adbc_driver_postgresql.__file__).parent\ncandidates = list(pkg.glob(\"*.so*\")) + list(pkg.glob(\"lib/*.so*\"))\nif not candidates:\n    sys.exit(\"Could not find libadbc_driver_postgresql.so in pip package\")\nprint(candates[0])\nPYEOF\n          )\n          echo \"Found PG ADBC driver: $DRIVER_PATH\"\n          echo \"PG_ADBC_DRIVER=$DRIVER_PATH\" >> \"$GITHUB_ENV\"\n\n      # The ADBC PostgreSQL driver is provided by the adbc Hex package (livebook-dev/adbc).      # We download the driver binary at test time via ADBC package manager.      # Install the official Apache ADBC PostgreSQL driver via pip.      # The Python wheel ships a precompiled libadbc_driver_postgresql.so that\nLet me verify the final state of both files:\n\nClean. Here's a summary of what changed and why:\n\nBug 1 — ExUnit.SkipError.exception/1 is undefined (broke every test)\n\nThe root cause is how Elixir's raise/2 macro works:\n\nraise ExUnit.SkipError, \"some message\"\nExUnit.SkipError is defined withefexception [:message], so exception/1 only accepts a keyword list ([message: ...]), not a bare string. The fix is to raise the struct directly, bypassing exception/1:\n\nraise %ExUnit.SkipError{message: \"...\"}\nThis also explains why the DuckDB tests were failing in the PostgreSQL CI job — when the DuckDB setup tried to skip (because DUCKDB_DRIVER wasn't t), the crash propagated and failed the whole suite instead of skipping.\n\nBug 2 — PostgreSQL driver not found\n\nThe old step ran System.put_env(\"PG_ADBC_DRIVE, path) inside a short-lived mix run process. That call only mutates the environment of that process, which exits immediately. The subsequent mix test step never sees the variable.\n\nThe new approach:\n\nInstalls adbc-driver-postgresql via pip — the official Apache ADBC Python wheel that ships libadbc_driver_postgresql.so for Linux x86_64\nFinds the .so with a short Python snippet and writes it to $GITHUB_ENV, which is GitHub Actions' mechanism for persisting env vars across steps\nThe test's pg_opts/0 now reads PG_ADBC_DRIVER and uses driver_path: when it's set, falling back to driver_name: for local dev where the driver may already be on the library path",
          "timestamp": "2026-03-10T09:37:20-04:00",
          "tree_id": "77e523ad86326ff9d8ae20e23ce1e2b2110e898b",
          "url": "https://github.com/thanos/ex_arrow/commit/d0749b68be5fa6ada0509f7cbd3b9c6318e1a6d7"
        },
        "date": 1773150156796,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "[adbc] Enum.map (comparable row-oriented)",
            "value": 456,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] open ipc stream (20 batches)",
            "value": 2265,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream schema peek (20 batches)",
            "value": 2517,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream collect all batches (20 batches)",
            "value": 37378,
            "unit": "ns/op"
          },
          {
            "name": "[flight] list_flights",
            "value": 204841,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_put (10 batches)",
            "value": 692574,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get stream_handle only (10 batches)",
            "value": 40656943,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get + collect (10 batches)",
            "value": 40990736,
            "unit": "ns/op"
          },
          {
            "name": "[flight] roundtrip put→get (10 batches)",
            "value": 41999868,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (10 batches)",
            "value": 1949,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (50 batches)",
            "value": 3882,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file handle (50 batches)",
            "value": 6374,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (10 batches)",
            "value": 20054,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (50 batches)",
            "value": 89298,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file + materialise (50 batches)",
            "value": 103666,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] term_to_binary (100 rows, 3 fields)",
            "value": 9845,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (10 batches)",
            "value": 16413,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (50 batches)",
            "value": 75012,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_file (50 batches)",
            "value": 622415,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] materialise → Flight (20 batches)",
            "value": 1490072,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] binary → Flight (20 batches)",
            "value": 1553829,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] file → Flight (zero-copy, 20 batches)",
            "value": 1609174,
            "unit": "ns/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "committer": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "distinct": true,
          "id": "73d7a778e756afd16adc7ed03c65f629f4aee50c",
          "message": "fix to compile error!",
          "timestamp": "2026-03-10T09:46:28-04:00",
          "tree_id": "369e4444ed283887bdfad152b930081af98e0f8c",
          "url": "https://github.com/thanos/ex_arrow/commit/73d7a778e756afd16adc7ed03c65f629f4aee50c"
        },
        "date": 1773150741273,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "[adbc] Enum.map (comparable row-oriented)",
            "value": 523,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] open ipc stream (20 batches)",
            "value": 2432,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream schema peek (20 batches)",
            "value": 2781,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream collect all batches (20 batches)",
            "value": 39498,
            "unit": "ns/op"
          },
          {
            "name": "[flight] list_flights",
            "value": 182656,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_put (10 batches)",
            "value": 437687,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get stream_handle only (10 batches)",
            "value": 40657468,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get + collect (10 batches)",
            "value": 41033462,
            "unit": "ns/op"
          },
          {
            "name": "[flight] roundtrip put→get (10 batches)",
            "value": 41364839,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (10 batches)",
            "value": 2042,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (50 batches)",
            "value": 3868,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file handle (50 batches)",
            "value": 7137,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (10 batches)",
            "value": 20723,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (50 batches)",
            "value": 98510,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file + materialise (50 batches)",
            "value": 108689,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] term_to_binary (100 rows, 3 fields)",
            "value": 8913,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (10 batches)",
            "value": 16158,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (50 batches)",
            "value": 73065,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_file (50 batches)",
            "value": 622679,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] materialise → Flight (20 batches)",
            "value": 1548868,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] file → Flight (zero-copy, 20 batches)",
            "value": 1614566,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] binary → Flight (20 batches)",
            "value": 1629281,
            "unit": "ns/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "committer": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "distinct": true,
          "id": "31ef9afced2d110c5b642498b600c3213c5d5131",
          "message": "fix to ci file",
          "timestamp": "2026-03-10T09:50:26-04:00",
          "tree_id": "c04bd17dc27c7f3bf27170b6ca6e4f6faf0b867b",
          "url": "https://github.com/thanos/ex_arrow/commit/31ef9afced2d110c5b642498b600c3213c5d5131"
        },
        "date": 1773151071605,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "[adbc] Enum.map (comparable row-oriented)",
            "value": 518,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] open ipc stream (20 batches)",
            "value": 2480,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream schema peek (20 batches)",
            "value": 3020,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream collect all batches (20 batches)",
            "value": 40590,
            "unit": "ns/op"
          },
          {
            "name": "[flight] list_flights",
            "value": 205469,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_put (10 batches)",
            "value": 457093,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get stream_handle only (10 batches)",
            "value": 40657818,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get + collect (10 batches)",
            "value": 41001186,
            "unit": "ns/op"
          },
          {
            "name": "[flight] roundtrip put→get (10 batches)",
            "value": 41413421,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (10 batches)",
            "value": 1858,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (50 batches)",
            "value": 3211,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file handle (50 batches)",
            "value": 7099,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (10 batches)",
            "value": 19459,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (50 batches)",
            "value": 94029,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file + materialise (50 batches)",
            "value": 108738,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] term_to_binary (100 rows, 3 fields)",
            "value": 9073,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (10 batches)",
            "value": 15807,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (50 batches)",
            "value": 71983,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_file (50 batches)",
            "value": 627495,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] file → Flight (zero-copy, 20 batches)",
            "value": 1524017,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] materialise → Flight (20 batches)",
            "value": 1569870,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] binary → Flight (20 batches)",
            "value": 1682804,
            "unit": "ns/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "committer": {
            "email": "thanosv@gmail.com",
            "name": "thanos",
            "username": "thanos"
          },
          "distinct": true,
          "id": "5053ed740a4e06fbf971bb9cedc3b4818f6616cb",
          "message": "trying to debug",
          "timestamp": "2026-03-10T10:11:15-04:00",
          "tree_id": "53428ceb2baf001a66e8f499995f80f3d04b77b7",
          "url": "https://github.com/thanos/ex_arrow/commit/5053ed740a4e06fbf971bb9cedc3b4818f6616cb"
        },
        "date": 1773152194781,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "[adbc] Enum.map (comparable row-oriented)",
            "value": 458,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] open ipc stream (20 batches)",
            "value": 2403,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream schema peek (20 batches)",
            "value": 2653,
            "unit": "ns/op"
          },
          {
            "name": "[adbc] stream collect all batches (20 batches)",
            "value": 36779,
            "unit": "ns/op"
          },
          {
            "name": "[flight] list_flights",
            "value": 208490,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_put (10 batches)",
            "value": 679756,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get stream_handle only (10 batches)",
            "value": 40662660,
            "unit": "ns/op"
          },
          {
            "name": "[flight] do_get + collect (10 batches)",
            "value": 40989787,
            "unit": "ns/op"
          },
          {
            "name": "[flight] roundtrip put→get (10 batches)",
            "value": 41494666,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (10 batches)",
            "value": 1935,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] stream_handle (50 batches)",
            "value": 3480,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file handle (50 batches)",
            "value": 6462,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (10 batches)",
            "value": 19699,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] materialise (50 batches)",
            "value": 92013,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_read] from_file + materialise (50 batches)",
            "value": 102630,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] term_to_binary (100 rows, 3 fields)",
            "value": 9786,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (10 batches)",
            "value": 15857,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_binary (50 batches)",
            "value": 71602,
            "unit": "ns/op"
          },
          {
            "name": "[ipc_write] ipc to_file (50 batches)",
            "value": 667125,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] binary → Flight (20 batches)",
            "value": 1466638,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] materialise → Flight (20 batches)",
            "value": 1651618,
            "unit": "ns/op"
          },
          {
            "name": "[pipeline] file → Flight (zero-copy, 20 batches)",
            "value": 1688140,
            "unit": "ns/op"
          }
        ]
      }
    ]
  }
}