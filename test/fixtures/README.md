# Test fixtures

- **IPC stream / file format (golden):** produced by
  `ExArrow.Native.ipc_test_fixture_binary/0` and
  `ipc_test_fixture_file_binary/0` (schema: `id` int64, `name` utf8; one
  batch of 2 rows).
- **Zstandard Parquet (interop):** `parquet_zstd.parquet` contains three rows
  with `id` (int64) and `name` (utf8), produced outside ExArrow (DuckDB) so we
  verify compressed reads against third-party writers. Contributed via
  [PR #243](https://github.com/thanos/ex_arrow/pull/243) (@mindreframer).
  Regenerate with:

  ```sh
  duckdb -c "COPY (SELECT * FROM (VALUES (1::BIGINT, 'alpha'), (2::BIGINT, 'beta'), (3::BIGINT, 'gamma')) AS t(id, name)) TO 'test/fixtures/parquet_zstd.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);"
  ```

- **Cross-language corpus (v0.8+):** optional suite under
  `test/fixtures/arrow_testing/` populated by
  `script/fetch_arrow_testing.sh` from
  [apache/arrow-testing](https://github.com/apache/arrow-testing).
  Run with `mix test --include arrow_testing` once fixtures are present.
  This mirrors the pure-Elixir [`arrow`](https://hex.pm/packages/arrow)
  package's conformance approach.
