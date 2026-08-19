# Arrow IPC fixtures

- **IPC stream / file format (golden):** produced by
  `ExArrow.Native.ipc_test_fixture_binary/0` and
  `ipc_test_fixture_file_binary/0` (schema: `id` int64, `name` utf8; one
  batch of 2 rows).
- **Cross-language corpus (v0.8+):** optional suite under
  `test/fixtures/arrow_testing/` populated by
  `script/fetch_arrow_testing.sh` from
  [apache/arrow-testing](https://github.com/apache/arrow-testing).
  Run with `mix test --include arrow_testing` once fixtures are present.
  This mirrors the pure-Elixir [`arrow`](https://hex.pm/packages/arrow)
  package's conformance approach.
