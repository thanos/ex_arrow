# Test fixtures

- **IPC file format (golden):** File-format bytes are produced by `ExArrow.Native.ipc_test_fixture_file_binary/0` (schema: `id` int64, `name` utf8; one batch of 2 rows). Tests use this for `ExArrow.IPC.File.from_binary/1` and for compatibility checks.
- **IPC from_file:** Tests that need a path write a temp file with `ExArrow.Native.ipc_file_writer_to_file/3` and remove it in an `after` block.

No pre-generated `.arrow` files are committed; the single file-format fixture is generated in Rust for reproducibility.
