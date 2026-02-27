#!/usr/bin/env elixir
# Run all ExArrow benchmarks in sequence.
# Usage: mix run bench/run_all.exs

IO.puts("""

================================================================
  ExArrow Benchmark Suite
  #{DateTime.utc_now() |> DateTime.to_string()}
================================================================
""")

scripts = [
  "bench/ipc_read_bench.exs",
  "bench/ipc_write_bench.exs",
  "bench/flight_bench.exs",
  "bench/adbc_bench.exs",
  "bench/pipeline_bench.exs"
]

for script <- scripts do
  IO.puts("\n>>> Running #{script}\n")
  Code.require_file(script)
end

IO.puts("""

================================================================
  All benchmarks complete.
  HTML reports written to bench/output/
================================================================
""")
