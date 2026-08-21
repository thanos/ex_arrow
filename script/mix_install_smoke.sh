#!/usr/bin/env bash
# Compile-smoke Mix.install with and without optional deps (separate VMs).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ELIXIR="${ELIXIR:-elixir}"

run_case() {
  local label="$1"
  shift
  echo "==> Mix.install smoke: $label"
  "$ELIXIR" --eval "
    System.put_env(\"EX_ARROW_SKIP_NIF\", \"1\")
    Mix.install([$*], force: true, consolidate_protocols: false)
    IO.puts(\"ok: $label\")
  "
}

# Alone — must compile without gen_stage / telemetry / etc.
run_case "ex_arrow-only" "{:ex_arrow, path: \"${ROOT}\"}"

# With gen_stage (the 0.7.1 failure mode)
run_case "ex_arrow+gen_stage" \
  "{:ex_arrow, path: \"${ROOT}\"}, {:gen_stage, \"~> 1.2\"}"

# With telemetry
run_case "ex_arrow+telemetry" \
  "{:ex_arrow, path: \"${ROOT}\"}, {:telemetry, \"~> 1.0\"}"

echo "All Mix.install smoke cases passed."
