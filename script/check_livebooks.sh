#!/usr/bin/env bash
# Verify Livebook Hex pins match mix.exs @version and that each .livemd has
# at least one Elixir code fence. Run by CI (lint job).

set -euo pipefail
cd "$(dirname "$0")/.."

mix_version=$(grep -E '^[[:space:]]*@version[[:space:]]+"' mix.exs | head -1 | sed -E 's/.*"([^"]+)".*/\1/')
if [ -z "$mix_version" ]; then
  echo "::error::could not parse @version from mix.exs"
  exit 1
fi

failed=0
shopt -s nullglob
for f in livebook/*.livemd livebook/*.exs; do
  if [[ "$f" == *.livemd ]] && ! grep -q '```elixir' "$f"; then
    echo "::error::$f has no \`\`\`elixir fence"
    failed=1
  fi

  while IFS= read -r line; do
    if [[ "$line" == *'path:'* ]]; then
      continue
    fi
    if [[ "$line" != *"~> $mix_version"* ]]; then
      echo "::error::$f Hex pin does not match mix.exs @version $mix_version: $line"
      failed=1
    fi
  done < <(grep -E '\{:ex_arrow, "~>' "$f" || true)
done

if [ "$failed" -ne 0 ]; then
  exit 1
fi

echo "livebooks OK: Hex pins match ~> $mix_version"
