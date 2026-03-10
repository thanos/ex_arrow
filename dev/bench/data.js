window.BENCHMARK_DATA = {
  "lastUpdate": 1773101969940,
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
      }
    ]
  }
}