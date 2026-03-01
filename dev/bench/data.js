window.BENCHMARK_DATA = {
  "lastUpdate": 1772401412466,
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
      }
    ]
  }
}