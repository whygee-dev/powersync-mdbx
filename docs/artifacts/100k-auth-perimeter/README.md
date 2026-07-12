# 100k auth-perimeter artifacts

Compact validation summary from a historical paired official-vs-Rust `100k`
processing-only run. The [benchmark methodology](../../benchmark.md) defines
the claim limits for the values in this directory.

The official service and MongoDB ran in Docker Desktop's resource-limited
Linux VM; Rust and MDBX ran natively on the host. The run predates the
current correctness and security changes and does not identify its Git
commit. It is retained for inspection, not as current-tree performance
evidence.

Run shape:

- 100,102 source task rows;
- the default `tasks` bucket and 250 concrete
  `tasks_by_auth_project(...)` buckets;
- routing through `user_project_access` filtered by `auth.user_id()`;
- an authorization-isolation probe in which a no-access user supplied an
  authorized project id and received no checkpoint or data buckets;
- 10 inserts, updates, and deletes per routed bucket after the initial cursor;
- one unrecorded warmup pair and five measured pairs with interleaved order;
- churn catch-up gated by PostgreSQL slot `confirmed_flush_lsn`;
- official baseline `journeyapps/powersync-service:1.21.0` without additional
  performance tuning.

The Rust slot was created before measured startup, while the official service
created its slot during startup. The current harness creates both slots after
the common start boundary.

[`parity-summary.json`](parity-summary.json) contains aggregate parity,
timing, execution-topology, and sample data. Raw per-bucket observations and `results.json`
were not retained, so the summary cannot be audited against raw observations or
regenerated from the retained files. The summary was also written by an
earlier revision of the export tooling: a fresh `export_artifacts.mjs` export
additionally records provenance, resource evidence, and per-boundary timings
absent here.

Across all five measured pairs, initial counts, PUT and semantic digests,
checkpoint counts, and checkpoint checksums matched for all 251 probed
buckets — 200,204 initial PUT operations per target per repeat. Churn PUT
and REMOVE object digests matched across all 15,000 incremental operations
per repeat. Wire digests and churn checkpoint checksums are target-local
because source-key and REMOVE subkey encodings differ.
