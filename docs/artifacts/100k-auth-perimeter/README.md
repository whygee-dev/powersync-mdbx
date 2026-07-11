# 100k auth-perimeter artifacts

Compact validation summary from a historical paired official-vs-Rust `100k`
processing-only run. Read the [benchmark methodology](../../benchmark.md)
before using any value from this directory.

The official service and MongoDB ran in Docker Desktop's resource-limited
Linux VM; Rust and MDBX ran natively on the host. The run predates the
current correctness and security implementation and does not identify its Git
commit. It is retained for inspection, not as current-tree or product-level
performance evidence.

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
timing, host, and sample data. Raw per-bucket observations and `results.json`
were not retained, so the summary is not independently reproducible.

Initial counts, PUT and semantic digests, checkpoint counts, and checkpoint
checksums matched for every probed bucket. Churn PUT and REMOVE object digests
also matched. Wire digests and churn checkpoint checksums are target-local
because source-key and REMOVE subkey encodings differ.
