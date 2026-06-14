# 1m auth-perimeter artifacts

Compact validation summaries from the paired official-vs-Rust `1m`
processing-only run.

This run used an asymmetric topology: the official service and MongoDB ran in
Docker Desktop's resource-limited Linux VM, while Rust and MDBX ran natively on
the host. These files are retained for inspection, not as a fair product
benchmark. The run predates the current correctness/security implementation and
does not identify its Git commit. See `docs/benchmark.md` before quoting a result.

Run shape:

- 1,000,402 source task rows.
- all 1000 concrete `tasks_by_auth_project(...)` buckets.
- bucket routing through `user_project_access` filtered by `auth.user_id()`
  (1,001 access rows).
- authz spoof probe: a no-access JWT user sending an authorized project id via
  client-controlled parameters received zero checkpoint/data buckets on both
  targets.
- 10 inserts, 10 updates, and 10 deletes per routed project bucket after the
  initial cursor; 30,000 verified incremental PUT/REMOVE ops.
- 1 unrecorded warmup pair and 5 measured paired repeats, with interleaved
  target order recorded in `parity-summary.json`.
- churn catch-up gated by each target's Postgres replication slot
  `confirmed_flush_lsn` (`POWERSYNC_USER_VALUE_CHURN_GATE_MODE=slot-lsn`).
- official baseline: `journeyapps/powersync-service:1.21.0` with the database,
  MongoDB, authentication, and sync rules required by the harness, and no
  additional performance tuning.
- lifecycle asymmetry: the Rust replication slot was created before timing,
  while the official service created its slot during measured startup. The
  current harness provisions both publications before timing and creates both
  slots after the measured start boundary.

Files kept in-repo:

- `parity-summary.json`: compact aggregate parity, timing, host, and sample
  summary derived by `scripts/export_artifacts.mjs`.

The raw per-bucket validation JSON and raw `results.json` were not retained.
Repeating the command recreates the configuration, not the exact historical
run. This artifact predates Git/file hash and image-digest capture.

The retained summary omits prose verdicts, winner labels, and interpolated p95
comparison fields. Its p50 values are labeled as medians. The underlying
median, delta, ratio, sample, and parity values were not changed.

Host metadata was captured in `parity-summary.json`. The deployment topology is
still asymmetric: official service + MongoDB run in Docker Desktop on macOS,
while Rust runs as a native host process writing MDBX to local disk. Treat the
ratios as this-machine, this-topology evidence.

Parity note: initial counts, PUT digests, semantic digests, checkpoint counts,
and checkpoint checksums are equal per bucket across targets. Churn PUT digests
and REMOVE object digests are equal per bucket. Wire digests and churn
checkpoint checksums are not expected to be byte-identical across targets
because source-key/subkey encodings are implementation-specific, and REMOVE
checksums hash the target-local subkey.
