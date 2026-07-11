# 1m parity-gated artifacts

Historical validation summary from one paired official-vs-Rust `1m`
processing-only run using subscription parameters. Read the
[benchmark methodology](../../benchmark.md) before using any value from this
directory.

This run used the earlier asymmetric Docker Desktop/native-host topology. It
predates current provenance capture and is not comparable to the later
auth-perimeter runs. It is retained as development history, not as supporting
performance evidence.

Run shape:

- 1,000,402 source task rows;
- 1,000 concrete `tasks_by_project(...)` buckets;
- 10 inserts, updates, and deletes per bucket after the initial cursor;
- 30,000 verified incremental operations;
- a Rust slot created before timing, while the official service created its
  slot during measured startup.

[`parity-summary.json`](parity-summary.json) preserves aggregate counts,
digest comparison flags, checkpoint recurrence, timings, and validator
metadata. Raw per-bucket observations were not retained.

Initial counts, PUT and semantic digests, checkpoint counts, and checkpoint
checksums matched for every probed bucket. Churn PUT and REMOVE object digests
also matched. Wire digests and churn checkpoint checksums are target-local
because source-key and REMOVE subkey encodings differ.
