# 1m parity-gated artifacts

Historical validation summary from one paired official-vs-Rust `1m`
processing-only run using subscription parameters. The
[benchmark methodology](../../benchmark.md) defines the claim limits for the
values in this directory.

This run used the earlier asymmetric Docker Desktop/native-host topology and
predates current provenance capture. It is retained as development history;
the auth-perimeter runs supersede it.

Run shape:

- 1,000,402 source task rows;
- 1,000 concrete `tasks_by_project(...)` buckets;
- 10 inserts, updates, and deletes per bucket after the initial cursor;
- 30,000 verified incremental operations;
- a Rust slot created before timing, while the official service created its
  slot during measured startup.

[`parity-summary.json`](parity-summary.json) preserves aggregate counts,
digest comparison flags, checkpoint recurrence, timings, and validator
metadata. Raw per-bucket observations were not retained, and the summary was
written by an earlier revision of the export tooling: a fresh
`export_artifacts.mjs` export additionally records provenance, resource
evidence, and per-boundary timings absent here.

Initial counts, PUT and semantic digests, checkpoint counts, and checkpoint
checksums matched for all 1,000 probed buckets in the recorded run, and churn
PUT and REMOVE object digests matched across the 30,000 verified incremental
operations. Wire digests and churn checkpoint checksums are target-local
because source-key and REMOVE subkey encodings differ.
