# 1m parity-gated artifacts

Historical validation summaries from a single paired official-vs-Rust `1m`
processing-only run using subscription parameters.

This artifact is not comparable to the later auth-perimeter runs: it has one
sample, predates the current provenance capture, and used the same asymmetric
Docker Desktop/native-host topology. It is retained to make the development
history visible, not as supporting performance evidence.

Run shape:

- 1,000,402 source task rows.
- all 1000 concrete `tasks_by_project(...)` buckets.
- 10 inserts, 10 updates, and 10 deletes per bucket after the initial cursor.
- 30,000 verified incremental churn ops.
- persisted current/tail checkpoint accumulator path enabled.
- the Rust replication slot was created before timing, while the official
  service created its slot during measured startup. The current harness
  provisions both publications before timing and creates both slots after the
  measured start boundary.

Files kept in-repo:

- `parity-summary.json`: compact aggregate parity/timing summary.

The raw per-bucket validation JSON was not retained. `parity-summary.json`
preserves aggregate counts, digest comparison flags, checksum recurrence,
timings, and protocol-validator metadata.

The retained summary omits derived single-sample `speedup` fields and an
implementation note that could not be tied to a source revision.

Parity note: initial counts, PUT digests, semantic digests, checkpoint counts, and checkpoint checksums are equal per bucket. Churn PUT digests and REMOVE object digests are equal per bucket. Wire digests and churn checkpoint checksums are not expected to be byte-identical across targets because source-key/subkey encodings are implementation-specific, and REMOVE checksums hash the target-local subkey.
