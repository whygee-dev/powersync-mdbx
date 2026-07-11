# Protocol validation gate

The benchmark's equivalence gate runs after all configured initial-boundary observers complete and before target shutdown.

It verifies:

1. the observed NDJSON prefix through the first completion marker is ordered `checkpoint -> data* -> checkpoint_complete`; the harness does not assert that the response ends there.
2. protocol line kinds are classified with public `@powersync/common` exports: `isStreamingSyncCheckpoint`, `isStreamingSyncData`, `isStreamingKeepalive`, and `isStreamingSyncCheckpointComplete`.
3. `checkpoint.last_op_id` equals `checkpoint_complete.last_op_id`.
4. reported bucket counts match the generated fixture when present.
5. initial bucket streams contain no duplicate PUT ids and no initial REMOVEs.
6. object id sets match the generated fixture exactly.
7. projected task payload fields match the fixture after scalar/timestamp normalization.
8. PUT checksums match the official hash input `put.${object_type}.${object_id}.${data}`.
9. REMOVE checksums match the official hash input `delete.${subkey}`.
10. checkpoint checksums use wrapping 32-bit addition and protocol signed-i32 representation.
11. client-visible operation digests match across official/Rust after excluding implementation-specific subkeys/checksum storage.

The gate compares resolved bucket semantics, not byte-for-byte chunk identity. Chunk page sizes, operation ids, and source-key/subkey encodings may differ while resolving to the same bucket state.

Rust checkpoint counts/checksums are served from persisted per-query accumulator keys keyed by object type, route constraints, filters, and projection. Current-state accumulators and tail/remainder accumulators are both maintained at write time, so incremental `after > 0` streams avoid rescanning bucket current rows or historical tail rows while preserving projection-specific checksum semantics.

For high-scale runs, the gate can also probe many actual project buckets in batches. `POWERSYNC_USER_VALUE_PROJECT_BUCKET_SAMPLES=1000` on the `1m` profile means all 1000 concrete project buckets on the same stream (`tasks_by_project(...)` in subscription mode, `tasks_by_auth_project(...)` in auth-perimeter mode), not 1000 generated streams.

For auth/perimeter runs, set `POWERSYNC_USER_VALUE_ACCESS_MODE=auth_perimeter`. The generated rules use a `with:` CTE over `user_project_access` filtered by `auth.user_id()`, and the protocol request subscribes to the stream once like a real client. The gate also signs in as a no-access user, sends an authorized project id in client-controlled request/subscription parameters, and verifies that this does not produce any checkpoint bucket or data.

When `POWERSYNC_USER_VALUE_CHURN_GATE=1` is enabled, the benchmark additionally:

1. keeps the target running after initial equivalence.
2. applies deterministic inserts, updates, and deletes across the sampled project buckets.
3. waits for the target to ingest the post-mutation replication state. With `POWERSYNC_USER_VALUE_CHURN_GATE_MODE=slot-lsn`, both targets wait for their Postgres replication slot to confirm the captured target LSN. In the default target-specific mode, official uses its Postgres/diagnostics LSN path and Rust uses the persisted `tail_ops_written` delta that backs incremental bucket reads. That delta advances on local persistence without the upstream slot ack, so the default mode can finish earlier for Rust; the checked-in historical runs use `slot-lsn` for a symmetric finish line.
4. requests each bucket from its initial cursor.
5. verifies expected incremental PUT and REMOVE object-id sets and updated payload fields.
6. validates each target's checkpoint count/checksum recurrence from its own emitted checksums.
7. compares client-visible PUT/REMOVE operation digests across targets separately from checksum/subkey wire digests.

Historical 1m auth-perimeter parity-gated artifact summary (source revision unidentified):

- artifact: `docs/artifacts/1m-auth-perimeter/parity-summary.json`.
- repeats: 5 paired interleaved official/Rust runs.
- access mode: auth perimeter; buckets are routed through `user_project_access` filtered by `auth.user_id()`.
- official: passed.
- Rust: passed.
- source task rows: 1,000,402.
- authz spoof probe: no-access user + spoofed project parameter returned zero buckets on both targets.
- initial buckets checked per repeat per target: 1000 actual `tasks_by_auth_project(...)` buckets.
- initial PUTs checked per repeat per target: 1,000,402.
- initial non-zero checksum records per repeat per target: 1,000,402.
- initial checkpoint count/checksum parity: equal per bucket across all repeats.
- churn buckets checked per repeat per target: 1000.
- churn mutation rows: 10 inserts, 10 updates, and 10 deletes per bucket.
- churn incremental PUT/REMOVE ops checked per repeat per target: 30,000.
- churn checkpoint recurrence: self-valid per target.
- protocol validator: `@powersync/common` 1.51.0 public exports listed in `parity-summary.json`.

Older subscription-parameter artifact: `docs/artifacts/1m-parity-gated/parity-summary.json`.

These checks establish parity for the selected buckets and fields. They do not
cover other protocol or rule-language forms, online rule-generation swaps, or
operator recovery after deleting all state.

Known parity boundary:

- Full wire digests are not asserted equal because source-key/subkey formats are implementation-specific. Initial PUT checksum/checkpoint parity is exact because PUT checksum ignores subkey. Churn DELETE checkpoint checksums are target-local because REMOVE checksum intentionally hashes the target's subkey.
