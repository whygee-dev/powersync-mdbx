# Scope

## Experiment

Measure an MDBX-backed materialization design for a large PostgreSQL dataset: source rows → constrained sync-rule evaluation → persisted bucket state → selected `/sync/stream` responses.

## Implemented experiment surface

- the sync-rule forms used by the generated benchmark fixture;
- PostgreSQL snapshot scanning and logical-replication ingestion;
- MDBX current-state, tail, index, and checkpoint-accumulator layouts;
- initial and post-write `/sync/stream` responses for selected streams;
- JWT-derived auth-perimeter routing through source-table access rows;
- deterministic insert/update/delete churn;
- comparisons of selected client-visible operations and checkpoint fields against the official service;
- high-scale generated fixtures and batched routed-bucket validation.

## Outside the implemented surface

- full PowerSync protocol, rule-language, SDK, or operational compatibility;
- automated backup/restore, failover, rolling upgrades, or unattended recovery;
- online storage-generation migration or sync-rule generation deployment;
- `TRUNCATE` support for materialized tables;
- PostgreSQL publication row filters, omitted columns, `publish_via_partition_root`, or partition/inheritance parent source tables;
- upload or CRUD APIs;
- partial-sync priority and per-subscription correlation semantics;
- browser first-screen performance;
- a general conclusion about Node.js, MongoDB, Rust, MDBX, or PowerSync outside this workload.

The implementation creates a logical slot with `EXPORT_SNAPSHOT`, imports that snapshot into the repeatable-read scan, and starts replication at the returned consistent point. Incomplete bootstraps are hidden by readiness, reset only when a matching durable intent proves ownership of the inactive slot, and advance a time-based cursor epoch before rebuilding.

That recovery path is for initial bootstrap, not online rule changes. Layout-changing deploy/reprocess requests remain rejected until a candidate generation can snapshot, catch up, and atomically activate. Removing the complete state directory is an operator reset outside the cursor-epoch protocol and requires clients to discard saved cursors.
