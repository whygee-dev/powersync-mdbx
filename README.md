# powersync-mdbx

An independent Rust/MDBX research implementation of a narrow PowerSync workload: read a consistent PostgreSQL snapshot, materialize bucket state, follow logical replication, and serve the covered data through `/sync/stream`.

This project is not affiliated with, endorsed by, or supported by PowerSync or Journey Mobile, Inc. It is not a fork of the PowerSync service or a drop-in replacement. The PowerSync name identifies the protocol and service used for comparison.

## Research question

Initial replication has been an operational bottleneck in a large deployment. This repository tests whether an embedded ordered store and a purpose-built materialization path can reduce the time from service start to protocol-observable readiness.

The comparison changes the language, runtime, storage engine, data layout, and parts of the service architecture. It cannot isolate MongoDB, Node.js, Rust, MDBX, or any other component as the cause of a measured difference. A result here is evidence about this implementation and workload only.

## Status and scope

This is a reviewable benchmark prototype, not a production service.

The implemented path covers:

- a constrained compiler for the sync-rule forms used by the benchmark fixture;
- an initial PostgreSQL scan tied to an exported logical-replication snapshot;
- logical replication for inserts, updates, and deletes;
- MDBX layouts for current bucket state, ordered tail operations, routing indexes, and checkpoint accumulators;
- initial and incremental `/sync/stream` responses for the supported request forms;
- JWT-derived routed subscriptions, including parameter-query buckets;
- exact count, checksum, operation-digest, authorization, and churn checks for selected buckets.

It does not claim:

- full PowerSync rule-language, protocol, SDK, or operational compatibility;
- online migration between storage or sync-rule generations;
- `TRUNCATE` support for materialized tables;
- upload or CRUD APIs;
- partial-sync priority and full subscription-correlation semantics;
- production reliability, disaster recovery, or security validation.

Unsupported layout-changing rule activation and `TRUNCATE` fail closed. The full boundary is documented in [scope](docs/scope.md), [correctness](docs/correctness.md), and [security](SECURITY.md).

Deleting the complete state directory outside the managed reset path also deletes cursor-epoch history; clients must discard saved cursors after that operator action. Parameter queries are concurrency-, time-, and row-bounded but currently open one PostgreSQL connection per evaluation rather than using a pool.

## Design

The logical slot exports the MVCC snapshot used by the initial scan. Replication then resumes from the slot's consistent point, closing the scan-to-WAL handoff gap.

MDBX holds the materialized state and replication tail in one local environment. Writes update current entries, routing indexes, ordered tail operations, and checkpoint accumulators transactionally. `/sync/stream` reads one MDBX transaction, encodes pages lazily, and applies entry, byte, concurrency, and admission-time limits.

Bootstrap state includes the PostgreSQL source, slot, rules, snapshot marker, durable LSN, and cursor epoch. Readiness remains closed until the bootstrap is durable and the configured source identity has been revalidated. An interrupted bootstrap can be reset only when its durable intent matches the inactive slot and source configuration.

TLS and JWT policy also fail closed: TCP PostgreSQL connections require either `verify-full` or an explicit `disable`, and configured JWT keys require exact audience and issuer policy.

## Current evidence

A bounded four-rung canary passed with both targets running as Linux containers on one Docker Desktop network. Each target received an aggregate limit of 4 CPUs and 8 GiB. Rust received the full limit; the official target split it between the PowerSync service (2.5 CPUs/5 GiB) and MongoDB (1.5 CPUs/3 GiB).

The common initial boundary was the first successful `/sync/stream` checkpoint/data/checkpoint-complete proof for the same routed subscription. These are scale canaries, not publication results: each cell is one measured run from an empty target store, every rung ran the official target first and Rust second, and OS and PostgreSQL caches were not flushed. One ordered pair cannot estimate a speedup distribution, tail latency, production reliability, or product-level performance.

| Source task rows | Official service | Rust/MDBX |
| ---: | ---: | ---: |
| 250,202 | 16.11 s | 1.83 s |
| 1,000,402 | 66.20 s | 7.02 s |
| 2,000,802 | 148.71 s | 12.26 s |
| 5,001,002 | 403.16 s | 31.87 s |

Both targets passed the selected-bucket equivalence, authorization-isolation, and incremental churn gates at every rung. The verifier sampled 200, 100, 100, and 50 routed buckets respectively. It compared checkpoint count/checksum recurrence and client-visible PUT/REMOVE semantics, not only readiness timings.

The path-scrubbed symmetric canary summary records the exact timings, tested commit, image inputs, resource split, and gate counts. Compressed validation sidecars were retained locally but are not checked in; the complete local run directories, including database state, total about 13 GiB.

Local release checks passed 227 Rust tests, five live PostgreSQL replication tests, and the Node harness/export/ladder suite, along with formatting, warnings-denied Clippy, dependency audits, the frontend build, Compose validation, and Actionlint.

The host was Docker Desktop on macOS rather than controlled native Linux hardware, the official service/MongoDB resource split has not been reviewed by the PowerSync team, and the local Rust image was identified by image ID rather than a registry digest. The harness revision used for this ladder did not record CPU, memory, I/O, network, storage growth, or WAL volume.

The other compact artifacts under `docs/artifacts/` are older exploratory runs from an asymmetric topology. They predate material correctness and security changes and are not evidence for the current tree. See the [benchmark methodology](docs/benchmark.md) before quoting any result.

## Build and test

Requirements:

- the Rust toolchain pinned in `rust-toolchain.toml`;
- Node.js 20;
- Docker with Compose;
- a C/C++ toolchain, `libclang`, `pg_config`, and PostgreSQL client development libraries.

Run the local checks:

```sh
npm --prefix e2e/official-sdk ci
cargo fmt -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked -q
node --check scripts/user_value_benchmark.mjs
node --check scripts/linux_canary_ladder.mjs
node --check scripts/export_artifacts.mjs
node --check scripts/export_canary_ladder.mjs
node --test scripts/*.test.mjs
npm --prefix e2e/official-sdk audit
npm --prefix e2e/official-sdk run build
cargo audit
```

The five ignored live-replication tests require PostgreSQL with logical WAL enabled. The repository CI configuration shows the required database setup, but the checks above can all be run locally.

## Benchmark workflow

Benchmark cost is intentionally tiered.

1. Ordinary changes run the local checks only.
2. Changes to ingestion, storage, protocol output, readiness, or the harness run a small symmetric-container smoke test.
3. A release candidate runs the bounded 250k/1m/2m/5m ladder once.
4. A public performance claim uses a frozen commit and a separate repeated matrix on controlled native Linux hardware.

Build the Linux benchmark image:

```sh
docker build -f Dockerfile.benchmark -t powersync-mdbx:benchmark .
```

Run a small symmetric smoke test:

```sh
POWERSYNC_USER_VALUE_RUNTIME=symmetric-docker \
POWERSYNC_USER_VALUE_RUST_IMAGE=powersync-mdbx:benchmark \
POWERSYNC_USER_VALUE_RUST_IMAGE_PULL=0 \
POWERSYNC_USER_VALUE_TARGET_CPUS=4 \
POWERSYNC_USER_VALUE_TARGET_MEMORY=8g \
POWERSYNC_USER_VALUE_SERVICE_CPUS=2.5 \
POWERSYNC_USER_VALUE_SERVICE_MEMORY=5g \
POWERSYNC_USER_VALUE_MONGO_CPUS=1.5 \
POWERSYNC_USER_VALUE_MONGO_MEMORY=3g \
POWERSYNC_RUST_ALLOW_COMPARISON=1 \
POWERSYNC_USER_VALUE_PROFILE=smoke \
POWERSYNC_USER_VALUE_PROCESSING_ONLY=1 \
POWERSYNC_USER_VALUE_ACCESS_MODE=auth_perimeter \
POWERSYNC_USER_VALUE_EQUIVALENCE_GATE=1 \
POWERSYNC_USER_VALUE_CHURN_GATE=1 \
POWERSYNC_USER_VALUE_CHURN_GATE_MODE=slot-lsn \
POWERSYNC_USER_VALUE_INITIAL_READINESS=sync-protocol \
POWERSYNC_USER_VALUE_PROJECT_BUCKET_SAMPLES=6 \
POWERSYNC_USER_VALUE_RETAIN_RAW_RECORDS=1 \
node scripts/user_value_benchmark.mjs
```

Run the bounded release ladder from a clean worktree:

```sh
node scripts/linux_canary_ladder.mjs
```

The ladder builds the Rust image, pins the official service, MongoDB, and PostgreSQL images, stops on the first failure, checks compressed raw records and resource evidence, and writes an append-only manifest under `tmp/linux-canary-ladder/`. It requires a Linux Docker server and up to 150 GiB of free disk before the 5m rung. It is a correctness and scale-safety gate, not a statistical performance matrix. On the release-canary host it took 17 minutes 31 seconds and produced about 13 GiB of artifacts; those figures are planning observations, not runtime guarantees.

The current harness records three initial-replication boundaries concurrently:

1. validated checkpoint completion for one routed subscription through `/sync/stream`;
2. target-specific evidence of complete initial source materialization through the fixture LSN;
3. the replication slot's `confirmed_flush_lsn` reaching that LSN.

The first is the common client-visible timing. The second is implementation-specific and must not be presented as a common protocol metric. Official reports an explicit completion flag and LSN; Rust exposes the LSN persisted atomically with its internal snapshot-complete marker, so completion is inferred from that implementation contract. The third records a source slot position, not a consumer acknowledgement or proof that every bucket is materialized.

Each repeat also records per-component CPU, cgroup lifetime peak memory, container init-process lifetime peak RSS, block I/O, network traffic, logical and allocated storage growth, and the cluster-wide inserted WAL-position delta. These high-water marks are lifetime diagnostics, not measurement-window peaks; MongoDB can include provisioning before the baseline. Native Linux uses cgroup v2 and `/proc`; Docker stats is retained only as a diagnostic fallback because it cannot provide cumulative CPU time or peak RSS. Component network counters are not summed because service-to-storage traffic appears in more than one namespace.

Publication runs additionally require a Linux host running the symmetric-container topology, immutable image digests including the Rust image, retained raw records, a clean tree, interleaved target order, warmups, at least 20 measured pairs, native cgroup/proc resource evidence, and explicit review of official-service tuning. `POWERSYNC_USER_VALUE_PUBLIC_RUN=1` enforces those controls. The complete methodology and configuration controls are in [docs/benchmark.md](docs/benchmark.md).

## Repository layout

- `crates/powersync-mdbx/`: compiler, replication, MDBX storage, protocol, and HTTP service;
- `scripts/user_value_benchmark.mjs`: paired benchmark and correctness harness;
- `scripts/linux_canary_ladder.mjs`: bounded release-candidate ladder;
- `scripts/resource_evidence.mjs`: Linux cgroup/proc and storage/WAL accounting;
- `e2e/official-sdk/`: protocol validation using the PowerSync JavaScript packages;
- `docs/`: scope, correctness boundary, methodology, and historical artifacts.

## Contributing

Contributions that improve correctness, benchmark fairness, or the official baseline are welcome. Benchmark changes must document any change to the measured interval, readiness boundary, dataset, protocol gate, deployment topology, or target tuning. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache-2.0. See [LICENSE](LICENSE) and [third-party notices](THIRD-PARTY-NOTICES.md).
