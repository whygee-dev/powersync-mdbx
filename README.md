# powersync-mdbx

An independent Rust/MDBX implementation of PostgreSQL-to-PowerSync bucket replication. It reads a consistent snapshot, materializes bucket state, follows logical replication, and serves the covered data through `/sync/stream`.

This project is not affiliated with, endorsed by, or supported by PowerSync or Journey Mobile, Inc. It is not a fork of the PowerSync service or a drop-in replacement. The PowerSync name identifies the protocol and service used for comparison.

## Motivation

Initial replication has been an operational bottleneck in a large deployment. This repository implements and evaluates an embedded ordered store and purpose-built materialization path for reducing the time from service start to protocol-observable readiness.

The comparison changes the language, runtime, storage engine, data layout, and parts of the service architecture. It cannot isolate MongoDB, Node.js, Rust, MDBX, or any other component as the cause of a measured difference. A result here is evidence about this implementation and workload only.

## Status and scope

This is a working, correctness-gated implementation of the covered replication path and a benchmark platform for evaluating it. The implemented surface is narrower than the official service and is listed below.

The implemented path covers:

- a constrained compiler for the sync-rule forms used by the benchmark fixture;
- an initial PostgreSQL scan tied to an exported logical-replication snapshot;
- logical replication for inserts, updates, and deletes;
- MDBX layouts for current bucket state, ordered tail operations, routing indexes, and checkpoint accumulators;
- initial and incremental `/sync/stream` responses for the supported request forms;
- JWT-derived routed subscriptions, including parameter-query buckets;
- exact count, checksum, operation-digest, authorization, and churn checks for selected buckets.

Out of scope:

- full PowerSync rule-language, protocol, SDK, or operational compatibility;
- online migration between storage or sync-rule generations;
- `TRUNCATE` support for materialized tables;
- PostgreSQL publication row filters, omitted columns, `publish_via_partition_root`, or partition/inheritance parent source tables;
- upload or CRUD APIs;
- partial-sync priority and full subscription-correlation semantics.

Unsupported layout-changing rule activation, publication transformations, and `TRUNCATE` fail closed. The full boundary is documented in [scope](docs/scope.md), [correctness](docs/correctness.md), and [security](SECURITY.md).

Deleting the complete state directory outside the managed reset path also deletes cursor-epoch history; clients must discard saved cursors after that operator action. Parameter queries are concurrency-, time-, and row-bounded but currently open one PostgreSQL connection per evaluation rather than using a pool.

## Design

The logical slot exports the MVCC snapshot used by the initial scan. Replication then resumes from the slot's consistent point, closing the scan-to-WAL handoff gap.

MDBX holds the materialized state and replication tail in one local environment. Writes update current entries, routing indexes, ordered tail operations, and checkpoint accumulators transactionally. Each `/sync/stream` checkpoint pass reads all requested buckets in one short MDBX transaction, encodes pages lazily, and applies entry, byte, concurrency, and admission-time limits.

Bootstrap state includes the PostgreSQL source, slot, rules, snapshot marker, durable LSN, and cursor epoch. In unified mode, readiness opens only after the bootstrap is durable, source identity and publication coverage are revalidated, and logical replication is connected; it closes when the replication runner exits. An interrupted bootstrap can be reset only when its durable intent matches the inactive slot and source configuration.

TLS and JWT policy also fail closed: TCP PostgreSQL connections require either `verify-full` or an explicit `disable`, and configured JWT keys require exact audience and issuer policy.

## Recorded scale canary (previous baseline)

This canary predates the current calibration harness and candidate official baseline. Both targets ran as Linux containers on one Docker Desktop network; each target had the same aggregate limit of 4 CPUs and 8 GiB. The recorded official target used the previous split: 2.5 CPUs/5 GiB for the PowerSync service and 1.5 CPUs/3 GiB for MongoDB. The current candidate instead assigns 1.5 CPUs/2 GiB to the service and 2.5 CPUs/6 GiB to MongoDB; the table below has not been rerun with that allocation.

The common initial boundary was the first successful `/sync/stream` checkpoint/data/checkpoint-complete proof for the same routed subscription. Each cell is one measured scale-canary run from an empty target store. Every rung ran the official target first and Rust second; OS and PostgreSQL caches were not flushed.

| Source task rows | Official service | Rust/MDBX |
| ---: | ---: | ---: |
| 250,202 | 16.11 s | 1.83 s |
| 1,000,402 | 66.20 s | 7.02 s |
| 2,000,802 | 148.71 s | 12.26 s |
| 5,001,002 | 403.16 s | 31.87 s |

The official service's elapsed time was 8.80x, 9.43x, 12.13x, and 12.65x the Rust/MDBX elapsed time across the four rungs. Those ratios describe these four runs. A repeated, counterbalanced matrix is required to estimate a performance distribution or tail latency.

Both targets passed the selected-bucket equivalence, authorization-isolation, and incremental churn gates at every rung. The verifier sampled 200, 100, 100, and 50 routed buckets respectively. It compared checkpoint count/checksum recurrence and client-visible PUT/REMOVE semantics, not only readiness timings.

The path-scrubbed symmetric canary summary records the exact timings, tested commit, image inputs, resource split, and gate counts. Compressed validation sidecars were retained locally but are not checked in.

Local release checks passed 245 Rust tests, six live PostgreSQL replication tests, and 65 Node harness/export/ladder tests, along with formatting, warnings-denied Clippy, dependency audits, and the frontend build.

The host was Docker Desktop on macOS rather than controlled native Linux hardware, the service/MongoDB split was not reviewed by the PowerSync team, and the local Rust image was identified by image ID rather than a registry digest. The harness revision used for this ladder did not record CPU, memory, I/O, network, storage growth, or WAL volume. The current candidate baseline and resource collector postdate these results, so the recorded ratios do not describe the new configuration.

The other compact artifacts under `docs/artifacts/` are older exploratory runs from an asymmetric topology. They predate material correctness and security changes and are not evidence for the current tree. See the [benchmark methodology](docs/benchmark.md) before quoting any result.

## Build and test

Requirements:

- the Rust toolchain pinned in `rust-toolchain.toml`;
- Node.js 24 LTS;
- Docker with Compose;
- PostgreSQL 13 or newer for a replication source; the benchmark and live suite use PostgreSQL 16;
- a C/C++ toolchain, `libclang`, `pg_config`, and PostgreSQL client development libraries.

Run the local checks:

```sh
npm --prefix e2e/official-sdk ci
cargo fmt -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked -q
node --check scripts/user_value_benchmark.mjs
node --check scripts/resource_evidence.mjs
node --check scripts/official_resource_calibration.mjs
node --check scripts/linux_canary_ladder.mjs
node --check scripts/export_artifacts.mjs
node --check scripts/export_canary_ladder.mjs
node --test scripts/*.test.mjs
npm --prefix e2e/official-sdk audit
npm --prefix e2e/official-sdk run build
cargo audit
```

The six ignored live-replication tests require PostgreSQL with logical WAL enabled. The repository CI configuration shows the required database setup, but the checks above can all be run locally.

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
POWERSYNC_USER_VALUE_SERVICE_CPUS=1.5 \
POWERSYNC_USER_VALUE_SERVICE_MEMORY=2g \
POWERSYNC_USER_VALUE_MONGO_CPUS=2.5 \
POWERSYNC_USER_VALUE_MONGO_MEMORY=6g \
POWERSYNC_USER_VALUE_MONGO_CACHE_GB=2 \
POWERSYNC_USER_VALUE_OFFICIAL_NODE_OPTIONS=--max-old-space-size-percentage=80 \
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

This command uses the current candidate official baseline: 1.5 CPU/2 GiB for the service and 2.5 CPU/6 GiB for MongoDB, with a 2 GiB WiredTiger cache. It is not the configuration used by the previous-baseline table.

Calibrate the official service/MongoDB CPU split at 250k before an expensive matrix:

```sh
node scripts/official_resource_calibration.mjs
```

The calibration holds the aggregate 4 CPU/8 GiB budget, service and MongoDB memory limits, WiredTiger cache, dataset, and correctness gates constant. It runs the 1/3, 1.5/2.5, 2/2, and 2.5/1.5 CPU splits twice in opposite orders and retains complete initial resource evidence for every sample.

Run the bounded release ladder from a clean worktree:

```sh
node scripts/linux_canary_ladder.mjs
```

The ladder builds the Rust image, pins the official service, MongoDB, and PostgreSQL images, stops on the first failure, checks compressed raw records and resource evidence, and writes an append-only manifest under `tmp/linux-canary-ladder/`. It requires a Linux Docker server and checks for 150 GiB of free disk before the 5m rung. It is a correctness and scale-safety gate, not a statistical performance matrix. The recorded previous-baseline canary took 17 minutes 31 seconds and produced about 13 GiB of artifacts; those figures do not estimate the cost of the current harness and allocation.

The current harness records three initial-replication boundaries concurrently:

1. validated checkpoint completion for one routed subscription through `/sync/stream`;
2. target-specific evidence of complete initial source materialization through the fixture LSN;
3. the replication slot's `confirmed_flush_lsn` reaching that LSN.

The first is the common client-visible timing. The second is implementation-specific and must not be presented as a common protocol metric. The official service reports an explicit completion flag and LSN; Rust exposes the LSN persisted atomically with its internal snapshot-complete marker, so completion is inferred from that implementation contract. The third records a source slot position, not a consumer acknowledgement or proof that every bucket is materialized.

Each repeat also records per-component CPU, cgroup lifetime peak memory, container init-process lifetime peak RSS, block I/O, network traffic, logical and allocated storage growth, and the cluster-wide inserted WAL-position delta. These high-water marks are lifetime diagnostics, not measurement-window peaks; MongoDB can include provisioning before the baseline. The runner reads container cgroup v2 and `/proc` counters directly, using `docker exec` when the Docker host is not local. Docker stats remains an incomplete fallback. Component network counters are not summed because service-to-storage traffic appears in more than one namespace.

Publication runs additionally require a Linux host running the symmetric-container topology, immutable image digests including the Rust image, retained raw records, a clean tree, interleaved target order, warmups, at least 20 measured pairs, complete Linux cgroup/proc resource evidence, and explicit attestations for official-service tuning, storage class, and durability policy. `POWERSYNC_USER_VALUE_PUBLIC_RUN=1` enforces the machine-checkable controls, requires target-specific storage and durability descriptions, and persists them in the artifact. The complete methodology and configuration controls are in [docs/benchmark.md](docs/benchmark.md).

## Repository layout

- `crates/powersync-mdbx/`: compiler, replication, MDBX storage, protocol, and HTTP service;
- `scripts/user_value_benchmark.mjs`: paired benchmark and correctness harness;
- `scripts/official_resource_calibration.mjs`: counterbalanced 250k official-service resource calibration;
- `scripts/linux_canary_ladder.mjs`: bounded release-candidate ladder;
- `scripts/resource_evidence.mjs`: Linux cgroup/proc and storage/WAL accounting;
- `e2e/official-sdk/`: protocol validation using the PowerSync JavaScript packages;
- `docs/`: scope, correctness boundary, methodology, and historical artifacts.

## Contributing

Contributions that improve correctness, benchmark fairness, or the official baseline are welcome. Benchmark changes must document any change to the measured interval, readiness boundary, dataset, protocol gate, deployment topology, or target tuning. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache-2.0. See [LICENSE](LICENSE) and [third-party notices](THIRD-PARTY-NOTICES.md).
