# powersync-mdbx

An independent Rust and MDBX research prototype for one PowerSync-shaped workload: materializing PostgreSQL rows into bucketed sync state and serving that state over `/sync/stream`.

This project is not affiliated with, endorsed by, or supported by PowerSync or Journey Mobile, Inc. It is not a fork of the PowerSync service and is not a drop-in replacement. The PowerSync name is used only to identify the protocol and product used for comparison.

## Why this exists

Initial replication time has been a persistent operational problem in a large deployment. This repository explores a narrow architecture question: can an embedded ordered store reduce the cost of building and reading bucket state?

The experiment changes several variables at once: language, runtime, storage engine, data layout, and parts of the service architecture. Its results cannot establish that MongoDB, Node.js, or the official PowerSync architecture is the cause of any measured difference. They are evidence that this alternative implementation is worth examining, not evidence that the official service uses the wrong tool.

MDBX stores current bucket entries, incremental operations, and checkpoint count/checksum accumulators. PostgreSQL logical replication supplies changes after the initial scan.

## Status

This is a benchmark prototype for engineering review. It is not suitable for production use.

Implemented:

- a constrained sync-rule compiler and execution plan;
- PostgreSQL snapshot and logical-replication ingestion;
- durable MDBX materialization;
- initial and incremental `/sync/stream` responses for the covered rule forms;
- JWT-derived routed subscriptions, including parameter-query buckets;
- PowerSync-style PUT/REMOVE and checkpoint recurrence checks;
- a paired harness against a configurable official PowerSync service image.

The current implementation addresses the specific data-loss windows identified during release review:

- the logical slot exports the exact MVCC snapshot used for the initial scan, and replication resumes from that slot's consistent point;
- readiness and `/sync/stream` stay closed until the snapshot marker and LSN are durable and the unified process has revalidated the configured PostgreSQL source identity;
- interrupted bootstrap recovery is authorized by a durable source/slot/rules fingerprint, resets partial MDBX state, and moves cursors into a later epoch;
- tail operations use one global order, route-changing updates are indexed in both old and new buckets, and stale or pruned cursors receive a clearing snapshot;
- every multi-bucket response reads one MDBX transaction; protocol pages are encoded lazily, with configurable entry and byte budgets;
- concurrent sync reads are admission-bounded before they scan storage;
- TCP PostgreSQL policy is explicit (`verify-full` or deliberate `disable`), and configured JWT keys require exact audience and issuer policy.

This still is not a service release. Online layout-changing rule deployment is rejected because there is no snapshot/catch-up/atomic generation swap. `TRUNCATE` on a materialized table is rejected. Deleting the entire state directory outside the managed reset path also deletes cursor-epoch history, so clients must discard old cursors after such an operator action. Parameter queries are concurrency-, time-, and row-bounded but currently open one source connection per query rather than using a pool. See [scope](docs/scope.md), [correctness](docs/correctness.md), and [security](SECURITY.md).

Resource ceilings can be adjusted with `POWERSYNC_RUST_MAX_CONCURRENT_SYNC_READS` (default `8`), `POWERSYNC_RUST_SYNC_READ_ADMISSION_TIMEOUT_MS` (default `2000`), `POWERSYNC_RUST_MAX_SYNC_READ_ENTRIES` (default `250000`), and `POWERSYNC_RUST_MAX_SYNC_READ_BYTES` (default `134217728`). Raising them increases the memory and read-transaction exposure of each process.

## Benchmark interpretation

The checked-in runs are historical exploratory measurements from June 2026. They predate the current snapshot, readiness, authentication, TLS, and cursor fixes, and the retained artifacts do not identify a Git commit. They are not measurements of the current tree and are not a fair product benchmark because the deployment topology is asymmetric:

- the official service and MongoDB ran inside Docker Desktop's Linux VM, limited to 4 CPUs and about 4.1 GiB RAM, with MongoDB journaling through a host bind mount;
- the Rust process ran natively on the host and wrote MDBX to local storage.

The historical measurements begin immediately before each target's startup sequence and stop at target-specific readiness signals. Official timing includes service-container start. Rust timing includes launching and waiting about 250 ms for the local PostgreSQL forwarding container before the native service process is spawned. The historical metric is therefore **target startup sequence to target-specific readiness**, not pure snapshot-processing time. Empty target stores were used, but OS and PostgreSQL caches were not controlled.

Both targets read the same PostgreSQL fixture. Target order was interleaved, one unrecorded warmup pair preceded five measured pairs, and post-churn catch-up used each replication slot's `confirmed_flush_lsn`. Protocol comparisons cover the selected buckets and fields; they are not proof of full protocol or rule compatibility.

Checked-in results from that topology:

| Profile | Source task rows | Buckets compared | Official median startup-to-ready | Rust median startup-to-ready | Ratio of medians |
| --- | ---: | ---: | ---: | ---: | ---: |
| `100k` | 100,102 | default `tasks` plus 250 routed buckets | 7,035.711 ms | 915.083 ms | 7.689x |
| `1m` | 1,000,402 | 1,000 routed buckets | 57,886.216 ms | 5,046.112 ms | 11.471x |

At `n=5`, these medians describe the checked-in runs only. Comparison-level p95 presentation was removed because five samples cannot support a tail-latency comparison; retained per-target timing p95 fields are traceability data, not SLO evidence. A publishable performance claim requires a symmetric Linux rerun with the same resource limits, storage class, process topology, immutable image digests, and a larger sample count.

The `100k` churn transaction applies 2,500 inserts, 2,500 updates, and 2,500 deletes. The verifier observes 15,000 bucket-visible operations because each mutation is present in both the default and routed bucket. The `1m` run applies 10,000 of each mutation and checks 30,000 routed-bucket operations.

Artifacts are under `docs/artifacts/`. They are compact validation summaries: the raw per-bucket records are not checked in, and the older `1m-parity-gated` directory is retained only as a historical subscription-parameter run. See [benchmark methodology](docs/benchmark.md) before quoting any number.

## Run the harness

Requirements:

- the Rust toolchain from `rust-toolchain.toml`;
- Node.js 20;
- Docker with Compose;
- a C/C++ build toolchain, `libclang`, `pg_config`, and PostgreSQL client development libraries for the Rust native dependencies.

The harness invokes `psql` inside the PostgreSQL container; a host `psql` installation is not required.

The command below keeps the native-Rust runner for local diagnosis. For a
symmetric container canary, first build the Rust image and add the runtime and
resource controls shown after the command.

```sh
POWERSYNC_USER_VALUE_PROFILE=1m \
POWERSYNC_USER_VALUE_TARGETS=official,rust \
POWERSYNC_OFFICIAL_IMAGE=journeyapps/powersync-service@sha256:b6b22fa7d0d862f04bdff62846e656756d17bcf3dd6eca399a0633671051438b \
POWERSYNC_USER_VALUE_MONGO_IMAGE=mongo@sha256:d5b3ca8c3f3cdce78d44870dc0871b76d5235e9b2ad4ea6bea5d1fbff8027703 \
POWERSYNC_USER_VALUE_SOCAT_IMAGE=alpine/socat@sha256:d85531a29ef5ba99dfb4717485c239307e2902d522a1bc010992a2728c92cfad \
POWERSYNC_USER_VALUE_POSTGRES_IMAGE=postgres@sha256:be01cf82fc7dbba824acf0a82e150b4b360f3ff93c6631d7844af431e841a95c \
POWERSYNC_RUST_ALLOW_COMPARISON=1 \
POWERSYNC_USER_VALUE_PROCESSING_ONLY=1 \
POWERSYNC_USER_VALUE_ACCESS_MODE=auth_perimeter \
POWERSYNC_USER_VALUE_EQUIVALENCE_GATE=1 \
POWERSYNC_USER_VALUE_CHURN_GATE=1 \
POWERSYNC_USER_VALUE_CHURN_GATE_MODE=slot-lsn \
POWERSYNC_USER_VALUE_INITIAL_READINESS=sync-protocol \
POWERSYNC_USER_VALUE_PROJECT_BUCKET_SAMPLES=1000 \
POWERSYNC_USER_VALUE_CHURN_ROWS_PER_BUCKET=10 \
POWERSYNC_USER_VALUE_LIFECYCLE_REPEATS=0 \
POWERSYNC_USER_VALUE_BROWSER_ITERATIONS=1 \
POWERSYNC_USER_VALUE_END_USER_REPEATS=5 \
POWERSYNC_USER_VALUE_BUCKET_PROBE_BATCH_SIZE=25 \
POWERSYNC_USER_VALUE_TIMEOUT_MS=3600000 \
POWERSYNC_USER_VALUE_WARMUP_PAIRS=1 \
POWERSYNC_USER_VALUE_RETAIN_RAW_RECORDS=1 \
node scripts/user_value_benchmark.mjs
```

```sh
docker build -f Dockerfile.benchmark -t powersync-mdbx:benchmark .

POWERSYNC_USER_VALUE_RUNTIME=symmetric-docker \
POWERSYNC_USER_VALUE_RUST_IMAGE=powersync-mdbx:benchmark \
POWERSYNC_USER_VALUE_RUST_IMAGE_PULL=0 \
POWERSYNC_USER_VALUE_TARGET_CPUS=6 \
POWERSYNC_USER_VALUE_TARGET_MEMORY=12g \
POWERSYNC_USER_VALUE_SERVICE_CPUS=4 \
POWERSYNC_USER_VALUE_SERVICE_MEMORY=8g \
POWERSYNC_USER_VALUE_MONGO_CPUS=2 \
POWERSYNC_USER_VALUE_MONGO_MEMORY=4g \
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

The total target budget must equal the official split: target CPU equals
service CPU plus MongoDB CPU, and target memory equals service memory plus
MongoDB memory. Rust receives the total budget directly. Both services use the
same Docker network and PostgreSQL address, and both stores are bind-mounted
below the run's artifact directory. MongoDB remains part of the official
architecture; its provisioning stays outside the measured startup window.

After the symmetric smoke passes, run the bounded scale canaries with:

```sh
node scripts/linux_canary_ladder.mjs
```

The ladder stops on the first failure and records an append-only manifest under
`tmp/linux-canary-ladder`. It intentionally samples 200, 100, 100, and 50
routed project buckets at `250k`, `1m`, `2m`, and `5m`; it is a scale-safety
check, not a full-coverage or statistically meaningful performance matrix.

`sync-protocol` ends the initial timing only after the same `/sync/stream` subscription checkpoint/data/checkpoint-complete proof succeeds for either target. In `auth_perimeter` mode, a dedicated benchmark identity has exactly one access row for project 1, so the readiness request resolves one small routed bucket. Target-specific diagnostics remain in the artifact but are not the headline boundary. The default `target-specific` mode remains available for local diagnosis. Deterministic protocol mismatches fail immediately; transient HTTP/network failures use bounded, rate-limited retries controlled by `POWERSYNC_USER_VALUE_PROTOCOL_READINESS_ATTEMPTS`.

The harness binds temporary host ports to `127.0.0.1`, records Git/file/image provenance, and removes its containers on normal exit or termination. Run directories are append-only under `tmp/user-value-benchmark`; set `POWERSYNC_USER_VALUE_ARTIFACT_ROOT` to retain a matrix elsewhere. `POWERSYNC_USER_VALUE_CLEAN_TMP=1` is rejected rather than deleting earlier samples.

Container inputs are configurable with `POWERSYNC_OFFICIAL_IMAGE`, `POWERSYNC_USER_VALUE_MONGO_IMAGE`, `POWERSYNC_USER_VALUE_POSTGRES_IMAGE`, and either `POWERSYNC_USER_VALUE_RUST_IMAGE` for the symmetric runner or `POWERSYNC_USER_VALUE_SOCAT_IMAGE` for the native runner. The local default official baseline is the stable `1.23.3` multi-platform manifest resolved on July 11, 2026; publication still requires deliberate baseline review and full `name@sha256:<digest>` references for every image. `POWERSYNC_USER_VALUE_RETAIN_RAW_RECORDS=1` writes gzip-compressed per-batch protocol records. JWTs are minted when each probe or browser run starts, with a lifetime derived from the request timeout; `POWERSYNC_USER_VALUE_JWT_TTL_SECONDS` can set a longer explicit lifetime.

`POWERSYNC_USER_VALUE_PUBLIC_RUN=1` enables a fail-fast publication preflight. A publication run requires `symmetric-docker` on a Linux host, an explicit matching total-resource split, digest-pinned official/MongoDB/PostgreSQL/Rust images, a clean worktree, at least 20 measured pairs, retained raw records, and the other gates described in the methodology. A locally built tag is suitable for smoke testing, not publication. Set `POWERSYNC_USER_VALUE_DEBUG_KEEP=1` only when deliberately retaining the runtime for debugging.

## Development checks

```sh
npm --prefix e2e/official-sdk ci
cargo fmt -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked -q
node --check scripts/user_value_benchmark.mjs
node --check scripts/linux_canary_ladder.mjs
node --check scripts/export_artifacts.mjs
node --test scripts/user_value_benchmark.test.mjs scripts/export_artifacts.test.mjs scripts/linux_canary_ladder.test.mjs
npm --prefix e2e/official-sdk audit
npm --prefix e2e/official-sdk run build
cargo audit
```

CI also runs the five ignored live-replication tests against PostgreSQL configured with logical WAL.

## License

Apache-2.0. See [LICENSE](LICENSE) and [third-party notices](THIRD-PARTY-NOTICES.md).
