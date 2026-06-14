# Benchmark methodology

## Claim boundary

The harness measures one generated workload and a constrained set of client-visible fields. It does not measure full PowerSync compatibility, production reliability, operational cost, or the causal effect of any single language or storage engine.

The checked-in data is historical and exploratory. It was recorded before the current correctness and security changes, does not identify a Git commit, and used different deployment topologies for the official and Rust targets. It is not evidence of current-tree performance. Do not use the ratios as product-level performance claims.

## Historical topology and current timing controls

- PostgreSQL: one shared `postgres:16` container.
- Official target: PowerSync service and MongoDB in Docker Desktop's Linux VM. The VM reported 4 CPUs and about 4.1 GiB RAM. MongoDB used a host bind mount and its normal journal.
- Rust target: native host process with MDBX on local storage. PostgreSQL traffic crossed a local `socat` forwarder.

Docker image pulls and MongoDB replica-set provisioning happen before measurement. The clock starts before each target's startup sequence. Official timing includes service-container start. Rust timing includes launching and waiting about 250 ms for the local PostgreSQL forwarding container before spawning the native service process. The historical metric ends on each target's own readiness condition, so it is named **target startup sequence to target-specific readiness**. It is not an isolated snapshot-processing clock.

Fresh runs can set `POWERSYNC_USER_VALUE_INITIAL_READINESS=sync-protocol`. That mode stops the initial clock only when the same externally visible `/sync/stream` subscription response reaches `checkpoint_complete` and proves the expected contents of one analytically generated routed project bucket. In `auth_perimeter` mode the fixture adds one access row for the dedicated `user-benchmark-readiness-probe` identity and project 1; its JWT can resolve exactly that bucket. In subscription mode the probe subscribes directly to project 1. The probe specification is built before the clock and scales with rows in one project, not total fixture rows. Persisted-LSN and control-plane readiness remain secondary diagnostics. The default `target-specific` mode exists for diagnosis and is not a publication boundary.

In the historical checked-in runs, the Rust replication slot was created before the measured service start while the official service created its slot during startup. The current harness provisions each target's publication with the fixture before timing, but no longer pre-creates Rust's slot; both slots are now created after the measured start boundary. This correction is another reason the historical timings cannot describe the current harness.

Target stores are empty at the start of a repeat. OS and PostgreSQL caches are not flushed or otherwise controlled.

The opt-in `symmetric-docker` runtime runs both services in Linux containers
on the same Docker network and PostgreSQL address. It applies one explicit
total CPU/memory budget per target: Rust receives the total directly, while
the official target splits the same total between its service and MongoDB.
Both MongoDB and MDBX use bind mounts below the same run artifact root. This
controls process model, aggregate resource ceiling, host filesystem, and
network placement; it does not pretend MongoDB and MDBX have identical engine
semantics. MongoDB provisioning remains outside the measured window because
it is a separate required storage service, while both measured service starts
create a fresh container.

## Workload

The fixture contains tasks, projects, organizations, memberships, comments, and, in `auth_perimeter` mode, a `user_project_access` table. A task can participate in the default task bucket, the dashboard stream, and an auth-routed project bucket. The checked protocol set does not cover every materialized bucket.

The auth-perimeter subscription derives project ids from `auth.user_id()` through `user_project_access`. A separate no-access JWT probe supplies a valid project id through client-controlled parameters and must receive no checkpoint or data buckets.

The retained historical artifacts do not identify their issuer-validation policy, so the spoof probe is evidence about routing authorization only, not parity of the products' complete JWT validation policies. The current harness supplies audience and issuer claims, configures audience validation on both targets and issuer validation on Rust, and records that asymmetry in fresh results.

For initial state, the verifier compares the selected bucket set, expected/observed counts, checkpoint counts and checksums, client operation digests, PUT digests, and semantic digests. Wire bytes are not required to match because source-key encodings are implementation-specific.

For churn, both targets wait for the PostgreSQL replication slot's `confirmed_flush_lsn` to reach the transaction's captured LSN. The verifier then compares selected incremental PUT/REMOVE semantics and validates each target's checkpoint recurrence. Cross-target delete checksums are not required to match because REMOVE checksums include a target-local subkey.

This is exact equivalence for the probed buckets and fields, not strict protocol equivalence.

## Historical checked-in runs

The `100k-auth-perimeter` and `1m-auth-perimeter` runs were recorded on June 14, 2026. They used one unrecorded warmup pair and five measured pairs with interleaved target order. Material changes since that run include snapshot/slot handoff, readiness, cursor recovery, authentication, TLS, tail retention, and response reads; the table must not be attributed to the current implementation.

| Profile | Source task rows | Probed buckets | Official median startup-to-ready | Rust median startup-to-ready | Ratio of medians |
| --- | ---: | ---: | ---: | ---: | ---: |
| `100k` | 100,102 | 251 | 7,035.711 ms | 915.083 ms | 7.689x |
| `1m` | 1,000,402 | 1,000 | 57,886.216 ms | 5,046.112 ms | 11.471x |

The table reports ratios of medians, not paired-repeat speedups. Five samples are enough to expose gross repeatability problems, but not enough to estimate tail latency. Comparison-level p95 fields from the original export were removed during the reproducible presentation normalization; retained per-target timing p95 fields are traceability data and must not be cited as SLO evidence.

The older `1m-parity-gated` artifact is a historical, single-sample subscription-parameter run. It is not comparable to the later auth-perimeter runs and should not be used as supporting evidence.

## Artifact limits

Checked-in artifacts omit raw per-bucket observations and therefore are validation summaries, not independent proof. The original runs recorded mutable Docker tags rather than resolved image digests and did not record Git/file hashes. A fresh harness run now records:

- Git commit and dirty state;
- SHA-256 hashes for both lockfiles, the harness, fixture, generated rules, and Rust executable;
- SHA-256 hashes of the generated official configurations after known secrets are replaced with fixed redaction markers, plus the separately supplied official configuration fragment;
- Rust build/profile controls, JWT audience/issuer policy, and a sanitized PostgreSQL transport/TLS policy;
- resolved Docker image ids and repository digests;
- the exact requested image references for the official service, MongoDB, PostgreSQL, and either the Rust service image or `socat`, depending on the runner.

Regenerating with the same configuration does not reproduce an old run exactly unless the toolchain, image digests, code, storage, host resources, and cache state also match.

Each run uses a unique append-only directory. The harness refuses `POWERSYNC_USER_VALUE_CLEAN_TMP=1`; it never bulk-deletes earlier matrix samples. `POWERSYNC_USER_VALUE_ARTIFACT_ROOT` selects a durable output root. With `POWERSYNC_USER_VALUE_RETAIN_RAW_RECORDS=1`, each validation batch writes a gzip-compressed sidecar containing the per-bucket protocol records while the ordinary JSON result retains compact digests and samples.

## Requirements for a public comparison

A defensible comparison should:

1. run both targets on Linux under the same process/container model;
2. apply the same aggregate CPU and memory budget to each target and record the official service/MongoDB split;
3. use the same storage class and durability expectations;
4. pin all container images by digest;
5. use identical PostgreSQL placement and network paths;
6. define one readiness boundary based on externally observable work;
7. collect at least 20 measured paired repeats and publish every sample;
8. retain raw per-bucket validation records as compressed release assets;
9. include official-service tuning proposed or reviewed by the PowerSync team;
10. rerun the current tree after every material snapshot, cursor, durability, authentication, or readiness change.

`POWERSYNC_USER_VALUE_PUBLIC_RUN=1` checks these conditions before starting PostgreSQL or pulling an image. It also requires a clean worktree, `symmetric-docker` on a Linux host, an explicit matching target budget and official service/MongoDB split, interleaved targets, at least one warmup pair, the initial and churn protocol gates, `slot-lsn`, `sync-protocol`, raw-record retention, digest-pinned official/MongoDB/PostgreSQL/Rust image references, and an explicit attestation that official tuning was reviewed. A locally built Rust tag can exercise the runner but cannot pass publication preflight.

## Command

The native and symmetric canary invocations are in the repository README. Set `POWERSYNC_USER_VALUE_CHURN_GATE_MODE=slot-lsn` for a common PostgreSQL-side churn finish line and `POWERSYNC_USER_VALUE_INITIAL_READINESS=sync-protocol` for the common client-visible initial finish line. Native runs remain exploratory; publication requires the symmetric runner and all preflight controls above.
