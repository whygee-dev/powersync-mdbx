# Benchmark methodology

## Claim boundary

The harness measures initial replication and deterministic churn for one generated workload and a constrained set of client-visible fields. It does not exercise unsupported protocol/rule forms, failover, backup/restore, rolling upgrades, or mixed read/write workloads, and it cannot isolate the effect of one language, storage engine, or architectural choice.

The checked-in `100k-auth-perimeter`, `1m-auth-perimeter`, and `1m-parity-gated` data is historical and exploratory. Those runs predate the current correctness and security changes, do not identify a Git commit, and used different deployment topologies for the official and Rust targets. They are not evidence of current-tree performance. The current [symmetric scale canary](artifacts/symmetric-canary/README.md) identifies its commit, uses a symmetric container topology, and records initial CPU, peak memory, block I/O, per-component network traffic, storage growth, and WAL. It supports the four observed official/Rust elapsed-time ratios for that workload and configuration; one ordered pair per rung does not estimate a performance distribution or tail latency and cannot isolate the contribution of any one component.

## Historical topology and current timing controls

- PostgreSQL: one shared `postgres:16` container.
- Official target: PowerSync service and MongoDB in Docker Desktop's Linux VM. The VM reported 4 CPUs and about 4.1 GiB RAM. MongoDB used a host bind mount and its normal journal.
- Rust target: native host process with MDBX on local storage. PostgreSQL traffic crossed a local `socat` forwarder.

Docker image pulls and MongoDB replica-set provisioning happen before measurement. The clock starts before each target's startup sequence. Official timing includes service-container start. Rust timing includes launching and waiting about 250 ms for the local PostgreSQL forwarding container before spawning the native service process. The historical metric ends on each target's own readiness condition, so it is named **target startup sequence to target-specific readiness**. It is not an isolated snapshot-processing clock.

Fresh runs can set `POWERSYNC_USER_VALUE_INITIAL_READINESS=sync-protocol`. The harness starts three observers from the same clock:

1. **Validated checkpoint completion for one routed subscription.** The same externally visible `/sync/stream` response must reach `checkpoint_complete` and prove the expected contents of one analytically generated routed project bucket. The timing includes downloading and validating that response; it is not the instant at which the first row became usable.
2. **Complete initial source materialization.** Official diagnostics must explicitly report initial replication complete and reach the captured fixture LSN. Rust must expose `last_persisted_end_lsn` at or beyond that LSN; the implementation writes it atomically with its internal snapshot-complete marker, so the harness infers completion from that contract. This is a target-specific boundary, not a common protocol signal.
3. **Replication-slot position.** PostgreSQL must report the target's replication slot `confirmed_flush_lsn` at or beyond the captured fixture LSN. Slot creation can establish this position before later consumer work, so it is not labeled an acknowledgement and does not prove that every bucket is materialized or durable.

In `auth_perimeter` mode the fixture adds one access row for the dedicated `user-benchmark-readiness-probe` identity and project 1; its JWT can resolve exactly that bucket. In subscription mode the probe subscribes directly to project 1. The probe specification is built before the clock and scales with rows in one project, not total fixture rows. The first boundary is the headline client-visible timing. The other two are reported separately and must not be conflated with it. The default `target-specific` mode exists for diagnosis and is not a publication boundary.

In the historical checked-in runs, the Rust replication slot was created before the measured service start while the official service created its slot during startup. The current harness provisions each target's publication with the fixture before timing, but no longer pre-creates Rust's slot; both slots are now created after the measured start boundary. This correction is another reason the historical timings cannot describe the current harness.

Target stores are empty at the start of a repeat. OS and PostgreSQL caches are not flushed or otherwise controlled.

The opt-in `symmetric-docker` runtime runs both services in Linux containers
on the same Docker network and PostgreSQL address. It applies one explicit
total CPU/memory budget per target: Rust receives the total directly, while
the official target splits the same total between its service and MongoDB.
Both MongoDB and MDBX use bind mounts below the same run artifact root. This
controls process model, aggregate resource ceiling, host filesystem, and
network placement. It does not equalize MongoDB and MDBX storage semantics.
MongoDB provisioning remains outside the measured window because
it is a separate required storage service, while both measured service starts
create a fresh container.

## Resource evidence

The symmetric runner captures a baseline immediately before measured service start and snapshots after initial readiness, browser work, initial equivalence, churn, and finalization. Each repeat retains phase deltas and the raw snapshots. Block reads and writes are cgroup-accounted I/O; filesystem storage growth is measured separately from the data paths.

Each Linux container is measured through cgroup v2 and its PID-1 `/proc` namespace: cumulative CPU time, cgroup current and lifetime peak memory, container init-process current and lifetime peak RSS, block reads/writes, and network receive/transmit counters. On native Linux the runner reads those files through the host; on Docker Desktop it reads the same container-scoped files with `docker exec`. Storage paths are walked to record both logical bytes and filesystem-allocated bytes. PostgreSQL insert-LSN distance records the cluster-wide inserted WAL-position delta during each phase.

The official target is reported as separate service and MongoDB components; Rust is reported as one service component. Network traffic is not summed across official components because service-to-MongoDB traffic is visible in both network namespaces. The memory high-water marks are lifetime diagnostics, not measurement-window peaks, and MongoDB can include provisioning before the baseline. WAL position is cluster-wide and is not the number of bytes decoded or stored by either target.

If container cgroup or `/proc` counters are unavailable, the harness falls back to Docker stats: instantaneous CPU percentage, current memory, and cumulative block/network counters. It reports cumulative CPU time and peak RSS as unavailable rather than zero. Publication mode rejects incomplete fallback evidence.

## Workload

The fixture contains tasks, projects, organizations, memberships, comments, and, in `auth_perimeter` mode, a `user_project_access` table. A task can participate in the default task bucket, the dashboard stream, and an auth-routed project bucket. The checked protocol set does not cover every materialized bucket.

The auth-perimeter subscription derives project ids from `auth.user_id()` through `user_project_access`. A separate no-access JWT probe supplies a valid project id through client-controlled parameters and must receive no checkpoint or data buckets.

The retained historical artifacts do not identify their issuer-validation policy, so the spoof probe is evidence about routing authorization only, not parity of the products' complete JWT validation policies. The current harness supplies audience and issuer claims, configures audience validation on both targets and issuer validation on Rust, and records that asymmetry in fresh results.

For initial state, the verifier compares the selected bucket set, expected/observed counts, checkpoint counts and checksums, client operation digests, PUT digests, and semantic digests. Wire bytes are not required to match because source-key encodings are implementation-specific.

For churn, both targets wait for the PostgreSQL replication slot's `confirmed_flush_lsn` to reach the transaction's captured LSN. The verifier then compares selected incremental PUT/REMOVE semantics and validates each target's checkpoint recurrence. Cross-target delete checksums are not required to match because REMOVE checksums include a target-local subkey.

This is exact equivalence for the probed buckets and fields, not strict protocol equivalence.

## Historical checked-in runs

The `100k-auth-perimeter` and `1m-auth-perimeter` runs used one unrecorded warmup pair and five measured pairs with interleaved target order. Material changes since those runs include snapshot/slot handoff, readiness, cursor recovery, authentication, TLS, tail retention, and response reads; the table must not be attributed to the current implementation.

| Profile | Source task rows | Probed buckets | Official median startup-to-ready | Rust median startup-to-ready | Ratio of medians |
| --- | ---: | ---: | ---: | ---: | ---: |
| `100k` | 100,102 | 251 | 7,035.711 ms | 915.083 ms | 7.689x |
| `1m` | 1,000,402 | 1,000 | 57,886.216 ms | 5,046.112 ms | 11.471x |

The table reports ratios of medians, not paired-repeat speedups. The five samples show the recorded run-to-run spread but do not estimate tail latency. Retained per-target timing p95 fields are traceability data, not SLO estimates.

The older `1m-parity-gated` artifact is a historical, single-sample subscription-parameter run. It is not comparable to the later auth-perimeter runs and should not be used as supporting evidence.

## Artifact limits

Checked-in artifacts omit raw per-bucket observations. They preserve aggregate validation outcomes but cannot be revalidated record by record. The original runs recorded mutable Docker tags rather than resolved image digests and did not record Git/file hashes. A fresh harness run now records:

- Git commit and dirty state;
- SHA-256 hashes for both lockfiles, the harness, resource collector, fixture, generated rules, and Rust executable;
- SHA-256 hashes of the generated official configurations after known secrets are replaced with fixed redaction markers, plus the separately supplied official configuration fragment;
- Rust build/profile controls, JWT audience/issuer policy, and a sanitized PostgreSQL transport/TLS policy;
- resolved Docker image ids and repository digests;
- the exact requested image references for the official service, MongoDB, PostgreSQL, and either the Rust service image or `socat`, depending on the runner.

Each run uses a unique append-only directory. The harness refuses `POWERSYNC_USER_VALUE_CLEAN_TMP=1`; it never bulk-deletes earlier matrix samples. `POWERSYNC_USER_VALUE_ARTIFACT_ROOT` selects a durable output root. With `POWERSYNC_USER_VALUE_RETAIN_RAW_RECORDS=1`, each validation batch writes a gzip-compressed sidecar containing the per-bucket protocol records while the ordinary JSON result retains compact digests and samples.

## Requirements for a public comparison

A public comparison must:

1. run both targets on Linux under the same process/container model;
2. apply the same aggregate CPU and memory budget to each target and record the official service/MongoDB split;
3. record each target's storage class and durability policy, then attest the storage classes are the same and the durability policies are comparable;
4. pin all container images by digest;
5. use identical PostgreSQL placement and network paths;
6. report client-visible readiness, target-specific complete materialization, and replication-slot confirmed-flush position as separate boundaries;
7. record CPU, clearly labeled memory high-water marks, block I/O, network traffic, storage growth, and cluster-wide inserted WAL-position deltas for every repeat;
8. collect at least 20 measured paired repeats and publish every sample;
9. retain raw per-bucket validation records as compressed release assets;
10. include official-service tuning proposed or reviewed by the PowerSync team;
11. rerun the current tree after every material snapshot, cursor, durability, authentication, or readiness change.

`POWERSYNC_USER_VALUE_PUBLIC_RUN=1` checks the static conditions before starting PostgreSQL or pulling an image, then rejects a run if any required Linux cgroup/proc resource field is unavailable. It also requires a clean worktree, `symmetric-docker` on a Linux host, an explicit matching target budget and official service/MongoDB split, interleaved targets, at least one warmup pair, the initial and churn protocol gates, `slot-lsn`, `sync-protocol`, raw-record retention, digest-pinned official/MongoDB/PostgreSQL/Rust image references, and explicit attestations that official tuning was reviewed, both targets use the same storage class, and their durability policies are comparable. Record the target-specific values in `POWERSYNC_USER_VALUE_OFFICIAL_STORAGE_CLASS`, `POWERSYNC_USER_VALUE_RUST_STORAGE_CLASS`, `POWERSYNC_USER_VALUE_OFFICIAL_DURABILITY_POLICY`, and `POWERSYNC_USER_VALUE_RUST_DURABILITY_POLICY`; the harness persists all four in `results.json`. Set `POWERSYNC_USER_VALUE_OFFICIAL_TUNING_REVIEWED=1`, `POWERSYNC_USER_VALUE_STORAGE_CLASS_ATTESTED=1`, and `POWERSYNC_USER_VALUE_DURABILITY_POLICY_ATTESTED=1` only after reviewing those controls. A locally built Rust image tag can exercise the runner but cannot pass publication preflight.

## Command

The native and symmetric canary invocations are in the repository README. Set `POWERSYNC_USER_VALUE_CHURN_GATE_MODE=slot-lsn` for a common PostgreSQL-side churn finish line and `POWERSYNC_USER_VALUE_INITIAL_READINESS=sync-protocol` for the common client-visible initial finish line. Native runs remain exploratory; publication requires the symmetric runner and all preflight controls above.

Run `node scripts/official_resource_calibration.mjs` before freezing a matrix configuration. It compares four official-service/MongoDB CPU splits at 250k, keeps the total budget and storage tuning fixed, reverses candidate order on the second pass, and rejects samples without complete initial CPU, memory, cgroup I/O, network, storage-growth, and WAL-position evidence.
