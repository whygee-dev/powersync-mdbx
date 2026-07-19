# Official-service CPU profile, 5m fixture

This artifact records a diagnostic V8 CPU attribution for the official PowerSync 1.23.3 service during initial replication of the 5,001,002-task-row fixture, captured on commit `170a2f16903b11bb1218316ce98c3964e2d0bf25` with the profiling recipe in [docs/benchmark.md](../../benchmark.md). It is a diagnostic, not benchmark evidence: the topology differs from the symmetric canary and no headline claim or cross-target ratio rests on it.

## Run configuration

The harness ran `scripts/user_value_benchmark.mjs` with profile `5m`, the official target only, processing-only mode, and `POWERSYNC_USER_VALUE_OFFICIAL_PROFILE_DIR` set; no other benchmark variables were set. Each of the three measured iterations loaded the fixture into PostgreSQL, started the official service and MongoDB from an empty store, and waited for the service's own initial-replication completion report. The equivalence and churn gates were off. Completion took 437.3 s, 472.2 s, and 447.3 s (harness p50 449.2 s).

Differences from the canary:

- The service and MongoDB containers ran without CPU or memory limits on the Docker Desktop VM (Linux/aarch64, 6 CPUs, 16,748,593,152 bytes). The symmetric canary caps the official target at 1.5 CPUs/2 GiB for the service and 2.5 CPUs/6 GiB for MongoDB, so the idle share below is not an artifact of CPU throttling.
- MongoDB ran without an explicit WiredTiger cache size, and the service without `--max-old-space-size-percentage=80`.
- Access mode was `subscription`: the same task/project fixture and routed streams, but without the `user_project_access` table and auth-perimeter parameter query the canary's `auth_perimeter` mode adds.
- `--cpu-prof --cpu-prof-dir=/profiles` was appended to the service's `NODE_OPTIONS`, and the container was stopped with `docker stop -t 60` so Node flushed each profile on graceful shutdown. Profiler overhead is included in the timings above; the unprofiled canary recorded 356.0 s for the same completion boundary at this rung.

## Attribution

`scripts/profile_rollup.mjs` attributes each sample's forward time delta to a category by frame name and source URL; [rollup.md](rollup.md) is its verbatim per-profile and aggregate output. The figures below are generated from the same profiles by `scripts/profile_charts.mjs`, which reuses the rollup's categorization.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="attribution-dark.svg">
  <img alt="Official service main thread: idle 45.1% of profiled time; of active CPU, BSON + MongoDB driver 32.9%, row processing 22.3%, GC 7.2%, runtime and dependencies 37.6%" src="attribution.svg" width="920">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="flamegraph-dark.svg">
  <img alt="Flamegraph of the merged active CPU time, dominated by BSON and MongoDB driver serialization under the service's replication and storage-batching frames" src="flamegraph.svg" width="960">
</picture>

Aggregate over the three profiles, 1,363.9 s of profiled self time:

| Category | Self time (ms) | Share |
| --- | ---: | ---: |
| idle | 614,447.6 | 45.1% |
| bson | 205,837.3 | 15.1% |
| node core | 121,261.0 | 8.9% |
| native builtins | 97,773.4 | 7.2% |
| gc | 54,294.0 | 4.0% |
| jsonbig | 51,792.1 | 3.8% |
| mongo storage | 43,016.3 | 3.2% |
| mongodb driver | 40,513.7 | 3.0% |
| postgres replication | 35,159.1 | 2.6% |
| sync-rules | 32,255.1 | 2.4% |
| other | 25,546.8 | 1.9% |
| program | 21,644.0 | 1.6% |
| logging | 15,196.6 | 1.1% |
| other service code | 5,152.0 | 0.4% |

Named categories cover 98.1% of self time; the residual `other` is dominated by `date-fns` ISO-date parsing and `uuid` parsing of source rows. Per-iteration idle was 44.3–45.7%. A prior run of the same recipe agreed with this one within 0.2 percentage points on every category.

Grouped, as arithmetic on the table: `idle` — the main thread off-CPU — is 45.1% of profiled time. Of the active remainder, `bson` plus `mongodb driver`, which marshal records to and from bucket storage, are 32.9%; `postgres replication`, `sync-rules`, `jsonbig`, `mongo storage`, and `other service code` — the service's own decode, rule-evaluation, serialization, and storage-batching code — are 22.3%; `gc` is 7.2%; and `native builtins`, `node core`, `program`, `logging`, and `other` — runtime and utility dependencies — are 37.6%.

## Decomposition

`scripts/profile_decompose.mjs` re-attributes the runtime buckets to the nearest categorized caller on each sampled stack and breaks the idle time into runs; [decompose.md](decompose.md) is its verbatim output. Nearest-caller attribution is a stack heuristic, not a measurement of causation, and idle runs shorter than the sampling interval are under-counted.

Counted by caller, 47.6% of active CPU sits under BSON/driver call trees: the 32.9% self time plus the native-builtin work they invoke, mostly `utf8ByteLength` (5.5% of active), `writeCommand`, `toHex`, and UTF-8 encoding. The service's own call trees carry 36.9%; inside them, `node:internal/crypto` alone is 8.6% of active CPU, called almost entirely from the service's `hashData` (5.3%), `uuidForRowBson` (1.6%), and `hashDelete` (1.5%) — per-row hashing and stable-ID derivation — and `date-fns` ISO parsing plus `uuid` string parsing of source rows dominate the uncategorized remainder. GC is 7.2%, logging 2.8%, and 5.6% is runtime work with no categorized caller.

Over the first 90% of wall clock the thread is 41.6% idle across roughly sixty idle runs per second; the final 10%, the drain into the completion report, is 76.0% idle and lifts the overall share to 45.1%. 91.5% of idle time sits in runs of 2–100 ms, and 68.0% immediately follows a marshalling frame — many short waits interleaved with driver work, consistent with storage round trips. The profile records what ran before each wait, not what it blocked on.

## Scope

The three profiles are the main thread of container PID 1, one per iteration; no other `.cpuprofile` file was produced, and a worker thread or child Node process would have written its own. Each profile spans its container lifetime, 439.3 s to 474.7 s, within about 2.5 s of the corresponding completion window, so the shares describe the replication window rather than idle tails.

The profile covers only the service process's main JavaScript thread. libuv pool and V8 background threads are not sampled, and `gc` is garbage collection observed on the profiled thread. MongoDB's server-side cost is invisible here; at the symmetric canary's 5m rung MongoDB consumed slightly more than half of the official target's total CPU. The idle share is off-CPU time, and the profiler cannot distinguish MongoDB round-trip waits from PostgreSQL stream waits or timer idle.

## Provenance

Images: `journeyapps/powersync-service@sha256:b6b22fa7d0d862f04bdff62846e656756d17bcf3dd6eca399a0633671051438b` (image ID `sha256:ebf0356eac30dab03174119a87670a0eaf24d4abec7abe14b1c35c56164266fe`), `mongo@sha256:d5b3ca8c3f3cdce78d44870dc0871b76d5235e9b2ad4ea6bea5d1fbff8027703` (image ID `sha256:525ab710fa91fefe0b12238d16ccc10e541dcc7145d0c29074cf8a123a892798`), and `postgres:16` (repository digest `sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20`). The official and MongoDB digests are the ones pinned by the symmetric canary. The harness ran commit `170a2f16903b11bb1218316ce98c3964e2d0bf25` with a clean tree; the Docker server reported version 29.4.2, Linux/aarch64.

The raw profiles are not checked into Git. Reproduction:

```sh
POWERSYNC_USER_VALUE_PROFILE=5m \
POWERSYNC_USER_VALUE_TARGETS=official \
POWERSYNC_USER_VALUE_PROCESSING_ONLY=1 \
POWERSYNC_USER_VALUE_OFFICIAL_PROFILE_DIR="$PWD/tmp/profiles/headline-5m" \
node scripts/user_value_benchmark.mjs
node scripts/profile_rollup.mjs tmp/profiles/headline-5m
node scripts/profile_decompose.mjs tmp/profiles/headline-5m
node scripts/profile_charts.mjs tmp/profiles/headline-5m docs/artifacts/official-cpu-profile-5m
```
