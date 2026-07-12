# Symmetric scale canary

This artifact records one passing 250k/1m/2m/5m scale ladder for commit `c43e1ce963d737dda25f883661531e58f98f6535`. It is a correctness and scale canary with one measured run per target and rung, not a repeated performance matrix.

Both targets ran in Linux containers on the same Docker Desktop network. Each target had an aggregate limit of 4 CPUs and 8 GiB. Rust received the full limit. The official PowerSync 1.23.3 target split it into 1.5 CPUs/2 GiB for the service and 2.5 CPUs/6 GiB for MongoDB; WiredTiger used a 2 GiB cache. That allocation came from the repository's local calibration harness and was not reviewed by the PowerSync team.

## Initial replication

Protocol readiness is the first successful expected-state proof for one routed subscription through `/sync/stream` and `checkpoint_complete`. Complete materialization is a separate target-specific boundary. The official service reports initial replication completion and its LSN; Rust persists the source LSN atomically with its internal snapshot-complete marker.

| Source task rows | Official protocol readiness | Rust/MDBX protocol readiness | Official / Rust |
| ---: | ---: | ---: | ---: |
| 250,202 | 19.262 s | 2.080 s | 9.260x |
| 1,000,402 | 81.065 s | 7.223 s | 11.223x |
| 2,000,802 | 163.499 s | 13.471 s | 12.137x |
| 5,001,002 | 407.988 s | 33.663 s | 12.120x |

Every rung ran the official target first and Rust second from an empty target store. OS and PostgreSQL caches were not flushed. The ratios describe these runs only.

| Source task rows | Official complete-materialization diagnostic | Rust/MDBX complete-materialization diagnostic |
| ---: | ---: | ---: |
| 250,202 | 18.769 s | 1.987 s |
| 1,000,402 | 80.723 s | 6.522 s |
| 2,000,802 | 163.090 s | 12.615 s |
| 5,001,002 | 407.533 s | 33.517 s |

The completion observers use different implementation contracts, so no cross-target ratio is computed from this table.

## Correctness gates

Both targets passed the initial-state and incremental-churn gates at every rung. The verifier compared the selected bucket set, expected and observed counts, checkpoint counts and checksums, client operation digests, PUT digests, semantic digests, authorization isolation, and incremental PUT/REMOVE semantics.

| Source task rows | Routed buckets | Initial PUTs per target | Churn PUTs per target | Churn REMOVEs per target |
| ---: | ---: | ---: | ---: | ---: |
| 250,202 | 200 | 100,082 | 4,000 | 2,000 |
| 1,000,402 | 100 | 100,042 | 2,000 | 1,000 |
| 2,000,802 | 100 | 100,042 | 2,000 | 1,000 |
| 5,001,002 | 50 | 100,022 | 1,000 | 500 |

## Initial-window resource evidence

CPU is cumulative CPU time. Memory is the cgroup lifetime peak; MongoDB's value includes replica-set provisioning before the measured window. Storage is filesystem-allocated growth from the pre-start baseline. WAL is the PostgreSQL cluster-wide inserted-WAL-position delta during the initial window.

| Rows | Official CPU, service + MongoDB | Rust CPU | Official peak memory, service / MongoDB | Rust peak memory | Official / Rust allocated storage growth | Official / Rust inserted WAL |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 250,202 | 22.82 CPU-s | 1.39 CPU-s | 252 / 1,508 MiB | 76 MiB | 0.17 / 0.39 GiB | 0.53 / 0.0004 MiB |
| 1,000,402 | 103.46 CPU-s | 4.71 CPU-s | 263 / 2,726 MiB | 85 MiB | 0.71 / 1.56 GiB | 2.28 / 0.026 MiB |
| 2,000,802 | 209.72 CPU-s | 8.86 CPU-s | 263 / 3,501 MiB | 98 MiB | 1.51 / 3.11 GiB | 3.01 / 0.026 MiB |
| 5,001,002 | 571.69 CPU-s | 24.74 CPU-s | 275 / 4,362 MiB | 111 MiB | 3.52 / 7.73 GiB | 6.68 / 1.13 MiB |

The machine-readable [canary summary](canary-summary.json) retains each component's CPU, cgroup peak memory, process peak RSS, block reads and writes, network receive and transmit counters, storage growth, and both initial-window and full-run WAL deltas. Official component network counters are not summed because PowerSync-to-MongoDB traffic appears in both namespaces.

## Provenance

The ladder ran the local Rust image by immutable image ID `sha256:59dbb72b5fd08be16508824869fbc7200c15af22a18d9554ddc89cc11ac23804`. PowerSync, MongoDB, and PostgreSQL inputs were pinned by registry digest. The Docker server reported Linux/aarch64, 6 CPUs, and 16,748,593,152 bytes of memory.

The full ladder took 18 minutes 44 seconds. Compressed per-bucket validation records and full resource snapshots occupy about 13 GiB locally and are not checked into Git.
