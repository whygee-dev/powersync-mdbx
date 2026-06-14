# Contributing

The most useful contributions improve correctness, benchmark fairness, or the official baseline. The repository is a research prototype, not a supported PowerSync distribution.

## Local checks

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

CI runs these checks for pull requests and pushes to `main`. It also starts PostgreSQL with logical WAL and runs the ignored live-replication smoke tests.

The Rust toolchain is pinned by `rust-toolchain.toml`. `.nvmrc` constrains Node to major version 20.

## Benchmark changes

Call out any change to a measured interval, readiness signal, dataset, protocol gate, or target deployment in the pull request. Update `docs/benchmark.md` in the same change.

Export a completed run with:

```sh
node scripts/export_artifacts.mjs <run-dir> <label>
```

Do not hand-edit generated JSON artifacts. Raw validation records should be attached to a release or discussion when a performance claim depends on them.

## Challenging a result

Open an issue with the exact disputed assumption and a proposed control, or submit a change that strengthens the official baseline. Official-service configuration can be supplied through `POWERSYNC_USER_VALUE_OFFICIAL_CONFIG_EXTRA`; MongoDB cache size can be fixed with `POWERSYNC_USER_VALUE_MONGO_CACHE_GB`.

The comparison should use the strongest credible baseline. A faster official configuration is useful evidence, not an inconvenience.
