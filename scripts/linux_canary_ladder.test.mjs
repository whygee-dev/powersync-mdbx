import assert from 'node:assert/strict';
import test from 'node:test';

import {
  canaryEnvironment,
  ladderComposeProject,
  ladderRustBuildImage,
  LADDER_PROFILES
} from './linux_canary_ladder.mjs';

test('scale ladder is ordered and uses bounded verifier samples', () => {
  assert.deepEqual(LADDER_PROFILES.map(({ profile }) => profile), ['250k', '1m', '2m', '5m']);
  assert.deepEqual(LADDER_PROFILES.map(({ projectSamples }) => projectSamples), [200, 100, 100, 50]);
  assert.ok(LADDER_PROFILES.every((rung, index, all) => index === 0 || rung.timeoutMs > all[index - 1].timeoutMs));
  assert.ok(LADDER_PROFILES.every((rung, index, all) => index === 0 || rung.mdbxMaxBytes > all[index - 1].mdbxMaxBytes));
});

test('each rung enables the symmetric correctness gates and predictable resource caps', () => {
  const rung = LADDER_PROFILES[2];
  const rustImageId = `sha256:${'a'.repeat(64)}`;
  const env = canaryEnvironment(
    rung,
    '/tmp/ladder',
    { NODE_OPTIONS: '--trace-warnings' },
    'run-a',
    rustImageId
  );
  assert.equal(env.POWERSYNC_USER_VALUE_RUNTIME, 'symmetric-docker');
  assert.equal(env.POWERSYNC_USER_VALUE_PROFILE, '2m');
  assert.equal(env.POWERSYNC_USER_VALUE_EQUIVALENCE_GATE, '1');
  assert.equal(env.POWERSYNC_USER_VALUE_CHURN_GATE, '1');
  assert.equal(env.POWERSYNC_USER_VALUE_CHURN_GATE_MODE, 'slot-lsn');
  assert.equal(env.POWERSYNC_USER_VALUE_INITIAL_READINESS, 'sync-protocol');
  assert.equal(env.POWERSYNC_USER_VALUE_END_USER_REPEATS, '1');
  assert.equal(env.POWERSYNC_USER_VALUE_WARMUP_PAIRS, '0');
  assert.equal(env.POWERSYNC_USER_VALUE_PROJECT_BUCKET_SAMPLES, '100');
  assert.equal(env.POWERSYNC_RUST_MDBX_MAX_SIZE_BYTES, `${16 * 1024 ** 3}`);
  assert.equal(env.POWERSYNC_USER_VALUE_SERVICE_CPUS, '1.5');
  assert.equal(env.POWERSYNC_USER_VALUE_SERVICE_MEMORY, '2g');
  assert.equal(env.POWERSYNC_USER_VALUE_MONGO_CPUS, '2.5');
  assert.equal(env.POWERSYNC_USER_VALUE_MONGO_MEMORY, '6g');
  assert.equal(env.POWERSYNC_USER_VALUE_MONGO_CACHE_GB, '2');
  assert.equal(env.POWERSYNC_USER_VALUE_OFFICIAL_NODE_OPTIONS, '--max-old-space-size-percentage=80');
  assert.equal(env.POWERSYNC_USER_VALUE_RUST_IMAGE, rustImageId);
  assert.equal(env.POWERSYNC_USER_VALUE_STORAGE_CLASS_ATTESTED, '1');
  assert.equal(env.POWERSYNC_USER_VALUE_DURABILITY_POLICY_ATTESTED, '1');
  assert.match(env.POWERSYNC_USER_VALUE_OFFICIAL_STORAGE_CLASS, /shared artifact root/);
  assert.match(env.POWERSYNC_USER_VALUE_RUST_STORAGE_CLASS, /shared artifact root/);
  assert.match(env.POWERSYNC_USER_VALUE_OFFICIAL_DURABILITY_POLICY, /WiredTiger journaling enabled/);
  assert.equal(env.POWERSYNC_USER_VALUE_RUST_DURABILITY_POLICY, 'libmdbx SyncMode::Durable');
  assert.match(env.NODE_OPTIONS, /--trace-warnings --max-old-space-size=8192/);
  assert.equal(
    Number(env.POWERSYNC_USER_VALUE_SERVICE_CPUS) + Number(env.POWERSYNC_USER_VALUE_MONGO_CPUS),
    Number(env.POWERSYNC_USER_VALUE_TARGET_CPUS)
  );
});

test('concurrent ladders use distinct Compose projects', () => {
  const rung = LADDER_PROFILES[0];
  const first = canaryEnvironment(rung, '/tmp/first', {}, 'run-a');
  const second = canaryEnvironment(rung, '/tmp/second', {}, 'run-b');
  assert.notEqual(first.POWERSYNC_BENCHMARK_COMPOSE_PROJECT, second.POWERSYNC_BENCHMARK_COMPOSE_PROJECT);
  assert.equal(first.POWERSYNC_BENCHMARK_COMPOSE_PROJECT, ladderComposeProject('run-a', rung.profile));
});

test('concurrent ladders build distinct Rust image tags', () => {
  assert.equal(ladderRustBuildImage('0123456789ab'), 'powersync-mdbx:ladder-0123456789ab');
  assert.notEqual(ladderRustBuildImage('0123456789ab'), ladderRustBuildImage('abcdef012345'));
  assert.throws(() => ladderRustBuildImage('shared'), /invalid ladder invocation id/);
});
