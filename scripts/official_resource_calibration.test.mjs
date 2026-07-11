import assert from 'node:assert/strict';
import test from 'node:test';

import {
  calibrationComposeProject,
  calibrationEnvironment,
  calibrationSchedule,
  compactCalibrationResult,
  DEFAULT_ALLOCATIONS,
  summarizeCalibrationRuns
} from './official_resource_calibration.mjs';

test('calibration rejects a successful-looking result with a missing boundary', () => {
  assert.throws(
    () =>
      compactCalibrationResult(
        { targets: { official: { endUser: { runs: [{ readiness: { processingMs: 10 } }] } } } },
        { id: 'candidate', serviceCpus: 2, mongoCpus: 2 },
        1,
        'run'
      ),
    /missing completeMaterializationMs/
  );
});

test('calibration counterbalances allocation order across two repeats', () => {
  const schedule = calibrationSchedule(2);
  assert.deepEqual(
    schedule.map(({ repeat, allocation }) => `${repeat}:${allocation.id}`),
    [
      ...DEFAULT_ALLOCATIONS.map((allocation) => `1:${allocation.id}`),
      ...[...DEFAULT_ALLOCATIONS].reverse().map((allocation) => `2:${allocation.id}`)
    ]
  );
});

test('calibration holds the aggregate budget and storage tuning constant', () => {
  for (const allocation of DEFAULT_ALLOCATIONS) {
    const env = calibrationEnvironment({
      allocation,
      artifactRoot: '/tmp/calibration',
      invocationId: 'invocation-a',
      base: {}
    });
    assert.equal(Number(env.POWERSYNC_USER_VALUE_SERVICE_CPUS) + Number(env.POWERSYNC_USER_VALUE_MONGO_CPUS), 4);
    assert.equal(env.POWERSYNC_USER_VALUE_SERVICE_MEMORY, '2g');
    assert.equal(env.POWERSYNC_USER_VALUE_MONGO_MEMORY, '6g');
    assert.equal(env.POWERSYNC_USER_VALUE_MONGO_CACHE_GB, '2');
    assert.equal(env.POWERSYNC_USER_VALUE_OFFICIAL_NODE_OPTIONS, '--max-old-space-size-percentage=80');
    assert.equal(env.POWERSYNC_USER_VALUE_TARGETS, 'official');
    assert.equal(env.POWERSYNC_USER_VALUE_PROFILE, '250k');
  }
});

test('calibration invocations use distinct Compose projects', () => {
  const allocation = DEFAULT_ALLOCATIONS[0];
  const first = calibrationEnvironment({
    allocation,
    artifactRoot: '/tmp/first',
    invocationId: 'invocation-a',
    base: {}
  });
  const second = calibrationEnvironment({
    allocation,
    artifactRoot: '/tmp/second',
    invocationId: 'invocation-b',
    base: {}
  });
  assert.notEqual(first.POWERSYNC_BENCHMARK_COMPOSE_PROJECT, second.POWERSYNC_BENCHMARK_COMPOSE_PROJECT);
  assert.equal(
    first.POWERSYNC_BENCHMARK_COMPOSE_PROJECT,
    calibrationComposeProject('invocation-a', allocation.id)
  );
});

test('calibration summary ranks complete materialization medians', () => {
  const sample = (allocation, repeat, materialization, protocol) => ({
    allocation,
    repeat,
    serviceCpus: allocation === 'a' ? 1 : 2,
    mongoCpus: allocation === 'a' ? 3 : 2,
    protocolReadyMs: protocol,
    completeMaterializationMs: materialization,
    walInsertedBytes: 100,
    components: {
      service: { cpuSeconds: 1, lifetimePeakMemoryBytes: 10 },
      mongo: { cpuSeconds: 2, lifetimePeakMemoryBytes: 20 }
    },
    storage: { 'mongo-db': { allocatedBytes: 30 } }
  });
  const summary = summarizeCalibrationRuns([
    sample('a', 1, 12, 15),
    sample('b', 1, 10, 14),
    sample('a', 2, 14, 17),
    sample('b', 2, 11, 13)
  ]);
  assert.deepEqual(summary.map(({ allocation }) => allocation), ['b', 'a']);
  assert.equal(summary[0].completeMaterializationMedianMs, 10.5);
  assert.equal(summary[1].protocolReadyMedianMs, 16);
});
