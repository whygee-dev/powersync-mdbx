import assert from 'node:assert/strict';
import test from 'node:test';

import { buildCanarySummary } from './export_canary_ladder.mjs';

test('canary export retains timings, gate counts, provenance, and limitations', () => {
  const ladder = {
    status: 'passed',
    finishedAt: '2000-01-01T00:01:00Z',
    gitCommit: 'abc',
    rustImageId: 'sha256:def',
    dockerServer: 'os=linux',
    runs: [
      {
        profile: '250k',
        artifactDir: '250k/run',
        status: 'passed',
        startedAt: '2000-01-01T00:00:00Z',
        finishedAt: '2000-01-01T00:01:00Z'
      }
    ]
  };
  const gate = { status: 'passed', buckets: [{ puts: 2, removes: 1 }] };
  const results = {
    profile: '250k',
    config: {
      projectBucketSampleCount: 1,
      retainRawValidationRecords: true,
      executionSchedule: [{ lane: 'endUser', repeat: 1, target: 'official' }],
      officialMongoCacheGb: '2',
      officialNodeOptions: '--max-old-space-size-percentage=80',
      officialTuningReviewed: false,
      storageClassAttested: true,
      durabilityPolicyAttested: true,
      storageClasses: { official: 'bind mount', rust: 'bind mount' },
      durabilityPolicies: { official: 'journaled', rust: 'durable' }
    },
    methodology: { equivalence: { datasetTaskRows: 250_202 } },
    targets: {
      official: { endUser: { runs: [{ readiness: { processingMs: 10 }, equivalence: gate, churn: gate }] } },
      rust: { endUser: { runs: [{ readiness: { processingMs: 2 }, equivalence: gate, churn: gate }] } }
    }
  };
  const summary = buildCanarySummary(ladder, () => results);
  assert.equal(summary.gitCommit, 'abc');
  assert.equal(Object.hasOwn(summary, 'recordedAt'), false);
  assert.equal(summary.dockerServer, 'os=linux');
  assert.equal(summary.methodology.resourceEvidence, 'not collected by this harness revision');
  assert.equal(summary.methodology.additionalInitialBoundaries, 'not collected by this harness revision');
  assert.equal(summary.rungs[0].sourceTaskRows, 250_202);
  assert.equal(summary.rungs[0].official.protocolReadinessMs, 10);
  assert.deepEqual(summary.rungs[0].executionSchedule, [
    { lane: 'endUser', repeat: 1, target: 'official' }
  ]);
  assert.deepEqual(summary.rungs[0].officialTuning, {
    mongoCacheGb: '2',
    nodeOptions: '--max-old-space-size-percentage=80',
    reviewedByPowerSync: false
  });
  assert.deepEqual(summary.rungs[0].storageControls, {
    classesAttested: true,
    durabilityPoliciesAttested: true,
    classes: { official: 'bind mount', rust: 'bind mount' },
    durabilityPolicies: { official: 'journaled', rust: 'durable' }
  });
  assert.deepEqual(summary.rungs[0].rust.churn, { status: 'passed', buckets: 1, puts: 2, removes: 1 });
});

test('canary export retains current materialization and slot-position boundaries', () => {
  const targetLsn = '0/123';
  const boundaryReadiness = {
    processingMs: 10,
    completeMaterialization: {
      completionBoundary: 'rust-persisted-initial-snapshot-lsn',
      processingMs: 9,
      targetLsn
    },
    sourceSlotPosition: {
      method: 'replication-slot-confirmed-flush-lsn',
      processingMs: 8,
      targetLsn,
      confirmed_flush_lsn: targetLsn
    }
  };
  const gate = { status: 'passed', buckets: [] };
  const ladder = {
    status: 'passed',
    finishedAt: '2000-01-01T00:01:00Z',
    runs: [{ profile: 'smoke', artifactDir: 'smoke/run', status: 'passed' }]
  };
  const measured = { readiness: boundaryReadiness, resources: { status: 'captured' }, equivalence: gate, churn: gate };
  const results = {
    profile: 'smoke',
    targets: {
      official: { endUser: { runs: [measured] } },
      rust: { endUser: { runs: [measured] } }
    }
  };
  const summary = buildCanarySummary(ladder, () => results);
  assert.match(summary.methodology.additionalInitialBoundaries, /captured for both targets/);
  assert.equal(summary.rungs[0].rust.completeMaterialization.targetLsn, targetLsn);
  assert.equal(summary.rungs[0].rust.sourceSlotPosition.confirmed_flush_lsn, targetLsn);
});

test('canary export retains compact initial and total resource evidence', () => {
  const gate = { status: 'passed', buckets: [] };
  const resources = {
    status: 'captured',
    artifactPath: '/private/path/resource-evidence.json',
    initial: {
      startedAt: '2000-01-01T00:00:00Z',
      finishedAt: '2000-01-01T00:00:10Z',
      wal: { startLsn: '0/1', endLsn: '0/2', insertedBytes: 123 },
      components: {
        service: {
          status: 'captured',
          source: 'linux-cgroup-v2',
          access: 'docker-exec',
          cpuSeconds: 4.5,
          cgroupLifetimePeakMemoryBytes: 100,
          mainProcessLifetimePeakRssBytes: 80,
          blockReadBytes: 10,
          blockWriteBytes: 20,
          networkRxBytes: 30,
          networkTxBytes: 40
        }
      },
      storage: { mdbx: { logicalBytes: 50, allocatedBytes: 60, files: 2 } }
    },
    total: {
      startedAt: '2000-01-01T00:00:00Z',
      finishedAt: '2000-01-01T00:00:20Z',
      wal: { insertedBytes: 456 },
      components: {},
      storage: {}
    }
  };
  const measured = {
    readiness: { processingMs: 10 },
    resources,
    equivalence: gate,
    churn: gate
  };
  const summary = buildCanarySummary(
    {
      status: 'passed',
      finishedAt: '2000-01-01T00:01:00Z',
      runs: [{ profile: 'smoke', artifactDir: 'smoke/run', status: 'passed' }]
    },
    () => ({
      profile: 'smoke',
      targets: {
        official: { endUser: { runs: [measured] } },
        rust: { endUser: { runs: [measured] } }
      }
    })
  );

  assert.deepEqual(summary.rungs[0].rust.resources.initial, {
    durationMs: 10_000,
    walInsertedBytes: 123,
    components: {
      service: {
        status: 'captured',
        source: 'linux-cgroup-v2',
        access: 'docker-exec',
        cpuSeconds: 4.5,
        cgroupLifetimePeakMemoryBytes: 100,
        mainProcessLifetimePeakRssBytes: 80,
        blockReadBytes: 10,
        blockWriteBytes: 20,
        networkRxBytes: 30,
        networkTxBytes: 40
      }
    },
    storageGrowth: { mdbx: { logicalBytes: 50, allocatedBytes: 60, files: 2 } }
  });
  assert.equal(summary.rungs[0].rust.resources.total.durationMs, 20_000);
  const serialized = JSON.stringify(summary);
  assert.doesNotMatch(serialized, /private\/path|2000-01-01T/);
  assert.doesNotMatch(serialized, /startedAt|finishedAt|startLsn|endLsn/);
});
