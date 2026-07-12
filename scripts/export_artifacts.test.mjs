import assert from 'node:assert/strict';
import test from 'node:test';

import { buildParitySummary } from './export_artifacts.mjs';
import { exportedParityStatus } from './export_artifacts_status.mjs';

const initialComparison = {
  repeatsCompared: 2,
  bucketSetsEqualAllRepeats: true,
  expectedRowsEqualPerBucketAllRepeats: true,
  putsEqualPerBucketAllRepeats: true,
  removesEqualPerBucketAllRepeats: true,
  checkpointCountEqualPerBucketAllRepeats: true,
  checkpointChecksumEqualPerBucketAllRepeats: true,
  clientOperationDigestEqualPerBucketAllRepeats: true,
  putDigestEqualPerBucketAllRepeats: true,
  semanticDigestEqualPerBucketAllRepeats: true,
  wireDigestEqualPerBucketAllRepeats: false
};

const churnComparison = {
  repeatsCompared: 2,
  bucketSetsEqualAllRepeats: true,
  expectedInsertsEqualPerBucketAllRepeats: true,
  expectedUpdatesEqualPerBucketAllRepeats: true,
  expectedDeletesEqualPerBucketAllRepeats: true,
  putsEqualPerBucketAllRepeats: true,
  removesEqualPerBucketAllRepeats: true,
  checkpointCountEqualPerBucketAllRepeats: true,
  checkpointChecksumEqualPerBucketAllRepeats: false,
  clientOperationDigestEqualPerBucketAllRepeats: true,
  clientPutDigestEqualPerBucketAllRepeats: true,
  putDigestEqualPerBucketAllRepeats: true,
  removeObjectDigestEqualPerBucketAllRepeats: true,
  wireDigestEqualPerBucketAllRepeats: false
};

function runs() {
  return [1, 2].map((repeat) => ({
    repeat,
    equivalence: { status: 'passed' },
    churn: { status: 'passed' }
  }));
}

function authz() {
  return {
    status: 'passed',
    checkpointBucketsReturnedAllZero: true,
    repeats: [1, 2].map((repeat) => ({ repeat }))
  };
}

function input(overrides = {}) {
  return {
    accessMode: 'auth_perimeter',
    expectedRepeats: 2,
    officialRuns: runs(),
    rustRuns: runs(),
    churnRequired: true,
    initialComparison: { ...initialComparison },
    churnComparison: { ...churnComparison },
    authz: authz(),
    ...overrides
  };
}

test('public parity export removes host identity, wall-clock time, paths, and raw WAL positions', () => {
  const resources = {
    status: 'captured',
    artifactPath: '/private/run/resource-evidence.json',
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
    total: null
  };
  const target = {
    endUser: {
      runs: [{ repeat: 1, resources }],
      summary: { resources: { cpuSeconds: { p50: 4.5 } } }
    }
  };
  const results = {
    profile: 'privacy-test',
    generatedAt: '2000-01-01T00:01:00Z',
    host: { cpuModel: 'private-workstation-model' },
    config: { targets: ['official', 'rust'], endUserSampleRepeats: 1 },
    methodology: { equivalence: { datasetTaskRows: 1 } },
    targets: { official: target, rust: target }
  };

  const summary = buildParitySummary({ results, compare: { comparisons: [] } });
  const serialized = JSON.stringify(summary);
  assert.equal(Object.hasOwn(summary, 'generatedAt'), false);
  assert.equal(Object.hasOwn(summary, 'artifactSource'), false);
  assert.equal(Object.hasOwn(summary, 'host'), false);
  assert.doesNotMatch(serialized, /private-workstation-model|2000-01-01T|private\/run/);
  assert.doesNotMatch(serialized, /startedAt|finishedAt|startLsn|endLsn/);
  assert.equal(summary.resources.rust.samples[0].initial.durationMs, 10_000);
  assert.equal(summary.resources.rust.samples[0].initial.walInsertedBytes, 123);
  assert.equal(summary.resources.rust.samples[0].initial.components.service.cpuSeconds, 4.5);
  assert.equal(resources.initial.startedAt, '2000-01-01T00:00:00Z');
});

test('passes complete auth-perimeter results with required parity', () => {
  assert.equal(exportedParityStatus(input()), 'passed');
});

test('requires the configured number of nonempty repeats', () => {
  assert.equal(exportedParityStatus(input({ officialRuns: [] })), 'unknown');
  assert.equal(exportedParityStatus(input({ rustRuns: runs().slice(0, 1) })), 'unknown');
  assert.equal(exportedParityStatus(input({ expectedRepeats: null })), 'unknown');
});

test('requires every local equivalence and churn gate to pass', () => {
  const officialRuns = runs();
  officialRuns[1].churn.status = 'failed';
  assert.equal(exportedParityStatus(input({ officialRuns })), 'unknown');
});

test('rejects a configured churn gate when both targets omit churn results', () => {
  const withoutChurn = runs().map(({ churn: _churn, ...run }) => run);
  assert.equal(
    exportedParityStatus(
      input({
        officialRuns: withoutChurn,
        rustRuns: withoutChurn,
        churnRequired: true
      })
    ),
    'unknown'
  );
});

test('requires authz results for every auth-perimeter repeat', () => {
  assert.equal(exportedParityStatus(input({ authz: { status: 'passed', repeats: [] } })), 'unknown');
  const incomplete = authz();
  incomplete.repeats.pop();
  assert.equal(exportedParityStatus(input({ authz: incomplete })), 'unknown');
});

test('does not require authz results outside auth-perimeter mode', () => {
  assert.equal(exportedParityStatus(input({ accessMode: 'subscription', authz: null })), 'passed');
});

test('requires all asserted initial cross-target parity fields', () => {
  assert.equal(
    exportedParityStatus(
      input({
        initialComparison: { ...initialComparison, semanticDigestEqualPerBucketAllRepeats: false }
      })
    ),
    'unknown'
  );
});

test('requires asserted churn parity while allowing target-local checksum and wire differences', () => {
  assert.equal(exportedParityStatus(input()), 'passed');
  assert.equal(
    exportedParityStatus(
      input({
        churnComparison: { ...churnComparison, removeObjectDigestEqualPerBucketAllRepeats: false }
      })
    ),
    'unknown'
  );
});
