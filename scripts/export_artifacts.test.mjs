import assert from 'node:assert/strict';
import test from 'node:test';

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
