const INITIAL_PARITY_FIELDS = [
  'bucketSetsEqualAllRepeats',
  'expectedRowsEqualPerBucketAllRepeats',
  'putsEqualPerBucketAllRepeats',
  'removesEqualPerBucketAllRepeats',
  'checkpointCountEqualPerBucketAllRepeats',
  'checkpointChecksumEqualPerBucketAllRepeats',
  'clientOperationDigestEqualPerBucketAllRepeats',
  'putDigestEqualPerBucketAllRepeats',
  'semanticDigestEqualPerBucketAllRepeats'
];

const CHURN_PARITY_FIELDS = [
  'bucketSetsEqualAllRepeats',
  'expectedInsertsEqualPerBucketAllRepeats',
  'expectedUpdatesEqualPerBucketAllRepeats',
  'expectedDeletesEqualPerBucketAllRepeats',
  'putsEqualPerBucketAllRepeats',
  'removesEqualPerBucketAllRepeats',
  'checkpointCountEqualPerBucketAllRepeats',
  'clientOperationDigestEqualPerBucketAllRepeats',
  'clientPutDigestEqualPerBucketAllRepeats',
  'putDigestEqualPerBucketAllRepeats',
  'removeObjectDigestEqualPerBucketAllRepeats'
];

export function exportedParityStatus({
  accessMode,
  expectedRepeats,
  officialRuns,
  rustRuns,
  churnRequired,
  initialComparison,
  churnComparison,
  authz
}) {
  if (!Number.isInteger(expectedRepeats) || expectedRepeats < 1) return 'unknown';
  if (
    !runsPassed(officialRuns, expectedRepeats, churnRequired) ||
    !runsPassed(rustRuns, expectedRepeats, churnRequired)
  ) {
    return 'unknown';
  }
  if (!comparisonPassed(initialComparison, expectedRepeats, INITIAL_PARITY_FIELDS)) return 'unknown';
  if (churnRequired && !comparisonPassed(churnComparison, expectedRepeats, CHURN_PARITY_FIELDS)) {
    return 'unknown';
  }
  if (accessMode === 'auth_perimeter' && !authzPassed(authz, expectedRepeats)) return 'unknown';
  return 'passed';
}

function runsPassed(runs, expectedRepeats, requireChurn) {
  if (!Array.isArray(runs) || runs.length !== expectedRepeats) return false;
  const repeatNumbers = new Set(runs.map((run) => run?.repeat));
  if (repeatNumbers.size !== expectedRepeats) return false;
  for (let repeat = 1; repeat <= expectedRepeats; repeat += 1) {
    if (!repeatNumbers.has(repeat)) return false;
  }
  return runs.every(
    (run) => run?.equivalence?.status === 'passed' && (!requireChurn || run?.churn?.status === 'passed')
  );
}

function comparisonPassed(comparison, expectedRepeats, requiredFields) {
  return (
    comparison?.repeatsCompared === expectedRepeats &&
    requiredFields.every((field) => comparison?.[field] === true)
  );
}

function authzPassed(authz, expectedRepeats) {
  if (authz?.status !== 'passed' || authz?.checkpointBucketsReturnedAllZero !== true) return false;
  if (!Array.isArray(authz.repeats) || authz.repeats.length !== expectedRepeats) return false;
  const repeatNumbers = new Set(authz.repeats.map((run) => run?.repeat));
  if (repeatNumbers.size !== expectedRepeats) return false;
  for (let repeat = 1; repeat <= expectedRepeats; repeat += 1) {
    if (!repeatNumbers.has(repeat)) return false;
  }
  return true;
}
