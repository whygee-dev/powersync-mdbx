#!/usr/bin/env node
// Exports compact, path-scrubbed benchmark artifacts from a harness run
// directory into docs/artifacts/<label>/ so a reviewer can mechanically
// diff a fresh run against the published numbers.
//
// Usage:
//   node scripts/export_artifacts.mjs <run-dir> <label>
//   node scripts/export_artifacts.mjs tmp/user-value-benchmark/1780945318151_71314 1m-auth-perimeter
//
// Derives a compact parity-summary.json from results.json and compare.json. The raw
// results.json can exceed tens of megabytes, so it is intentionally not kept in
// docs/artifacts/. Existing files in the destination are overwritten.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { exportedParityStatus } from './export_artifacts_status.mjs';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

const [runDirArg, label] = process.argv.slice(2);
if (!runDirArg || !label) {
  console.error('usage: node scripts/export_artifacts.mjs <run-dir> <label>');
  process.exit(1);
}
if (!/^[a-z0-9][a-z0-9-]*$/.test(label)) {
  console.error(`label must be kebab-case, got ${JSON.stringify(label)}`);
  process.exit(1);
}

const runDir = path.resolve(repoRoot, runDirArg);
if (!fs.existsSync(runDir)) {
  console.error(`run directory not found: ${runDir}`);
  process.exit(1);
}
const scrubbedRunDir = `<repo>/${path.relative(repoRoot, runDir)}`;

const outDir = path.join(repoRoot, 'docs', 'artifacts', label);
fs.mkdirSync(outDir, { recursive: true });
for (const legacyName of ['compare.json', 'summary.md']) {
  fs.rmSync(path.join(outDir, legacyName), { force: true });
}

function scrub(content) {
  return content.split(runDir).join(scrubbedRunDir)
    .split(repoRoot).join('<repo>');
}

const exported = [];

const resultsPath = path.join(runDir, 'results.json');
const comparePath = path.join(runDir, 'compare.json');
if (fs.existsSync(resultsPath) && fs.existsSync(comparePath)) {
  const results = JSON.parse(fs.readFileSync(resultsPath, 'utf8'));
  const compare = JSON.parse(fs.readFileSync(comparePath, 'utf8'));
  fs.writeFileSync(
    path.join(outDir, 'parity-summary.json'),
    `${JSON.stringify(buildParitySummary({ results, compare }), null, 2)}\n`
  );
  exported.push('parity-summary.json');
} else {
  console.warn('skipping parity-summary.json: results.json or compare.json missing');
}

if (exported.length === 0) {
  console.error('nothing exported');
  process.exit(1);
}
console.log(`exported ${exported.join(', ')} -> ${path.relative(repoRoot, outDir)}`);
console.log('Add or update the artifact README to describe the run configuration and evidence retained.');

function buildParitySummary({ results, compare }) {
  const comparison = compare.comparisons?.[0] ?? {};
  const official = results.targets?.official;
  const rust = results.targets?.rust;
  const officialRuns = official?.endUser?.runs ?? [];
  const rustRuns = rust?.endUser?.runs ?? [];
  const validator = findProtocolValidator(results);
  const churnObserved =
    officialRuns.some((run) => run?.churn) || rustRuns.some((run) => run?.churn);
  const churnRequired = results.config?.churnGateEnabled === true || churnObserved;
  const initialComparison = compareInitialRuns(officialRuns, rustRuns);
  const churnComparison = compareChurnRuns(officialRuns, rustRuns);
  const authz = summarizeAuthz(officialRuns, rustRuns);
  const parityStatus = exportedParityStatus({
    accessMode: results.config?.accessMode,
    expectedRepeats: results.config?.endUserSampleRepeats,
    officialRuns,
    rustRuns,
    churnRequired,
    initialComparison,
    churnComparison,
    authz
  });

  return {
    profile: results.profile,
    generatedAt: results.generatedAt,
    artifactSource: scrub(runDir),
    sourceTaskRows: results.methodology?.equivalence?.datasetTaskRows ?? null,
    targets: results.config?.targets ?? Object.keys(results.targets ?? {}),
    sampleCount: results.config?.endUserSampleRepeats ?? officialRuns.length,
    percentileMethod: 'linear interpolation over successful samples, same as scripts/user_value_benchmark.mjs',
    host: results.host ?? null,
    provenance: results.provenance ?? null,
    config: {
      accessMode: results.config?.accessMode ?? null,
      projectBucketSampleCount: results.config?.projectBucketSampleCount ?? null,
      authPerimeterRows: results.methodology?.equivalence?.authPerimeterRows ?? null,
      churnRowsPerBucket: results.config?.churnRowsPerBucket ?? null,
      bucketProbeBatchSize: results.config?.bucketProbeBatchSize ?? null,
      processingOnly: results.config?.processingOnly ?? null,
      warmupPairs: results.config?.warmupPairs ?? null,
      churnGateMode: results.config?.churnGateMode ?? null,
      officialImage: results.methodology?.fairness?.sameOfficialImage ?? null,
      interleavedTargets: results.config?.interleaveTargets ?? null,
      executionSchedule: results.config?.executionSchedule ?? [],
      coldOpenReadiness: official?.methodology?.coldOpenReadiness ?? null,
      protocolValidator: validator
    },
    timing: timingSummary(results.targets ?? {}),
    resources: resourceSummary(results.targets ?? {}),
    comparison: {
      processing: comparisonMetricSummary(comparison.endUserExperience?.['processing.processingMs']),
      completeMaterialization: comparisonMetricSummary(
        comparison.endUserExperience?.['processing.completeMaterializationMs']
      ),
      sourceSlotPosition: comparisonMetricSummary(
        comparison.endUserExperience?.['processing.sourceSlotPositionMs']
      ),
      churnSlotAckCatchup: comparisonMetricSummary(
        comparison.endUserExperience?.['churn.slotAckCatchupMs']
      ),
      churnProtocolProbe: comparisonMetricSummary(
        comparison.endUserExperience?.['churn.protocolProbeMs']
      ),
      churnToProtocolVerified: comparisonMetricSummary(
        comparison.endUserExperience?.['churn.churnToProtocolVerifiedMs']
      )
    },
    parity: {
      status: parityStatus,
      initial: {
        official: summarizeInitialRuns(officialRuns),
        rust: summarizeInitialRuns(rustRuns),
        officialVsRust: initialComparison
      },
      authz,
      churn: {
        official: summarizeChurnRuns(officialRuns),
        rust: summarizeChurnRuns(rustRuns),
        officialVsRust: churnComparison,
        checkpointChecksumNote:
          'Churn checkpoint checksums are self-validated per target. Cross-target equality is not required for REMOVE ops because the checksum hashes the target-local subkey.'
      }
    },
    samples: sampleTimings(results.targets ?? {})
  };
}

function resourceSummary(targets) {
  return Object.fromEntries(
    Object.entries(targets).map(([label, target]) => [
      label,
      {
        summary: target.endUser?.summary?.resources ?? null,
        samples: (target.endUser?.runs ?? []).map((run) => ({
          repeat: run.repeat,
          status: run.resources?.status ?? 'unavailable',
          initial: run.resources?.initial ?? null,
          total: run.resources?.total ?? null
        }))
      }
    ])
  );
}

function comparisonMetricSummary(metric) {
  if (!metric) return null;
  return {
    officialMedianMs: metric.baselineP50Ms ?? null,
    rustMedianMs: metric.candidateP50Ms ?? null,
    medianDeltaMs: metric.deltaMs ?? null,
    ratioOfMedians: metric.ratioOfP50s ?? metric.speedupVsBaseline ?? null
  };
}

function timingSummary(targets) {
  return Object.fromEntries(
    Object.entries(targets).map(([label, target]) => {
      const summary = target.endUser?.summary ?? {};
      return [
        label,
        {
          processingMs: summary.processing?.processingMs ?? null,
          completeMaterializationMs: summary.processing?.completeMaterializationMs ?? null,
          sourceSlotPositionMs: summary.processing?.sourceSlotPositionMs ?? null,
          churnApplySqlMs: summary.churn?.applySqlMs ?? null,
          churnReplicationCatchupMs: summary.churn?.replicationCatchupMs ?? null,
          churnSlotAckCatchupMs: summary.churn?.slotAckCatchupMs ?? null,
          churnProtocolProbeMs: summary.churn?.protocolProbeMs ?? null,
          churnToProtocolVerifiedMs: summary.churn?.churnToProtocolVerifiedMs ?? null
        }
      ];
    })
  );
}

function sampleTimings(targets) {
  return Object.fromEntries(
    Object.entries(targets).map(([label, target]) => [
      label,
      (target.endUser?.runs ?? []).map((run) => ({
        repeat: run.repeat,
        processingMs: run.readiness?.processingMs ?? null,
        completeMaterializationMs: run.readiness?.completeMaterialization?.processingMs ?? null,
        sourceSlotPositionMs: run.readiness?.sourceSlotPosition?.processingMs ?? null,
        churnApplySqlMs: run.churn?.applySqlMs ?? null,
        churnReplicationCatchupMs: run.churn?.replicationCatchupMs ?? null,
        churnSlotAckCatchupMs: run.churn?.slotAckCatchupMs ?? null,
        churnProtocolProbeMs: run.churn?.protocolProbeMs ?? null,
        churnToProtocolVerifiedMs: run.churn?.churnToProtocolVerifiedMs ?? null
      }))
    ])
  );
}

function summarizeInitialRuns(runs) {
  const equivalenceRuns = runs.filter((run) => run.equivalence);
  return {
    repeats: equivalenceRuns.length,
    bucketsPerRepeat: equivalenceRuns.map((run) => run.equivalence.buckets?.length ?? 0),
    putsPerRepeat: equivalenceRuns.map((run) => sumBuckets(run.equivalence.buckets, 'puts')),
    removesPerRepeat: equivalenceRuns.map((run) => sumBuckets(run.equivalence.buckets, 'removes')),
    checkpointCountSumPerRepeat: equivalenceRuns.map((run) => sumBuckets(run.equivalence.buckets, 'checkpointCount')),
    checkpointChecksumSumPerRepeat: equivalenceRuns.map((run) => sumBuckets(run.equivalence.buckets, 'checkpointChecksum')),
    nonZeroEntryChecksumsPerRepeat: equivalenceRuns.map((run) =>
      sumBuckets(run.equivalence.buckets, (bucket) => bucket.entryChecksums?.nonZeroCount)
    ),
    statusPerRepeat: equivalenceRuns.map((run) => run.equivalence.status)
  };
}

function summarizeChurnRuns(runs) {
  const churnRuns = runs.filter((run) => run.churn);
  return {
    repeats: churnRuns.length,
    bucketsPerRepeat: churnRuns.map((run) => run.churn.buckets?.length ?? 0),
    expectedInsertsPerRepeat: churnRuns.map((run) => sumBuckets(run.churn.buckets, 'expectedInserts')),
    expectedUpdatesPerRepeat: churnRuns.map((run) => sumBuckets(run.churn.buckets, 'expectedUpdates')),
    expectedDeletesPerRepeat: churnRuns.map((run) => sumBuckets(run.churn.buckets, 'expectedDeletes')),
    putsPerRepeat: churnRuns.map((run) => sumBuckets(run.churn.buckets, 'puts')),
    removesPerRepeat: churnRuns.map((run) => sumBuckets(run.churn.buckets, 'removes')),
    checkpointCountSumPerRepeat: churnRuns.map((run) => sumBuckets(run.churn.buckets, 'checkpointCount')),
    expectedCheckpointCountSumPerRepeat: churnRuns.map((run) =>
      sumBuckets(run.churn.buckets, 'expectedCheckpointCount')
    ),
    checkpointChecksumSumPerRepeat: churnRuns.map((run) => sumBuckets(run.churn.buckets, 'checkpointChecksum')),
    expectedCheckpointChecksumSumPerRepeat: churnRuns.map((run) =>
      sumBuckets(run.churn.buckets, 'expectedCheckpointChecksum')
    ),
    nonZeroEntryChecksumsPerRepeat: churnRuns.map((run) =>
      sumBuckets(run.churn.buckets, (bucket) => bucket.entryChecksums?.nonZeroCount)
    ),
    checkpointChecksumMatchesExpectedAllRepeats: churnRuns.every((run) =>
      (run.churn.buckets ?? []).every(
        (bucket) => bucket.checkpointChecksum === bucket.expectedCheckpointChecksum
      )
    ),
    statusPerRepeat: churnRuns.map((run) => run.churn.status)
  };
}

function compareInitialRuns(officialRuns, candidateRuns) {
  return compareBucketRuns({
    officialRuns,
    candidateRuns,
    gate: 'equivalence',
    bucketFields: ['expectedRows', 'puts', 'removes', 'checkpointCount', 'checkpointChecksum'],
    checksumFields: ['clientOperationDigest', 'putDigest', 'semanticDigest', 'wireDigest']
  });
}

function compareChurnRuns(officialRuns, candidateRuns) {
  return compareBucketRuns({
    officialRuns,
    candidateRuns,
    gate: 'churn',
    bucketFields: [
      'expectedInserts',
      'expectedUpdates',
      'expectedDeletes',
      'puts',
      'removes',
      'checkpointCount',
      'checkpointChecksum'
    ],
    checksumFields: ['clientOperationDigest', 'clientPutDigest', 'putDigest', 'removeObjectDigest', 'wireDigest']
  });
}

function compareBucketRuns({ officialRuns, candidateRuns, gate, bucketFields, checksumFields }) {
  const comparisons = Object.fromEntries([...bucketFields, ...checksumFields].map((field) => [field, true]));
  let bucketSetsEqualAllRepeats = true;
  let repeatsCompared = 0;

  for (const officialRun of officialRuns) {
    const candidateRun = candidateRuns.find((run) => run.repeat === officialRun.repeat);
    if (!officialRun?.[gate] || !candidateRun?.[gate]) {
      bucketSetsEqualAllRepeats = false;
      continue;
    }
    repeatsCompared += 1;
    const officialBuckets = new Map((officialRun[gate].buckets ?? []).map((bucket) => [bucket.bucket, bucket]));
    const candidateBuckets = new Map((candidateRun[gate].buckets ?? []).map((bucket) => [bucket.bucket, bucket]));
    if (!sameSet([...officialBuckets.keys()], [...candidateBuckets.keys()])) {
      bucketSetsEqualAllRepeats = false;
      continue;
    }
    for (const [bucketName, officialBucket] of officialBuckets) {
      const candidateBucket = candidateBuckets.get(bucketName);
      for (const field of bucketFields) {
        comparisons[field] &&= officialBucket[field] === candidateBucket?.[field];
      }
      for (const field of checksumFields) {
        comparisons[field] &&= officialBucket.entryChecksums?.[field] === candidateBucket?.entryChecksums?.[field];
      }
    }
  }

  return {
    repeatsCompared,
    bucketSetsEqualAllRepeats,
    ...Object.fromEntries(Object.entries(comparisons).map(([field, value]) => [`${field}EqualPerBucketAllRepeats`, value]))
  };
}

function summarizeAuthz(officialRuns, candidateRuns) {
  const authRuns = officialRuns
    .map((officialRun) => {
      const candidateRun = candidateRuns.find((run) => run.repeat === officialRun.repeat);
      const officialAuth = officialRun.equivalence?.authorization;
      const candidateAuth = candidateRun?.equivalence?.authorization;
      if (!officialAuth && !candidateAuth) return null;
      return {
        repeat: officialRun.repeat,
        officialStatus: officialAuth?.status ?? null,
        rustStatus: candidateAuth?.status ?? null,
        officialCheckpointBucketsReturned: officialAuth?.checkpointBucketsReturned ?? null,
        rustCheckpointBucketsReturned: candidateAuth?.checkpointBucketsReturned ?? null
      };
    })
    .filter(Boolean);
  return {
    status: authRuns.every((run) => run.officialStatus === 'passed' && run.rustStatus === 'passed')
      ? 'passed'
      : 'unknown',
    checkpointBucketsReturnedAllZero: authRuns.every(
      (run) => run.officialCheckpointBucketsReturned === 0 && run.rustCheckpointBucketsReturned === 0
    ),
    repeats: authRuns
  };
}

function findProtocolValidator(results) {
  for (const target of Object.values(results.targets ?? {})) {
    for (const run of target.endUser?.runs ?? []) {
      const authValidator = run.equivalence?.authorization?.protocolValidator;
      if (authValidator) return authValidator;
      for (const bucket of run.equivalence?.buckets ?? []) {
        if (bucket.protocolValidator) return bucket.protocolValidator;
      }
      for (const bucket of run.churn?.buckets ?? []) {
        if (bucket.protocolValidator) return bucket.protocolValidator;
      }
    }
  }
  return null;
}

function sumBuckets(buckets = [], fieldOrFn) {
  return buckets.reduce((sum, bucket) => {
    const value = typeof fieldOrFn === 'function' ? fieldOrFn(bucket) : bucket[fieldOrFn];
    return sum + (Number.isFinite(value) ? value : 0);
  }, 0);
}

function sameSet(left, right) {
  if (left.length !== right.length) return false;
  const rightSet = new Set(right);
  return left.every((value) => rightSet.has(value));
}
