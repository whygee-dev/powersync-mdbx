#!/usr/bin/env node
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import net from 'node:net';
import http from 'node:http';
import https from 'node:https';
import zlib from 'node:zlib';
import { createRequire } from 'node:module';
import { spawn, spawnSync } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { performance } from 'node:perf_hooks';
import { fileURLToPath, pathToFileURL } from 'node:url';

import {
  captureResourceSnapshot,
  diffResourceSnapshots
} from './resource_evidence.mjs';

import {
  buildBenchmarkFixture,
  TABLES,
  orgId,
  projectId,
  resolveProfile,
  taskId,
  userId,
  makeSentinelTaskRow
} from '../e2e/official-sdk/src/benchmark-fixture.mjs';

const powersyncBenchmarkDir = new URL('../', import.meta.url);
const composeCwd = fileURLToPath(powersyncBenchmarkDir);
const composeProjectName = process.env.POWERSYNC_BENCHMARK_COMPOSE_PROJECT ?? 'powersync_mdbx';
const composeNetwork = `${composeProjectName}_default`;
const postgresContainerName = `${composeProjectName}-postgres-1`;
const sdkDir = path.join(composeCwd, 'e2e', 'official-sdk');
const sdkRequire = createRequire(path.join(sdkDir, 'package.json'));
const defaultArtifactRoot = path.join(composeCwd, 'tmp', 'user-value-benchmark');
const artifactRoot = path.resolve(
  process.env.POWERSYNC_USER_VALUE_ARTIFACT_ROOT ?? defaultArtifactRoot
);
const projectRoot = composeCwd;
const repoRoot = projectRoot;
const rustManifestPath =
  process.env.POWERSYNC_RUST_MANIFEST_PATH ?? path.join(projectRoot, 'crates', 'powersync-mdbx', 'Cargo.toml');

const profileName = process.env.POWERSYNC_USER_VALUE_PROFILE ?? process.env.POWERSYNC_BENCHMARK_PROFILE ?? 'smoke';
const profile = resolveProfile(profileName);
const fixture = buildBenchmarkFixture(profileName);
const scaleProfile = ['100k', '250k', '1m', '2m', '5m'].includes(profileName);
const authPerimeterTable = 'user_project_access';
const authPerimeterStream = 'tasks_by_auth_project';
const readinessProbeUserId = 'user-benchmark-readiness-probe';
const accessMode = (process.env.POWERSYNC_USER_VALUE_ACCESS_MODE ?? 'subscription').trim();
const authPerimeterMode = accessMode === 'auth_perimeter';
const routeStreamsDisabled = process.env.POWERSYNC_USER_VALUE_ROUTE_STREAMS === '0';
const routeStreamsEnabled = authPerimeterMode || (!routeStreamsDisabled && scaleProfile);
const syncRuleFanout = Math.max(0, Number.parseInt(process.env.POWERSYNC_USER_VALUE_RULE_FANOUT ?? '0', 10));
const fullBucketEquivalenceMaxRows = Math.max(
  0,
  Number.parseInt(process.env.POWERSYNC_USER_VALUE_FULL_BUCKET_EQUIVALENCE_MAX_ROWS ?? '150000', 10)
);
const defaultProjectBucketSampleCount = expectedTaskRowCount() >= 1_000_000 ? 200 : 0;
const requestedProjectBucketSampleCount = Math.max(
  0,
  Number.parseInt(
    process.env.POWERSYNC_USER_VALUE_PROJECT_BUCKET_SAMPLES ?? `${defaultProjectBucketSampleCount}`,
    10
  )
);
const projectBucketSampleCount = effectiveProjectBucketSampleCount({
  requested: requestedProjectBucketSampleCount,
  availableProjects: profile.projectsPerOrg,
  enabled: routeStreamsEnabled
});
const bucketProbeBatchSize = Math.max(
  1,
  Number.parseInt(process.env.POWERSYNC_USER_VALUE_BUCKET_PROBE_BATCH_SIZE ?? '25', 10)
);
const includeDefaultEquivalenceBucket =
  process.env.POWERSYNC_USER_VALUE_INCLUDE_DEFAULT_EQUIVALENCE_BUCKET == null
    ? expectedTaskRowCount() <= fullBucketEquivalenceMaxRows
    : process.env.POWERSYNC_USER_VALUE_INCLUDE_DEFAULT_EQUIVALENCE_BUCKET !== '0';
const includeOrgEquivalenceBucket =
  process.env.POWERSYNC_USER_VALUE_INCLUDE_ORG_EQUIVALENCE_BUCKET == null
    ? expectedTaskRowCount() <= fullBucketEquivalenceMaxRows
    : process.env.POWERSYNC_USER_VALUE_INCLUDE_ORG_EQUIVALENCE_BUCKET !== '0';
const suffix = `${Date.now()}_${Math.floor(Math.random() * 100000)}`;
const benchmarkDir = path.join(artifactRoot, suffix);
const summaryPath = path.join(benchmarkDir, 'summary.md');
const comparePath = path.join(benchmarkDir, 'compare.json');
const resultsPath = path.join(benchmarkDir, 'results.json');
const destructiveArtifactCleanupRequested = process.env.POWERSYNC_USER_VALUE_CLEAN_TMP === '1';
const audience = process.env.POWERSYNC_USER_VALUE_AUDIENCE ?? 'powersync-user-value-benchmark';
const issuer = process.env.POWERSYNC_USER_VALUE_ISSUER ?? 'https://benchmark.powersync-mdbx.invalid';
const jwtSecret = process.env.POWERSYNC_USER_VALUE_JWT_SECRET ?? `user-value-secret-${suffix}`;
const apiToken = process.env.POWERSYNC_USER_VALUE_API_TOKEN ?? `user-value-api-token-${suffix}`;
// Stable 1.23.3 multi-platform manifest, resolved 2026-07-11. Publication
// runs must still choose and attest the baseline explicitly.
const officialImage =
  process.env.POWERSYNC_OFFICIAL_IMAGE ??
  'journeyapps/powersync-service@sha256:b6b22fa7d0d862f04bdff62846e656756d17bcf3dd6eca399a0633671051438b';
const mongoImage = process.env.POWERSYNC_USER_VALUE_MONGO_IMAGE ?? 'mongo:7';
const socatImage = process.env.POWERSYNC_USER_VALUE_SOCAT_IMAGE ?? 'alpine/socat';
const postgresImage = process.env.POWERSYNC_USER_VALUE_POSTGRES_IMAGE ?? 'postgres:16';
const rustImage = process.env.POWERSYNC_USER_VALUE_RUST_IMAGE ?? 'powersync-mdbx:benchmark';
const runtimeMode = process.env.POWERSYNC_USER_VALUE_RUNTIME ?? 'native-rust';
if (!['native-rust', 'symmetric-docker'].includes(runtimeMode)) {
  throw new Error(
    `unsupported POWERSYNC_USER_VALUE_RUNTIME=${JSON.stringify(runtimeMode)} (expected native-rust or symmetric-docker)`
  );
}
const symmetricDockerRuntime = runtimeMode === 'symmetric-docker';
const targetCpuLimit = positiveNumberEnv('POWERSYNC_USER_VALUE_TARGET_CPUS', 6);
const targetMemoryLimit = memoryLimitEnv('POWERSYNC_USER_VALUE_TARGET_MEMORY', '12g');
const serviceCpuLimit = positiveNumberEnv('POWERSYNC_USER_VALUE_SERVICE_CPUS', 4);
const serviceMemoryLimit = memoryLimitEnv('POWERSYNC_USER_VALUE_SERVICE_MEMORY', '8g');
const mongoCpuLimit = positiveNumberEnv('POWERSYNC_USER_VALUE_MONGO_CPUS', 2);
const mongoMemoryLimit = memoryLimitEnv('POWERSYNC_USER_VALUE_MONGO_MEMORY', '4g');
const targetLimitsExplicit =
  process.env.POWERSYNC_USER_VALUE_TARGET_CPUS != null &&
  process.env.POWERSYNC_USER_VALUE_TARGET_MEMORY != null;
const serviceLimitsExplicit =
  process.env.POWERSYNC_USER_VALUE_SERVICE_CPUS != null &&
  process.env.POWERSYNC_USER_VALUE_SERVICE_MEMORY != null;
const mongoLimitsExplicit =
  process.env.POWERSYNC_USER_VALUE_MONGO_CPUS != null &&
  process.env.POWERSYNC_USER_VALUE_MONGO_MEMORY != null;
const resourceBudgetMatches =
  Math.abs(serviceCpuLimit + mongoCpuLimit - targetCpuLimit) < 1e-9 &&
  memoryLimitBytes(serviceMemoryLimit) + memoryLimitBytes(mongoMemoryLimit) ===
    memoryLimitBytes(targetMemoryLimit);
const officialMongoContainer = `powersync_user_value_mongo_${suffix}`;
const officialContainer = `powersync_user_value_official_${suffix}`;
const rustContainer = `powersync_user_value_rust_${suffix}`;
const rustPostgresForwarderContainer = `powersync_user_value_pgforward_${suffix}`;
const groupId = `user-value-group-${suffix}`;
const benchmarkPublication = `pub_user_value_${suffix}`;
const benchmarkSlot = `slot_user_value_${suffix}`;
const rustPublication = `pub_user_value_rust_${suffix}`;
const rustSlot = `slot_user_value_rust_${suffix}`;
const officialSlotPrefix = `uservalue_${suffix}_`;
const timeoutMs = Number(process.env.POWERSYNC_USER_VALUE_TIMEOUT_MS ?? Math.max(profile.timeoutMs, 60_000));
const jwtTtlSeconds = positiveIntegerEnv(
  'POWERSYNC_USER_VALUE_JWT_TTL_SECONDS',
  Math.ceil(Math.max(timeoutMs, 3_600_000) / 1_000) + 300
);
const lifecycleRepeats = Math.max(0, Number.parseInt(process.env.POWERSYNC_USER_VALUE_LIFECYCLE_REPEATS ?? '0', 10));
const browserIterations = Math.max(
  1,
  Number.parseInt(process.env.POWERSYNC_USER_VALUE_BROWSER_ITERATIONS ?? `${profile.iterations}`, 10)
);
const endUserSampleRepeats = Math.max(
  1,
  Number.parseInt(process.env.POWERSYNC_USER_VALUE_END_USER_REPEATS ?? `${browserIterations}`, 10)
);
const concurrentClients = Math.max(1, Number.parseInt(process.env.POWERSYNC_USER_VALUE_CONCURRENT_CLIENTS ?? `${profile.concurrentClients}`, 10));
const readinessPollMs = Math.max(50, Number.parseInt(process.env.POWERSYNC_USER_VALUE_READINESS_POLL_MS ?? '100', 10));
const readinessAttempts = Math.max(1, Number.parseInt(process.env.POWERSYNC_USER_VALUE_READINESS_ATTEMPTS ?? '300', 10));
const protocolReadinessAttempts = positiveIntegerEnv(
  'POWERSYNC_USER_VALUE_PROTOCOL_READINESS_ATTEMPTS',
  defaultProtocolReadinessAttempts(timeoutMs, readinessAttempts)
);
const interleaveTargets = process.env.POWERSYNC_USER_VALUE_INTERLEAVE_TARGETS !== '0';
const coldStartSyncProbeFallbackAllowed = process.env.POWERSYNC_USER_VALUE_ALLOW_COLD_START_SYNC_PROBE === '1';
const processingOnly = process.env.POWERSYNC_USER_VALUE_PROCESSING_ONLY === '1';
const equivalenceGateEnabled = process.env.POWERSYNC_USER_VALUE_EQUIVALENCE_GATE === '1';
const churnGateEnabled = process.env.POWERSYNC_USER_VALUE_CHURN_GATE === '1';
const churnRowsPerBucket = positiveIntegerEnv('POWERSYNC_USER_VALUE_CHURN_ROWS_PER_BUCKET', 1);
const churnProtocolSettleMs = positiveIntegerEnv(
  'POWERSYNC_USER_VALUE_CHURN_PROTOCOL_SETTLE_MS',
  Math.min(timeoutMs, 60_000)
);
const checksumRecordDataPreview = process.env.POWERSYNC_USER_VALUE_CHECKSUM_RECORD_DATA_PREVIEW === '1';
const retainRawValidationRecords = process.env.POWERSYNC_USER_VALUE_RETAIN_RAW_RECORDS === '1';
const publicRun = process.env.POWERSYNC_USER_VALUE_PUBLIC_RUN === '1';
const initialReadinessMode = process.env.POWERSYNC_USER_VALUE_INITIAL_READINESS ?? 'target-specific';
const officialTuningReviewed = process.env.POWERSYNC_USER_VALUE_OFFICIAL_TUNING_REVIEWED === '1';
const rustComparable = process.env.POWERSYNC_RUST_ALLOW_COMPARISON === '1';
// Unrecorded paired repeats before the measured ones, to absorb cold-cache /
// first-pull effects. 0 keeps the historical behavior.
const warmupPairs = (() => {
  const raw = process.env.POWERSYNC_USER_VALUE_WARMUP_PAIRS;
  if (raw == null || raw === '') return 0;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`POWERSYNC_USER_VALUE_WARMUP_PAIRS must be a non-negative integer, got ${JSON.stringify(raw)}`);
  }
  return parsed;
})();
// 'target-specific' (default): official gates on slot/diagnostics LSN, rust on
// its persisted tail-op counter. 'slot-lsn': both targets gate on their
// replication slot's confirmed_flush_lsn in Postgres - the same finish line,
// including the ack back to the source.
const churnGateMode = process.env.POWERSYNC_USER_VALUE_CHURN_GATE_MODE ?? 'target-specific';
if (!['target-specific', 'slot-lsn'].includes(churnGateMode)) {
  throw new Error(
    `unsupported POWERSYNC_USER_VALUE_CHURN_GATE_MODE="${churnGateMode}" (expected target-specific or slot-lsn)`
  );
}
const rustReplicationStatusIntervalMsOverride = optionalPositiveIntegerEnv(
  'POWERSYNC_RUST_REPLICATION_STATUS_INTERVAL_MS'
);
const rustReplicationIdleWakeupIntervalMsOverride = optionalPositiveIntegerEnv(
  'POWERSYNC_RUST_REPLICATION_IDLE_WAKEUP_INTERVAL_MS'
);
const rustReplicationFeedback = {
  statusIntervalMs: rustReplicationStatusIntervalMsOverride,
  statusSource: rustReplicationStatusIntervalMsOverride == null ? 'not-set-by-harness' : 'env-override',
  idleWakeupIntervalMs: rustReplicationIdleWakeupIntervalMsOverride,
  idleWakeupSource: rustReplicationIdleWakeupIntervalMsOverride == null ? 'not-set-by-harness' : 'env-override',
  currentSourceDefaults: {
    statusIntervalMs: 1_000,
    idleWakeupIntervalMs: 1_000
  }
};
const officialMongoCacheGb = process.env.POWERSYNC_USER_VALUE_MONGO_CACHE_GB ?? null;
const officialNodeOptions = process.env.POWERSYNC_USER_VALUE_OFFICIAL_NODE_OPTIONS ?? null;
const officialConfigExtraYaml = process.env.POWERSYNC_USER_VALUE_OFFICIAL_CONFIG_EXTRA ?? null;
const rustLiveUnifiedBench = process.env.POWERSYNC_RUST_LIVE_UNIFIED_BENCH !== '0';
const rustInitialSnapshotEnabled = process.env.POWERSYNC_RUST_INITIAL_SNAPSHOT ?? '1';
const rustPersistRawBatches = process.env.POWERSYNC_RUST_PERSIST_RAW_BATCHES ?? '0';
const rustPrebuildEnabled = process.env.POWERSYNC_RUST_PREBUILD !== '0';
// Default to the prebuilt binary so cargo's freshness check (or worse, a
// dirty rebuild) never runs inside the measured processing window.
const rustUsePrebuiltBinary = process.env.POWERSYNC_RUST_USE_PREBUILT_BINARY !== '0';
const rustCargoProfile = (process.env.POWERSYNC_RUST_CARGO_PROFILE ?? 'release').trim() || 'release';
const rustCargoBuildArgs = rustProfileArgs(rustCargoProfile);
const rustTargetProfileDir = rustCargoProfile === 'dev' ? 'debug' : rustCargoProfile;
let rustExecutablePath = process.env.POWERSYNC_RUST_EXECUTABLE_PATH ?? null;
const targetLabels = (process.env.POWERSYNC_USER_VALUE_TARGETS ?? 'official,rust')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);

if (!rustCargoBuildArgs) {
  throw new Error(
    `unsupported POWERSYNC_RUST_CARGO_PROFILE="${rustCargoProfile}" (expected dev, release, or custom profile name)`
  );
}
if (!['subscription', 'auth_perimeter'].includes(accessMode)) {
  throw new Error(
    `unsupported POWERSYNC_USER_VALUE_ACCESS_MODE="${accessMode}" (expected subscription or auth_perimeter)`
  );
}
if (authPerimeterMode && routeStreamsDisabled) {
  throw new Error('POWERSYNC_USER_VALUE_ACCESS_MODE=auth_perimeter requires routed sync streams');
}
if (churnGateEnabled && !equivalenceGateEnabled) {
  throw new Error('POWERSYNC_USER_VALUE_CHURN_GATE=1 requires POWERSYNC_USER_VALUE_EQUIVALENCE_GATE=1 for bucket cursors');
}
if (!['target-specific', 'sync-protocol'].includes(initialReadinessMode)) {
  throw new Error(
    `unsupported POWERSYNC_USER_VALUE_INITIAL_READINESS="${initialReadinessMode}" (expected target-specific or sync-protocol)`
  );
}
if (churnGateEnabled && churnRowsPerBucket * 2 > profile.tasksPerProject) {
  throw new Error(
    `POWERSYNC_USER_VALUE_CHURN_ROWS_PER_BUCKET=${churnRowsPerBucket} requires at least ${
      churnRowsPerBucket * 2
    } tasks per project, but profile ${profileName} has ${profile.tasksPerProject}`
  );
}

const baseRulesYaml = buildUserValueRulesYaml({ includeExtraStream: false });
const dashboardRulesYaml = buildUserValueRulesYaml({ includeExtraStream: true });
const protocolReadinessSpec =
  initialReadinessMode === 'sync-protocol' ? buildInitialReadinessSpec() : null;
let officialRunning = false;
let officialMongoRunning = false;
let officialLogPath = null;
let officialMongoDataDir = null;
let rustProcess = null;
let rustContainerRunning = false;
let rustLogPath = null;
let rustPostgresForwarderRunning = false;
let cleanupStarted = false;
let runtimeSetupStarted = false;
const executionSchedule = [];
let powersyncCommonProtocol = null;

async function main() {
try {
  if (destructiveArtifactCleanupRequested) {
    throw new Error(
      'POWERSYNC_USER_VALUE_CLEAN_TMP=1 is no longer supported: benchmark runs are append-only; remove a specific run directory manually'
    );
  }
  if (publicRun) assertPublicRunPreflight(runtimePublicRunPreflightInput());
  fs.mkdirSync(benchmarkDir, { recursive: true });
  writeComposeOverride();

  log('docker compose up postgres');
  run('docker', composeCommandArgs('up', '-d', 'postgres'), {
    cwd: composeCwd
  });
  runtimeSetupStarted = true;

  await waitForBenchmarkPostgres();
  await ensureBenchmarkTooling();
  if (initialReadinessMode === 'sync-protocol') await loadPowerSyncCommonProtocol();
  await ensureDockerImagesPulled();
  await prepareRustServiceRuntime();

  const results = {
    generatedAt: new Date().toISOString(),
    profile: profileName,
    config: {
      profile: profileName,
      lifecycleRepeats,
      browserIterations,
      endUserSampleRepeats,
      concurrentClients,
      processingOnly,
      equivalenceGateEnabled,
      accessMode,
      rustInitialSnapshotEnabled,
      rustPersistRawBatches,
      rustPrebuildEnabled,
      rustUsePrebuiltBinary,
      rustCargoProfile,
      runtimeMode,
      resourceEvidenceEnabled: symmetricDockerRuntime,
      targetResources: {
        cpus: targetCpuLimit,
        memory: targetMemoryLimit,
        explicit: targetLimitsExplicit,
        splitMatchesTotal: resourceBudgetMatches
      },
      officialServiceResources: {
        cpus: serviceCpuLimit,
        memory: serviceMemoryLimit,
        explicit: serviceLimitsExplicit
      },
      officialStorageResources: {
        cpus: mongoCpuLimit,
        memory: mongoMemoryLimit,
        explicit: mongoLimitsExplicit
      },
      syncRuleFanout,
      fullBucketEquivalenceMaxRows,
      includeDefaultEquivalenceBucket,
      includeOrgEquivalenceBucket,
      requestedProjectBucketSampleCount,
      projectBucketSampleCount,
      bucketProbeBatchSize,
      churnGateEnabled,
      churnRowsPerBucket,
      artifactRoot,
      artifactRetention: 'append-only',
      retainRawValidationRecords,
      publicRun,
      initialReadinessMode,
      jwtTtlSeconds,
      targets: targetLabels,
      timeoutMs,
      protocolReadinessAttempts,
      interleaveTargets,
      warmupPairs,
      churnGateMode,
      rustReplicationFeedbackOverrides: {
        statusIntervalMs: rustReplicationStatusIntervalMsOverride,
        idleWakeupIntervalMs: rustReplicationIdleWakeupIntervalMsOverride
      },
      rustReplicationFeedback,
      officialMongoCacheGb,
      officialNodeOptions,
      officialTuningReviewed,
      dockerImageInputs: dockerImageInputs(),
      officialConfigExtraApplied: officialConfigExtraYaml != null,
      officialConfigExtraSha256:
        officialConfigExtraYaml == null ? null : sha256(officialConfigExtraYaml.trim()),
      executionSchedule
    },
    host: collectHostMetadata(),
    provenance: collectProvenance(),
    methodology: {
      equivalence: {
        datasetTaskRows: expectedTaskRowCount(),
        authPerimeterRows: authPerimeterMode ? authPerimeterRows().length : 0,
        readinessProbe:
          initialReadinessMode === 'sync-protocol'
            ? {
                subject: initialReadinessSubject(),
                projectId: fixture.ids.primaryProjectId,
                fixtureAccessRows: authPerimeterMode ? 1 : 0,
                requestKind: 'subscription',
                stream: authPerimeterMode ? authPerimeterStream : 'tasks_by_project'
              }
            : null,
        baseRulesSha256: sha256(baseRulesYaml),
        dashboardRulesSha256: sha256(dashboardRulesYaml),
        targetUserId: fixture.targetUserId,
        targetOrgId: fixture.targetOrgId,
        accessMode
      },
      fairness: {
        sameDataset: true,
        sameBrowserFixture: true,
        sameTargetUser: true,
        sameOfficialImage: officialImage,
        interleavedTargets: interleaveTargets,
        deploymentTopology:
          symmetricDockerRuntime
            ? `SYMMETRIC TARGET BUDGET: both targets run as Linux containers on one Docker network and receive ${targetCpuLimit} CPU / ${targetMemoryLimit} memory in total. Rust receives the full budget; the official target splits it between service (${serviceCpuLimit} CPU / ${serviceMemoryLimit}) and MongoDB (${mongoCpuLimit} CPU / ${mongoMemoryLimit}). Both stores are bind-mounted below the same artifact root. Image pulls and Mongo provisioning happen outside the measured window; both measured startup sequences include creating their service container.`
            : 'ASYMMETRIC: official service + mongo run as docker containers (mongo data on a host bind mount); rust runs as a native host process writing MDBX to local disk. Mongo provisioning and image pulls happen outside the measured window. Official timing includes service-container start; Rust timing includes launching and waiting about 250 ms for its PostgreSQL forwarding container before the native service is spawned.',
        churnGateMode,
        explicitBucketSamples:
          projectBucketSampleCount > 0
            ? `${projectBucketSampleCount} effective project buckets on one stream (${requestedProjectBucketSampleCount} requested), not generated streams`
            : 'disabled',
        routedAccess:
          accessMode === 'auth_perimeter'
            ? `JWT user_id resolves ${authPerimeterProjectIds().length} ${authPerimeterTable} rows; client parameters do not grant projects`
            : 'subscription parameters drive routed buckets',
        authenticationPolicy: {
          audience,
          tokenIssuer: issuer,
          officialIssuerValidation: 'not configured by this harness',
          rustIssuerValidation: issuer
        },
        postgresTransport: {
          official: 'Docker private network; sslmode=disable',
          rust: symmetricDockerRuntime
            ? 'Docker private network; sslmode=disable'
            : process.env.POWERSYNC_RUST_POSTGRES_REPLICATION_URI
            ? sanitizedPostgresPolicy(process.env.POWERSYNC_RUST_POSTGRES_REPLICATION_URI)
            : 'host-to-Docker loopback forwarder; sslmode=disable'
        },
        endUserColdOpenReadiness:
          initialReadinessMode === 'sync-protocol'
            ? 'three concurrent boundaries: validated checkpoint completion for one routed /sync/stream subscription, target-specific complete materialization through the captured source LSN, and the replication slot confirmed-flush position for that LSN'
            : coldStartSyncProbeFallbackAllowed === true
            ? 'control-plane first; sync probe fallback allowed by env override'
            : 'control-plane only; no pre-measurement sync probe allowed'
      }
    },
    targets: {}
  };

  const targetStates = targetLabels.map((label) => createTargetState(createTarget(label)));
  await runBenchmarksAcrossTargets(targetStates);
  for (const state of targetStates) {
    results.targets[state.target.label] = finalizeTargetState(state);
  }
  assertPairedProtocolParity(results.targets);

  const comparisons = buildComparisons(results.targets, results.config);
  const comparisonSet = {
    generatedAt: new Date().toISOString(),
    profile: profileName,
    benchmarkDir,
    comparisons
  };
  const markdown = renderMarkdown({ results, comparisons });

  fs.writeFileSync(resultsPath, JSON.stringify(results, null, 2));
  fs.writeFileSync(comparePath, JSON.stringify(comparisonSet, null, 2));
  fs.writeFileSync(summaryPath, markdown);

  console.log(
    JSON.stringify(
      {
        status: 'ok',
        profile: profileName,
        targets: targetLabels,
        artifacts: {
          benchmarkDir,
          results: resultsPath,
          comparison: comparePath,
          summary: summaryPath
        },
        comparisons
      },
      null,
      2
    )
  );
} finally {
  if (process.env.POWERSYNC_USER_VALUE_DEBUG_KEEP !== '1') {
    await cleanupBenchmarkRuntime();
  }
}
}

if (isDirectRun()) {
  process.once('SIGINT', () => void shutdownOnSignal('SIGINT'));
  process.once('SIGTERM', () => void shutdownOnSignal('SIGTERM'));
  await main();
}

async function shutdownOnSignal(signal) {
  if (process.env.POWERSYNC_USER_VALUE_DEBUG_KEEP !== '1') {
    await cleanupBenchmarkRuntime();
  } else {
    console.error(
      `[user-value-benchmark] ${signal}: retaining benchmark runtime because POWERSYNC_USER_VALUE_DEBUG_KEEP=1`
    );
  }
  process.exit(signal === 'SIGINT' ? 130 : 143);
}

async function cleanupBenchmarkRuntime() {
  if (cleanupStarted) return;
  cleanupStarted = true;
  if (!runtimeSetupStarted) return;
  await stopOfficialService();
  await stopRustService();
  try {
    await cleanupDataset();
  } catch (error) {
    console.error('[user-value-benchmark] cleanup warning:', error.message);
  }
  spawnSync(
    'docker',
    composeCommandArgs('down', '--remove-orphans'),
    { cwd: composeCwd, stdio: 'ignore' }
  );
}

function isDirectRun() {
  const entrypoint = process.argv[1];
  return Boolean(entrypoint) && import.meta.url === pathToFileURL(entrypoint).href;
}

export {
  assertCursorProgression,
  assertPublicationResourceEvidence,
  assertPublicRunPreflight,
  assertTargetProtocolParityAgainstOfficial,
  attachChurnExpectations,
  activeSyncRulesStateMatches,
  buildComparisons,
  buildChurnBucketSpecs,
  buildInitialReadinessSpec,
  collectInitialReadinessBoundaries,
  buildReadinessSubscriptionSpec,
  buildPublicationReadiness,
  createBenchmarkJwt,
  churnMutationSql,
  churnEquivalenceRequestBody,
  collectEndUserMeasurementIssues,
  compareDecimalCursors,
  describeRustReplicationFeedback,
  defaultProtocolReadinessAttempts,
  effectiveProjectBucketSampleCount,
  extractMutationTargetLsn,
  normalizeDecimalCursor,
  initialEquivalenceRequestBody,
  initialReadinessSubject,
  initialMaterializationDiagnosticsReached,
  isRetryableChurnProtocolConvergenceError,
  isRetryableProtocolReadinessError,
  observeRustChurnMetricsAfterPublicTiming,
  percentile,
  readinessAuthPerimeterRow,
  resetObservedBucketState,
  renderMarkdown,
  resourceEvidenceForSnapshots,
  sumAvailable,
  summarizeSamples,
  syncRulesStateMatches
};

function positiveIntegerEnv(name, defaultValue) {
  const raw = process.env[name];
  if (raw == null || raw === '') return defaultValue;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer, got ${JSON.stringify(raw)}`);
  }
  return parsed;
}

function defaultProtocolReadinessAttempts(timeoutMsValue, minimumAttempts) {
  return Math.max(minimumAttempts, Math.ceil(timeoutMsValue / 1_000) + 1);
}

function positiveNumberEnv(name, defaultValue) {
  const raw = process.env[name];
  if (raw == null || raw === '') return defaultValue;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive number, got ${JSON.stringify(raw)}`);
  }
  return parsed;
}

function memoryLimitEnv(name, defaultValue) {
  const raw = process.env[name] ?? defaultValue;
  if (!/^\d+(?:\.\d+)?[bkmg]$/i.test(raw)) {
    throw new Error(`${name} must be a Docker memory limit such as 8g or 4096m, got ${JSON.stringify(raw)}`);
  }
  return raw.toLowerCase();
}

function memoryLimitBytes(value) {
  const match = value.match(/^(\d+(?:\.\d+)?)([bkmg])$/i);
  if (!match) throw new Error(`invalid Docker memory limit ${JSON.stringify(value)}`);
  const scale = { b: 1, k: 1024, m: 1024 ** 2, g: 1024 ** 3 }[match[2].toLowerCase()];
  return Number(match[1]) * scale;
}

function effectiveProjectBucketSampleCount({ requested, availableProjects, enabled }) {
  if (!enabled) return 0;
  return Math.min(Math.max(1, requested), availableProjects);
}

function optionalPositiveIntegerEnv(name) {
  const raw = process.env[name];
  if (raw == null || raw === '') return null;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer, got ${JSON.stringify(raw)}`);
  }
  return parsed;
}

function runtimePublicRunPreflightInput() {
  return {
    platform: os.platform(),
    targets: targetLabels,
    deploymentModel: symmetricDockerRuntime
      ? 'symmetric-linux-containers'
      : 'official-and-mongo-docker-rust-native',
    equalCpuMemoryLimits:
      symmetricDockerRuntime &&
      targetLimitsExplicit &&
      serviceLimitsExplicit &&
      mongoLimitsExplicit &&
      resourceBudgetMatches,
    sameStorageClass: symmetricDockerRuntime,
    samePostgresNetworkPath:
      symmetricDockerRuntime && process.env.POWERSYNC_RUST_POSTGRES_REPLICATION_URI == null,
    imageInputs: dockerImageInputs(),
    readinessBoundary:
      initialReadinessMode === 'sync-protocol'
        ? 'sync-protocol-checkpoint-complete'
        : 'target-specific-diagnostics',
    measuredRepeats: endUserSampleRepeats,
    warmupPairs,
    interleaveTargets,
    equivalenceGateEnabled,
    churnGateEnabled,
    churnGateMode,
    rawValidationRecordsRetained: retainRawValidationRecords,
    resourceEvidenceEnabled: symmetricDockerRuntime,
    appendOnlyArtifacts: !destructiveArtifactCleanupRequested,
    officialTuningReviewed,
    gitDirty: Boolean(tryCaptureVersion('git', ['status', '--porcelain']))
  };
}

function assertPublicRunPreflight(input) {
  const issues = [];
  if (input.platform !== 'linux') issues.push(`host platform must be linux, got ${input.platform ?? 'unknown'}`);
  if (
    !Array.isArray(input.targets) ||
    input.targets.length !== 2 ||
    !input.targets.includes('official') ||
    !input.targets.includes('rust')
  ) {
    issues.push('targets must be exactly official,rust');
  }
  if (input.deploymentModel !== 'symmetric-linux-containers') {
    issues.push(`deployment model must be symmetric Linux containers, got ${input.deploymentModel ?? 'unknown'}`);
  }
  if (input.equalCpuMemoryLimits !== true) issues.push('identical explicit CPU and memory limits are required');
  if (input.sameStorageClass !== true) issues.push('both targets must use the same storage class and durability policy');
  if (input.samePostgresNetworkPath !== true) issues.push('both targets must use the same PostgreSQL placement and network path');
  const requiredImages =
    input.deploymentModel === 'symmetric-linux-containers'
      ? ['official', 'mongo', 'postgres', 'rust']
      : ['official', 'mongo', 'postgres', 'socat'];
  for (const name of requiredImages) {
    const image = input.imageInputs?.[name];
    if (!isDigestPinnedImage(image)) issues.push(`${name} image must be pinned by a full sha256 digest, got ${image ?? 'unset'}`);
  }
  if (input.readinessBoundary !== 'sync-protocol-checkpoint-complete') {
    issues.push('initial readiness must use the shared sync-protocol checkpoint-complete boundary');
  }
  if (!Number.isInteger(input.measuredRepeats) || input.measuredRepeats < 20) {
    issues.push(`at least 20 measured paired repeats are required, got ${input.measuredRepeats ?? 'unset'}`);
  }
  if (!Number.isInteger(input.warmupPairs) || input.warmupPairs < 1) {
    issues.push(`at least one unmeasured warmup pair is required, got ${input.warmupPairs ?? 'unset'}`);
  }
  if (input.interleaveTargets !== true) issues.push('target order must be interleaved');
  if (input.equivalenceGateEnabled !== true) issues.push('initial protocol equivalence gate must be enabled');
  if (input.churnGateEnabled !== true) issues.push('churn protocol gate must be enabled');
  if (input.churnGateMode !== 'slot-lsn') issues.push('churn gate mode must be slot-lsn');
  if (input.rawValidationRecordsRetained !== true) {
    issues.push('compressed raw per-bucket validation records must be retained');
  }
  if (input.resourceEvidenceEnabled !== true) {
    issues.push('CPU, memory, I/O, storage, network, and WAL resource evidence must be enabled');
  }
  if (input.appendOnlyArtifacts !== true) issues.push('artifact storage must be append-only for the matrix');
  if (input.officialTuningReviewed !== true) issues.push('official-service tuning must be reviewed by the PowerSync team');
  if (input.gitDirty !== false) issues.push('the benchmark must run from a clean Git worktree');
  if (issues.length > 0) {
    throw new Error(`public benchmark preflight failed:\n- ${issues.join('\n- ')}`);
  }
}

function isDigestPinnedImage(image) {
  return typeof image === 'string' && /@sha256:[0-9a-f]{64}$/i.test(image);
}

// A first-time image pull inside a measured window would be attributed to
// the target's processing time; pull everything up front instead.
async function ensureDockerImagesPulled() {
  const images = [postgresImage];
  if (targetLabels.includes('official')) images.push(officialImage, mongoImage);
  if (targetLabels.includes('rust')) {
    if (symmetricDockerRuntime) images.push(rustImage);
    else images.push(socatImage);
  }
  for (const image of images) {
    if (
      image === rustImage &&
      symmetricDockerRuntime &&
      process.env.POWERSYNC_USER_VALUE_RUST_IMAGE_PULL === '0'
    ) {
      if (inspectDockerImage(image) == null) {
        throw new Error(`local Rust image ${image} is unavailable; build it before using RUST_IMAGE_PULL=0`);
      }
      log(`use local docker image ${image}`);
      continue;
    }
    log(`ensure docker image ${image}`);
    run('docker', ['pull', image]);
  }
}

function dockerImageInputs() {
  const inputs = {
    official: officialImage,
    mongo: mongoImage,
    postgres: postgresImage
  };
  if (symmetricDockerRuntime) inputs.rust = rustImage;
  else inputs.socat = socatImage;
  return inputs;
}

function composeCommandArgs(...args) {
  return [
    'compose',
    '--project-name',
    composeProjectName,
    '-f',
    path.join(composeCwd, 'compose.yaml'),
    '-f',
    path.join(benchmarkDir, 'compose.benchmark.override.yaml'),
    ...args
  ];
}

function writeComposeOverride() {
  fs.writeFileSync(
    path.join(benchmarkDir, 'compose.benchmark.override.yaml'),
    `services:\n  postgres:\n    image: ${JSON.stringify(postgresImage)}\n`
  );
}

function tryCaptureVersion(command, args) {
  try {
    const result = spawnSync(command, args, { encoding: 'utf8', timeout: 10_000 });
    return result.status === 0 ? result.stdout.trim() : null;
  } catch {
    return null;
  }
}

function collectHostMetadata() {
  const cpus = os.cpus();
  return {
    platform: os.platform(),
    osRelease: os.release(),
    osVersion: process.platform === 'darwin' ? tryCaptureVersion('sw_vers', ['-productVersion']) : null,
    arch: os.arch(),
    cpuModel: cpus[0]?.model ?? 'unknown',
    cpuCount: cpus.length,
    totalMemBytes: os.totalmem(),
    nodeVersion: process.version,
    rustcVersion: tryCaptureVersion('rustc', ['--version']),
    cargoVersion: tryCaptureVersion('cargo', ['--version']),
    dockerVersion: tryCaptureVersion('docker', ['--version']),
    dockerRuntime: tryCaptureVersion('docker', [
      'info',
      '--format',
      'server={{.ServerVersion}} cpus={{.NCPU}} memBytes={{.MemTotal}} os={{.OperatingSystem}}'
    ])
  };
}

function collectProvenance() {
  const redactedOfficialBaseConfig = redactOfficialConfig(officialConfigYaml(baseRulesYaml));
  const redactedOfficialDashboardConfig = redactOfficialConfig(
    officialConfigYaml(dashboardRulesYaml)
  );
  return {
    git: {
      commit: tryCaptureVersion('git', ['rev-parse', 'HEAD']),
      dirty: Boolean(tryCaptureVersion('git', ['status', '--porcelain']))
    },
    sha256: {
      cargoLock: sha256FileIfPresent(path.join(repoRoot, 'Cargo.lock')),
      npmLock: sha256FileIfPresent(path.join(sdkDir, 'package-lock.json')),
      benchmarkHarness: sha256FileIfPresent(fileURLToPath(import.meta.url)),
      resourceEvidenceCollector: sha256FileIfPresent(
        path.join(repoRoot, 'scripts', 'resource_evidence.mjs')
      ),
      benchmarkFixture: sha256FileIfPresent(
        path.join(sdkDir, 'src', 'benchmark-fixture.mjs')
      ),
      rustExecutable: rustExecutablePath ? sha256FileIfPresent(rustExecutablePath) : null,
      baseRules: sha256(baseRulesYaml),
      dashboardRules: sha256(dashboardRulesYaml),
      officialBaseConfigRedacted: sha256(redactedOfficialBaseConfig),
      officialDashboardConfigRedacted: sha256(redactedOfficialDashboardConfig)
    },
    dockerImageInputs: dockerImageInputs(),
    dockerImages: Object.fromEntries(
      Object.values(dockerImageInputs())
        .filter((image, index, images) => images.indexOf(image) === index)
        .map((image) => [image, inspectDockerImage(image)])
    )
  };
}

function sha256FileIfPresent(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return crypto.createHash('sha256').update(fs.readFileSync(filePath)).digest('hex');
}

function inspectDockerImage(image) {
  const result = spawnSync(
    'docker',
    ['image', 'inspect', '--format', '{{json .RepoDigests}}\t{{.Id}}', image],
    { encoding: 'utf8', timeout: 10_000 }
  );
  if (result.status !== 0) return null;
  const [repoDigestsJson, id] = result.stdout.trim().split('\t');
  try {
    return { id: id || null, repoDigests: JSON.parse(repoDigestsJson) };
  } catch {
    return { id: id || null, repoDigests: [] };
  }
}

function createTarget(label) {
  if (label === 'official') {
    return {
      label,
      endpoint: null,
      readinessPath: '/probes/liveness',
      // Provisioning the storage engine (mongo container + replica set
      // election) happens outside the measured processing window; the rust
      // target's embedded MDBX has no equivalent provisioning step.
      prepare: () => ensureOfficialMongo(),
      start: (syncRulesYaml) => startOfficialService(syncRulesYaml),
      stop: stopOfficialService,
      includeApiTokenInConfig: true,
      supportsControlPlane: true
    };
  }
  if (label === 'rust') {
    if (!rustComparable) {
      throw new Error('target rust requested without POWERSYNC_RUST_ALLOW_COMPARISON=1');
    }
    return {
      label,
      endpoint: null,
      readinessPath: '/healthz',
      start: (syncRulesYaml) => startRustService(syncRulesYaml),
      stop: stopRustService,
      supportsControlPlane: true
    };
  }

  throw new Error(`unsupported target label: ${label}`);
}

function createTargetState(target) {
  const targetDir = path.join(benchmarkDir, target.label);
  fs.mkdirSync(targetDir, { recursive: true });
  return {
    target,
    targetDir,
    endUserRuns: [],
    lifecycleSamples: {
      redeployDashboard: [],
      reprocessCurrent: [],
      redeployDashboardUnderWriteLoad: []
    }
  };
}

function captureBenchmarkResourceSnapshot(targetLabel, walLsn) {
  if (!symmetricDockerRuntime) {
    return {
      capturedAt: new Date().toISOString(),
      walLsn,
      components: {},
      storage: {},
      unavailable: 'resource evidence currently requires the symmetric-container runtime'
    };
  }
  const components =
    targetLabel === 'official'
      ? [
          { label: 'service', container: officialContainer },
          { label: 'mongo', container: officialMongoContainer }
        ]
      : [{ label: 'service', container: rustContainer }];
  const storagePaths =
    targetLabel === 'official'
      ? officialMongoDataDir == null
        ? []
        : [
            { label: 'mongo-db', filePath: path.join(officialMongoDataDir, 'db') },
            { label: 'mongo-config', filePath: path.join(officialMongoDataDir, 'configdb') }
          ]
      : [{ label: 'mdbx', filePath: path.join(benchmarkDir, 'rust-container-data') }];
  return captureResourceSnapshot({ components, storagePaths, walLsn });
}

function resourceEvidenceForSnapshots({ target, repeat, runtimeMode: mode, snapshots }) {
  const baseline = snapshots.baseline;
  const final = snapshots.final;
  const windows = {
    initial:
      baseline != null && snapshots.initialBoundaries != null
        ? diffResourceSnapshots(baseline, snapshots.initialBoundaries)
        : null,
    browser:
      snapshots.initialBoundaries != null && snapshots.browser != null
        ? diffResourceSnapshots(snapshots.initialBoundaries, snapshots.browser)
        : null,
    equivalence:
      snapshots.browser != null && snapshots.equivalence != null
        ? diffResourceSnapshots(snapshots.browser, snapshots.equivalence)
        : null,
    churn:
      snapshots.equivalence != null && snapshots.churn != null
        ? diffResourceSnapshots(snapshots.equivalence, snapshots.churn)
        : null,
    total: baseline != null && final != null ? diffResourceSnapshots(baseline, final) : null
  };
  const captured =
    baseline != null &&
    final != null &&
    baseline.unavailable == null &&
    [windows.initial, windows.total].every(
      (window) =>
        window != null &&
        Object.values(window.components ?? {}).every((component) => component.status === 'captured')
    );
  return {
    schemaVersion: 1,
    target,
    repeat,
    runtimeMode: mode,
    status: captured ? 'captured' : 'unavailable',
    limitations: [
      'cgroup and process peak-memory fields are lifetime peaks; MongoDB includes provisioning before the measured window',
      'component network counters are not summed because service-to-storage traffic appears in both namespaces',
      'Docker stats fallback reports instantaneous CPU and current memory, not cumulative CPU or peak RSS',
      'WAL bytes report the cluster-wide inserted WAL-position delta during the phase, not bytes decoded by a target'
    ],
    snapshots,
    windows
  };
}

async function runBenchmarksAcrossTargets(targetStates) {
  await runEndUserLaneAcrossTargets(targetStates);
  await runLifecycleLaneAcrossTargets(targetStates);
}

async function runEndUserLaneAcrossTargets(targetStates) {
  for (let warmup = 1; warmup <= warmupPairs; warmup += 1) {
    const orderedTargets = interleaveTargets ? orderTargetsForRepeat(targetStates, warmup, 0) : [...targetStates];
    for (const state of orderedTargets) {
      executionSchedule.push({ lane: 'endUserWarmup', repeat: warmup, target: state.target.label });
      log(`warmup pair ${warmup}/${warmupPairs}: ${state.target.label} (unrecorded)`);
      await runEndUserLaneOnce(state.target, state.targetDir, warmup, { warmup: true });
    }
  }
  for (let repeat = 1; repeat <= endUserSampleRepeats; repeat += 1) {
    const orderedTargets = interleaveTargets ? orderTargetsForRepeat(targetStates, repeat, 0) : [...targetStates];
    for (const state of orderedTargets) {
      executionSchedule.push({ lane: 'endUser', repeat, target: state.target.label });
      state.endUserRuns.push(await runEndUserLaneOnce(state.target, state.targetDir, repeat));
    }
  }
}

async function runEndUserLaneOnce(target, targetDir, repeat, { warmup = false } = {}) {
  await prepareScenarioState(target.label, { syncRulesYaml: dashboardRulesYaml, cleanStorage: true });
  if (target.prepare) await target.prepare();
  const targetLsn = await captureTargetLsn({ underWriteLoad: false, operation: 'startup' });
  const resourcePath = path.join(
    targetDir,
    warmup ? `resource-evidence-warmup.r${repeat}.json` : `resource-evidence.r${repeat}.json`
  );
  const resourceSnapshots = {};
  if (!warmup) {
    resourceSnapshots.baseline = captureBenchmarkResourceSnapshot(target.label, queryCurrentInsertLsn());
  }
  const processingStartedAt = performance.now();
  let endpoint = null;
  let equivalence = null;
  const runResult = {};
  let publicationResourceError = null;
  try {
    ({ endpoint } = await startTargetAndWaitReady(target, dashboardRulesYaml, publicRun ? 1 : 2));
    const readiness = await waitForScenarioReady({
      target,
      endpoint,
      expectedRules: dashboardRulesYaml,
      targetLsn,
      processingStartedAt,
      allowSyncProbeFallback: coldStartSyncProbeFallbackAllowed,
      publicationBoundary: initialReadinessMode === 'sync-protocol',
      repeat
    });
    if (!warmup) {
      resourceSnapshots.initialBoundaries = captureBenchmarkResourceSnapshot(
        target.label,
        queryCurrentInsertLsn()
      );
    }

    const resultPath = path.join(
      targetDir,
      warmup
        ? `end-user-warmup.r${repeat}.json`
        : processingOnly
          ? `end-user-processing-only.r${repeat}.json`
          : `end-user-playwright.r${repeat}.json`
    );
    const browserResult =
      processingOnly || warmup
        ? {
            processingOnly: true,
            scenarios: {}
          }
        : await runPlaywrightAsync({
          targetLabel: compactBrowserLabel(`${target.label}-user-r${repeat}`),
          endpoint,
          resultPath,
          scenarios: ['coldInitialSync', 'warmReconnect', 'insertPropagation', 'updatePropagation', 'deletePropagation'],
          iterations: 1
        });
    if (processingOnly || warmup) {
      fs.writeFileSync(resultPath, JSON.stringify(browserResult, null, 2));
    }
    if (!warmup) {
      resourceSnapshots.browser = captureBenchmarkResourceSnapshot(target.label, queryCurrentInsertLsn());
    }
    const diagnostics = await fetchTargetMetrics(endpoint).catch((error) => ({
      ok: false,
      error: compactErrorMessage(error.message)
    }));
    equivalence =
      equivalenceGateEnabled && !warmup
        ? await runBucketProtocolEquivalenceGate({ target, endpoint, repeat, targetDir })
        : null;
    if (!warmup) {
      resourceSnapshots.equivalence = captureBenchmarkResourceSnapshot(target.label, queryCurrentInsertLsn());
    }
    let churn = null;
    if (churnGateEnabled && !warmup) {
      churn = await runChurnProtocolGate({
        target,
        endpoint,
        repeat,
        targetDir,
        readiness,
        initialEquivalence: equivalence
      });
    }
    if (!warmup) {
      resourceSnapshots.churn = captureBenchmarkResourceSnapshot(target.label, queryCurrentInsertLsn());
    }
    Object.assign(runResult, {
      repeat,
      readiness,
      diagnostics,
      equivalence,
      churn,
      artifactPath: resultPath,
      raw: browserResult
    });
    return runResult;
  } finally {
    if (equivalence?.verificationSpecs) delete equivalence.verificationSpecs;
    let stopError = null;
    try {
      if (!warmup) {
        let resourceEvidence = null;
        try {
          resourceSnapshots.final = captureBenchmarkResourceSnapshot(target.label, queryCurrentInsertLsn());
          resourceEvidence = resourceEvidenceForSnapshots({
            target: target.label,
            repeat,
            runtimeMode,
            snapshots: resourceSnapshots
          });
          if (publicRun) {
            assertPublicationResourceEvidence(resourceEvidence);
            resourceEvidence.publicationValidation = { status: 'passed' };
          }
          fs.writeFileSync(resourcePath, `${JSON.stringify(resourceEvidence, null, 2)}\n`);
          runResult.resources = {
            status: resourceEvidence.status,
            artifactPath: resourcePath,
            initial: resourceEvidence.windows.initial,
            total: resourceEvidence.windows.total
          };
        } catch (error) {
          const resourceError = compactErrorMessage(error.message);
          const failureArtifact =
            resourceEvidence == null
              ? { schemaVersion: 1, target: target.label, repeat, status: 'failed', error: resourceError }
              : {
                  ...resourceEvidence,
                  publicationValidation: { status: 'failed', error: resourceError }
                };
          fs.writeFileSync(resourcePath, `${JSON.stringify(failureArtifact, null, 2)}\n`);
          runResult.resources = { status: 'failed', artifactPath: resourcePath, error: resourceError };
          if (publicRun) publicationResourceError = error;
        }
      }
    } finally {
      try {
        await target.stop();
      } catch (error) {
        stopError = error;
      }
    }
    if (publicationResourceError != null && stopError != null) {
      throw new AggregateError([publicationResourceError, stopError], 'resource validation and target teardown failed');
    }
    if (publicationResourceError != null) throw publicationResourceError;
    if (stopError != null) throw stopError;
  }
}

async function runLifecycleLaneAcrossTargets(targetStates) {
  const scenarioDefinitions = [
    {
      scenarioName: 'redeployDashboard',
      baseRules: baseRulesYaml,
      nextRules: dashboardRulesYaml,
      operation: 'deploy',
      underWriteLoad: false
    },
    {
      scenarioName: 'reprocessCurrent',
      baseRules: dashboardRulesYaml,
      nextRules: dashboardRulesYaml,
      operation: 'reprocess',
      underWriteLoad: false
    },
    {
      scenarioName: 'redeployDashboardUnderWriteLoad',
      baseRules: baseRulesYaml,
      nextRules: dashboardRulesYaml,
      operation: 'deploy',
      underWriteLoad: true
    }
  ];
  for (let repeat = 1; repeat <= lifecycleRepeats; repeat += 1) {
    for (let scenarioIndex = 0; scenarioIndex < scenarioDefinitions.length; scenarioIndex += 1) {
      const scenarioDefinition = scenarioDefinitions[scenarioIndex];
      const orderedTargets = interleaveTargets
        ? orderTargetsForRepeat(targetStates, repeat, scenarioIndex)
        : [...targetStates];
      for (const state of orderedTargets) {
        executionSchedule.push({
          lane: 'lifecycle',
          scenario: scenarioDefinition.scenarioName,
          repeat,
          target: state.target.label
        });
        state.lifecycleSamples[scenarioDefinition.scenarioName].push(
          await runLifecycleScenario(state.target, {
            ...scenarioDefinition,
            repeat,
            targetDir: state.targetDir
          })
        );
      }
    }
  }
}

function finalizeTargetState(state) {
  const { target, targetDir } = state;
  const endUser = summarizeEndUserRuns(state.endUserRuns, targetDir);
  const lifecycle = summarizeLifecycleSamples(state.lifecycleSamples, targetDir);
  const result = {
    label: target.label,
    generatedAt: new Date().toISOString(),
    methodology: {
      interleavedTargets: interleaveTargets,
      endUserSampleRepeats,
      lifecycleRepeats,
      coldOpenReadiness:
        initialReadinessMode === 'sync-protocol'
          ? 'validated checkpoint completion for one routed subscription, target-specific complete materialization, and replication-slot confirmed-flush position are recorded separately'
          : coldStartSyncProbeFallbackAllowed === true
          ? 'control-plane first with opt-in sync probe fallback'
          : 'control-plane only; no pre-measurement sync probe'
    },
    artifacts: {
      dir: targetDir,
      endUser: endUser.artifactPath,
      lifecycle: lifecycle.artifactPath
    },
    endUser: endUser.payload,
    developerUsability: lifecycle.payload.developerUsability,
    recovery: lifecycle.payload.recovery,
    lifecycleScenarios: lifecycle.payload.scenarios
  };

  fs.writeFileSync(path.join(targetDir, 'target-result.json'), JSON.stringify(result, null, 2));
  return result;
}

function summarizeEndUserRuns(runs, targetDir) {
  const mergedScenarios = mergeBrowserScenarioRuns(runs);
  const samples = mergedScenarios;
  const coldInitialSyncSamples = samples.coldInitialSync?.samples ?? [];
  const warmReconnectSamples = samples.warmReconnect?.samples ?? [];
  const insertPropagationSamples = samples.insertPropagation?.samples ?? [];
  const updatePropagationSamples = samples.updatePropagation?.samples ?? [];
  const deletePropagationSamples = samples.deletePropagation?.samples ?? [];
  const payload = {
    runs: runs.map((run) => ({
      repeat: run.repeat,
      readiness: run.readiness,
      diagnostics: run.diagnostics,
      equivalence: run.equivalence,
      churn: run.churn,
      resources: run.resources,
      artifactPath: run.artifactPath
    })),
    raw: {
      scenarios: mergedScenarios
    },
    summary: {
      processing: summarizeReadinessRuns(runs),
      resources: summarizeResourceRuns(runs),
      coldOpen: summarizeSamples(coldInitialSyncSamples, ['connectedMs', 'firstSyncMs', 'steadyMs']),
      warmReconnect: summarizeSamples(warmReconnectSamples, ['connectedMs', 'steadyMs']),
      liveInsert: summarizeSamples(insertPropagationSamples, ['visibleMs']),
      liveUpdate: summarizeSamples(updatePropagationSamples, ['visibleMs']),
      liveDelete: summarizeSamples(deletePropagationSamples, ['visibleMs']),
      liveChangeToVisible: summarizeSamples(
        [
          ...insertPropagationSamples,
          ...updatePropagationSamples,
          ...deletePropagationSamples
        ],
        ['visibleMs']
      ),
      churn: summarizeSamples(
        runs.map((run) => run.churn).filter(Boolean),
        [
          'applySqlMs',
          'replicationCatchupMs',
          'slotAckCatchupMs',
          'protocolProbeMs',
          'churnToProtocolVerifiedMs'
        ]
      )
    }
  };

  const artifactPath = path.join(targetDir, 'end-user-summary.json');
  fs.writeFileSync(artifactPath, JSON.stringify(payload, null, 2));
  return { payload, artifactPath };
}

function mergeBrowserScenarioRuns(runs) {
  const scenarioNames = new Set();
  for (const run of runs) {
    for (const scenarioName of Object.keys(run.raw?.scenarios ?? {})) {
      scenarioNames.add(scenarioName);
    }
  }

  const merged = {};
  for (const scenarioName of scenarioNames) {
    const sourceScenario = runs.find((run) => run.raw?.scenarios?.[scenarioName])?.raw?.scenarios?.[scenarioName] ?? {};
    merged[scenarioName] = {
      ...sourceScenario,
      samples: runs.flatMap((run) => run.raw?.scenarios?.[scenarioName]?.samples ?? [])
    };
  }
  return merged;
}

function orderTargetsForRepeat(targets, repeat, groupIndex = 0) {
  if (targets.length <= 1) return [...targets];
  const offset = (repeat - 1 + groupIndex) % targets.length;
  return [...targets.slice(offset), ...targets.slice(0, offset)];
}

function summarizeLifecycleSamples(scenarios, targetDir) {
  const payload = {
    scenarios,
    developerUsability: {
      redeployDashboard: summarizeSamples(scenarios.redeployDashboard, [
        'requestAcceptedMs',
        'publishToActiveMs',
        'publishToReadyMs',
        'publishToFreshUserReadyMs',
        'publishToUsableMs'
      ]),
      reprocessCurrent: summarizeSamples(scenarios.reprocessCurrent, [
        'requestAcceptedMs',
        'publishToActiveMs',
        'publishToReadyMs',
        'publishToFreshUserReadyMs',
        'publishToUsableMs'
      ]),
      redeployDashboardUnderWriteLoad: summarizeSamples(scenarios.redeployDashboardUnderWriteLoad, [
        'requestAcceptedMs',
        'publishToActiveMs',
        'publishToReadyMs',
        'publishToFreshUserReadyMs',
        'publishToUsableMs'
      ])
    },
    recovery: {
      redeployDashboard: summarizeSamples(scenarios.redeployDashboard, [
        'redeployToFirstDataMs',
        'redeployToUsableAppMs'
      ]),
      redeployDashboardUnderWriteLoad: summarizeSamples(scenarios.redeployDashboardUnderWriteLoad, [
        'redeployToFirstDataMs',
        'redeployToUsableAppMs'
      ]),
      reprocessCurrent: summarizeSamples(scenarios.reprocessCurrent, [
        'redeployToFirstDataMs',
        'redeployToUsableAppMs'
      ])
    }
  };

  const artifactPath = path.join(targetDir, 'lifecycle-summary.json');
  fs.writeFileSync(artifactPath, JSON.stringify(payload, null, 2));
  return { payload, artifactPath };
}

async function runLifecycleScenario(
  target,
  { scenarioName, repeat, baseRules, nextRules, operation, underWriteLoad, targetDir }
) {
  let endpoint = null;
  try {
    log(`lifecycle ${target.label}/${scenarioName}#${repeat}: prepare base state`);
    await prepareScenarioState(target.label, { syncRulesYaml: baseRules, cleanStorage: true });
    ({ endpoint } = await startTargetAndWaitReady(target, baseRules));
    await waitForScenarioReady({
      target,
      endpoint,
      expectedRules: baseRules
    });
    log(`lifecycle ${target.label}/${scenarioName}#${repeat}: base state ready`);

    const beforeState = await fetchCurrentState(endpoint);
    const beforeVersion = beforeState.version;
    const scenarioStartedAt = performance.now();

    const loadController = underWriteLoad ? startWriteLoad({ targetLabel: target.label, scenarioName, repeat }) : null;
    log(`lifecycle ${target.label}/${scenarioName}#${repeat}: ${operation} requested`);
    const mutationResponse = await performMutation({
      target,
      endpoint,
      operation,
      nextRules,
      beforeVersion,
      scenarioName,
      repeat
    });
    const requestAcceptedMs = round(performance.now() - scenarioStartedAt);
    if (!mutationResponse.ok) {
      throw new Error(
        `mutation request failed with ${mutationResponse.statusCode}: ${JSON.stringify(
          mutationResponse.body ?? mutationResponse.rawBody ?? null
        )}`
      );
    }

    if (loadController) {
      await delay(250);
      await loadController.stop();
      await delay(150);
    }

    const targetLsn = await captureTargetLsn({ underWriteLoad, operation });
    const browserResultPath = path.join(targetDir, `${scenarioName}.r${repeat}.playwright.json`);
    const browserPromise = runPlaywrightAsync({
      targetLabel: compactBrowserLabel(`${target.label}-${scenarioName}-r${repeat}`),
      endpoint,
      resultPath: browserResultPath,
      scenarios: ['coldInitialSync'],
      iterations: 1
    });
    const expectedState = expectedMutationState(beforeVersion, mutationResponse.body, nextRules);
    const activePromise = waitForActiveState(endpoint, {
      expectedVersion: expectedState.version,
      expectedContent: expectedState.content,
      expectedSlotName: expectedState.slotName,
      timeoutMs
    });

    const activeOutcome = await settleOutcome(activePromise);
    const activeState = activeOutcome.ok ? activeOutcome.value : null;
    const issues = [];
    if (!activeOutcome.ok) {
      issues.push(`active state: ${compactErrorMessage(activeOutcome.error?.message ?? activeOutcome.error)}`);
    }

    const readyPromise =
      activeState == null
        ? Promise.reject(new Error('ready state skipped because active sync-rules state never settled'))
        : (async () => {
            const replicationSlotName =
              target.label === 'rust' ? rustSlot : activeState?.slotName ?? expectedState.slotName ?? null;
            if (target.label === 'rust') {
              return await waitForRustMetricsToReach({
                endpoint,
                targetLsn,
                timeoutMs,
                pollIntervalMs: 100
              });
            }
            if (replicationSlotName) {
              return await waitForReplicationSlotToReach({
                slotName: replicationSlotName,
                targetLsn,
                timeoutMs,
                pollIntervalMs: 100
              });
            }
            return await waitForDiagnosticsToReach({
              endpoint,
              targetLsn,
              expectedVersion: expectedState.version,
              timeoutMs,
              pollIntervalMs: 100
            });
          })();

    const [browserOutcome, readyOutcome] = await Promise.all([
      settleOutcome(browserPromise),
      settleOutcome(readyPromise)
    ]);

    if (!readyOutcome.ok) {
      issues.push(`ready state: ${compactErrorMessage(readyOutcome.error?.message ?? readyOutcome.error)}`);
    }
    if (!browserOutcome.ok) {
      throw browserOutcome.error;
    }

    const browserResult = browserOutcome.value;
    const readyState = readyOutcome.ok ? readyOutcome.value : null;
    log(
      `lifecycle ${target.label}/${scenarioName}#${repeat}: ${readyState ? 'ready' : 'usable (ready state partial)'}`
    );
    const coldSample = browserResult.scenarios.coldInitialSync.samples[0];

    return {
      status: issues.length === 0 ? 'passed' : 'partial',
      scenarioName,
      repeat,
      operation,
      underWriteLoad,
      requestAcceptedMs,
      ...(issues.length > 0 ? { issues } : {}),
      publishToActiveMs: Number.isFinite(activeState?.elapsedMs) ? round(activeState.elapsedMs) : null,
      publishToReadyMs: Number.isFinite(readyState?.elapsedMs) ? round(readyState.elapsedMs) : null,
      publishToFreshUserReadyMs: round(requestAcceptedMs + coldSample.firstSyncMs),
      publishToUsableMs: round(requestAcceptedMs + coldSample.steadyMs),
      redeployToFirstDataMs: round(requestAcceptedMs + coldSample.firstSyncMs),
      redeployToUsableAppMs: round(requestAcceptedMs + coldSample.steadyMs),
      activeVersion: activeState?.version ?? null,
      readyLastLsn: readyState?.last_lsn ?? readyState?.confirmed_flush_lsn ?? null,
      targetLsn,
      controlPlane: {
        acceptedStatus: mutationResponse.statusCode,
        acceptedBody: mutationResponse.body,
        active: activeState,
        ready: readyState,
        ...(issues.length > 0 ? { issues } : {})
      },
      browser: coldSample
    };
  } catch (error) {
    const errorMessage = compactErrorMessage(error?.message ?? String(error));
    log(`lifecycle ${target.label}/${scenarioName}#${repeat}: failed (${errorMessage})`);
    return {
      status: 'failed',
      scenarioName,
      repeat,
      operation,
      underWriteLoad,
      error: errorMessage
    };
  } finally {
    if (endpoint != null) {
      await target.stop();
    }
  }
}

async function prepareScenarioState(targetLabel, { syncRulesYaml, cleanStorage }) {
  if (cleanStorage) {
    await stopOfficialService();
    await stopRustService();
    await resetTargetState(targetLabel);
  }

  await prepareDatasetSchema(targetLabel);
  await loadDatasetRows();
}

async function startTargetAndWaitReady(target, syncRulesYaml, attempts = 2) {
  let lastError = null;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const started = (await target.start(syncRulesYaml)) ?? {};
      const endpoint = started.endpoint;
      await waitForReadiness(`${endpoint}${target.readinessPath}`);
      return { endpoint };
    } catch (error) {
      lastError = error;
      log(
        `startup ${target.label} attempt ${attempt}/${attempts} failed: ${compactErrorMessage(
          error?.message ?? error
        )}`
      );
      await target.stop();
      if (attempt < attempts) {
        await delay(500);
      }
    }
  }
  throw lastError;
}

async function resetTargetState(targetLabel) {
  if (targetLabel === 'official') {
    runSql(`
DROP SCHEMA IF EXISTS powersync CASCADE;
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE active = FALSE
  AND slot_name LIKE '${escapeLiteral(officialSlotPrefix)}%';
`.trim());
    return;
  }

  if (targetLabel === 'rust') {
    runSql(`
DROP SCHEMA IF EXISTS powersync CASCADE;
DROP PUBLICATION IF EXISTS ${rustPublication};
SELECT pg_drop_replication_slot('${escapeLiteral(rustSlot)}')
WHERE EXISTS (
  SELECT 1
  FROM pg_replication_slots
  WHERE slot_name = '${escapeLiteral(rustSlot)}'
    AND active = FALSE
);
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE active = FALSE
  AND slot_name LIKE '${escapeLiteral(officialSlotPrefix)}%';
`.trim());
    fs.rmSync(path.join(benchmarkDir, 'rust-sync-edge'), { recursive: true, force: true });
    fs.rmSync(path.join(benchmarkDir, 'rust-wire-mdbx'), { recursive: true, force: true });
    fs.rmSync(path.join(benchmarkDir, 'rust-wire-mdbx-tail'), { recursive: true, force: true });
    fs.rmSync(path.join(benchmarkDir, 'rust-sync-rules-state.json'), { force: true });
    fs.rmSync(path.join(benchmarkDir, 'rust-container-data'), { recursive: true, force: true });
    return;
  }

  throw new Error(`unsupported benchmark target reset: ${targetLabel}`);
}

async function prepareDatasetSchema(targetLabel) {
  runSql(schemaSql(targetLabel));
  assertBenchmarkSchemaInvariants();
}

function assertBenchmarkSchemaInvariants() {
  const taskReplicaIdentity = runSqlQuery(
    `SELECT relreplident FROM pg_class WHERE oid = '${TABLES.tasks}'::regclass`
  );
  if (taskReplicaIdentity !== 'd') {
    throw new Error(
      `${TABLES.tasks} must use default replica identity for benchmark churn tail-op counting; got ${JSON.stringify(taskReplicaIdentity)}`
    );
  }
}

async function loadDatasetRows() {
  const insertBatchSize = Number.parseInt(process.env.POWERSYNC_USER_VALUE_INSERT_BATCH_SIZE ?? '1000', 10);
  const batchSize = Number.isFinite(insertBatchSize) && insertBatchSize > 0 ? Math.floor(insertBatchSize) : 1000;
  let rowCount = 0;
  const accessRows = authPerimeterRows();
  await runSqlStream(async (stdin) => {
    await writeSql(
      stdin,
      `${authPerimeterMode ? `DELETE FROM ${authPerimeterTable}; ` : ''}DELETE FROM ${TABLES.comments}; DELETE FROM ${TABLES.tasks}; DELETE FROM ${TABLES.projects}; DELETE FROM ${TABLES.memberships}; DELETE FROM ${TABLES.organizations};\n`
    );
    if (accessRows.length > 0) {
      await writeSql(stdin, `${insertAuthPerimeterRowsSql(accessRows)}\n`);
    }
    let batch = [];
    for (const row of initialTaskRows()) {
      batch.push(row);
      rowCount += 1;
      if (batch.length >= batchSize) {
        await writeSql(stdin, `${insertTaskRowsSql(batch)}\n`);
        batch = [];
      }
    }
    if (batch.length > 0) {
      await writeSql(stdin, `${insertTaskRowsSql(batch)}\n`);
    }
  });
  log(
    `loaded ${rowCount} task rows in batches of ${batchSize}` +
      (accessRows.length > 0 ? ` plus ${accessRows.length} auth-perimeter rows` : '')
  );
}

async function cleanupDataset() {
  runSql(cleanupSql());
}

function schemaSql(targetLabel) {
  const publicationTables = benchmarkTableNames().join(', ');
  const extraPublication =
    targetLabel === 'rust'
      ? `CREATE PUBLICATION ${rustPublication} FOR TABLE ${publicationTables};`
      : '';
  const authPerimeterSchema = authPerimeterMode
    ? `
CREATE TABLE ${authPerimeterTable} (
  id text PRIMARY KEY,
  user_id text NOT NULL,
  org_id text NOT NULL,
  project_id text NOT NULL,
  role text NOT NULL,
  updated_at timestamptz NOT NULL
);
CREATE INDEX ON ${authPerimeterTable} (user_id, project_id);
CREATE INDEX ON ${authPerimeterTable} (org_id, project_id);
`
    : '';

  return `
DROP SCHEMA IF EXISTS powersync CASCADE;
DROP PUBLICATION IF EXISTS powersync;
DROP PUBLICATION IF EXISTS ${benchmarkPublication};
DROP PUBLICATION IF EXISTS ${rustPublication};
${authPerimeterMode ? `DROP TABLE IF EXISTS ${authPerimeterTable};` : ''}
DROP TABLE IF EXISTS ${TABLES.comments};
DROP TABLE IF EXISTS ${TABLES.tasks};
DROP TABLE IF EXISTS ${TABLES.projects};
DROP TABLE IF EXISTS ${TABLES.memberships};
DROP TABLE IF EXISTS ${TABLES.organizations};
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE active = FALSE
  AND (
    slot_name = '${escapeLiteral(benchmarkSlot)}'
    OR slot_name = '${escapeLiteral(rustSlot)}'
    OR slot_name LIKE '${escapeLiteral(officialSlotPrefix)}%'
  );

CREATE TABLE ${TABLES.organizations} (
  id text PRIMARY KEY,
  name text NOT NULL,
  owner_id text NOT NULL,
  plan text NOT NULL,
  region text NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE ${TABLES.memberships} (
  id text PRIMARY KEY,
  org_id text NOT NULL,
  user_id text NOT NULL,
  owner_id text NOT NULL,
  role text NOT NULL,
  display_name text NOT NULL,
  email text NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE ${TABLES.projects} (
  id text PRIMARY KEY,
  org_id text NOT NULL,
  code text NOT NULL,
  name text NOT NULL,
  status text NOT NULL,
  priority integer NOT NULL,
  owner_id text NOT NULL,
  updated_at timestamptz NOT NULL,
  summary text NOT NULL
);

CREATE TABLE ${TABLES.tasks} (
  id text PRIMARY KEY,
  org_id text NOT NULL,
  project_id text NOT NULL,
  owner_id text NOT NULL,
  title text NOT NULL,
  status text NOT NULL,
  priority integer NOT NULL,
  assignee_id text NOT NULL,
  story_points integer NOT NULL,
  updated_at timestamptz NOT NULL,
  summary text NOT NULL
);

CREATE TABLE ${TABLES.comments} (
  id text PRIMARY KEY,
  org_id text NOT NULL,
  task_id text NOT NULL,
  owner_id text NOT NULL,
  author_id text NOT NULL,
  body text NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

${authPerimeterSchema}
CREATE INDEX ON ${TABLES.tasks} (org_id, project_id);
CREATE PUBLICATION powersync FOR TABLE ${publicationTables};
CREATE PUBLICATION ${benchmarkPublication} FOR TABLE ${publicationTables};
SELECT * FROM pg_create_logical_replication_slot('${escapeLiteral(benchmarkSlot)}', 'pgoutput');
${extraPublication}
`.trim();
}

function buildUserValueRulesYaml({ includeExtraStream = false } = {}) {
  const routeStreams =
    routeStreamsEnabled && authPerimeterMode
      ? `  ${authPerimeterStream}:
    with:
      accessible_projects: SELECT project_id AS project_id FROM ${authPerimeterTable} WHERE user_id = auth.user_id()
    queries:
      - SELECT ${TABLES.tasks}.id, ${TABLES.tasks}.org_id, ${TABLES.tasks}.project_id, ${TABLES.tasks}.title, ${TABLES.tasks}.status, ${TABLES.tasks}.priority, ${TABLES.tasks}.assignee_id, ${TABLES.tasks}.story_points, ${TABLES.tasks}.updated_at, ${TABLES.tasks}.summary FROM ${TABLES.tasks}, accessible_projects AS bucket WHERE ${TABLES.tasks}.project_id = bucket.project_id
`
      : routeStreamsEnabled
        ? `  tasks_by_project:
    query: SELECT id, org_id, project_id, title, status, priority, assignee_id, story_points, updated_at, summary FROM ${TABLES.tasks} WHERE project_id = subscription.parameter('project_id')
  tasks_by_org:
    query: SELECT id, org_id, project_id, title, status, priority, assignee_id, story_points, updated_at, summary FROM ${TABLES.tasks} WHERE org_id = subscription.parameter('org_id')
`
        : '';
  const extraStream = includeExtraStream
    ? `  tasks_dashboard:
    auto_subscribe: true
    query: SELECT id, org_id, project_id, title, status, priority, assignee_id, story_points, updated_at, summary FROM ${TABLES.tasks}
`
    : '';
  const fanoutStreams = buildSyncRuleFanoutStreams();

  return `
config:
  edition: 3
streams:
  tasks:
    auto_subscribe: true
    query: SELECT id, org_id, project_id, title, status, priority, assignee_id, story_points, updated_at, summary FROM ${TABLES.tasks}
${routeStreams}
${extraStream}${fanoutStreams}`.trimStart();
}

function buildSyncRuleFanoutStreams() {
  if (syncRuleFanout <= 0) return '';
  const routeColumns = ['org_id', 'project_id', 'status', 'assignee_id', 'priority', 'owner_id'];
  let yaml = '';
  for (let index = 0; index < syncRuleFanout; index += 1) {
    const column = routeColumns[index % routeColumns.length];
    yaml += `  tasks_fanout_${index + 1}_${column}:
    query: SELECT id, org_id, project_id, title, status, priority, assignee_id, story_points, updated_at, summary FROM ${TABLES.tasks} WHERE ${column} = subscription.parameter('${column}_${index + 1}')
`;
  }
  return yaml;
}

function* initialTaskRows() {
  yield* baseTaskRows();
  yield initialUpdateRow();
  yield fixture.ids.deleteTaskRow;
  yield* fixture.ids.batchDeleteRows;
}

function* baseTaskRows() {
  for (let orgNumber = 1; orgNumber <= profile.orgCount; orgNumber += 1) {
    for (let projectIndex = 1; projectIndex <= profile.projectsPerOrg; projectIndex += 1) {
      for (let taskIndexValue = 1; taskIndexValue <= profile.tasksPerProject; taskIndexValue += 1) {
        yield generatedTaskRow(orgNumber, projectIndex, taskIndexValue);
      }
    }
  }
}

function generatedTaskRow(orgNumber, projectIndex, taskIndexValue) {
  const currentOrgId = orgId(orgNumber);
  return {
    id: taskId(orgNumber, projectIndex, taskIndexValue),
    org_id: currentOrgId,
    project_id: projectId(orgNumber, projectIndex),
    owner_id: fixture.targetUserId,
    title: generatedTaskTitle(projectIndex, taskIndexValue),
    status:
      taskIndexValue % 7 === 0
        ? 'blocked'
        : taskIndexValue % 5 === 0
          ? 'done'
          : taskIndexValue % 3 === 0
            ? 'in_progress'
            : 'todo',
    priority: ((taskIndexValue - 1) % 5) + 1,
    assignee_id: userId(orgNumber, ((projectIndex + taskIndexValue - 2) % profile.usersPerOrg) + 1),
    story_points: ((taskIndexValue - 1) % 8) + 1,
    updated_at: '2026-01-01T00:00:00Z',
    summary: repeatMd5(`${currentOrgId}:${projectIndex}:${taskIndexValue}`, 3)
  };
}

function generatedTaskTitle(projectIndex, taskIndexValue) {
  return `Task ${projectIndex}.${taskIndexValue}`;
}

function initialUpdateRow() {
  return {
    ...makeSentinelTaskRow('delete', 1, fixture.targetOrgId, profile),
    id: fixture.ids.updateTaskId,
    title: fixture.ids.updateTaskOriginalTitle,
    status: 'todo',
    summary: 'sentinel:update:1'
  };
}

function expectedTaskRowCount() {
  return profile.orgCount * profile.projectsPerOrg * profile.tasksPerProject + 2 + fixture.ids.batchDeleteRows.length;
}

function repeatMd5(value, times) {
  return Array.from({ length: times }, () => crypto.createHash('md5').update(value).digest('hex')).join('');
}

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function chunkArray(values, requestedSize) {
  const size = Number.isFinite(requestedSize) && requestedSize > 0 ? Math.floor(requestedSize) : 1000;
  const chunks = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

function insertTaskRowsSql(rows) {
  if (rows.length === 0) return '';
  const values = rows.map(taskRowValuesSql).join(',\n');
  return `
INSERT INTO ${TABLES.tasks}
  (id, org_id, project_id, title, status, priority, assignee_id, story_points, updated_at, summary, owner_id)
VALUES
${values}
ON CONFLICT (id) DO UPDATE SET
  org_id = EXCLUDED.org_id,
  project_id = EXCLUDED.project_id,
  title = EXCLUDED.title,
  status = EXCLUDED.status,
  priority = EXCLUDED.priority,
  assignee_id = EXCLUDED.assignee_id,
  story_points = EXCLUDED.story_points,
  updated_at = EXCLUDED.updated_at,
  summary = EXCLUDED.summary,
  owner_id = EXCLUDED.owner_id;
`.trim();
}

function taskRowValuesSql(row) {
  return `  (
    '${escapeLiteral(row.id)}',
    '${escapeLiteral(row.org_id)}',
    '${escapeLiteral(row.project_id)}',
    '${escapeLiteral(row.title)}',
    '${escapeLiteral(row.status)}',
    ${Number(row.priority)},
    '${escapeLiteral(row.assignee_id)}',
    ${Number(row.story_points)},
    '${escapeLiteral(row.updated_at)}',
    '${escapeLiteral(row.summary)}',
    '${escapeLiteral(row.owner_id)}'
  )`;
}

function benchmarkTableNames() {
  return authPerimeterMode ? [...Object.values(TABLES), authPerimeterTable] : Object.values(TABLES);
}

function authPerimeterProjectIds() {
  if (!authPerimeterMode) return [];
  return projectBucketSampleCount > 0
    ? sampledProjectIds(projectBucketSampleCount)
    : [fixture.ids.primaryProjectId];
}

function authPerimeterRows() {
  if (!authPerimeterMode) return [];
  const rows = [];
  for (const projectIdValue of authPerimeterProjectIds()) {
    rows.push({
      id: `access-${fixture.targetUserId}-${projectIdValue}`,
      user_id: fixture.targetUserId,
      org_id: fixture.targetOrgId,
      project_id: projectIdValue,
      role: 'member',
      updated_at: '2026-01-01T00:00:00Z'
    });
  }
  rows.push(readinessAuthPerimeterRow());
  return rows;
}

function readinessAuthPerimeterRow() {
  const readinessProject = fixture.ids.primaryProjectId;
  return {
    id: `access-${readinessProbeUserId}-${readinessProject}`,
    user_id: readinessProbeUserId,
    org_id: fixture.targetOrgId,
    project_id: readinessProject,
    role: 'member',
    updated_at: '2026-01-01T00:00:00Z'
  };
}

function insertAuthPerimeterRowsSql(rows) {
  if (rows.length === 0) return '';
  const values = rows
    .map(
      (row) => `  (
    '${escapeLiteral(row.id)}',
    '${escapeLiteral(row.user_id)}',
    '${escapeLiteral(row.org_id)}',
    '${escapeLiteral(row.project_id)}',
    '${escapeLiteral(row.role)}',
    '${escapeLiteral(row.updated_at)}'
  )`
    )
    .join(',\n');
  return `
INSERT INTO ${authPerimeterTable}
  (id, user_id, org_id, project_id, role, updated_at)
VALUES
${values}
ON CONFLICT (id) DO UPDATE SET
  user_id = EXCLUDED.user_id,
  org_id = EXCLUDED.org_id,
  project_id = EXCLUDED.project_id,
  role = EXCLUDED.role,
  updated_at = EXCLUDED.updated_at;
`.trim();
}

function cleanupSql() {
  return `
DROP SCHEMA IF EXISTS powersync CASCADE;
DROP PUBLICATION IF EXISTS powersync;
DROP PUBLICATION IF EXISTS ${benchmarkPublication};
DROP PUBLICATION IF EXISTS ${rustPublication};
${authPerimeterMode ? `DROP TABLE IF EXISTS ${authPerimeterTable};` : ''}
DROP TABLE IF EXISTS ${TABLES.comments};
DROP TABLE IF EXISTS ${TABLES.tasks};
DROP TABLE IF EXISTS ${TABLES.projects};
DROP TABLE IF EXISTS ${TABLES.memberships};
DROP TABLE IF EXISTS ${TABLES.organizations};
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE active = FALSE
  AND (
    slot_name = '${escapeLiteral(benchmarkSlot)}'
    OR slot_name = '${escapeLiteral(rustSlot)}'
    OR slot_name LIKE '${escapeLiteral(officialSlotPrefix)}%'
  );
`.trim();
}

async function ensureBenchmarkTooling() {
  if (process.env.POWERSYNC_BENCHMARK_SKIP_TOOLING_INSTALL === '1') {
    log('skip benchmark dependency install (POWERSYNC_BENCHMARK_SKIP_TOOLING_INSTALL=1)');
    return;
  }

  log('install browser benchmark dependencies');
  run('npm', ['ci'], { cwd: sdkDir });
  log('install playwright chromium');
  run('npx', ['playwright', 'install', 'chromium'], { cwd: sdkDir });
}

async function prepareRustServiceRuntime() {
  if (!targetLabels.includes('rust')) return;
  if (symmetricDockerRuntime) return;

  if (rustPrebuildEnabled) {
    log(`prebuild rust service (profile=${rustCargoProfile})`);
    run('cargo', ['build', '--locked', '--manifest-path', rustManifestPath, ...rustCargoBuildArgs], {
      cwd: repoRoot
    });
  }

  if (!rustUsePrebuiltBinary || rustExecutablePath) return;

  const metadataRaw = runCapture(
    'cargo',
    ['metadata', '--locked', '--manifest-path', rustManifestPath, '--format-version', '1', '--no-deps'],
    { cwd: repoRoot }
  );
  const metadata = JSON.parse(metadataRaw);
  const executableName = process.platform === 'win32' ? 'powersync-mdbx.exe' : 'powersync-mdbx';
  rustExecutablePath = path.join(metadata.target_directory, rustTargetProfileDir, executableName);
  if (!fs.existsSync(rustExecutablePath)) {
    throw new Error(`expected prebuilt Rust executable at ${rustExecutablePath}`);
  }
}

async function startOfficialService(syncRulesYaml, { port: portOverride } = {}) {
  await ensureOfficialMongo();
  const port = portOverride ?? (symmetricDockerRuntime ? null : await freePort());
  const configYaml = officialConfigYaml(syncRulesYaml);
  const encodedConfig = Buffer.from(configYaml, 'utf8').toString('base64');

  run('docker', [
    'run',
    '-d',
    '--name',
    officialContainer,
    '--network',
    composeNetwork,
    '--rm',
    ...serviceResourceArgs('official'),
    '-p',
    port == null ? '127.0.0.1::8080' : `127.0.0.1:${port}:8080`,
    '-e',
    `POWERSYNC_CONFIG_B64=${encodedConfig}`,
    ...(officialNodeOptions ? ['-e', `NODE_OPTIONS=${officialNodeOptions}`] : []),
    officialImage
  ]);
  officialRunning = true;
  officialLogPath = path.join(benchmarkDir, `official-service-${Date.now()}.log`);
  const publishedPort = port ?? (await waitForPublishedPort(officialContainer, 8080));
  return { endpoint: `http://127.0.0.1:${publishedPort}` };
}

async function stopOfficialService() {
  return await stopOfficialServiceWithOptions();
}

async function stopOfficialServiceWithOptions({ keepMongo = false } = {}) {
  if (officialRunning) {
    captureDockerLogs(officialContainer, officialLogPath);
    spawnSync('docker', ['rm', '-f', officialContainer], { stdio: 'ignore' });
    officialRunning = false;
    officialLogPath = null;
    await delay(250);
  }
  if (!keepMongo && officialMongoRunning) {
    spawnSync('docker', ['rm', '-f', officialMongoContainer], { stdio: 'ignore' });
    officialMongoRunning = false;
    if (officialMongoDataDir != null) {
      fs.rmSync(officialMongoDataDir, { recursive: true, force: true });
      officialMongoDataDir = null;
    }
    await delay(250);
  }
}

function captureDockerLogs(containerName, logPath) {
  if (!logPath) return;
  const result = spawnSync('docker', ['logs', containerName], {
    encoding: 'utf8',
    maxBuffer: 50 * 1024 * 1024
  });
  if (result.status === 0 || result.stdout || result.stderr) {
    fs.writeFileSync(logPath, `${result.stdout ?? ''}${result.stderr ?? ''}`);
  }
}

async function ensureOfficialMongo() {
  if (officialMongoRunning) return;
  officialMongoDataDir = path.join(benchmarkDir, 'official-mongo');
  fs.rmSync(officialMongoDataDir, { recursive: true, force: true });
  fs.mkdirSync(path.join(officialMongoDataDir, 'db'), { recursive: true });
  fs.mkdirSync(path.join(officialMongoDataDir, 'configdb'), { recursive: true });
  run('docker', [
    'run',
    '-d',
    '--rm',
    '--name',
    officialMongoContainer,
    '--network',
    composeNetwork,
    ...(symmetricDockerRuntime
      ? ['--cpus', `${mongoCpuLimit}`, '--memory', mongoMemoryLimit]
      : []),
    '-v',
    `${path.join(officialMongoDataDir, 'db')}:/data/db`,
    '-v',
    `${path.join(officialMongoDataDir, 'configdb')}:/data/configdb`,
    mongoImage,
    'mongod',
    '--replSet',
    'rs0',
    '--bind_ip_all',
    // Without an explicit cache size, WiredTiger sizes itself from the
    // Docker VM's memory allocation, which is unrecorded and host-dependent.
    ...(officialMongoCacheGb ? ['--wiredTigerCacheSizeGB', officialMongoCacheGb] : [])
  ]);
  officialMongoRunning = true;
  await delay(3_000);
  run('docker', [
    'exec',
    officialMongoContainer,
    'mongosh',
    '--quiet',
    '--eval',
    `rs.initiate({_id: 'rs0', members: [{_id: 0, host: '${officialMongoContainer}:27017'}]})`
  ]);
  await waitForMongoPrimary();
}

async function waitForMongoPrimary(attempts = 30) {
  for (let index = 0; index < attempts; index += 1) {
    const result = spawnSync('docker', [
      'exec',
      officialMongoContainer,
      'mongosh',
      '--quiet',
      '--eval',
      'db.hello().isWritablePrimary'
    ], { encoding: 'utf8' });
    if (result.status === 0 && /true/i.test(result.stdout)) return;
    await delay(1_000);
  }
  throw new Error('mongo replica set did not become primary in time');
}

async function startRustService(syncRulesYaml) {
  if (symmetricDockerRuntime) return await startRustContainer(syncRulesYaml);
  if (rustProcess) throw new Error('rust service is already running');
  if (!fs.existsSync(rustManifestPath)) {
    throw new Error(`rust manifest not found at ${rustManifestPath}`);
  }

  const rustPort = await freePort();
  const rustPgForwardPort = await freePort();
  if (rustLiveUnifiedBench) {
    await ensureRustPostgresForwarder(rustPgForwardPort);
  }

  const rustLogPath = path.join(benchmarkDir, `rust-service-${Date.now()}.log`);
  const rustLogFd = fs.openSync(rustLogPath, 'a');
  const env = {
    ...process.env,
    POWERSYNC_RUST_PORT: `${rustPort}`,
    POWERSYNC_RUST_STORAGE_BACKEND: process.env.POWERSYNC_RUST_STORAGE_BACKEND ?? 'wire-mdbx',
    POWERSYNC_RUST_SYNC_EDGE_PATH:
      process.env.POWERSYNC_RUST_SYNC_EDGE_PATH ?? path.join(benchmarkDir, 'rust-sync-edge'),
    POWERSYNC_RUST_MDBX_PATH:
      process.env.POWERSYNC_RUST_MDBX_PATH ?? path.join(benchmarkDir, 'rust-wire-mdbx'),
    POWERSYNC_RUST_MDBX_TAIL_PATH:
      process.env.POWERSYNC_RUST_MDBX_TAIL_PATH ?? path.join(benchmarkDir, 'rust-wire-mdbx-tail'),
    POWERSYNC_RUST_SYNC_RULES: syncRulesYaml,
    POWERSYNC_RUST_SYNC_RULES_STATE_PATH: path.join(benchmarkDir, 'rust-sync-rules-state.json'),
    POWERSYNC_RUST_API_TOKENS: apiToken,
    POWERSYNC_RUST_JWT_AUDIENCES: audience,
    POWERSYNC_RUST_JWT_ISSUERS: issuer,
      POWERSYNC_RUST_JWKS_JSON: JSON.stringify({
        keys: [{ kty: 'oct', alg: 'HS256', k: base64Url(jwtSecret) }]
      }),
      POWERSYNC_RUST_INITIAL_SNAPSHOT: rustInitialSnapshotEnabled,
      POWERSYNC_RUST_PERSIST_RAW_BATCHES: rustPersistRawBatches
  };

  if (rustLiveUnifiedBench) {
    Object.assign(env, {
      POWERSYNC_RUST_SERVICE_MODE: 'unified',
      POWERSYNC_RUST_REPLICATION_ENABLED: '1',
      POWERSYNC_RUST_POSTGRES_REPLICATION_URI:
        process.env.POWERSYNC_RUST_POSTGRES_REPLICATION_URI ??
        `postgres://postgres:postgres@127.0.0.1:${rustPgForwardPort}/powersync_benchmark_test?sslmode=disable`,
      POWERSYNC_RUST_REPLICATION_SLOT: process.env.POWERSYNC_RUST_REPLICATION_SLOT ?? rustSlot,
      POWERSYNC_RUST_REPLICATION_PUBLICATION:
        process.env.POWERSYNC_RUST_REPLICATION_PUBLICATION ?? rustPublication,
      ...(rustReplicationStatusIntervalMsOverride == null
        ? {}
        : {
            POWERSYNC_RUST_REPLICATION_STATUS_INTERVAL_MS: `${rustReplicationStatusIntervalMsOverride}`
          }),
      ...(rustReplicationIdleWakeupIntervalMsOverride == null
        ? {}
        : {
            POWERSYNC_RUST_REPLICATION_IDLE_WAKEUP_INTERVAL_MS: `${rustReplicationIdleWakeupIntervalMsOverride}`
          })
    });
  }

  rustProcess = rustExecutablePath
    ? spawn(rustExecutablePath, [], {
        cwd: repoRoot,
        env,
        stdio: ['ignore', rustLogFd, rustLogFd]
      })
    : spawn('cargo', ['run', '--locked', '--manifest-path', rustManifestPath, ...rustCargoBuildArgs], {
        cwd: repoRoot,
        env,
        stdio: ['ignore', rustLogFd, rustLogFd]
      });
  fs.closeSync(rustLogFd);

  await delay(500);
  if (rustProcess.exitCode != null) {
    throw new Error(`rust service exited early with code ${rustProcess.exitCode}`);
  }
  return { endpoint: `http://127.0.0.1:${rustPort}` };
}

async function startRustContainer(syncRulesYaml) {
  if (rustContainerRunning) throw new Error('rust service container is already running');
  const dataDir = path.join(benchmarkDir, 'rust-container-data');
  fs.mkdirSync(dataDir, { recursive: true });
  rustLogPath = path.join(benchmarkDir, `rust-service-${Date.now()}.log`);
  const env = rustServiceEnvironment(syncRulesYaml, {
    port: 8080,
    syncEdgePath: '/benchmark-data/sync-edge',
    mdbxPath: '/benchmark-data/wire-mdbx',
    mdbxTailPath: '/benchmark-data/wire-mdbx-tail',
    syncRulesStatePath: '/benchmark-data/sync-rules-state.json',
    postgresUri: 'postgres://postgres:postgres@postgres:5432/powersync_benchmark_test?sslmode=disable'
  });
  const uid = typeof process.getuid === 'function' ? process.getuid() : 1000;
  const gid = typeof process.getgid === 'function' ? process.getgid() : 1000;
  const envArgs = Object.entries(env).flatMap(([name, value]) => ['-e', `${name}=${value}`]);
  run('docker', [
    'run',
    '-d',
    '--name',
    rustContainer,
    '--network',
    composeNetwork,
    ...serviceResourceArgs('rust'),
    '--user',
    `${uid}:${gid}`,
    '-p',
    '127.0.0.1::8080',
    '-v',
    `${dataDir}:/benchmark-data`,
    ...envArgs,
    rustImage
  ]);
  rustContainerRunning = true;
  const port = await waitForPublishedPort(rustContainer, 8080);
  return { endpoint: `http://127.0.0.1:${port}` };
}

function rustServiceEnvironment(syncRulesYaml, paths) {
  const env = {
    ...rustPassthroughEnvironment(),
    POWERSYNC_RUST_PORT: `${paths.port}`,
    POWERSYNC_RUST_STORAGE_BACKEND: 'wire-mdbx',
    POWERSYNC_RUST_SYNC_EDGE_PATH: paths.syncEdgePath,
    POWERSYNC_RUST_MDBX_PATH: paths.mdbxPath,
    POWERSYNC_RUST_MDBX_TAIL_PATH: paths.mdbxTailPath,
    POWERSYNC_RUST_SYNC_RULES: syncRulesYaml,
    POWERSYNC_RUST_SYNC_RULES_STATE_PATH: paths.syncRulesStatePath,
    POWERSYNC_RUST_API_TOKENS: apiToken,
    POWERSYNC_RUST_JWT_AUDIENCES: audience,
    POWERSYNC_RUST_JWT_ISSUERS: issuer,
    POWERSYNC_RUST_JWKS_JSON: JSON.stringify({
      keys: [{ kty: 'oct', alg: 'HS256', k: base64Url(jwtSecret) }]
    }),
    POWERSYNC_RUST_INITIAL_SNAPSHOT: rustInitialSnapshotEnabled,
    POWERSYNC_RUST_PERSIST_RAW_BATCHES: rustPersistRawBatches,
    POWERSYNC_RUST_SERVICE_MODE: 'unified',
    POWERSYNC_RUST_REPLICATION_ENABLED: '1',
    POWERSYNC_RUST_POSTGRES_REPLICATION_URI: paths.postgresUri,
    POWERSYNC_RUST_REPLICATION_SLOT: rustSlot,
    POWERSYNC_RUST_REPLICATION_PUBLICATION: rustPublication
  };
  if (rustReplicationStatusIntervalMsOverride != null) {
    env.POWERSYNC_RUST_REPLICATION_STATUS_INTERVAL_MS = `${rustReplicationStatusIntervalMsOverride}`;
  }
  if (rustReplicationIdleWakeupIntervalMsOverride != null) {
    env.POWERSYNC_RUST_REPLICATION_IDLE_WAKEUP_INTERVAL_MS = `${rustReplicationIdleWakeupIntervalMsOverride}`;
  }
  return env;
}

function rustPassthroughEnvironment() {
  return Object.fromEntries(
    Object.entries(process.env).filter(
      ([name, value]) => name.startsWith('POWERSYNC_RUST_') && value != null
    )
  );
}

function serviceResourceArgs(targetLabel) {
  if (!symmetricDockerRuntime) return [];
  return targetLabel === 'official'
    ? ['--cpus', `${serviceCpuLimit}`, '--memory', serviceMemoryLimit]
    : ['--cpus', `${targetCpuLimit}`, '--memory', targetMemoryLimit];
}

async function ensureRustPostgresForwarder(port) {
  if (rustPostgresForwarderRunning) return;
  spawnSync('docker', ['rm', '-f', rustPostgresForwarderContainer], { stdio: 'ignore' });
  run('docker', [
    'run',
    '-d',
    '--rm',
    '--name',
    rustPostgresForwarderContainer,
    '--network',
    composeNetwork,
    '-p',
    `127.0.0.1:${port}:15432`,
    socatImage,
    'tcp-listen:15432,fork,reuseaddr',
    'tcp:postgres:5432'
  ]);
  rustPostgresForwarderRunning = true;
  await delay(250);
}

async function stopRustService() {
  if (rustContainerRunning) {
    captureDockerLogs(rustContainer, rustLogPath);
    spawnSync('docker', ['rm', '-f', rustContainer], { stdio: 'ignore' });
    rustContainerRunning = false;
    rustLogPath = null;
    await delay(250);
  }
  if (rustProcess) {
    if (rustProcess.exitCode == null && rustProcess.signalCode == null) {
      rustProcess.kill('SIGTERM');
      await waitForChildExit(rustProcess, 2_000);
    }
    if (rustProcess.exitCode == null && rustProcess.signalCode == null) {
      rustProcess.kill('SIGKILL');
      await waitForChildExit(rustProcess, 1_000);
    }
    rustProcess = null;
  }
  if (rustPostgresForwarderRunning) {
    spawnSync('docker', ['rm', '-f', rustPostgresForwarderContainer], { stdio: 'ignore' });
    rustPostgresForwarderRunning = false;
    await delay(250);
  }
}

async function waitForChildExit(child, timeoutMsValue) {
  if (child.exitCode != null || child.signalCode != null) return;
  await Promise.race([
    new Promise((resolve) => child.once('exit', resolve)),
    delay(timeoutMsValue)
  ]);
}

async function waitForReadiness(url, attempts = readinessAttempts) {
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const result = await runSimpleHttpProbe(url);
    if (result.ok) return result;
    await delay(readinessPollMs);
  }
  throw new Error(`service at ${url} did not become ready in time`);
}

async function runSimpleHttpProbe(urlString) {
  const url = new URL(urlString);
  const transport = url.protocol === 'https:' ? https : http;
  const startedAt = performance.now();
  return await new Promise((resolve) => {
    const request = transport.request(
      url,
      { method: 'GET', agent: false, headers: { Connection: 'close' } },
      (response) => {
        response.resume();
        response.on('end', () => {
          resolve({
            ok: (response.statusCode ?? 0) >= 200 && (response.statusCode ?? 0) < 300,
            wallMs: round(performance.now() - startedAt),
            statusCode: response.statusCode ?? 0
          });
        });
      }
    );
    request.setTimeout(1_000, () => request.destroy(new Error('timeout')));
    request.on('error', () => resolve({ ok: false, wallMs: round(performance.now() - startedAt), statusCode: 0 }));
    request.end();
  });
}

async function waitForPublishedPort(containerName, containerPort, attempts = 40) {
  for (let index = 0; index < attempts; index += 1) {
    const publishedPort = publishedHostPort(containerName, containerPort);
    if (publishedPort != null) return publishedPort;
    await delay(250);
  }
  throw new Error(`container ${containerName} did not publish ${containerPort}/tcp in time`);
}

function publishedHostPort(containerName, containerPort) {
  const result = spawnSync('docker', ['port', containerName, `${containerPort}/tcp`], { encoding: 'utf8' });
  if (result.status !== 0) return null;
  const line = String(result.stdout ?? '').trim().split('\n').find(Boolean);
  const match = line?.match(/:(\d+)\s*$/);
  return match ? Number.parseInt(match[1], 10) : null;
}

function officialConfigYaml(syncRulesYaml) {
  // POWERSYNC_USER_VALUE_OFFICIAL_CONFIG_EXTRA lets reviewers (e.g. the
  // PowerSync team) append performance tuning to the baseline config;
  // whether it was used is recorded in results.json.
  const extra = officialConfigExtraYaml ? `\n${officialConfigExtraYaml.trim()}\n` : '';
  return `
replication:
  connections:
    - type: postgresql
      uri: postgres://postgres:postgres@postgres:5432/powersync_benchmark_test
      sslmode: disable
      slot_name_prefix: ${officialSlotPrefix}
storage:
  type: mongodb
  uri: mongodb://${officialMongoContainer}:27017/powersync_user_value_${suffix}?replicaSet=rs0
sync_rules:
  content: |
${indent(syncRulesYaml, 4)}
client_auth:
  jwks:
    keys:
      - kty: oct
        alg: HS256
        k: ${base64Url(jwtSecret)}
  audience:
    - ${audience}
api:
  tokens:
    - ${apiToken}
healthcheck:
  probes:
    use_http: true
${extra}`.trimStart();
}

function redactOfficialConfig(configYaml) {
  return configYaml
    .replaceAll(base64Url(jwtSecret), '<redacted-jwk-key>')
    .replaceAll(apiToken, '<redacted-api-token>');
}

function sanitizedPostgresPolicy(uri) {
  try {
    const parsed = new URL(uri);
    const sslmode = parsed.searchParams.get('sslmode') ?? 'not specified';
    const host = ['127.0.0.1', 'localhost', '::1'].includes(parsed.hostname)
      ? 'loopback'
      : 'non-loopback';
    return `${host}; sslmode=${sslmode}`;
  } catch {
    return 'non-URL connection string supplied; policy not safely introspected';
  }
}

async function waitForInitialSyncData(endpoint, authToken, attempts = 120) {
  for (let index = 0; index < attempts; index += 1) {
    const body = JSON.stringify({
      buckets: [],
      include_checksum: true,
      raw_data: true,
      binary_data: false,
      client_id: `probe-${Date.now()}-${index + 1}`,
      parameters: {},
      streams: {
        include_defaults: true,
        subscriptions: []
      },
      app_metadata: {}
    });

    const result = spawnSync(
      'curl',
      [
        '-fsS',
        '--max-time',
        '5',
        '-X',
        'POST',
        `${endpoint}/sync/stream`,
        '-H',
        `Authorization: Bearer ${authToken}`,
        '-H',
        'Content-Type: application/json',
        '--data-binary',
        body
      ],
      { encoding: 'utf8', maxBuffer: 10 * 1024 * 1024 }
    );

    if (String(result.stdout ?? '').includes('"data"')) return;
    await delay(1_000);
  }
  throw new Error(`service at ${endpoint} did not publish initial sync data in time`);
}

async function runBucketProtocolEquivalenceGate({ target, endpoint, repeat, targetDir }) {
  const startedAt = performance.now();
  const bucketSpecs = attachExpectedRowsToBucketSpecs(buildInitialEquivalenceBucketSpecs());

  const buckets = [];
  const rawRecordArtifacts = [];
  let batchIndex = 0;
  for (const batch of bucketProbeBatches(bucketSpecs)) {
    batchIndex += 1;
    const verified = await proveInitialBucketProtocolEquivalenceBatch({ endpoint, repeat, specs: batch });
    if (retainRawValidationRecords) {
      rawRecordArtifacts.push(
        writeCompressedValidationRecords({
          targetDir,
          targetLabel: target.label,
          repeat,
          phase: 'initial',
          batchIndex,
          buckets: verified
        })
      );
    }
    buckets.push(...stripRawValidationRecords(verified));
  }
  const authorization = authPerimeterMode
    ? await proveAuthPerimeterAuthorizationProbe({ endpoint, repeat })
    : null;

  const payload = {
    target: target.label,
    repeat,
    profile: profileName,
    generatedAt: new Date().toISOString(),
    status: 'passed',
    processingScopeOnly: true,
    elapsedMs: round(performance.now() - startedAt),
    gate: 'bucket-protocol-equivalence',
    requestMode: 'batched-explicit-cursors-or-stream-subscriptions',
    datasetTaskRows: expectedTaskRowCount(),
    bucketProbeBatchSize,
    fullBucketEquivalenceMaxRows,
    includeDefaultEquivalenceBucket,
    includeOrgEquivalenceBucket,
    requestedProjectBucketSampleCount,
    projectBucketSampleCount,
    accessMode,
    authorization,
    rawRecordArtifacts,
    buckets
  };
  const artifactPath = path.join(targetDir, `bucket-protocol-equivalence.r${repeat}.json`);
  fs.writeFileSync(artifactPath, JSON.stringify(payload, null, 2));
  log(
    `equivalence ${target.label}#${repeat}: passed ${buckets.length} buckets / ${buckets.reduce(
      (sum, bucket) => sum + bucket.puts,
      0
    )} PUTs`
  );
  const result = { status: 'passed', artifactPath, elapsedMs: payload.elapsedMs, authorization, buckets };
  if (churnGateEnabled) {
    Object.defineProperty(result, 'verificationSpecs', {
      value: bucketSpecs,
      enumerable: false,
      configurable: true
    });
  }
  return result;
}

function buildInitialEquivalenceBucketSpecs() {
  const specs = [];
  if (includeDefaultEquivalenceBucket) {
    specs.push({
      requestKind: 'explicit',
      label: 'tasks-default',
      stream: 'tasks',
      bucket: bucketNameForStream('tasks', [])
    });
  }

  if (!routeStreamsEnabled) return specs;

  if (authPerimeterMode) {
    for (const sampledProjectId of authPerimeterProjectIds()) {
      specs.push({
        requestKind: 'subscription',
        subscriptionMode: 'auth_perimeter',
        label: `tasks-by-auth-project(${sampledProjectId})`,
        stream: authPerimeterStream,
        bucket: bucketNameForStream(authPerimeterStream, [sampledProjectId]),
        routeParameters: { project_id: sampledProjectId }
      });
    }
    return specs;
  }

  const projectIds =
    projectBucketSampleCount > 0
      ? sampledProjectIds(projectBucketSampleCount)
      : [fixture.ids.primaryProjectId];
  for (const sampledProjectId of projectIds) {
    specs.push({
      requestKind: 'subscription',
      label: `tasks-by-project(${sampledProjectId})`,
      stream: 'tasks_by_project',
      bucket: bucketNameForStream('tasks_by_project', [sampledProjectId]),
      subscriptionParameters: { project_id: sampledProjectId }
    });
  }

  if (includeOrgEquivalenceBucket) {
    specs.push({
      requestKind: 'subscription',
      label: `tasks-by-org(${fixture.targetOrgId})`,
      stream: 'tasks_by_org',
      bucket: bucketNameForStream('tasks_by_org', [fixture.targetOrgId]),
      subscriptionParameters: { org_id: fixture.targetOrgId }
    });
  }

  return specs;
}

function buildInitialReadinessSpec() {
  if (!routeStreamsEnabled) {
    throw new Error(
      'POWERSYNC_USER_VALUE_INITIAL_READINESS=sync-protocol requires routed streams (use a scale profile, auth_perimeter mode, or enable route streams)'
    );
  }
  const projectIndex = 1;
  const projectIdValue = projectId(1, projectIndex);
  const expectedRows = Array.from({ length: profile.tasksPerProject }, (_, index) =>
    generatedTaskRow(1, projectIndex, index + 1)
  );
  for (const row of [initialUpdateRow(), fixture.ids.deleteTaskRow, ...fixture.ids.batchDeleteRows]) {
    if (row.project_id === projectIdValue) expectedRows.push(row);
  }
  return buildReadinessSubscriptionSpec({
    authPerimeter: authPerimeterMode,
    projectIdValue,
    expectedRows
  });
}

function buildReadinessSubscriptionSpec({ authPerimeter, projectIdValue, expectedRows }) {
  const stream = authPerimeter ? authPerimeterStream : 'tasks_by_project';
  return {
    requestKind: 'subscription',
    ...(authPerimeter
      ? {
          subscriptionMode: 'auth_perimeter',
          routeParameters: { project_id: projectIdValue }
        }
      : { subscriptionParameters: { project_id: projectIdValue } }),
    label: `initial-readiness(${projectIdValue})`,
    stream,
    bucket: bucketNameForStream(stream, [projectIdValue]),
    expectedRows
  };
}

function initialReadinessSubject({ authPerimeter = authPerimeterMode } = {}) {
  return authPerimeter ? readinessProbeUserId : fixture.targetUserId;
}

function sampledProjectIds(requestedCount) {
  const count = Math.min(Math.max(0, requestedCount), profile.projectsPerOrg);
  if (count === 0) return [];
  const indices = new Set([1]);
  for (let offset = 0; indices.size < count && offset < profile.projectsPerOrg; offset += 1) {
    indices.add(1 + Math.floor((offset * profile.projectsPerOrg) / count));
  }
  for (let index = 1; indices.size < count && index <= profile.projectsPerOrg; index += 1) {
    indices.add(index);
  }
  return [...indices].sort((left, right) => left - right).map((index) => projectId(1, index));
}

function attachExpectedRowsToBucketSpecs(specs) {
  const enriched = specs.map((spec) => ({ ...spec, expectedRows: [] }));
  const defaultSpecs = enriched.filter((spec) => spec.stream === 'tasks');
  const orgSpecs = groupSpecsByParameter(enriched, 'org_id');
  const projectSpecs = groupSpecsByParameter(enriched, 'project_id');

  for (const row of initialTaskRows()) {
    for (const spec of defaultSpecs) spec.expectedRows.push(row);
    for (const spec of orgSpecs.get(row.org_id) ?? []) spec.expectedRows.push(row);
    for (const spec of projectSpecs.get(row.project_id) ?? []) spec.expectedRows.push(row);
  }

  return enriched;
}

function groupSpecsByParameter(specs, parameterName) {
  const grouped = new Map();
  for (const spec of specs) {
    const value = specRouteParameter(spec, parameterName);
    if (typeof value !== 'string') continue;
    const current = grouped.get(value) ?? [];
    current.push(spec);
    grouped.set(value, current);
  }
  return grouped;
}

function specRouteParameter(spec, parameterName) {
  return spec.routeParameters?.[parameterName] ?? spec.subscriptionParameters?.[parameterName];
}

function bucketProbeBatches(specs) {
  return [
    ...chunkArray(
      specs.filter((spec) => spec.requestKind === 'explicit'),
      bucketProbeBatchSize
    ),
    specs.filter(isAuthPerimeterSpec),
    ...chunkArray(
      specs.filter((spec) => spec.requestKind === 'subscription' && !isAuthPerimeterSpec(spec)),
      bucketProbeBatchSize
    )
  ].filter((batch) => batch.length > 0);
}

async function proveInitialBucketProtocolEquivalenceBatch({
  endpoint,
  repeat,
  specs,
  authToken = createBenchmarkJwt()
}) {
  const protocol = await loadPowerSyncCommonProtocol();
  const requestBody = initialEquivalenceRequestBody(specs, repeat);
  const batchLabel = specs.length === 1 ? specs[0].label : `${specs[0].label}+${specs.length - 1}`;
  const states = new Map(
    specs.map((spec) => [
      spec.bucket,
      {
        spec,
        expectedById: new Map(spec.expectedRows.map((row) => [row.id, row])),
        putIds: new Set(),
        payloadMismatches: [],
        checksumValues: new Set(),
        checksumRecords: [],
        checksumSum: 0,
        nonZeroChecksumCount: 0,
        nonZeroClearChecksumCount: 0,
        clearCount: 0,
        removeCount: 0,
        dataLines: 0,
        previousAfter: '0'
      }
    ])
  );

  const response = await postNdjsonProtocol(
    `${endpoint}/sync/stream`,
    requestBody,
    authToken,
    Math.max(timeoutMs, 120_000),
    {
      label: `${endpoint}/sync/stream ${batchLabel}`,
      protocol,
      onData(data) {
        const state = states.get(data.bucket);
        if (!state) {
          throw new Error(`equivalence ${batchLabel}: unexpected data bucket ${data.bucket}`);
        }
        state.dataLines += 1;
        assertCursorProgression(state, data, `equivalence ${state.spec.label}`);

        for (const entry of data.data ?? []) {
          if (entry.op === 'CLEAR') {
            state.clearCount += 1;
            resetObservedBucketState(state);
            recordEntryChecksum(state, entry);
            continue;
          }
          recordEntryChecksum(state, entry);
          if (entry.op === 'REMOVE') {
            state.removeCount += 1;
            continue;
          }
          if (entry.op !== 'PUT') {
            throw new Error(`equivalence ${state.spec.label}: unsupported op ${entry.op}`);
          }
          if (entry.object_type !== TABLES.tasks) {
            throw new Error(`equivalence ${state.spec.label}: object_type ${entry.object_type} != ${TABLES.tasks}`);
          }
          const objectId = entry.object_id;
          if (!objectId) throw new Error(`equivalence ${state.spec.label}: PUT missing object_id`);
          if (state.putIds.has(objectId)) {
            throw new Error(`equivalence ${state.spec.label}: duplicate PUT ${objectId}`);
          }
          state.putIds.add(objectId);

          const expected = state.expectedById.get(objectId);
          if (!expected) continue;
          const actual = normalizeProtocolData(entry.data);
          const mismatch = firstTaskPayloadMismatch(actual, expected);
          if (mismatch && state.payloadMismatches.length < 5) {
            state.payloadMismatches.push({ objectId, ...mismatch });
          }
        }
      }
    }
  );
  if (!response.ok) {
    throw new Error(
      `equivalence ${batchLabel} request failed with ${response.statusCode}: ${response.rawBody.slice(0, 400)}`
    );
  }

  return specs.map((spec) => initialBucketValidationFromState({
    state: states.get(spec.bucket),
    checkpoint: response.checkpoint,
    protocol,
    responseBytes: response.responseBytes,
    batchSize: specs.length
  }));
}

async function proveAuthPerimeterAuthorizationProbe({ endpoint, repeat }) {
  const protocol = await loadPowerSyncCommonProtocol();
  const spoofedProjectId = authPerimeterProjectIds()[0] ?? fixture.ids.primaryProjectId;
  const noAccessUserId = `user-no-access-${suffix}`;
  const noAccessToken = createBenchmarkJwt({ subject: noAccessUserId });
  const requestBody = {
    buckets: [],
    include_checksum: true,
    raw_data: true,
    binary_data: false,
    client_id: `probe-authz-${profileName}-${repeat}-${suffix}`,
    parameters: probeRequestParameters(),
    streams: {
      include_defaults: false,
      subscriptions: [
        {
          stream: authPerimeterStream,
          parameters: { project_id: spoofedProjectId, org_id: fixture.targetOrgId },
          override_priority: 3
        }
      ]
    },
    app_metadata: {}
  };
  const response = await postNdjsonProtocol(
    `${endpoint}/sync/stream`,
    requestBody,
    noAccessToken,
    Math.max(timeoutMs, 120_000),
    {
      label: `${endpoint}/sync/stream auth-perimeter-probe`,
      protocol,
      onData(data) {
        throw new Error(`auth perimeter probe: spoofed data bucket ${data?.bucket}`);
      }
    }
  );
  if (!response.ok) {
    throw new Error(
      `auth perimeter probe failed with ${response.statusCode}: ${response.rawBody.slice(0, 400)}`
    );
  }

  const checkpointBuckets = response.checkpoint.buckets ?? [];
  if (checkpointBuckets.length !== 0) {
    throw new Error(
      `auth perimeter probe: no-access user received checkpoint buckets ${JSON.stringify(checkpointBuckets.slice(0, 5))}`
    );
  }

  return {
    status: 'passed',
    stream: authPerimeterStream,
    authorizedProjectBuckets: authPerimeterProjectIds().length,
    noAccessUserId,
    spoofedProjectId,
    checkpointBucketsReturned: checkpointBuckets.length,
    protocolValidator: protocol.artifactSummary
  };
}

function initialEquivalenceRequestBody(specs, repeat) {
  const requestKind = specs[0]?.requestKind;
  if (specs.some((spec) => spec.requestKind !== requestKind)) {
    throw new Error('cannot mix explicit bucket and stream-subscription specs in one equivalence request');
  }

  if (requestKind === 'subscription') {
    if (specs.every((spec) => spec.subscriptionMode === 'auth_perimeter')) {
      return {
        buckets: [],
        include_checksum: true,
        raw_data: true,
        binary_data: false,
        client_id: `probe-equivalence-${profileName}-${repeat}-${specs.length}-${suffix}`,
        parameters: probeRequestParameters(),
        streams: {
          include_defaults: false,
          subscriptions: [
            {
              stream: authPerimeterStream,
              parameters: {},
              override_priority: 3
            }
          ]
        },
        app_metadata: {}
      };
    }
    if (specs.some((spec) => spec.subscriptionMode === 'auth_perimeter')) {
      throw new Error('cannot mix auth-perimeter and parameter-subscription specs in one equivalence request');
    }
    return {
      buckets: [],
      include_checksum: true,
      raw_data: true,
      binary_data: false,
      client_id: `probe-equivalence-${profileName}-${repeat}-${specs.length}-${suffix}`,
      parameters: probeRequestParameters(),
      streams: {
        include_defaults: false,
        subscriptions: specs.map((spec) => ({
          stream: spec.stream,
          parameters: spec.subscriptionParameters ?? {},
          override_priority: 3
        }))
      },
      app_metadata: {}
    };
  }

  return {
    buckets: specs.map((spec) => ({ name: spec.bucket, after: '0' })),
    include_checksum: true,
    raw_data: true,
    binary_data: false,
    client_id: `probe-equivalence-${profileName}-${repeat}-${specs.length}-${suffix}`,
    parameters: probeRequestParameters(),
    app_metadata: {}
  };
}

function probeRequestParameters() {
  return {
    org_id: fixture.targetOrgId,
    project_id: authPerimeterMode ? unauthorizedProjectId() : fixture.ids.primaryProjectId
  };
}

function unauthorizedProjectId() {
  return projectId(1, profile.projectsPerOrg + 1);
}

function initialBucketValidationFromState({ state, checkpoint, protocol, responseBytes, batchSize }) {
  const {
    spec,
    expectedById,
    putIds,
    payloadMismatches,
    checksumValues,
    nonZeroChecksumCount,
    clearCount,
    removeCount,
    dataLines
  } = state;
  const checkpointBucket = (checkpoint.buckets ?? []).find((bucket) => bucket.bucket === spec.bucket);
  if (!checkpointBucket) {
    throw new Error(`equivalence ${spec.label}: checkpoint did not include ${spec.bucket}`);
  }
  if (Number.isFinite(Number(checkpointBucket.count)) && Number(checkpointBucket.count) !== expectedById.size) {
    throw new Error(
      `equivalence ${spec.label}: checkpoint count ${checkpointBucket.count} != expected ${expectedById.size}`
    );
  }

  if (removeCount !== 0) throw new Error(`equivalence ${spec.label}: initial bucket snapshot emitted ${removeCount} REMOVEs`);
  if (putIds.size !== expectedById.size) {
    const missing = [...expectedById.keys()].filter((id) => !putIds.has(id)).slice(0, 10);
    const extra = [...putIds].filter((id) => !expectedById.has(id)).slice(0, 10);
    throw new Error(
      `equivalence ${spec.label}: PUT id set mismatch expected=${expectedById.size} actual=${putIds.size}` +
        ` missing=${JSON.stringify(missing)} extra=${JSON.stringify(extra)}`
    );
  }
  if (payloadMismatches.length > 0) {
    throw new Error(`equivalence ${spec.label}: payload mismatch ${JSON.stringify(payloadMismatches)}`);
  }

  const checkpointLastOpId = normalizeDecimalCursor(
    checkpoint.last_op_id,
    `equivalence ${spec.label} checkpoint.last_op_id`
  );
  const cursorAfter = dataLines > 0 ? state.previousAfter : checkpointLastOpId;
  const proof = {
    label: spec.label,
    stream: spec.stream,
    bucket: spec.bucket,
    expectedRows: expectedById.size,
    protocolValidator: protocol.artifactSummary,
    checkpointCount: Number(checkpointBucket.count),
    checkpointChecksum: normalizeOptionalProtocolNumber(checkpointBucket.checksum),
    entryChecksums: entryChecksumSummary(checksumValues, nonZeroChecksumCount, state.checksumRecords),
    dataLines,
    clears: clearCount,
    puts: putIds.size,
    removes: removeCount,
    lastOpId: checkpointLastOpId,
    cursorAfter,
    bytes: responseBytes,
    batchSize,
    status: 'passed'
  };

  assertCheckpointChecksumMatches({
    label: `equivalence ${spec.label}`,
    checkpointChecksum: proof.checkpointChecksum,
    expectedChecksum: protocolChecksumI32(state.checksumSum)
  });
  if (nonZeroChecksumCount !== putIds.size + state.nonZeroClearChecksumCount) {
    throw new Error(
      `equivalence ${spec.label}: non-zero entry checksums ${nonZeroChecksumCount} != PUT/CLEAR count ${
        putIds.size + state.nonZeroClearChecksumCount
      }`
    );
  }
  return proof;
}

function normalizeDecimalCursor(value, label = 'cursor') {
  if (typeof value === 'string' && /^\d+$/.test(value)) return value;
  if (typeof value === 'bigint' && value >= 0n) return value.toString();
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return String(value);
  const rendered = typeof value === 'bigint' ? `${value}n` : JSON.stringify(value);
  throw new Error(`${label} must be a non-negative decimal string or safe integer, got ${rendered}`);
}

function compareDecimalCursors(left, right) {
  const leftString = normalizeDecimalCursor(left, 'left cursor');
  const rightString = normalizeDecimalCursor(right, 'right cursor');
  const leftValue = BigInt(leftString);
  const rightValue = BigInt(rightString);
  return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
}

function assertCursorProgression(state, data, label) {
  const previousAfter = normalizeDecimalCursor(state.previousAfter ?? '0', `${label} previous cursor`);
  const after = normalizeDecimalCursor(data.after ?? '0', `${label} data.after`);
  const nextAfter = normalizeDecimalCursor(data.next_after ?? after, `${label} data.next_after`);
  if (compareDecimalCursors(after, previousAfter) < 0) {
    throw new Error(`${label}: data after cursor regressed ${after} < ${state.previousAfter}`);
  }
  if (compareDecimalCursors(nextAfter, after) < 0) {
    throw new Error(`${label}: next_after ${nextAfter} < after ${after}`);
  }
  state.previousAfter = nextAfter;
}

function resetObservedBucketState(state) {
  state.putIds?.clear();
  state.seenPuts?.clear();
  state.seenRemoves?.clear();
  if (Array.isArray(state.payloadMismatches)) state.payloadMismatches.length = 0;
  state.resetObserved = true;
}

function recordEntryChecksum(state, entry) {
  const checksum = normalizeOptionalProtocolNumber(entry.checksum);
  if (checksum == null) return;
  const checksumU32 = checksumToU32(checksum);
  assertEntryChecksumSemantics(entry, checksumU32);
  state.checksumValues.add(checksum);
  state.checksumSum = addChecksumU32(state.checksumSum ?? 0, checksumU32);
  state.checksumRecords?.push(entryChecksumRecord(entry, checksumU32));
  if (checksum !== 0) {
    state.nonZeroChecksumCount += 1;
    if (entry.op === 'CLEAR') state.nonZeroClearChecksumCount = (state.nonZeroClearChecksumCount ?? 0) + 1;
  }
}

function entryChecksumSummary(checksumValues, nonZeroChecksumCount, checksumRecords = []) {
  const summary = {
    uniqueCount: checksumValues.size,
    nonZeroCount: nonZeroChecksumCount,
    sample: [...checksumValues].sort((left, right) => left - right).slice(0, 5),
    semanticDigest: checksumRecordsDigest(checksumRecords, { includeChecksum: true, includeSubkey: false }),
    wireDigest: checksumRecordsDigest(checksumRecords, { includeChecksum: true, includeSubkey: true }),
    clientOperationDigest: checksumRecordsDigest(checksumRecords, {
      includeChecksum: false,
      includeSubkey: false
    }),
    clientPutDigest: checksumRecordsDigest(checksumRecords, {
      includeChecksum: false,
      includeSubkey: false,
      op: 'PUT'
    }),
    putDigest: checksumRecordsDigest(checksumRecords, {
      includeChecksum: true,
      includeSubkey: false,
      op: 'PUT'
    }),
    removeObjectDigest: checksumRecordsDigest(checksumRecords, {
      includeChecksum: false,
      includeSubkey: false,
      op: 'REMOVE'
    }),
    recordsSample: checksumRecords
      .slice()
      .sort(compareChecksumRecords)
      .slice(0, 5)
  };
  if (retainRawValidationRecords) {
    summary.rawRecords = checksumRecords.slice().sort(compareChecksumRecords);
  }
  return summary;
}

function writeCompressedValidationRecords({ targetDir, targetLabel, repeat, phase, batchIndex, buckets }) {
  const artifactPath = path.join(
    targetDir,
    `${phase}-protocol-records.r${repeat}.batch${String(batchIndex).padStart(4, '0')}.json.gz`
  );
  const payload = {
    schemaVersion: 1,
    target: targetLabel,
    profile: profileName,
    repeat,
    phase,
    batchIndex,
    buckets: buckets.map((bucket) => ({
      bucket: bucket.bucket,
      records: bucket.entryChecksums?.rawRecords ?? []
    }))
  };
  fs.writeFileSync(artifactPath, zlib.gzipSync(Buffer.from(JSON.stringify(payload))));
  return artifactPath;
}

function stripRawValidationRecords(buckets) {
  return buckets.map((bucket) => {
    if (!bucket.entryChecksums?.rawRecords) return bucket;
    const { rawRecords: _rawRecords, ...entryChecksums } = bucket.entryChecksums;
    return { ...bucket, entryChecksums };
  });
}

function normalizeOptionalProtocolNumber(value) {
  if (value == null || value === '') return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function assertEntryChecksumSemantics(entry, checksumU32) {
  if (entry.op === 'PUT') {
    const objectType = entry.object_type;
    const objectId = entry.object_id;
    const data = protocolDataString(entry.data);
    if (!objectType || !objectId || data == null) {
      throw new Error(`PUT checksum cannot be validated without object_type/object_id/data: ${JSON.stringify(entry)}`);
    }
    const expected = hashPutChecksum(objectType, objectId, data);
    if (checksumU32 !== expected) {
      throw new Error(
        `PUT checksum mismatch ${objectType}/${objectId}: got=${checksumU32} expected=${expected} data=${data.slice(
          0,
          160
        )}`
      );
    }
  } else if (entry.op === 'REMOVE') {
    const subkey = entry.subkey ?? 'null';
    const expected = hashRemoveChecksum(subkey);
    if (checksumU32 !== expected) {
      throw new Error(
        `REMOVE checksum mismatch ${entry.object_type}/${entry.object_id}: got=${checksumU32} expected=${expected} subkey=${subkey}`
      );
    }
  }
}

function protocolDataString(data) {
  if (data == null) return null;
  if (typeof data === 'string') return data;
  return JSON.stringify(data);
}

function hashPutChecksum(objectType, objectId, data) {
  const hash = crypto.createHash('sha256');
  hash.update(`put.${objectType}.${objectId}.${data}`);
  return hash.digest().readUInt32LE(0);
}

function hashRemoveChecksum(subkey) {
  const hash = crypto.createHash('sha256');
  hash.update(`delete.${subkey}`);
  return hash.digest().readUInt32LE(0);
}

function addChecksumU32(left, right) {
  return (checksumToU32(left) + checksumToU32(right)) >>> 0;
}

function checksumToU32(value) {
  return Number(BigInt(Math.trunc(Number(value))) & 0xffffffffn);
}

function protocolChecksumI32(value) {
  const u32 = checksumToU32(value);
  return u32 > 0x7fffffff ? u32 - 0x100000000 : u32;
}

function assertCheckpointChecksumMatches({ label, checkpointChecksum, expectedChecksum }) {
  if (checkpointChecksum == null) {
    throw new Error(`${label}: checkpoint checksum is missing`);
  }
  if (protocolChecksumI32(checkpointChecksum) !== protocolChecksumI32(expectedChecksum)) {
    throw new Error(
      `${label}: checkpoint checksum ${checkpointChecksum} != expected ${protocolChecksumI32(expectedChecksum)}`
    );
  }
}

function entryChecksumRecord(entry, checksumU32) {
  const data = protocolDataString(entry.data);
  const record = {
    op: entry.op ?? null,
    objectType: entry.object_type ?? null,
    objectId: entry.object_id ?? null,
    subkey: entry.subkey ?? null,
    checksum: checksumU32,
    dataSha256: data == null ? null : sha256(data)
  };
  if (checksumRecordDataPreview) {
    record.dataPreview = data == null ? null : data.slice(0, 512);
  }
  return record;
}

function checksumRecordsDigest(records, { includeChecksum, includeSubkey, op = null }) {
  const lines = records
    .filter((record) => op == null || record.op === op)
    .slice()
    .sort(compareChecksumRecords)
    .map((record) =>
      [
        record.op ?? '',
        record.objectType ?? '',
        record.objectId ?? '',
        includeSubkey ? record.subkey ?? '' : '',
        includeChecksum ? `${record.checksum}` : '',
        record.dataSha256 ?? ''
      ].join('\t')
    );
  return sha256(lines.join('\n'));
}

function compareChecksumRecords(left, right) {
  return (
    String(left.op ?? '').localeCompare(String(right.op ?? '')) ||
    String(left.objectType ?? '').localeCompare(String(right.objectType ?? '')) ||
    String(left.objectId ?? '').localeCompare(String(right.objectId ?? '')) ||
    String(left.subkey ?? '').localeCompare(String(right.subkey ?? '')) ||
    Number(left.checksum ?? 0) - Number(right.checksum ?? 0) ||
    String(left.dataSha256 ?? '').localeCompare(String(right.dataSha256 ?? ''))
  );
}

async function runChurnProtocolGate({ target, endpoint, repeat, targetDir, readiness, initialEquivalence }) {
  if (!initialEquivalence?.buckets?.length) {
    throw new Error('churn gate requires initial equivalence bucket cursors');
  }

  const mutation = buildChurnMutationPlan(repeat);
  const specs = attachChurnExpectations(buildChurnBucketSpecs(initialEquivalence), mutation);
  const expectedTailOpsDelta = expectedDefaultReplicaIdentityChurnTailOpsDelta(mutation);
  const rustBaselineMetrics =
    target.label === 'rust'
      ? await fetchTargetMetrics(endpoint).catch((error) => ({
          ok: false,
          error: compactErrorMessage(error.message)
        }))
      : null;
  const startedAt = performance.now();
  const applyStartedAt = performance.now();
  const targetLsn = applyChurnMutationAndCaptureLsn(mutation);
  const applySqlMs = round(performance.now() - applyStartedAt);

  const catchupStartedAt = performance.now();
  const ready = await waitForChurnCatchup({
    target,
    endpoint,
    targetLsn,
    readiness,
    rustBaselineMetrics,
    expectedTailOpsDelta
  });
  const replicationCatchupMs = round(performance.now() - catchupStartedAt);

  const probeStartedAt = performance.now();
  const buckets = [];
  const rawRecordArtifacts = [];
  let batchIndex = 0;
  for (const batch of churnProbeBatches(specs)) {
    batchIndex += 1;
    const verified = await waitForChurnProtocolEquivalenceBatch({ endpoint, repeat, specs: batch });
    if (retainRawValidationRecords) {
      rawRecordArtifacts.push(
        writeCompressedValidationRecords({
          targetDir,
          targetLabel: target.label,
          repeat,
          phase: 'churn',
          batchIndex,
          buckets: verified
        })
      );
    }
    buckets.push(...stripRawValidationRecords(verified));
  }
  const protocolProbeMs = round(performance.now() - probeStartedAt);
  const churnToProtocolVerifiedMs = round(performance.now() - startedAt);
  const rustPersistedObservation =
    target.label === 'rust' && churnGateMode === 'slot-lsn'
      ? await observeRustChurnMetricsAfterPublicTiming({
          endpoint,
          baseline: rustBaselineMetrics,
          expectedTailOpsDelta
        })
      : null;

  const payload = {
    target: target.label,
    repeat,
    profile: profileName,
    generatedAt: new Date().toISOString(),
    status: 'passed',
    gate: 'incremental-update-delete-churn-equivalence',
    requestMode: 'explicit-bucket-cursor-after-initial-equivalence',
    targetLsn,
    applySqlMs,
    replicationCatchupMs,
    slotAckCatchupMs: ready?.method === 'replication-slot' ? replicationCatchupMs : null,
    protocolProbeMs,
    churnToProtocolVerifiedMs,
    mutation: {
      projectBucketsTouched: mutation.projectIds.length,
      rowsPerBucket: mutation.rowsPerBucket,
      inserts: mutation.insertRows.length,
      updates: mutation.updateRows.length,
      deletes: mutation.deleteRows.length
    },
    ready,
    catchupGateMethod: ready?.method ?? null,
    rustPersistedObservation:
      rustPersistedObservation?.persistedReady == null
        ? null
        : {
            ...rustPersistedObservation.persistedReady,
            timing: 'observed-after-public-churn-timing'
          },
    rustPersistedError: rustPersistedObservation?.error ?? null,
    rawRecordArtifacts,
    buckets
  };
  const artifactPath = path.join(targetDir, `churn-protocol-equivalence.r${repeat}.json`);
  fs.writeFileSync(artifactPath, JSON.stringify(payload, null, 2));
  log(
    `churn ${target.label}#${repeat}: passed ${buckets.length} buckets / ` +
      `${buckets.reduce((sum, bucket) => sum + bucket.puts + bucket.removes, 0)} incremental ops`
  );
  return { ...payload, artifactPath };
}

function buildChurnBucketSpecs(initialEquivalence) {
  const initialByBucket = new Map(initialEquivalence.buckets.map((bucket) => [bucket.bucket, bucket]));
  const verificationSpecs =
    initialEquivalence.verificationSpecs ??
    attachExpectedRowsToBucketSpecs(buildInitialEquivalenceBucketSpecs());
  const churnSpecs = verificationSpecs
    .filter((spec) => initialByBucket.has(spec.bucket))
    .map((spec) => {
      const initial = initialByBucket.get(spec.bucket);
      if (!Array.isArray(spec.expectedRows)) {
        throw new Error(`churn ${spec.label}: initial verification spec is missing expectedRows`);
      }
      return {
        ...spec,
        requestKind: 'explicit',
        after: normalizeDecimalCursor(
          initial.cursorAfter ?? initial.lastOpId,
          `churn ${spec.label} source cursor`
        ),
        initialCheckpointChecksum: initial.checkpointChecksum,
        initialCheckpointCount: initial.checkpointCount
      };
    });
  if (churnSpecs.length !== initialByBucket.size) {
    const available = new Set(churnSpecs.map((spec) => spec.bucket));
    const missing = [...initialByBucket.keys()].filter((bucket) => !available.has(bucket));
    throw new Error(`churn: missing enriched initial verification specs for ${JSON.stringify(missing.slice(0, 10))}`);
  }
  return churnSpecs;
}

function churnProbeBatches(specs) {
  return [
    ...chunkArray(
      specs.filter((spec) => !isAuthPerimeterSpec(spec) && !spec.subscriptionParameters),
      bucketProbeBatchSize
    ),
    specs.filter(isAuthPerimeterSpec),
    ...chunkArray(
      specs.filter((spec) => spec.subscriptionParameters),
      bucketProbeBatchSize
    )
  ].filter((batch) => batch.length > 0);
}

function isAuthPerimeterSpec(spec) {
  return spec.subscriptionMode === 'auth_perimeter';
}

function buildChurnMutationPlan(repeat) {
  const projectIds =
    authPerimeterMode
      ? authPerimeterProjectIds()
      : routeStreamsEnabled && projectBucketSampleCount > 0
        ? sampledProjectIds(projectBucketSampleCount)
        : [fixture.ids.primaryProjectId];
  const batchTag = `churn-r${repeat}`;
  const insertRows = projectIds.flatMap((projectIdValue, projectIndex) =>
    Array.from({ length: churnRowsPerBucket }, (_, rowIndex) =>
      churnInsertedRow(projectIdValue, batchTag, projectIndex + 1, rowIndex + 1)
    )
  );
  const updateRows = projectIds.flatMap((projectIdValue) =>
    Array.from({ length: churnRowsPerBucket }, (_, rowIndex) =>
      churnUpdatedRow(projectIdValue, batchTag, rowIndex + 1)
    )
  );
  const deleteRows = projectIds.flatMap((projectIdValue) =>
    Array.from({ length: churnRowsPerBucket }, (_, rowIndex) => churnDeletedRow(projectIdValue, rowIndex + 1))
  );
  return {
    batchTag,
    projectIds,
    rowsPerBucket: churnRowsPerBucket,
    insertRows,
    updateRows,
    deleteRows
  };
}

function expectedDefaultReplicaIdentityChurnTailOpsDelta(mutation) {
  // This benchmark schema uses PostgreSQL's default replica identity and the
  // churn UPDATE mutates only non-key columns. The logical update therefore
  // arrives without an old tuple and Rust derives one persisted PUT, not a
  // REMOVE+PUT pair. If the fixture switches to replica identity FULL, this
  // must be changed to derive the count from the decoded WAL shape.
  return mutation.insertRows.length + mutation.updateRows.length + mutation.deleteRows.length;
}

function churnInsertedRow(projectIdValue, batchTag, projectOrdinal, rowOrdinal) {
  const rowTag = String(rowOrdinal).padStart(4, '0');
  return {
    id: `task-runtime-insert-${batchTag}-${projectIdValue}-${rowTag}`,
    org_id: fixture.targetOrgId,
    project_id: projectIdValue,
    owner_id: fixture.targetUserId,
    title: `Churn inserted project ${String(projectOrdinal).padStart(4, '0')} row ${rowTag}`,
    status: 'todo',
    priority: 4,
    assignee_id: fixture.targetUserId,
    story_points: 3,
    updated_at: '2026-03-01T00:00:00Z',
    summary: `churn:insert:${batchTag}:${projectIdValue}:${rowTag}`
  };
}

function churnUpdatedRow(projectIdValue, batchTag, rowOrdinal) {
  const projectIndex = projectIndexFromProjectId(projectIdValue);
  const rowTag = String(rowOrdinal).padStart(4, '0');
  return {
    ...generatedTaskRow(1, projectIndex, rowOrdinal),
    title: `Churn updated ${batchTag} ${projectIdValue} row ${rowTag}`,
    updated_at: '2026-03-01T00:00:00Z',
    summary: `churn:update:${batchTag}:${projectIdValue}:${rowTag}`
  };
}

function churnDeletedRow(projectIdValue, rowOrdinal) {
  return generatedTaskRow(1, projectIndexFromProjectId(projectIdValue), churnRowsPerBucket + rowOrdinal);
}

function projectIndexFromProjectId(projectIdValue) {
  const raw = String(projectIdValue).split('-').at(-1);
  const index = Number.parseInt(raw, 10);
  if (!Number.isFinite(index) || index < 1) {
    throw new Error(`invalid generated project id: ${projectIdValue}`);
  }
  return index;
}

function churnMutationSql(mutation) {
  const updateIds = mutation.updateRows.map((row) => row.id);
  const deleteIds = mutation.deleteRows.map((row) => row.id);
  return `
BEGIN;
${insertTaskRowsSql(mutation.insertRows)}
UPDATE ${TABLES.tasks}
SET
  title = CASE id
    ${mutation.updateRows.map((row) => `WHEN '${escapeLiteral(row.id)}' THEN '${escapeLiteral(row.title)}'`).join('\n    ')}
    ELSE title
  END,
  summary = CASE id
    ${mutation.updateRows.map((row) => `WHEN '${escapeLiteral(row.id)}' THEN '${escapeLiteral(row.summary)}'`).join('\n    ')}
    ELSE summary
  END,
  updated_at = TIMESTAMPTZ '2026-03-01T00:00:00Z'
WHERE id IN (${updateIds.map((id) => `'${escapeLiteral(id)}'`).join(', ')});
DELETE FROM ${TABLES.tasks}
WHERE id IN (${deleteIds.map((id) => `'${escapeLiteral(id)}'`).join(', ')});
COMMIT;
SELECT 'benchmark_target_lsn=' || pg_current_wal_lsn()::text;
`.trim();
}

function applyChurnMutationAndCaptureLsn(mutation) {
  const result = runSqlCommand(
    ['-qAt', '-v', 'ON_ERROR_STOP=1', '-f', '-'],
    `SET search_path TO public;\n${churnMutationSql(mutation)}`
  );
  if (result.status !== 0) {
    throw new Error(`churn SQL failed: ${result.stderr || result.stdout}`);
  }
  return extractMutationTargetLsn(result.stdout);
}

function extractMutationTargetLsn(output) {
  const matches = [...String(output ?? '').matchAll(/^\s*benchmark_target_lsn=([0-9A-F]+\/[0-9A-F]+)\s*$/gim)];
  if (matches.length !== 1) {
    throw new Error(`churn SQL returned ${matches.length} target LSN markers; expected exactly one`);
  }
  return matches[0][1].toUpperCase();
}

function attachChurnExpectations(specs, mutation) {
  return specs.map((spec) => {
    const expectedInsertRows = mutation.insertRows.filter((row) => bucketSpecMatchesRow(spec, row));
    const expectedUpdateRows = mutation.updateRows.filter((row) => bucketSpecMatchesRow(spec, row));
    const expectedDeleteRows = mutation.deleteRows.filter((row) => bucketSpecMatchesRow(spec, row));
    const expectedSnapshotById = new Map(spec.expectedRows.map((row) => [row.id, row]));
    for (const row of expectedDeleteRows) expectedSnapshotById.delete(row.id);
    for (const row of [...expectedUpdateRows, ...expectedInsertRows]) expectedSnapshotById.set(row.id, row);
    return {
      ...spec,
      expectedPuts: [...expectedInsertRows, ...expectedUpdateRows],
      expectedRemoves: expectedDeleteRows.map((row) => row.id),
      expectedSnapshotRows: [...expectedSnapshotById.values()],
      expectedInserts: expectedInsertRows.length,
      expectedUpdates: expectedUpdateRows.length,
      expectedDeletes: expectedDeleteRows.length
    };
  });
}

function bucketSpecMatchesRow(spec, row) {
  if (spec.stream === 'tasks') return true;
  if (spec.stream === 'tasks_by_project') return row.project_id === specRouteParameter(spec, 'project_id');
  if (spec.stream === authPerimeterStream) return row.project_id === specRouteParameter(spec, 'project_id');
  if (spec.stream === 'tasks_by_org') return row.org_id === specRouteParameter(spec, 'org_id');
  return false;
}

async function waitForChurnCatchup({
  target,
  endpoint,
  targetLsn,
  readiness,
  rustBaselineMetrics,
  expectedTailOpsDelta
}) {
  if (churnGateMode === 'slot-lsn') {
    // Same finish line for both targets: the replication slot's
    // confirmed_flush_lsn in Postgres, i.e. persisted AND acked upstream.
    let slotName;
    if (target.label === 'rust') {
      slotName = process.env.POWERSYNC_RUST_REPLICATION_SLOT ?? rustSlot;
    } else {
      // The readiness snapshot is captured during the startup probe, but the
      // official service only names its slot once replication begins
      // streaming, which can lag "ready" under load. When the snapshot lacks
      // the name, discover the slot directly in Postgres by its well-known
      // prefix — the finish line (confirmed_flush_lsn) is identical either way.
      slotName = extractReadinessSlotName(readiness) ?? resolveOfficialReplicationSlotName();
    }
    if (slotName) {
      const ready = await waitForReplicationSlotToReach({
        slotName,
        targetLsn,
        timeoutMs,
        pollIntervalMs: 100
      });
      return { ...ready, method: 'replication-slot' };
    }
    throw new Error(`churn slot-lsn gate requires a replication slot name for ${target.label}`);
  }
  return target.label === 'rust'
    ? await waitForRustChurnMetricsToReach({
        endpoint,
        baseline: rustBaselineMetrics,
        expectedTailOpsDelta,
        timeoutMs,
        pollIntervalMs: 100
      })
    : await waitForTargetLsnReach({ target, endpoint, targetLsn, readiness });
}

async function waitForTargetLsnReach({ target, endpoint, targetLsn, readiness }) {
  if (target.label === 'rust') {
    return await waitForRustMetricsToReach({
      endpoint,
      targetLsn,
      timeoutMs,
      pollIntervalMs: 100
    });
  }

  const slotName = extractReadinessSlotName(readiness);
  if (slotName) {
    return await waitForReplicationSlotToReach({
      slotName,
      targetLsn,
      timeoutMs,
      pollIntervalMs: 100
    });
  }

  return await waitForDiagnosticsToReach({
    endpoint,
    targetLsn,
    expectedVersion: readiness?.activeVersion ?? null,
    timeoutMs,
    pollIntervalMs: 100
  });
}

function extractReadinessSlotName(readiness) {
  const connections = readiness?.ready?.body?.active_sync_rules?.connections;
  if (!Array.isArray(connections)) return null;
  return connections.find((connection) => typeof connection?.slot_name === 'string')?.slot_name ?? null;
}

async function proveChurnProtocolEquivalenceBatch({ endpoint, repeat, specs }) {
  const protocol = await loadPowerSyncCommonProtocol();
  const requestBody = churnEquivalenceRequestBody(specs, repeat);
  const batchLabel = specs.length === 1 ? specs[0].label : `${specs[0].label}+${specs.length - 1}`;
  const states = new Map(
    specs.map((spec) => [
      spec.bucket,
      {
        spec,
        expectedPutsById: new Map(spec.expectedPuts.map((row) => [row.id, row])),
        expectedSnapshotById: new Map(spec.expectedSnapshotRows.map((row) => [row.id, row])),
        expectedRemoveIds: new Set(spec.expectedRemoves),
        seenPuts: new Set(),
        seenRemoves: new Set(),
        payloadMismatches: [],
        checksumValues: new Set(),
        checksumRecords: [],
        checksumSum: 0,
        nonZeroChecksumCount: 0,
        nonZeroClearChecksumCount: 0,
        dataLines: 0,
        clearCount: 0,
        previousAfter: normalizeDecimalCursor(spec.after ?? '0', `churn ${spec.label} initial cursor`),
        resetObserved: false
      }
    ])
  );

  const response = await postNdjsonProtocol(
    `${endpoint}/sync/stream`,
    requestBody,
    createBenchmarkJwt(),
    Math.max(timeoutMs, 120_000),
    {
      label: `${endpoint}/sync/stream churn ${batchLabel}`,
      protocol,
      onData(data) {
        const state = states.get(data.bucket);
        if (!state) {
          throw new Error(`churn ${batchLabel}: unexpected data bucket ${data.bucket}`);
        }
        state.dataLines += 1;
        assertCursorProgression(state, data, `churn ${state.spec.label}`);

        for (const entry of data.data ?? []) {
          if (entry.op === 'CLEAR') {
            state.clearCount += 1;
            resetObservedBucketState(state);
            recordEntryChecksum(state, entry);
            continue;
          }
          recordEntryChecksum(state, entry);
          if (entry.object_type !== TABLES.tasks) {
            throw new Error(`churn ${state.spec.label}: object_type ${entry.object_type} != ${TABLES.tasks}`);
          }
          const objectId = entry.object_id;
          if (!objectId) throw new Error(`churn ${state.spec.label}: op missing object_id`);
          if (entry.op === 'REMOVE') {
            if (!state.expectedRemoveIds.has(objectId)) {
              throw new Error(`churn ${state.spec.label}: unexpected REMOVE ${objectId}`);
            }
            state.seenRemoves.add(objectId);
            continue;
          }
          if (entry.op !== 'PUT') {
            throw new Error(`churn ${state.spec.label}: unsupported op ${entry.op}`);
          }
          const expected = state.resetObserved
            ? state.expectedSnapshotById.get(objectId)
            : state.expectedPutsById.get(objectId);
          if (!expected) {
            throw new Error(`churn ${state.spec.label}: unexpected PUT ${objectId}`);
          }
          if (state.seenPuts.has(objectId)) {
            throw new Error(`churn ${state.spec.label}: duplicate PUT ${objectId}`);
          }
          state.seenPuts.add(objectId);
          const mismatch = firstTaskPayloadMismatch(normalizeProtocolData(entry.data), expected);
          if (mismatch && state.payloadMismatches.length < 5) {
            state.payloadMismatches.push({ objectId, ...mismatch });
          }
        }
      }
    }
  );
  if (!response.ok) {
    throw new Error(
      `churn ${batchLabel} request failed with ${response.statusCode}: ${response.rawBody.slice(0, 400)}`
    );
  }

  return specs.map((spec) => churnBucketValidationFromState({
    state: states.get(spec.bucket),
    checkpoint: response.checkpoint,
    protocol,
    responseBytes: response.responseBytes,
    batchSize: specs.length
  }));
}

async function waitForChurnProtocolEquivalenceBatch({ endpoint, repeat, specs }) {
  const deadline = Date.now() + churnProtocolSettleMs;
  let attempt = 0;
  let lastError = null;

  while (Date.now() < deadline) {
    attempt += 1;
    try {
      return await proveChurnProtocolEquivalenceBatch({
        endpoint,
        repeat: `${repeat}-settle-${attempt}`,
        specs
      });
    } catch (error) {
      if (!isRetryableChurnProtocolConvergenceError(error)) throw error;
      lastError = error;
      if (attempt === 1 || attempt % 10 === 0) {
        log(`churn protocol view pending attempt ${attempt}: ${compactErrorMessage(error?.message)}`);
      }
      if (Date.now() < deadline) await delay(Math.min(1_000, readinessPollMs * attempt));
    }
  }

  throw new Error(
    `churn protocol view did not converge within ${churnProtocolSettleMs}ms: ${compactErrorMessage(
      lastError?.message
    )}`
  );
}

function isRetryableChurnProtocolConvergenceError(error) {
  if (isRetryableProtocolReadinessError(error)) return true;
  const message = String(error?.message ?? error);
  return /^churn .*: (?:PUT mismatch|REMOVE mismatch|checkpoint did not include|checkpoint count|checkpoint checksum)/i.test(
    message
  );
}

function churnEquivalenceRequestBody(specs, repeat) {
  const base = {
    buckets: specs.map((spec) => ({ name: spec.bucket, after: spec.after })),
    include_checksum: true,
    raw_data: true,
    binary_data: false,
    client_id: `probe-churn-${profileName}-${repeat}-${specs.length}-${suffix}`,
    parameters: probeRequestParameters(),
    app_metadata: {}
  };

  if (specs.every(isAuthPerimeterSpec)) {
    return {
      ...base,
      streams: {
        include_defaults: false,
        subscriptions: [
          {
            stream: authPerimeterStream,
            parameters: {},
            override_priority: 3
          }
        ]
      }
    };
  }

  if (specs.every((spec) => spec.subscriptionParameters)) {
    return {
      ...base,
      streams: {
        include_defaults: false,
        subscriptions: specs.map((spec) => ({
          stream: spec.stream,
          parameters: spec.subscriptionParameters ?? {},
          override_priority: 3
        }))
      }
    };
  }

  if (specs.some((spec) => spec.subscriptionParameters || isAuthPerimeterSpec(spec))) {
    throw new Error('cannot mix explicit default buckets and stream subscriptions in one churn request');
  }

  return base;
}

function churnBucketValidationFromState({ state, checkpoint, protocol, responseBytes, batchSize }) {
  const {
    spec,
    expectedPutsById,
    expectedRemoveIds,
    seenPuts,
    seenRemoves,
    payloadMismatches,
    checksumValues,
    nonZeroChecksumCount
  } = state;
  const checkpointBucket = (checkpoint.buckets ?? []).find((bucket) => bucket.bucket === spec.bucket);
  if (!checkpointBucket) {
    throw new Error(`churn ${spec.label}: checkpoint did not include ${spec.bucket}`);
  }
  const validationPutsById = state.resetObserved ? state.expectedSnapshotById : expectedPutsById;
  const validationRemoveIds = state.resetObserved ? new Set() : expectedRemoveIds;
  if (seenPuts.size !== validationPutsById.size) {
    throw new Error(
      `churn ${spec.label}: PUT mismatch expected=${validationPutsById.size} actual=${seenPuts.size}` +
        ` missing=${JSON.stringify([...validationPutsById.keys()].filter((id) => !seenPuts.has(id)).slice(0, 10))}`
    );
  }
  if (seenRemoves.size !== validationRemoveIds.size) {
    throw new Error(
      `churn ${spec.label}: REMOVE mismatch expected=${validationRemoveIds.size} actual=${seenRemoves.size}` +
        ` missing=${JSON.stringify([...validationRemoveIds].filter((id) => !seenRemoves.has(id)).slice(0, 10))}`
    );
  }
  if (payloadMismatches.length > 0) {
    throw new Error(`churn ${spec.label}: payload mismatch ${JSON.stringify(payloadMismatches)}`);
  }

  const checkpointChecksum = normalizeOptionalProtocolNumber(checkpointBucket.checksum);
  const expectedCheckpointChecksum = protocolChecksumI32(
    addChecksumU32(spec.initialCheckpointChecksum ?? 0, state.checksumSum)
  );
  const expectedCheckpointCount =
    Number(spec.initialCheckpointCount ?? 0) + expectedPutsById.size + expectedRemoveIds.size;
  if (Number.isFinite(Number(checkpointBucket.count)) && Number(checkpointBucket.count) !== expectedCheckpointCount) {
    throw new Error(
      `churn ${spec.label}: checkpoint count ${checkpointBucket.count} != expected ${expectedCheckpointCount}`
    );
  }
  assertCheckpointChecksumMatches({
    label: `churn ${spec.label}`,
    checkpointChecksum,
    expectedChecksum: expectedCheckpointChecksum
  });
  if (nonZeroChecksumCount !== seenPuts.size + seenRemoves.size + state.nonZeroClearChecksumCount) {
    throw new Error(
      `churn ${spec.label}: non-zero entry checksums ${nonZeroChecksumCount} != incremental op count ${
        seenPuts.size + seenRemoves.size + state.nonZeroClearChecksumCount
      }`
    );
  }

  return {
    label: spec.label,
    stream: spec.stream,
    bucket: spec.bucket,
    protocolValidator: protocol.artifactSummary,
    checkpointCount: Number(checkpointBucket.count),
    checkpointChecksum,
    expectedCheckpointChecksum,
    expectedCheckpointCount,
    initialCheckpointChecksum: spec.initialCheckpointChecksum,
    initialCheckpointCount: spec.initialCheckpointCount,
    entryChecksums: entryChecksumSummary(checksumValues, nonZeroChecksumCount, state.checksumRecords),
    after: spec.after,
    lastOpId: normalizeDecimalCursor(checkpoint.last_op_id, `churn ${spec.label} checkpoint.last_op_id`),
    deliveryMode: state.resetObserved ? 'reset-snapshot' : 'incremental',
    clears: state.clearCount,
    expectedInserts: spec.expectedInserts,
    expectedUpdates: spec.expectedUpdates,
    expectedDeletes: spec.expectedDeletes,
    dataLines: state.dataLines,
    puts: seenPuts.size,
    removes: seenRemoves.size,
    bytes: responseBytes,
    batchSize,
    status: 'passed'
  };
}

async function loadPowerSyncCommonProtocol() {
  if (powersyncCommonProtocol) return powersyncCommonProtocol;

  let resolved;
  try {
    resolved = sdkRequire.resolve('@powersync/common');
  } catch (error) {
    throw new Error(
      `equivalence gate requires the public @powersync/common package from ${path.relative(
        projectRoot,
        sdkDir
      )}; run npm ci there first (${error.message})`
    );
  }

  const common = await import(resolved);
  const required = [
    'isStreamingKeepalive',
    'isStreamingSyncCheckpoint',
    'isStreamingSyncCheckpointComplete',
    'isStreamingSyncData'
  ];
  for (const name of required) {
    if (typeof common[name] !== 'function') {
      throw new Error(`@powersync/common missing public protocol helper ${name}`);
    }
  }

  const packageRoot = path.dirname(path.dirname(resolved));
  let version = null;
  try {
    version = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).version ?? null;
  } catch {
    version = null;
  }

  powersyncCommonProtocol = {
    isStreamingKeepalive: common.isStreamingKeepalive,
    isStreamingSyncCheckpoint: common.isStreamingSyncCheckpoint,
    isStreamingSyncCheckpointComplete: common.isStreamingSyncCheckpointComplete,
    isStreamingSyncData: common.isStreamingSyncData,
    artifactSummary: {
      sdk: '@powersync/common',
      version,
      publicExports: required
    }
  };
  return powersyncCommonProtocol;
}

function bucketNameForStream(streamName, values) {
  return `1#${streamName}|0${JSON.stringify(values)}`;
}

function normalizeProtocolData(data) {
  if (data == null) return {};
  if (typeof data === 'string') {
    try {
      return JSON.parse(data);
    } catch {
      return {};
    }
  }
  if (typeof data === 'object') return data;
  return {};
}

function firstTaskPayloadMismatch(actual, expected) {
  for (const key of [
    'id',
    'org_id',
    'project_id',
    'title',
    'status',
    'priority',
    'assignee_id',
    'story_points',
    'updated_at',
    'summary'
  ]) {
    const actualValue = key === 'updated_at' ? normalizeTimestamp(actual?.[key]) : normalizeScalar(actual?.[key]);
    const expectedValue = key === 'updated_at' ? normalizeTimestamp(expected?.[key]) : normalizeScalar(expected?.[key]);
    if (actualValue !== expectedValue) {
      return { field: key, expected: expectedValue, actual: actualValue };
    }
  }
  return null;
}

function normalizeScalar(value) {
  if (value == null) return '';
  return `${value}`;
}

function normalizeTimestamp(value) {
  if (value == null) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return `${value}`;
  return date.toISOString();
}

async function waitForScenarioReady({
  target,
  endpoint,
  expectedRules,
  targetLsn = null,
  processingStartedAt = null,
  allowSyncProbeFallback = true,
  publicationBoundary = false,
  repeat = 0
}) {
  if (publicationBoundary) {
    const boundaries = await collectInitialReadinessBoundaries({
      observeProtocol: () =>
        waitForProtocolObservableInitialReadiness({ endpoint, repeat, processingStartedAt }),
      observeCompleteMaterialization: () =>
        waitForTargetSpecificScenarioReady({
          target,
          endpoint,
          expectedRules,
          targetLsn,
          processingStartedAt,
          allowSyncProbeFallback: false,
          requireActiveState: true,
          requireExpectedRules: true
        }),
      observeSlotPosition: () =>
        waitForInitialSlotPosition({ target, targetLsn, processingStartedAt })
    });
    return buildPublicationReadiness({
      observableReady: boundaries.observableReady,
      completeMaterialization: boundaries.completeMaterialization,
      sourceSlotPosition: boundaries.sourceSlotPosition,
      targetLsn
    });
  }

  return await waitForTargetSpecificScenarioReady({
    target,
    endpoint,
    expectedRules,
    targetLsn,
    processingStartedAt,
    allowSyncProbeFallback
  });
}

async function collectInitialReadinessBoundaries({
  observeProtocol,
  observeCompleteMaterialization,
  observeSlotPosition
}) {
  const observations = [
    ['protocol readiness', settleOutcome(Promise.resolve().then(observeProtocol))],
    ['complete materialization', settleOutcome(Promise.resolve().then(observeCompleteMaterialization))],
    ['replication-slot position', settleOutcome(Promise.resolve().then(observeSlotPosition))]
  ];
  const outcomes = await Promise.all(observations.map(([, promise]) => promise));
  const failures = outcomes
    .map((outcome, index) => ({ outcome, label: observations[index][0] }))
    .filter(({ outcome }) => !outcome.ok);
  if (failures.length > 0) {
    throw new Error(
      `initial readiness boundary failed: ${failures
        .map(({ label, outcome }) => `${label}: ${compactErrorMessage(outcome.error?.message)}`)
        .join('; ')}`
    );
  }
  return {
    observableReady: outcomes[0].value,
    completeMaterialization: outcomes[1].value,
    sourceSlotPosition: outcomes[2].value
  };
}

async function waitForTargetSpecificScenarioReady({
  target,
  endpoint,
  expectedRules,
  targetLsn,
  processingStartedAt,
  allowSyncProbeFallback,
  requireActiveState = false,
  requireExpectedRules = false
}) {
  let activeState = null;
  try {
    activeState = await waitForActiveState(endpoint, {
      expectedVersion: null,
      expectedContent: expectedRules,
      expectedSlotName: null,
      requireExpectedContent: requireExpectedRules,
      timeoutMs
    });
  } catch (error) {
    if (requireActiveState) throw error;
    log(`sync-rules state fallback for ${target.label}: ${error.message}`);
  }

  if (target.supportsControlPlane) {
    try {
      if (target.label === 'rust' && targetLsn != null) {
        const readyState = await waitForRustMetricsToReach({
          endpoint,
          targetLsn,
          timeoutMs,
          pollIntervalMs: 100
        });
        log(`startup ${target.label}: active + persisted-LSN ready`);
        return {
          method: 'persisted-lsn',
          completionBoundary: 'rust-persisted-initial-snapshot-lsn',
          activeVersion: activeState?.version ?? null,
          processingMs: processingStartedAt == null ? readyState.elapsedMs : round(performance.now() - processingStartedAt),
          targetLsn,
          ready: readyState
        };
      }
      const readyState = await waitForDiagnosticsToReach({
        endpoint,
        targetLsn,
        expectedVersion: activeState?.version ?? null,
        timeoutMs,
        pollIntervalMs: 100
      });
      log(`startup ${target.label}: active + ready`);
      return {
        method: 'control-plane',
        completionBoundary: 'official-diagnostics-initial-replication-lsn',
        activeVersion: activeState?.version ?? null,
        processingMs: processingStartedAt == null ? readyState.elapsedMs : round(performance.now() - processingStartedAt),
        targetLsn,
        ready: readyState
      };
    } catch (error) {
      log(
        `control-plane readiness fallback for ${target.label}: ${error.message}`
      );
    }
  }

  if (!allowSyncProbeFallback) {
    throw new Error(
      `target ${target.label} required a sync probe fallback before cold-open measurement; refusing because fairness guard is enabled`
    );
  }

  await waitForInitialSyncData(endpoint, createBenchmarkJwt());
  return {
    method: 'sync-probe-fallback',
    activeVersion: activeState?.version ?? null,
    processingMs: processingStartedAt == null ? null : round(performance.now() - processingStartedAt)
  };
}

async function waitForProtocolObservableInitialReadiness({ endpoint, repeat, processingStartedAt }) {
  const spec = protocolReadinessSpec;
  if (!spec) throw new Error('public readiness requires at least one protocol equivalence bucket');

  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  for (let attempt = 1; attempt <= protocolReadinessAttempts && Date.now() < deadline; attempt += 1) {
    try {
      const [proof] = await proveInitialBucketProtocolEquivalenceBatch({
        endpoint,
        repeat: `${repeat}-readiness-${attempt}`,
        specs: [spec],
        authToken: createBenchmarkJwt({ subject: initialReadinessSubject() })
      });
      return {
        method: 'sync-protocol-checkpoint-complete',
        processingMs: processingStartedAt == null ? null : round(performance.now() - processingStartedAt),
        attempts: attempt,
        bucket: proof.bucket,
        expectedRows: proof.expectedRows,
        lastOpId: proof.lastOpId,
        cursorAfter: proof.cursorAfter
      };
    } catch (error) {
      lastError = error;
      if (!isRetryableProtocolReadinessError(error)) {
        throw new Error(
          `common initial protocol readiness failed deterministically on attempt ${attempt}: ${compactErrorMessage(
            error?.message
          )}`
        );
      }
      if (attempt === 1 || attempt % 10 === 0) {
        log(
          `common initial protocol readiness pending attempt ${attempt}/${protocolReadinessAttempts}: ${compactErrorMessage(
            error?.message
          )}`
        );
      }
      if (attempt < protocolReadinessAttempts && Date.now() < deadline) {
        await delay(Math.min(1_000, readinessPollMs * Math.max(1, attempt)));
      }
    }
  }
  throw new Error(
    `service at ${endpoint} did not complete the common initial protocol readiness probe after at most ${
      protocolReadinessAttempts
    } attempts: ${compactErrorMessage(lastError?.message)}`
  );
}

function isRetryableProtocolReadinessError(error) {
  const message = String(error?.message ?? error);
  return (
    /request failed with (?:404|408|409|425|429|500|502|503|504)\b/.test(message) ||
    /\b(?:ECONNREFUSED|ECONNRESET|EPIPE|ETIMEDOUT|socket hang up|timeout requesting)\b/i.test(message)
  );
}

function buildPublicationReadiness({ observableReady, completeMaterialization, sourceSlotPosition, targetLsn }) {
  if (observableReady?.method !== 'sync-protocol-checkpoint-complete') {
    throw new Error('publication readiness requires a completed sync-protocol checkpoint boundary');
  }
  if (!Number.isFinite(observableReady?.processingMs)) {
    throw new Error('publication readiness requires finite protocol timing');
  }
  const recognizedCompletionBoundaries = new Set([
    'official-diagnostics-initial-replication-lsn',
    'rust-persisted-initial-snapshot-lsn'
  ]);
  if (!recognizedCompletionBoundaries.has(completeMaterialization?.completionBoundary)) {
    throw new Error('publication readiness requires a recognized complete-materialization boundary');
  }
  if (!Number.isFinite(completeMaterialization?.processingMs)) {
    throw new Error('publication readiness requires finite complete-materialization timing');
  }
  if (sourceSlotPosition?.method !== 'replication-slot-confirmed-flush-lsn') {
    throw new Error('publication readiness requires a replication-slot confirmed-flush position');
  }
  if (!Number.isFinite(sourceSlotPosition?.processingMs)) {
    throw new Error('publication readiness requires finite replication-slot timing');
  }
  if (
    typeof targetLsn !== 'string' ||
    completeMaterialization.targetLsn !== targetLsn ||
    sourceSlotPosition.targetLsn !== targetLsn
  ) {
    throw new Error('publication readiness boundaries must use the captured fixture LSN');
  }
  return {
    method: observableReady.method,
    processingMs: observableReady.processingMs,
    activeVersion: completeMaterialization?.activeVersion ?? null,
    ready: observableReady,
    completeMaterialization,
    sourceSlotPosition,
    targetSpecificDiagnostic: completeMaterialization
  };
}

async function runPlaywrightAsync({ targetLabel, endpoint, resultPath, scenarios, iterations }) {
  const harnessPort = await freePort();
  const env = {
    ...process.env,
    POWERSYNC_BENCHMARK_TARGET: targetLabel,
    POWERSYNC_TOKEN: createBenchmarkJwt(),
    POWERSYNC_HARNESS_PORT: `${harnessPort}`,
    POWERSYNC_BENCHMARK_PROFILE: profileName,
    POWERSYNC_BENCHMARK_ITERATIONS: `${iterations}`,
    POWERSYNC_BENCHMARK_CONCURRENT_CLIENTS: `${concurrentClients}`,
    POWERSYNC_TIMEOUT_MS: `${timeoutMs}`,
    POWERSYNC_BENCHMARK_RESULT_PATH: resultPath,
    POWERSYNC_BENCHMARK_SCENARIOS: scenarios.join(','),
    POWERSYNC_POSTGRES_CONTAINER: postgresContainerName,
    POWERSYNC_POSTGRES_DATABASE: 'powersync_benchmark_test',
    POWERSYNC_ENDPOINT: endpoint
  };

  return await new Promise((resolve, reject) => {
    const child = spawn('npx', ['playwright', 'test', 'tests/browser-benchmark.spec.mjs'], {
      cwd: sdkDir,
      env,
      stdio: ['ignore', 'pipe', 'pipe']
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => {
      const text = chunk.toString();
      stdout += text;
      process.stdout.write(text);
    });
    child.stderr.on('data', (chunk) => {
      const text = chunk.toString();
      stderr += text;
      process.stderr.write(text);
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`playwright failed for ${targetLabel} with exit code ${code}: ${stderr || stdout}`));
        return;
      }
      try {
        resolve(JSON.parse(fs.readFileSync(resultPath, 'utf8')));
      } catch (error) {
        reject(error);
      }
    });
  });
}

async function performMutation({ target, endpoint, operation, nextRules, beforeVersion, scenarioName, repeat }) {
  if (target.label === 'official') {
    const port = Number(new URL(endpoint).port);
    await stopOfficialServiceWithOptions({ keepMongo: true });
    const restarted = await startOfficialService(nextRules, { port });
    return {
      ok: true,
      statusCode: 202,
      endpoint: restarted.endpoint,
      body: {
        data: {
          operation,
          accepted_via: 'config-restart',
          current: {
            content: nextRules
          }
        }
      }
    };
  }

  const base = Number.isFinite(beforeVersion) ? { base_version: beforeVersion } : {};
  const body =
    operation === 'deploy'
      ? {
          content: nextRules,
          ...base,
          intent_token: `${scenarioName}-${repeat}-${suffix}`
        }
      : {
          ...base,
          intent_token: `${scenarioName}-${repeat}-${suffix}`
        };

  return await postJson(`${endpoint}/api/sync-rules/v1/${operation}`, body, apiToken);
}

async function fetchJson(url, tokenValue = null) {
  return await requestJson(url, {
    method: 'GET',
    token: tokenValue
  });
}

async function postJson(url, body, tokenValue = null) {
  return await requestJson(url, {
    method: 'POST',
    body,
    token: tokenValue
  });
}

async function postNdjsonProtocol(urlString, body, bearerToken, timeoutMsValue, { label, protocol, onData }) {
  const url = new URL(urlString);
  const transport = url.protocol === 'https:' ? https : http;
  const payload = JSON.stringify(body);

  return await new Promise((resolve, reject) => {
    let settled = false;
    let request = null;
    let responseBytes = 0;
    let errorPreview = '';
    let lineBuffer = '';
    let checkpoint = null;
    let complete = null;
    let protocolLineCount = 0;

    const finish = (response) => {
      if (settled) return;
      settled = true;
      resolve({
        ok: (response.statusCode ?? 0) >= 200 && (response.statusCode ?? 0) < 300,
        statusCode: response.statusCode ?? 0,
        headers: response.headers,
        rawBody: errorPreview,
        checkpoint,
        complete,
        responseBytes,
        protocolLineCount
      });
      response.destroy();
      request?.destroy();
    };
    const fail = (error, response = null) => {
      if (settled) return;
      settled = true;
      response?.destroy();
      request?.destroy();
      reject(error);
    };
    const processLine = (line) => {
      if (line.trim().length === 0) return false;
      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch (error) {
        throw new Error(`invalid NDJSON from ${label} at protocol line ${protocolLineCount + 1}: ${error.message}`);
      }

      if (protocol.isStreamingKeepalive(parsed)) return false;

      if (checkpoint == null) {
        if (!protocol.isStreamingSyncCheckpoint(parsed)) {
          throw new Error(`${label}: first protocol line is not checkpoint`);
        }
        checkpoint = parsed.checkpoint;
        protocolLineCount += 1;
        return false;
      }

      if (protocol.isStreamingSyncData(parsed)) {
        onData?.(parsed.data, parsed);
        protocolLineCount += 1;
        return false;
      }

      if (protocol.isStreamingSyncCheckpointComplete(parsed)) {
        complete = parsed.checkpoint_complete;
        protocolLineCount += 1;
        const checkpointCursor = normalizeDecimalCursor(
          checkpoint.last_op_id,
          `${label} checkpoint.last_op_id`
        );
        const completeCursor = normalizeDecimalCursor(
          complete.last_op_id,
          `${label} checkpoint_complete.last_op_id`
        );
        if (compareDecimalCursors(checkpointCursor, completeCursor) !== 0) {
          throw new Error(
            `${label}: checkpoint last_op_id ${checkpoint.last_op_id} != complete ${complete.last_op_id}`
          );
        }
        return true;
      }

      throw new Error(
        `${label}: unexpected protocol line keys=${JSON.stringify(Object.keys(parsed))} line=${JSON.stringify(parsed).slice(0, 400)}`
      );
    };
    request = transport.request(
      url,
      {
        method: 'POST',
        agent: false,
        headers: {
          Accept: 'application/x-ndjson',
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload),
          ...(bearerToken ? { Authorization: `Bearer ${bearerToken}` } : {})
        }
      },
      (response) => {
        const responseOk = (response.statusCode ?? 0) >= 200 && (response.statusCode ?? 0) < 300;
        response.setEncoding('utf8');
        response.on('data', (chunk) => {
          responseBytes += Buffer.byteLength(chunk);
          if (!responseOk) {
            if (errorPreview.length < 8192) {
              errorPreview += chunk.slice(0, 8192 - errorPreview.length);
            }
            return;
          }

          lineBuffer += chunk;
          try {
            let newlineIndex = lineBuffer.indexOf('\n');
            while (newlineIndex >= 0) {
              const line = lineBuffer.slice(0, newlineIndex).replace(/\r$/, '');
              lineBuffer = lineBuffer.slice(newlineIndex + 1);
              if (processLine(line)) {
                finish(response);
                return;
              }
              newlineIndex = lineBuffer.indexOf('\n');
            }
          } catch (error) {
            fail(error, response);
          }
        });
        response.on('end', () => {
          if (settled) return;
          if (!responseOk) {
            finish(response);
            return;
          }
          try {
            if (lineBuffer.trim().length > 0 && processLine(lineBuffer.replace(/\r$/, ''))) {
              finish(response);
              return;
            }
            if (complete == null) {
              throw new Error(`${label}: expected checkpoint/data/complete protocol, got ${protocolLineCount} lines`);
            }
            finish(response);
          } catch (error) {
            fail(error, response);
          }
        });
      }
    );
    request.setTimeout(timeoutMsValue, () => request.destroy(new Error(`timeout requesting ${urlString}`)));
    request.on('error', (error) => fail(error));
    request.write(payload);
    request.end();
  });
}

async function requestJson(urlString, { method, body, token }) {
  const url = new URL(urlString);
  const transport = url.protocol === 'https:' ? https : http;
  const payload = body == null ? null : JSON.stringify(body);

  return await new Promise((resolve, reject) => {
    const request = transport.request(
      url,
      {
        method,
        agent: false,
        headers: {
          Accept: 'application/json',
          ...(payload
            ? {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(payload)
              }
            : {}),
          ...(token ? { Authorization: `Token ${token}` } : {})
        }
      },
      (response) => {
        let raw = '';
        response.setEncoding('utf8');
        response.on('data', (chunk) => {
          raw += chunk;
        });
        response.on('end', () => {
          let parsed = null;
          if (raw.trim().length > 0) {
            try {
              parsed = JSON.parse(raw);
            } catch (error) {
              reject(
                new Error(
                  `invalid JSON from ${method} ${urlString}: ${error.message}; body=${raw.slice(0, 400)}`
                )
              );
              return;
            }
          }
          resolve({
            ok: (response.statusCode ?? 0) >= 200 && (response.statusCode ?? 0) < 300,
            statusCode: response.statusCode ?? 0,
            headers: response.headers,
            body: parsed,
            rawBody: raw
          });
        });
      }
    );
    request.setTimeout(5_000, () => request.destroy(new Error(`timeout requesting ${urlString}`)));
    request.on('error', reject);
    if (payload) request.write(payload);
    request.end();
  });
}

async function fetchCurrentState(endpoint) {
  const response = await fetchJson(`${endpoint}/api/sync-rules/v1/current`, apiToken);
  const body = response.body?.data ?? response.body ?? {};
  const current = body.current ?? {};
  const versionCandidate =
    current.version ?? current.plan_version ?? body.plan_version ?? body.version ?? response.body?.plan_version ?? null;
  const version = normalizeOptionalNumber(versionCandidate);
  const content = typeof current.content === 'string' ? current.content : null;
  const slotName = typeof current.slot_name === 'string' && current.slot_name.length > 0 ? current.slot_name : null;
  return { version, content, slotName, raw: response.body };
}

async function fetchTargetMetrics(endpoint) {
  const response = await fetchJson(`${endpoint}/debug/metrics`, apiToken);
  return {
    ok: response.ok,
    statusCode: response.statusCode,
    body: response.body
  };
}

function expectedMutationState(beforeVersion, responseBody, nextRules) {
  const data = responseBody?.data ?? responseBody ?? {};
  const nextVersion = data.current?.version ?? data.plan_version ?? data.version ?? null;
  const version = Number.isFinite(Number(nextVersion))
    ? Number(nextVersion)
    : Number.isFinite(beforeVersion)
      ? Number(beforeVersion) + 1
      : null;
  const slotName =
    typeof data.current?.slot_name === 'string' && data.current.slot_name.length > 0
      ? data.current.slot_name
      : null;
  return {
    version,
    slotName,
    content: typeof nextRules === 'string' ? nextRules : null
  };
}

function syncRulesStateMatches(state, expectedContent) {
  if (typeof expectedContent !== 'string') {
    return state?.content == null;
  }
  if (typeof state?.content !== 'string') {
    return false;
  }
  return normalizeSyncRulesContent(state.content) === normalizeSyncRulesContent(expectedContent);
}

function normalizeSyncRulesContent(content) {
  return content
    .replace(/\r\n/g, '\n')
    .replace(/\s+/g, ' ')
    .trim();
}

function activeSyncRulesStateMatches(
  state,
  { expectedVersion, expectedContent, expectedSlotName, requireExpectedContent = false }
) {
  const versionMatches =
    Number.isFinite(expectedVersion) &&
    (Number.isFinite(state?.version) ? state.version >= expectedVersion : false);
  const slotMatches =
    typeof expectedSlotName === 'string' &&
    expectedSlotName.length > 0 &&
    state?.slotName === expectedSlotName;
  const contentMatches = syncRulesStateMatches(state, expectedContent);
  return requireExpectedContent
    ? contentMatches
    : versionMatches ||
        slotMatches ||
        ((!Number.isFinite(expectedVersion) || !Number.isFinite(state?.version)) &&
          (!expectedSlotName || slotMatches || contentMatches));
}

async function waitForActiveState(
  endpoint,
  {
    expectedVersion,
    expectedContent,
    expectedSlotName,
    requireExpectedContent = false,
    timeoutMs: timeoutMsValue
  }
) {
  const startedAt = performance.now();
  const deadline = Date.now() + timeoutMsValue;
  let lastState = null;

  while (Date.now() < deadline) {
    const state = await fetchCurrentState(endpoint).catch(() => null);
    lastState = state;
    const stateMatches = activeSyncRulesStateMatches(state, {
      expectedVersion,
      expectedContent,
      expectedSlotName,
      requireExpectedContent
    });
    if (stateMatches) {
      return {
        version: state?.version ?? null,
        content: state?.content ?? null,
        slotName: state?.slotName ?? null,
        elapsedMs: round(performance.now() - startedAt)
      };
    }
    await delay(100);
  }

  throw new Error(`timed out waiting for active sync rules; last=${JSON.stringify(lastState)}`);
}

async function waitForDiagnosticsToReach({ endpoint, targetLsn, expectedVersion, timeoutMs: timeoutMsValue, pollIntervalMs }) {
  const startedAt = performance.now();
  const deadline = Date.now() + timeoutMsValue;
  let lastState = null;

  while (Date.now() < deadline) {
    const diagnostics = await fetchDiagnosticsState({ endpoint, targetLsn, expectedVersion }).catch((error) => ({
      reached: false,
      error: compactErrorMessage(error.message)
    }));
    lastState = diagnostics;
    if (diagnostics.reached) {
      return {
        ...diagnostics,
        elapsedMs: round(performance.now() - startedAt)
      };
    }
    await delay(pollIntervalMs);
  }

  throw new Error(`timed out waiting for diagnostics to reach ${targetLsn}; last=${JSON.stringify(lastState)}`);
}

async function waitForReplicationSlotToReach({ slotName, targetLsn, timeoutMs: timeoutMsValue, pollIntervalMs }) {
  const startedAt = performance.now();
  const deadline = Date.now() + timeoutMsValue;
  let lastState = null;

  while (Date.now() < deadline) {
    const slotState = queryReplicationSlotState(slotName);
    lastState = slotState;
    const comparison = compareLsn(slotState?.confirmedFlushLsn, targetLsn);
    if (targetLsn == null || (comparison != null && comparison >= 0)) {
      return {
        slotName,
        confirmed_flush_lsn: slotState?.confirmedFlushLsn ?? null,
        restart_lsn: slotState?.restartLsn ?? null,
        active: slotState?.active ?? null,
        elapsedMs: round(performance.now() - startedAt)
      };
    }
    await delay(pollIntervalMs);
  }

  throw new Error(
    `timed out waiting for replication slot ${slotName} to reach ${targetLsn}; last=${JSON.stringify(lastState)}`
  );
}

async function waitForInitialSlotPosition({ target, targetLsn, processingStartedAt }) {
  const startedAt = performance.now();
  const deadline = Date.now() + timeoutMs;
  let lastState = null;
  let resolvedSlotName = target.label === 'rust' ? rustSlot : null;
  while (Date.now() < deadline) {
    if (resolvedSlotName == null) resolvedSlotName = await resolveOfficialReplicationSlotNameAsync();
    const slotState = await queryReplicationSlotStateAsync(resolvedSlotName);
    lastState = { slotName: resolvedSlotName, ...slotState };
    const comparison = compareLsn(slotState?.confirmedFlushLsn, targetLsn);
    if (comparison != null && comparison >= 0) {
      return {
        method: 'replication-slot-confirmed-flush-lsn',
        processingMs:
          processingStartedAt == null
            ? round(performance.now() - startedAt)
            : round(performance.now() - processingStartedAt),
        targetLsn,
        slotName: resolvedSlotName,
        confirmed_flush_lsn: slotState.confirmedFlushLsn,
        restart_lsn: slotState.restartLsn,
        active: slotState.active
      };
    }
    await delay(250);
  }
  throw new Error(
    `timed out waiting for ${target.label} replication-slot confirmed_flush_lsn to reach ${targetLsn}; last=${JSON.stringify(lastState)}`
  );
}

async function waitForRustMetricsToReach({ endpoint, targetLsn, timeoutMs: timeoutMsValue, pollIntervalMs }) {
  const startedAt = performance.now();
  const deadline = Date.now() + timeoutMsValue;
  let lastState = null;

  while (Date.now() < deadline) {
    const response = await fetchTargetMetrics(endpoint);
    const lastPersistedEndLsn = response.body?.last_persisted_end_lsn ?? null;
    const comparison = compareLsn(lastPersistedEndLsn, targetLsn);
    lastState = {
      ok: response.ok,
      statusCode: response.statusCode,
      last_persisted_end_lsn: lastPersistedEndLsn,
      metrics: response.body?.metrics ?? null
    };
    if (response.ok && (targetLsn == null || (comparison != null && comparison >= 0))) {
      return {
        method: 'persisted-lsn',
        last_persisted_end_lsn: lastPersistedEndLsn,
        elapsedMs: round(performance.now() - startedAt),
        metrics: response.body?.metrics ?? null
      };
    }
    await delay(pollIntervalMs);
  }

  throw new Error(
    `timed out waiting for rust persisted LSN to reach ${targetLsn}; last=${JSON.stringify(lastState)}`
  );
}

async function waitForRustChurnMetricsToReach({
  endpoint,
  baseline,
  expectedTailOpsDelta,
  timeoutMs: timeoutMsValue,
  pollIntervalMs
}) {
  const startedAt = performance.now();
  const deadline = Date.now() + timeoutMsValue;
  if (!baseline?.ok) {
    throw new Error(
      `rust baseline metrics unavailable before churn; status=${baseline?.statusCode ?? 'n/a'} error=${baseline?.error ?? 'n/a'}`
    );
  }
  const baselineTailOps = metricNumber(baseline?.body, 'tail_ops_written');
  if (baselineTailOps == null) {
    throw new Error(`rust baseline metrics missing tail_ops_written before churn`);
  }
  const expectedTailOps = baselineTailOps + expectedTailOpsDelta;
  let lastState = null;

  while (Date.now() < deadline) {
    const response = await fetchTargetMetrics(endpoint);
    const tailOpsWritten = metricNumber(response.body, 'tail_ops_written');
    lastState = {
      ok: response.ok,
      statusCode: response.statusCode,
      baseline_tail_ops_written: baselineTailOps,
      expected_tail_ops_written: expectedTailOps,
      tail_ops_written: tailOpsWritten,
      last_persisted_end_lsn: response.body?.last_persisted_end_lsn ?? null,
      metrics: response.body?.metrics ?? null
    };
    if (response.ok && tailOpsWritten != null && tailOpsWritten >= expectedTailOps) {
      return {
        method: 'rust-tail-ops',
        baseline_tail_ops_written: baselineTailOps,
        expected_tail_ops_written: expectedTailOps,
        tail_ops_written: tailOpsWritten,
        last_persisted_end_lsn: response.body?.last_persisted_end_lsn ?? null,
        elapsedMs: round(performance.now() - startedAt),
        metrics: response.body?.metrics ?? null
      };
    }
    await delay(pollIntervalMs);
  }

  throw new Error(
    `timed out waiting for rust tail ops to reach ${expectedTailOps}; last=${JSON.stringify(lastState)}`
  );
}

async function observeRustChurnMetricsAfterPublicTiming({
  endpoint,
  baseline,
  expectedTailOpsDelta,
  fetchMetrics = fetchTargetMetrics
}) {
  try {
    if (!baseline?.ok) {
      throw new Error(
        `rust baseline metrics unavailable before churn; status=${baseline?.statusCode ?? 'n/a'} error=${baseline?.error ?? 'n/a'}`
      );
    }
    const baselineTailOps = metricNumber(baseline?.body, 'tail_ops_written');
    if (baselineTailOps == null) {
      throw new Error(`rust baseline metrics missing tail_ops_written before churn`);
    }
    const expectedTailOps = baselineTailOps + expectedTailOpsDelta;
    const response = await fetchMetrics(endpoint);
    const tailOpsWritten = metricNumber(response.body, 'tail_ops_written');
    const observed = {
      method: 'rust-tail-ops-after-public-churn-timing',
      ok: response.ok,
      statusCode: response.statusCode,
      baseline_tail_ops_written: baselineTailOps,
      expected_tail_ops_written: expectedTailOps,
      tail_ops_written: tailOpsWritten,
      last_persisted_end_lsn: response.body?.last_persisted_end_lsn ?? null,
      metrics: response.body?.metrics ?? null
    };
    if (!response.ok || tailOpsWritten == null || tailOpsWritten < expectedTailOps) {
      throw new Error(`rust tail ops not observed after public churn timing; observed=${JSON.stringify(observed)}`);
    }
    return {
      persistedReady: observed,
      error: null
    };
  } catch (error) {
    return {
      persistedReady: null,
      error: compactErrorMessage(error.message)
    };
  }
}

function metricNumber(metricsPayload, key) {
  const value = metricsPayload?.metrics?.[key];
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

async function fetchDiagnosticsState({ endpoint, targetLsn, expectedVersion }) {
  const response = await postJson(`${endpoint}/api/admin/v1/diagnostics`, { sync_rules_content: false }, apiToken);
  const diagnostics = normalizeDiagnosticsPayload(response.body);
  validateDiagnosticsPayload(diagnostics, endpoint);
  const connection = extractDiagnosticsConnection(diagnostics);
  const version = extractLifecycleVersion(diagnostics);
  const comparison = compareLsn(connection?.last_lsn, targetLsn);
  const initialReplicationDone = connection?.initial_replication_done === true;
  const lsnReached = targetLsn == null || (comparison != null && comparison >= 0);
  const versionReached = expectedVersion == null || version == null || version >= expectedVersion;
  return {
    reached: initialMaterializationDiagnosticsReached({
      responseOk: response.ok,
      initialReplicationDone,
      lsnReached,
      versionReached
    }),
    version,
    last_lsn: connection?.last_lsn ?? null,
    last_checkpoint: connection?.last_checkpoint ?? null,
    initial_replication_done: initialReplicationDone,
    responseStatus: response.statusCode,
    body: diagnostics
  };
}

function initialMaterializationDiagnosticsReached({
  responseOk,
  initialReplicationDone,
  lsnReached,
  versionReached
}) {
  return responseOk === true && initialReplicationDone === true && lsnReached === true && versionReached === true;
}

function normalizeDiagnosticsPayload(body) {
  return body?.data ?? body;
}

function validateDiagnosticsPayload(payload, service) {
  const topLevelConnections = Array.isArray(payload?.connections) ? payload.connections : [];
  const activeConnections = Array.isArray(payload?.active_sync_rules?.connections)
    ? payload.active_sync_rules.connections
    : [];
  if (topLevelConnections.length > 1) {
    throw new Error(`${service} diagnostics returned ${topLevelConnections.length} top-level connections; expected one`);
  }
  if (activeConnections.length > 1) {
    throw new Error(`${service} diagnostics returned ${activeConnections.length} active connections; expected one`);
  }
}

function extractDiagnosticsConnection(payload) {
  const activeConnections = Array.isArray(payload?.active_sync_rules?.connections)
    ? payload.active_sync_rules.connections
    : [];
  const deployingConnections = Array.isArray(payload?.deploying_sync_rules?.connections)
    ? payload.deploying_sync_rules.connections
    : [];
  const candidates = [...activeConnections, ...deployingConnections];
  return candidates.find((connection) => typeof connection?.last_lsn === 'string') ?? candidates[0] ?? null;
}

function extractLifecycleVersion(payload) {
  const candidates = [
    payload?.lifecycle?.version,
    payload?.active_sync_rules?.version,
    payload?.active_sync_rules?.plan_version,
    payload?.current?.version,
    payload?.data?.current?.version
  ];
  for (const candidate of candidates) {
    const value = normalizeOptionalNumber(candidate);
    if (value != null) return value;
  }
  return null;
}

function normalizeOptionalNumber(value) {
  if (value == null || value === '') return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function startWriteLoad({ targetLabel, scenarioName, repeat }) {
  let stopped = false;
  let writes = 0;
  const interval = setInterval(() => {
    if (stopped) return;
    writes += 1;
    const rowId = fixture.ids.batchUpdateIds[writes % fixture.ids.batchUpdateIds.length];
    const title = `write-load ${targetLabel} ${scenarioName} ${repeat} ${writes}`;
    try {
      runSql(`
UPDATE ${TABLES.tasks}
SET title = '${escapeLiteral(title)}',
    updated_at = NOW(),
    summary = '${escapeLiteral(`write-load:${writes}`)}'
WHERE id = '${escapeLiteral(rowId)}';
`);
    } catch {
      // ignore transient load errors during teardown
    }
  }, 30);
  interval.unref?.();

  return {
    async stop() {
      stopped = true;
      clearInterval(interval);
      return { writes };
    }
  };
}

async function captureTargetLsn({ underWriteLoad, operation }) {
  const settleMs = underWriteLoad ? 500 : operation === 'reprocess' ? 200 : 100;
  if (settleMs > 0) {
    await delay(settleMs);
  }

  let lastLsn = queryCurrentFlushLsn();
  let stablePolls = 0;
  const deadline = Date.now() + Math.min(2_000, Math.max(400, Math.floor(timeoutMs / 6)));

  while (Date.now() < deadline) {
    await delay(100);
    const currentLsn = queryCurrentFlushLsn();
    if (currentLsn === lastLsn) {
      stablePolls += 1;
      if (stablePolls >= 2) return currentLsn;
    } else {
      lastLsn = currentLsn;
      stablePolls = 0;
    }
  }

  return lastLsn;
}

function queryCurrentFlushLsn() {
  return runSqlQuery('SELECT pg_current_wal_flush_lsn()');
}

function queryCurrentInsertLsn() {
  return runSqlQuery('SELECT pg_current_wal_insert_lsn()');
}

async function settleOutcome(promise) {
  try {
    return { ok: true, value: await promise };
  } catch (error) {
    return { ok: false, error };
  }
}

function buildComparisons(targets, config) {
  const baseline = targets.official;
  if (!baseline) return [];
  const churnCatchupMetricPath =
    config?.churnGateMode === 'slot-lsn' ? 'churn.slotAckCatchupMs' : 'churn.replicationCatchupMs';
  return Object.entries(targets)
    .filter(([label]) => label !== 'official')
    .map(([label, candidate]) => ({
      baselineLabel: 'official',
      candidateLabel: label,
      developerUsability: buildLaneComparisons(
        baseline.developerUsability,
        candidate.developerUsability,
        [
          'redeployDashboard.publishToReadyMs',
          'redeployDashboard.publishToUsableMs',
          'reprocessCurrent.publishToReadyMs',
          'redeployDashboardUnderWriteLoad.publishToReadyMs'
        ]
      ),
      endUserExperience: buildLaneComparisons(
        baseline.endUser.summary,
        candidate.endUser.summary,
        [
          'processing.processingMs',
          'processing.completeMaterializationMs',
          'processing.sourceSlotPositionMs',
          'coldOpen.firstSyncMs',
          'coldOpen.steadyMs',
          'warmReconnect.steadyMs',
          'liveChangeToVisible.visibleMs',
          churnCatchupMetricPath,
          'churn.protocolProbeMs',
          'churn.churnToProtocolVerifiedMs'
        ]
      ),
      recovery: buildLaneComparisons(
        baseline.recovery,
        candidate.recovery,
        [
          'redeployDashboard.redeployToFirstDataMs',
          'redeployDashboard.redeployToUsableAppMs',
          'redeployDashboardUnderWriteLoad.redeployToUsableAppMs'
        ]
      )
    }));
}

function assertPairedProtocolParity(targets) {
  if (!equivalenceGateEnabled) return;
  const official = targets.official;
  if (!official) return;

  for (const [label, candidate] of Object.entries(targets)) {
    if (label === 'official') continue;
    assertTargetProtocolParityAgainstOfficial(official, candidate);
  }
}

function assertTargetProtocolParityAgainstOfficial(official, candidate) {
  const officialRuns = official.endUser?.runs ?? [];
  const candidateRuns = candidate.endUser?.runs ?? [];
  for (const officialRun of officialRuns) {
    const candidateRun = candidateRuns.find((run) => run.repeat === officialRun.repeat);
    if (!candidateRun) {
      throw new Error(`protocol parity: ${candidate.label} missing repeat ${officialRun.repeat}`);
    }
    assertInitialGateParity(officialRun.equivalence, candidateRun.equivalence, candidate.label, officialRun.repeat);
    if (churnGateEnabled) {
      assertChurnGateParity(officialRun.churn, candidateRun.churn, candidate.label, officialRun.repeat);
    }
  }
}

function assertInitialGateParity(officialGate, candidateGate, candidateLabel, repeat) {
  if (!officialGate || !candidateGate) {
    throw new Error(`protocol parity: missing initial equivalence artifacts for ${candidateLabel} repeat ${repeat}`);
  }
  if (officialGate.authorization || candidateGate.authorization) {
    assertEqualProtocolField(
      candidateLabel,
      repeat,
      'auth-perimeter',
      'authorization.status',
      officialGate.authorization?.status,
      candidateGate.authorization?.status
    );
    assertEqualProtocolField(
      candidateLabel,
      repeat,
      'auth-perimeter',
      'authorization.checkpointBucketsReturned',
      officialGate.authorization?.checkpointBucketsReturned,
      candidateGate.authorization?.checkpointBucketsReturned
    );
  }
  const officialBuckets = new Map((officialGate.buckets ?? []).map((bucket) => [bucket.bucket, bucket]));
  const candidateBuckets = new Map((candidateGate.buckets ?? []).map((bucket) => [bucket.bucket, bucket]));
  assertEqualProtocolField(
    candidateLabel,
    repeat,
    'bucket-set',
    'initial.bucketCount',
    officialBuckets.size,
    candidateBuckets.size
  );
  for (const [bucketName, officialBucket] of officialBuckets) {
    const candidateBucket = candidateBuckets.get(bucketName);
    if (!candidateBucket) {
      throw new Error(`protocol parity: ${candidateLabel} missing initial bucket ${bucketName} repeat ${repeat}`);
    }
    assertEqualProtocolField(candidateLabel, repeat, bucketName, 'initial.expectedRows', officialBucket.expectedRows, candidateBucket.expectedRows);
    assertEqualProtocolField(candidateLabel, repeat, bucketName, 'initial.puts', officialBucket.puts, candidateBucket.puts);
    assertEqualProtocolField(candidateLabel, repeat, bucketName, 'initial.removes', officialBucket.removes, candidateBucket.removes);
    assertEqualProtocolField(
      candidateLabel,
      repeat,
      bucketName,
      'initial.checkpointCount',
      officialBucket.checkpointCount,
      candidateBucket.checkpointCount
    );
    assertEqualProtocolField(
      candidateLabel,
      repeat,
      bucketName,
      'initial.checkpointChecksum',
      protocolChecksumI32(officialBucket.checkpointChecksum),
      protocolChecksumI32(candidateBucket.checkpointChecksum)
    );
    assertEqualProtocolField(
      candidateLabel,
      repeat,
      bucketName,
      'initial.clientOperationDigest',
      officialBucket.entryChecksums?.clientOperationDigest,
      candidateBucket.entryChecksums?.clientOperationDigest
    );
    assertEqualProtocolField(
      candidateLabel,
      repeat,
      bucketName,
      'initial.putDigest',
      officialBucket.entryChecksums?.putDigest,
      candidateBucket.entryChecksums?.putDigest
    );
    assertEqualProtocolField(
      candidateLabel,
      repeat,
      bucketName,
      'initial.semanticDigest',
      officialBucket.entryChecksums?.semanticDigest,
      candidateBucket.entryChecksums?.semanticDigest
    );
  }
}

function assertChurnGateParity(officialGate, candidateGate, candidateLabel, repeat) {
  if (!officialGate || !candidateGate) {
    throw new Error(`protocol parity: missing churn artifacts for ${candidateLabel} repeat ${repeat}`);
  }
  const officialBuckets = new Map((officialGate.buckets ?? []).map((bucket) => [bucket.bucket, bucket]));
  const candidateBuckets = new Map((candidateGate.buckets ?? []).map((bucket) => [bucket.bucket, bucket]));
  assertEqualProtocolField(
    candidateLabel,
    repeat,
    'bucket-set',
    'churn.bucketCount',
    officialBuckets.size,
    candidateBuckets.size
  );
  for (const [bucketName, officialBucket] of officialBuckets) {
    const candidateBucket = candidateBuckets.get(bucketName);
    if (!candidateBucket) {
      throw new Error(`protocol parity: ${candidateLabel} missing churn bucket ${bucketName} repeat ${repeat}`);
    }
    for (const field of ['expectedInserts', 'expectedUpdates', 'expectedDeletes', 'puts', 'removes', 'checkpointCount']) {
      assertEqualProtocolField(candidateLabel, repeat, bucketName, `churn.${field}`, officialBucket[field], candidateBucket[field]);
    }
    assertEqualProtocolField(
      candidateLabel,
      repeat,
      bucketName,
      'churn.clientOperationDigest',
      officialBucket.entryChecksums?.clientOperationDigest,
      candidateBucket.entryChecksums?.clientOperationDigest
    );
    assertEqualProtocolField(
      candidateLabel,
      repeat,
      bucketName,
      'churn.clientPutDigest',
      officialBucket.entryChecksums?.clientPutDigest,
      candidateBucket.entryChecksums?.clientPutDigest
    );
    assertEqualProtocolField(
      candidateLabel,
      repeat,
      bucketName,
      'churn.putDigest',
      officialBucket.entryChecksums?.putDigest,
      candidateBucket.entryChecksums?.putDigest
    );
    assertEqualProtocolField(
      candidateLabel,
      repeat,
      bucketName,
      'churn.removeObjectDigest',
      officialBucket.entryChecksums?.removeObjectDigest,
      candidateBucket.entryChecksums?.removeObjectDigest
    );
    assertEqualProtocolField(
      candidateLabel,
      repeat,
      bucketName,
      'churn.nonZeroChecksumCount',
      officialBucket.entryChecksums?.nonZeroCount,
      candidateBucket.entryChecksums?.nonZeroCount
    );
  }
}

function assertEqualProtocolField(candidateLabel, repeat, bucketName, field, expected, actual) {
  if (expected !== actual) {
    throw new Error(
      `protocol parity: ${candidateLabel} repeat ${repeat} bucket ${bucketName} ${field} mismatch official=${expected} candidate=${actual}`
    );
  }
}

function buildLaneComparisons(baselineSummary, candidateSummary, metricPaths) {
  const comparisons = {};
  for (const metricPath of metricPaths) {
    const baselineValue = deepMetricValue(baselineSummary, metricPath);
    const candidateValue = deepMetricValue(candidateSummary, metricPath);
    comparisons[metricPath] = compareMetric({
      baselineValue,
      candidateValue
    });
  }
  return comparisons;
}

function deepMetricValue(summary, metricPath) {
  const parts = metricPath.split('.');
  let current = summary;
  for (const part of parts) {
    current = current?.[part];
  }
  return current ?? null;
}

function compareMetric({ baselineValue, candidateValue }) {
  const baselineP50 = metricQuantile(baselineValue, 'p50');
  const candidateP50 = metricQuantile(candidateValue, 'p50');
  const baselineP95 = metricQuantile(baselineValue, 'p95');
  const candidateP95 = metricQuantile(candidateValue, 'p95');
  if (!Number.isFinite(baselineP50) || !Number.isFinite(candidateP50)) {
    return {
      baselineP50Ms: baselineP50,
      candidateP50Ms: candidateP50,
      baselineP95Ms: baselineP95,
      candidateP95Ms: candidateP95,
      deltaMs: null,
      speedupVsBaseline: null,
      p95Ratio: Number.isFinite(baselineP95) && Number.isFinite(candidateP95) ? round(baselineP95 / candidateP95) : null
    };
  }
  const deltaMs = round(candidateP50 - baselineP50);
  const speedupVsBaseline = round(baselineP50 / candidateP50);
  return {
    baselineP50Ms: round(baselineP50),
    candidateP50Ms: round(candidateP50),
    baselineP95Ms: Number.isFinite(baselineP95) ? round(baselineP95) : null,
    candidateP95Ms: Number.isFinite(candidateP95) ? round(candidateP95) : null,
    deltaMs,
    speedupVsBaseline,
    p95Ratio: Number.isFinite(baselineP95) && Number.isFinite(candidateP95) ? round(baselineP95 / candidateP95) : null
  };
}

function metricQuantile(value, key) {
  if (Number.isFinite(value)) return Number(value);
  if (value && typeof value === 'object' && Number.isFinite(Number(value[key]))) {
    return Number(value[key]);
  }
  return null;
}

function summarizeSamples(samples, metrics) {
  const successfulSamples = samples.filter((sample) => sample?.status !== 'failed');
  const failedSamples = samples.filter((sample) => sample?.status === 'failed');
  // A `partial` sample reached a degraded/timed-out state, so its timings are
  // not a clean measurement and are excluded from the percentiles. No-status
  // samples are clean passes and are kept.
  const timingSamples = successfulSamples.filter((sample) => sample?.status !== 'partial');
  const result = {
    health: {
      total: samples.length,
      passedCount: successfulSamples.length,
      failedCount: failedSamples.length,
      lastError: failedSamples.at(-1)?.error ?? null
    }
  };
  for (const metric of metrics) {
    const values = timingSamples
      .map((sample) => sample?.[metric])
      .filter((value) => value != null)
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value))
      .sort((a, b) => a - b);
    if (values.length === 0) continue;
    result[metric] = {
      count: values.length,
      min: round(values[0]),
      p50: round(percentile(values, 0.5)),
      p95: round(percentile(values, 0.95)),
      mean: round(values.reduce((sum, value) => sum + value, 0) / values.length),
      max: round(values[values.length - 1])
    };
  }
  return result;
}

function summarizeReadinessRuns(runs) {
  return summarizeSamples(
    runs
      .map((run) => ({
        processingMs: run.readiness?.processingMs ?? null,
        completeMaterializationMs: run.readiness?.completeMaterialization?.processingMs ?? null,
        sourceSlotPositionMs: run.readiness?.sourceSlotPosition?.processingMs ?? null
      }))
      .filter((sample) => sample.processingMs != null),
    ['processingMs', 'completeMaterializationMs', 'sourceSlotPositionMs']
  );
}

function summarizeResourceRuns(runs) {
  const capturedRuns = runs
    .filter((run) => run.resources?.status === 'captured' && run.resources?.initial != null);
  const samples = capturedRuns
    .map((run) => {
      const initial = run.resources?.initial;
      const components = Object.values(initial.components ?? {});
      return {
        cpuSeconds: sumAvailable(components.map((component) => component.cpuSeconds)),
        blockReadBytes: sumAvailable(components.map((component) => component.blockReadBytes)),
        blockWriteBytes: sumAvailable(components.map((component) => component.blockWriteBytes)),
        storageAllocatedBytes: sumAvailable(
          Object.values(initial.storage ?? {}).map((entry) => entry.allocatedBytes)
        ),
        walInsertedBytes: initial.wal?.insertedBytes ?? null
      };
    });
  const summary = summarizeSamples(samples, [
    'cpuSeconds',
    'blockReadBytes',
    'blockWriteBytes',
    'storageAllocatedBytes',
    'walInsertedBytes'
  ]);
  const componentLabels = new Set(
    capturedRuns.flatMap((run) => Object.keys(run.resources.initial.components ?? {}))
  );
  summary.components = Object.fromEntries(
    [...componentLabels].map((label) => [
      label,
      summarizeSamples(
        capturedRuns.map((run) => run.resources.initial.components?.[label] ?? {}),
        [
          'cgroupLifetimePeakMemoryBytes',
          'mainProcessLifetimePeakRssBytes',
          'networkRxBytes',
          'networkTxBytes'
        ]
      )
    ])
  );
  const storageLabels = new Set(
    capturedRuns.flatMap((run) => Object.keys(run.resources.initial.storage ?? {}))
  );
  summary.storage = Object.fromEntries(
    [...storageLabels].map((label) => [
      label,
      summarizeSamples(
        capturedRuns.map((run) => run.resources.initial.storage?.[label] ?? {}),
        ['logicalBytes', 'allocatedBytes']
      )
    ])
  );
  return summary;
}

function sumAvailable(values) {
  const available = values
    .filter((value) => value != null)
    .map(Number)
    .filter(Number.isFinite);
  return available.length === 0 ? null : available.reduce((sum, value) => sum + value, 0);
}

function assertPublicationResourceEvidence(evidence) {
  const issues = [];
  if (evidence?.status !== 'captured') issues.push('resource snapshots were not captured');
  const expectedComponents = evidence?.target === 'official' ? ['service', 'mongo'] : ['service'];
  const expectedStorage = evidence?.target === 'official' ? ['mongo-db', 'mongo-config'] : ['mdbx'];
  for (const windowName of ['initial', 'browser', 'equivalence', 'churn', 'total']) {
    const window = evidence?.windows?.[windowName];
    if (window == null) {
      issues.push(`${windowName} resource window was not captured`);
      continue;
    }
    const components = Object.entries(window.components ?? {});
    for (const label of expectedComponents) {
      if (!Object.hasOwn(window.components ?? {}, label)) issues.push(`${windowName}.${label} is missing`);
    }
    for (const [label, component] of components) {
      if (component.status !== 'captured') issues.push(`${windowName}.${label} counters were not captured`);
      if (component.source !== 'linux-cgroup-v2') {
        issues.push(`${windowName}.${label} counters came from ${component.source ?? 'an unknown source'}, not Linux cgroup v2`);
      }
      for (const field of [
        'cpuSeconds',
        'cgroupLifetimePeakMemoryBytes',
        'mainProcessLifetimePeakRssBytes',
        'blockReadBytes',
        'blockWriteBytes',
        'networkRxBytes',
        'networkTxBytes'
      ]) {
        if (!Number.isFinite(component[field])) issues.push(`${windowName}.${label}.${field} is unavailable`);
      }
    }
    const storage = Object.entries(window.storage ?? {});
    for (const label of expectedStorage) {
      if (!Object.hasOwn(window.storage ?? {}, label)) issues.push(`${windowName}.${label} storage is missing`);
    }
    for (const [label, entry] of storage) {
      for (const field of ['logicalBytes', 'allocatedBytes']) {
        if (!Number.isFinite(entry[field])) issues.push(`${windowName}.${label}.${field} is unavailable`);
      }
    }
    if (!Number.isFinite(window.wal?.insertedBytes)) issues.push(`${windowName} inserted WAL-position delta is unavailable`);
  }
  if (issues.length > 0) {
    throw new Error(`public benchmark resource evidence failed:\n- ${issues.join('\n- ')}`);
  }
}

function percentile(values, quantile) {
  if (values.length === 0) return null;
  if (values.length === 1) return values[0];
  const position = (values.length - 1) * quantile;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  if (lower === upper) return values[lower];
  const weight = position - lower;
  return values[lower] * (1 - weight) + values[upper] * weight;
}

function compactErrorMessage(message) {
  return String(message ?? '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 400);
}

function describeRustReplicationFeedback(config) {
  const feedback = config?.rustReplicationFeedback ?? {};
  if (feedback.statusSource || feedback.idleWakeupSource || feedback.currentSourceDefaults) {
    return `status=${formatIntervalWithSource(feedback.statusIntervalMs, feedback.statusSource, feedback.currentSourceDefaults?.statusIntervalMs)}, idle=${formatIntervalWithSource(feedback.idleWakeupIntervalMs, feedback.idleWakeupSource, feedback.currentSourceDefaults?.idleWakeupIntervalMs)}`;
  }
  const overrides = config?.rustReplicationFeedbackOverrides ?? {};
  const status = overrides.statusIntervalMs;
  const idle = overrides.idleWakeupIntervalMs;
  if (status == null && idle == null) {
    return 'runtime defaults';
  }
  return `env override status=${status == null ? 'default' : `${status}ms`}, idle=${idle == null ? 'default' : `${idle}ms`}`;
}

function formatIntervalWithSource(value, source, currentSourceDefaultMs) {
  if (source === 'env-override') {
    return `${value}ms (env override)`;
  }
  const sourceDefault =
    currentSourceDefaultMs == null ? '' : `; checked-in source default ${currentSourceDefaultMs}ms`;
  return `Rust binary default (not set by harness${sourceDefault})`;
}

function renderMarkdown({ results, comparisons }) {
  const methodology = results.methodology ?? {};
  const lines = [];
  lines.push('# Benchmark run summary');
  lines.push('');
  lines.push(`Profile: \`${results.profile}\``);
  lines.push(`Generated: ${results.generatedAt}`);
  lines.push('');
  lines.push('## Method');
  lines.push('');
  lines.push('- same dataset, sync rules, browser fixture, auth token shape, and target user across all targets');
  lines.push(`- routed access mode: ${methodology.fairness?.routedAccess ?? results.config?.accessMode ?? 'n/a'}`);
  lines.push('- parity gates compare checkpoint counts/checksums plus client-visible operation digests, not only timings');
  lines.push(
    `- target order ${methodology.fairness?.interleavedTargets ? 'is interleaved across repeats/scenarios' : 'is fixed (interleaving disabled)'}`
  );
  lines.push(`- cold-open readiness: ${methodology.fairness?.endUserColdOpenReadiness ?? 'n/a'}`);
  lines.push(`- rust replication feedback intervals: ${describeRustReplicationFeedback(results.config)}`);
  lines.push(
    '- lifecycle scenarios are diagnostic only when explicitly enabled; Rust rejects reprocessing and layout-changing deploys until atomic generation activation exists'
  );
  lines.push('');
  for (const [label, target] of Object.entries(results.targets)) {
    const churnRows = [];
    if (results.config?.churnGateMode === 'slot-lsn') {
      churnRows.push(['Slot-LSN ack catch-up', 'slotAckCatchupMs']);
    } else {
      churnRows.push(['Subsequent churn catch-up', 'replicationCatchupMs']);
    }
    churnRows.push(['Churn to protocol verified', 'churnToProtocolVerifiedMs']);

    lines.push(`## ${label}`);
    lines.push('');
    lines.push('### Developer usability');
    lines.push('');
    lines.push(renderScenarioTable(target.developerUsability, {
      redeployDashboard: [
        ['Publish to ready', 'publishToReadyMs'],
        ['Publish to usable', 'publishToUsableMs']
      ],
      reprocessCurrent: [['Reprocess to ready', 'publishToReadyMs']],
      redeployDashboardUnderWriteLoad: [['Publish to ready under write load', 'publishToReadyMs']]
    }));
    const lifecycleFailures = collectScenarioFailures(target.lifecycleScenarios);
    if (lifecycleFailures.length > 0) {
      lines.push('');
      lines.push('Developer-lifecycle issues:');
      for (const failure of lifecycleFailures) {
        lines.push(`- ${failure}`);
      }
    }
    lines.push('');
    lines.push('### End-user experience');
    lines.push('');
    lines.push(renderScenarioTable(target.endUser.summary, {
      processing: [[
        results.config?.initialReadinessMode === 'sync-protocol'
          ? 'Target startup to validated checkpoint completion for one routed subscription'
          : 'Target startup to target-specific readiness',
        'processingMs'
      ],
      ['Target startup to target-specific complete materialization', 'completeMaterializationMs'],
      ['Target startup to replication-slot confirmed-flush position', 'sourceSlotPositionMs']],
      coldOpen: [
        ['Open to first data', 'firstSyncMs'],
        ['Open to usable screen', 'steadyMs']
      ],
      warmReconnect: [['Reopen to current', 'steadyMs']],
      liveChangeToVisible: [['Live change to visible', 'visibleMs']],
      churn: churnRows
    }));
    const endUserIssues = collectEndUserMeasurementIssues(target.endUser?.runs);
    if (endUserIssues.length > 0) {
      lines.push('');
      lines.push('End-user measurement issues:');
      for (const issue of endUserIssues) {
        lines.push(`- ${issue}`);
      }
    }
    lines.push('');
    lines.push('### Resources through all initial gates');
    lines.push('');
    lines.push(renderResourceSummaryTable(target.endUser.summary?.resources));
    lines.push('');
    lines.push('### Deploy-to-user recovery');
    lines.push('');
    lines.push(renderScenarioTable(target.recovery, {
      redeployDashboard: [
        ['Redeploy to first data', 'redeployToFirstDataMs'],
        ['Redeploy to usable app', 'redeployToUsableAppMs']
      ],
      redeployDashboardUnderWriteLoad: [['Redeploy under load to usable app', 'redeployToUsableAppMs']],
      reprocessCurrent: [['Reprocess to usable app', 'redeployToUsableAppMs']]
    }));
    lines.push('');
  }

  if (comparisons.length > 0) {
    lines.push('## Official vs Rust comparisons');
    lines.push('');
    for (const comparison of comparisons) {
      lines.push(`### official vs ${comparison.candidateLabel}`);
      lines.push('');
      lines.push('| Metric | Official p50 | Candidate p50 | Delta | Speedup |');
      lines.push('| --- | ---: | ---: | ---: | ---: |');
      for (const [metricPath, metric] of Object.entries({
        ...comparison.developerUsability,
        ...comparison.endUserExperience,
        ...comparison.recovery
      })) {
        lines.push(
          `| ${metricPath} | ${formatMs(metric.baselineP50Ms)} | ${formatMs(metric.candidateP50Ms)} | ${formatMs(metric.deltaMs)} | ${formatFold(metric.speedupVsBaseline)} |`
        );
      }
      lines.push('');
    }
  }

  lines.push('Raw artifacts:');
  lines.push(`- results: \`${resultsPath}\``);
  lines.push(`- comparison: \`${comparePath}\``);
  lines.push(`- summary: \`${summaryPath}\``);
  lines.push('');

  return lines.join('\n');
}

function renderScenarioTable(summary, scenarioMap) {
  const lines = [];
  lines.push('| Metric | p50 | p95 | Sample count |');
  lines.push('| --- | ---: | ---: | ---: |');
  for (const [scenarioName, metrics] of Object.entries(scenarioMap)) {
    const scenario = summary?.[scenarioName] ?? {};
    for (const [label, metricKey] of metrics) {
      const metric = scenario?.[metricKey];
      lines.push(`| ${label} | ${formatMs(metric?.p50)} | ${formatMs(metric?.p95)} | ${metric?.count ?? 0} |`);
    }
  }
  return lines.join('\n');
}

function renderResourceSummaryTable(summary) {
  const rows = [
    ['CPU time', 'cpuSeconds', formatSeconds],
    ['Block reads', 'blockReadBytes', formatBytes],
    ['Block writes', 'blockWriteBytes', formatBytes],
    ['Allocated storage growth', 'storageAllocatedBytes', formatBytes],
    ['Cluster-wide inserted WAL-position delta', 'walInsertedBytes', formatBytes]
  ];
  const lines = ['| Metric | p50 | p95 | Sample count |', '| --- | ---: | ---: | ---: |'];
  for (const [label, key, format] of rows) {
    const metric = summary?.[key];
    lines.push(`| ${label} | ${format(metric?.p50)} | ${format(metric?.p95)} | ${metric?.count ?? 0} |`);
  }
  for (const [label, component] of Object.entries(summary?.components ?? {})) {
    for (const [metricLabel, key] of [
      ['cgroup lifetime peak memory', 'cgroupLifetimePeakMemoryBytes'],
      ['container init-process lifetime peak RSS', 'mainProcessLifetimePeakRssBytes'],
      ['network received', 'networkRxBytes'],
      ['network transmitted', 'networkTxBytes']
    ]) {
      const metric = component?.[key];
      lines.push(`| ${label}: ${metricLabel} | ${formatBytes(metric?.p50)} | ${formatBytes(metric?.p95)} | ${metric?.count ?? 0} |`);
    }
  }
  for (const [label, storage] of Object.entries(summary?.storage ?? {})) {
    for (const [metricLabel, key] of [
      ['logical growth', 'logicalBytes'],
      ['allocated growth', 'allocatedBytes']
    ]) {
      const metric = storage?.[key];
      lines.push(`| ${label}: ${metricLabel} | ${formatBytes(metric?.p50)} | ${formatBytes(metric?.p95)} | ${metric?.count ?? 0} |`);
    }
  }
  return lines.join('\n');
}

function collectScenarioFailures(lifecycleScenarios) {
  return Object.values(lifecycleScenarios ?? {})
    .flatMap((samples) => samples ?? [])
    .flatMap((sample) => {
      if (sample?.status === 'failed') {
        return [`${sample.scenarioName} r${sample.repeat}: ${sample.error}`];
      }
      if (sample?.status === 'partial' && Array.isArray(sample.issues) && sample.issues.length > 0) {
        return sample.issues.map((issue) => `${sample.scenarioName} r${sample.repeat}: ${issue}`);
      }
      return [];
    });
}

function collectEndUserMeasurementIssues(runs) {
  return (runs ?? [])
    .flatMap((run) => {
      const error = run?.churn?.rustPersistedError;
      if (!error) return [];
      return [`churn r${run.repeat}: Rust persisted catch-up observation unavailable: ${error}`];
    });
}

function compactBrowserLabel(value) {
  return String(value)
    .replace(/[^A-Za-z0-9_-]+/g, '-')
    .replace(/-{2,}/g, '-')
    .slice(0, 32);
}

function rustProfileArgs(profile) {
  if (profile === 'dev') return [];
  if (profile === 'release') return ['--release'];
  if (!/^[A-Za-z0-9_-]+$/.test(profile)) return null;
  return ['--profile', profile];
}

function runSql(sql) {
  const result = runSqlCommand(['-v', 'ON_ERROR_STOP=1', '-f', '-'], `SET search_path TO public;\n${sql}`);
  if (result.status !== 0) {
    throw new Error(`psql failed: ${result.stderr || result.stdout}`);
  }
}

async function runSqlStream(writeInput) {
  const child = spawn(
    'docker',
    ['exec', '-i', postgresContainerName, 'psql', '-U', 'postgres', '-d', 'powersync_benchmark_test', '-v', 'ON_ERROR_STOP=1', '-f', '-'],
    {
      stdio: ['pipe', 'pipe', 'pipe']
    }
  );
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (chunk) => {
    stdout += chunk.toString();
  });
  child.stderr.on('data', (chunk) => {
    stderr += chunk.toString();
  });

  const exitPromise = new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', (code) => resolve(code));
  });

  try {
    await writeSql(child.stdin, 'SET search_path TO public;\n');
    await writeInput(child.stdin);
    child.stdin.end();
  } catch (error) {
    child.stdin.destroy(error);
    throw error;
  }

  const code = await exitPromise;
  if (code !== 0) {
    throw new Error(`psql stream failed: ${stderr || stdout}`);
  }
}

async function writeSql(stdin, sql) {
  if (stdin.write(sql)) return;
  await new Promise((resolve, reject) => {
    const cleanup = () => {
      stdin.off('drain', onDrain);
      stdin.off('error', onError);
    };
    const onDrain = () => {
      cleanup();
      resolve();
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };
    stdin.once('drain', onDrain);
    stdin.once('error', onError);
  });
}

function runSqlQuery(sql) {
  const result = runSqlCommand(['-Atc', sql]);
  if (result.status !== 0) {
    throw new Error(`psql query failed: ${result.stderr || result.stdout}`);
  }
  return String(result.stdout ?? '').trim();
}

function resolveOfficialReplicationSlotName() {
  // The official service's slot carries a deterministic per-run prefix, so a
  // single LIKE match resolves it without depending on the diagnostics
  // snapshot. There is at most one such slot at a time (resetTargetState drops
  // inactive ones); ORDER BY keeps the pick deterministic if that ever changes.
  const slotName = runSqlQuery(
    `SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE '${escapeLiteral(officialSlotPrefix)}%' ORDER BY slot_name LIMIT 1`
  );
  return slotName || null;
}

async function resolveOfficialReplicationSlotNameAsync() {
  const slotName = await runSqlQueryAsync(
    `SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE '${escapeLiteral(officialSlotPrefix)}%' ORDER BY slot_name LIMIT 1`
  );
  return slotName || null;
}

function queryReplicationSlotState(slotName) {
  if (typeof slotName !== 'string' || slotName.length === 0) return null;
  const result = runSqlCommand(
    ['-AtF', '\t', '-c', `SELECT COALESCE(confirmed_flush_lsn::text, ''), COALESCE(restart_lsn::text, ''), CASE WHEN active THEN '1' ELSE '0' END FROM pg_replication_slots WHERE slot_name = '${escapeLiteral(slotName)}' LIMIT 1`]
  );
  if (result.status !== 0) {
    throw new Error(`failed to query replication slot ${slotName}: ${result.stderr || result.stdout}`);
  }
  const row = String(result.stdout ?? '').trim();
  if (!row) return null;
  const [confirmedFlushLsnRaw, restartLsnRaw, activeRaw] = row.split('\t');
  return {
    confirmedFlushLsn: confirmedFlushLsnRaw || null,
    restartLsn: restartLsnRaw || null,
    active: activeRaw === '1'
  };
}

async function queryReplicationSlotStateAsync(slotName) {
  if (typeof slotName !== 'string' || slotName.length === 0) return null;
  const result = await runSqlCommandAsync([
    '-AtF',
    '\t',
    '-c',
    `SELECT COALESCE(confirmed_flush_lsn::text, ''), COALESCE(restart_lsn::text, ''), CASE WHEN active THEN '1' ELSE '0' END FROM pg_replication_slots WHERE slot_name = '${escapeLiteral(slotName)}' LIMIT 1`
  ]);
  const row = result.stdout.trim();
  if (!row) return null;
  const [confirmedFlushLsnRaw, restartLsnRaw, activeRaw] = row.split('\t');
  return {
    confirmedFlushLsn: confirmedFlushLsnRaw || null,
    restartLsn: restartLsnRaw || null,
    active: activeRaw === '1'
  };
}

async function runSqlQueryAsync(sql) {
  const result = await runSqlCommandAsync(['-Atc', sql]);
  return result.stdout.trim();
}

async function runSqlCommandAsync(psqlArgs) {
  const child = spawn(
    'docker',
    ['exec', '-i', postgresContainerName, 'psql', '-U', 'postgres', '-d', 'powersync_benchmark_test', ...psqlArgs],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (chunk) => {
    stdout += chunk.toString();
  });
  child.stderr.on('data', (chunk) => {
    stderr += chunk.toString();
  });
  const code = await new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', resolve);
  });
  if (code !== 0) {
    throw new Error(`psql query failed: ${stderr || stdout}`);
  }
  return { stdout, stderr };
}

function runSqlCommand(psqlArgs, input = undefined) {
  return spawnSync(
    'docker',
    ['exec', '-i', postgresContainerName, 'psql', '-U', 'postgres', '-d', 'powersync_benchmark_test', ...psqlArgs],
    {
      encoding: 'utf8',
      input
    }
  );
}

async function waitForBenchmarkPostgres(attempts = 30, delayMs = 1_000) {
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const result = runSqlCommand(['-Atc', 'SELECT 1']);
    if (result.status === 0 && String(result.stdout ?? '').trim() === '1') return;
    await delay(delayMs);
  }
  throw new Error('benchmark postgres did not become ready in time');
}

function compareLsn(left, right) {
  const leftValue = parseLsn(left);
  const rightValue = parseLsn(right);
  if (leftValue == null || rightValue == null) return null;
  if (leftValue < rightValue) return -1;
  if (leftValue > rightValue) return 1;
  return 0;
}

function parseLsn(value) {
  if (typeof value !== 'string' || !/^[0-9A-F]+\/[0-9A-F]+$/i.test(value)) return null;
  const [upper, lower] = value.split('/');
  return (BigInt(`0x${upper}`) << 32n) + BigInt(`0x${lower}`);
}

function signJwt(secret, payload) {
  const header = { alg: 'HS256', typ: 'JWT' };
  const encodedHeader = base64UrlJson(header);
  const encodedPayload = base64UrlJson(payload);
  const input = `${encodedHeader}.${encodedPayload}`;
  const signature = crypto.createHmac('sha256', secret).update(input).digest('base64url');
  return `${input}.${signature}`;
}

function createBenchmarkJwt({
  subject = fixture.targetUserId,
  nowSeconds = Math.floor(Date.now() / 1_000),
  ttlSeconds = jwtTtlSeconds
} = {}) {
  if (!Number.isSafeInteger(nowSeconds) || nowSeconds < 0) {
    throw new Error(`benchmark JWT nowSeconds must be a non-negative safe integer, got ${nowSeconds}`);
  }
  if (!Number.isSafeInteger(ttlSeconds) || ttlSeconds < 1) {
    throw new Error(`benchmark JWT ttlSeconds must be a positive safe integer, got ${ttlSeconds}`);
  }
  return signJwt(jwtSecret, {
    sub: subject,
    aud: audience,
    iss: issuer,
    org_id: fixture.targetOrgId,
    iat: nowSeconds,
    exp: nowSeconds + ttlSeconds
  });
}

function base64UrlJson(value) {
  return Buffer.from(JSON.stringify(value)).toString('base64url');
}

function base64Url(value) {
  return Buffer.from(value, 'utf8').toString('base64url');
}

function indent(value, spaces) {
  const prefix = ' '.repeat(spaces);
  return value
    .trimEnd()
    .split('\n')
    .map((line) => `${prefix}${line}`)
    .join('\n');
}

function escapeLiteral(value) {
  return String(value).replaceAll("'", "''");
}

function formatMs(value) {
  return value == null ? 'n/a' : `${value.toFixed(1)} ms`;
}

function formatSeconds(value) {
  return value == null ? 'n/a' : `${value.toFixed(3)} s`;
}

function formatBytes(value) {
  if (value == null) return 'n/a';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let scaled = Number(value);
  let unit = 0;
  while (Math.abs(scaled) >= 1024 && unit < units.length - 1) {
    scaled /= 1024;
    unit += 1;
  }
  return `${scaled.toFixed(unit === 0 ? 0 : 2)} ${units[unit]}`;
}

function formatFold(value) {
  return value == null ? 'n/a' : `${value.toFixed(2)}x`;
}

function round(value) {
  return Number.isFinite(value) ? Math.round(value * 1_000) / 1_000 : null;
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    stdio: 'inherit',
    ...options,
    env: options.env ?? process.env
  });
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed with exit code ${result.status ?? 'unknown'}`);
  }
}

function runCapture(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8',
    ...options,
    env: options.env ?? process.env
  });
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed with exit code ${result.status ?? 'unknown'}`);
  }
  return result.stdout ?? '';
}

function log(message) {
  console.error(`[user-value-benchmark] ${message}`);
}

async function freePort() {
  return await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (!address || typeof address === 'string') {
        reject(new Error('failed to allocate port'));
        return;
      }
      server.close(() => resolve(address.port));
    });
  });
}
