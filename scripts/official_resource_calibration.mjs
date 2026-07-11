#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { randomBytes } from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const defaultRoot = path.join(repoRoot, 'tmp', 'official-resource-calibration');

export const DEFAULT_ALLOCATIONS = Object.freeze([
  { id: 'service-1_mongo-3', serviceCpus: 1, mongoCpus: 3 },
  { id: 'service-1_5_mongo-2_5', serviceCpus: 1.5, mongoCpus: 2.5 },
  { id: 'service-2_mongo-2', serviceCpus: 2, mongoCpus: 2 },
  { id: 'service-2_5_mongo-1_5', serviceCpus: 2.5, mongoCpus: 1.5 }
]);

const pinnedImages = Object.freeze({
  POWERSYNC_OFFICIAL_IMAGE:
    'journeyapps/powersync-service@sha256:b6b22fa7d0d862f04bdff62846e656756d17bcf3dd6eca399a0633671051438b',
  POWERSYNC_USER_VALUE_MONGO_IMAGE:
    'mongo@sha256:d5b3ca8c3f3cdce78d44870dc0871b76d5235e9b2ad4ea6bea5d1fbff8027703',
  POWERSYNC_USER_VALUE_POSTGRES_IMAGE:
    'postgres@sha256:be01cf82fc7dbba824acf0a82e150b4b360f3ff93c6631d7844af431e841a95c'
});

export function calibrationSchedule(repeats, allocations = DEFAULT_ALLOCATIONS) {
  if (!Number.isInteger(repeats) || repeats < 1) throw new Error('calibration repeats must be a positive integer');
  return Array.from({ length: repeats }, (_, repeatIndex) => {
    const ordered = repeatIndex % 2 === 0 ? allocations : [...allocations].reverse();
    return ordered.map((allocation) => ({ repeat: repeatIndex + 1, allocation }));
  }).flat();
}

export function calibrationEnvironment({ allocation, artifactRoot, invocationId, base = process.env }) {
  if (!invocationId) throw new Error('calibration invocation id is required');
  return {
    ...base,
    ...pinnedImages,
    POWERSYNC_BENCHMARK_COMPOSE_PROJECT: calibrationComposeProject(invocationId, allocation.id),
    POWERSYNC_BENCHMARK_SKIP_TOOLING_INSTALL: '1',
    POWERSYNC_USER_VALUE_ARTIFACT_ROOT: artifactRoot,
    POWERSYNC_USER_VALUE_RUNTIME: 'symmetric-docker',
    POWERSYNC_USER_VALUE_TARGETS: 'official',
    POWERSYNC_USER_VALUE_TARGET_CPUS: '4',
    POWERSYNC_USER_VALUE_TARGET_MEMORY: '8g',
    POWERSYNC_USER_VALUE_SERVICE_CPUS: `${allocation.serviceCpus}`,
    POWERSYNC_USER_VALUE_SERVICE_MEMORY: '2g',
    POWERSYNC_USER_VALUE_MONGO_CPUS: `${allocation.mongoCpus}`,
    POWERSYNC_USER_VALUE_MONGO_MEMORY: '6g',
    POWERSYNC_USER_VALUE_MONGO_CACHE_GB: '2',
    POWERSYNC_USER_VALUE_OFFICIAL_NODE_OPTIONS: '--max-old-space-size-percentage=80',
    POWERSYNC_USER_VALUE_PROFILE: '250k',
    POWERSYNC_USER_VALUE_PROCESSING_ONLY: '1',
    POWERSYNC_USER_VALUE_ACCESS_MODE: 'auth_perimeter',
    POWERSYNC_USER_VALUE_EQUIVALENCE_GATE: '1',
    POWERSYNC_USER_VALUE_CHURN_GATE: '1',
    POWERSYNC_USER_VALUE_CHURN_GATE_MODE: 'slot-lsn',
    POWERSYNC_USER_VALUE_INITIAL_READINESS: 'sync-protocol',
    POWERSYNC_USER_VALUE_PROJECT_BUCKET_SAMPLES: '50',
    POWERSYNC_USER_VALUE_CHURN_ROWS_PER_BUCKET: '10',
    POWERSYNC_USER_VALUE_LIFECYCLE_REPEATS: '0',
    POWERSYNC_USER_VALUE_BROWSER_ITERATIONS: '1',
    POWERSYNC_USER_VALUE_END_USER_REPEATS: '1',
    POWERSYNC_USER_VALUE_BUCKET_PROBE_BATCH_SIZE: '25',
    POWERSYNC_USER_VALUE_TIMEOUT_MS: '1800000',
    POWERSYNC_USER_VALUE_WARMUP_PAIRS: '0',
    POWERSYNC_USER_VALUE_RETAIN_RAW_RECORDS: '0'
  };
}

export function calibrationComposeProject(invocationId, allocationId) {
  const normalize = (value) => String(value).toLowerCase().replaceAll(/[^a-z0-9_-]+/g, '-').replaceAll(/^-+|-+$/g, '');
  const invocation = normalize(invocationId).slice(0, 16);
  const allocation = normalize(allocationId).slice(0, 28);
  if (!invocation || !allocation) throw new Error('calibration compose identity is empty after normalization');
  return `powersync_mdbx_cal_${invocation}_${allocation}`;
}

export function compactCalibrationResult(results, allocation, repeat, artifactDir) {
  const run = results?.targets?.official?.endUser?.runs?.[0];
  if (run == null) throw new Error(`official result is missing for ${allocation.id} repeat ${repeat}`);
  const boundaries = {
    protocolReadyMs: run.readiness?.processingMs ?? null,
    completeMaterializationMs: run.readiness?.completeMaterialization?.processingMs ?? null,
    sourceSlotPositionMs: run.readiness?.sourceSlotPosition?.processingMs ?? null
  };
  for (const [name, value] of Object.entries(boundaries)) {
    if (!Number.isFinite(value)) {
      throw new Error(`${allocation.id} repeat ${repeat} is missing ${name}`);
    }
  }
  const initial = run.resources?.initial;
  assertCompleteInitialResources(initial, allocation.id, repeat);
  return {
    allocation: allocation.id,
    repeat,
    serviceCpus: allocation.serviceCpus,
    mongoCpus: allocation.mongoCpus,
    serviceMemory: '2g',
    mongoMemory: '6g',
    mongoCacheGb: 2,
    artifactDir,
    ...boundaries,
    walInsertedBytes: initial.wal?.insertedBytes ?? null,
    components: Object.fromEntries(
      Object.entries(initial.components).map(([label, component]) => [
        label,
        {
          source: component.source,
          access: component.access,
          cpuSeconds: component.cpuSeconds,
          lifetimePeakMemoryBytes: component.cgroupLifetimePeakMemoryBytes,
          lifetimePeakRssBytes: component.mainProcessLifetimePeakRssBytes,
          blockReadBytes: component.blockReadBytes,
          blockWriteBytes: component.blockWriteBytes,
          networkRxBytes: component.networkRxBytes,
          networkTxBytes: component.networkTxBytes
        }
      ])
    ),
    storage: initial.storage
  };
}

export function summarizeCalibrationRuns(runs) {
  const byAllocation = Map.groupBy(runs, (run) => run.allocation);
  return [...byAllocation.entries()].map(([allocation, samples]) => ({
    allocation,
    samples: samples.length,
    serviceCpus: samples[0].serviceCpus,
    mongoCpus: samples[0].mongoCpus,
    protocolReadyMedianMs: median(samples.map((sample) => sample.protocolReadyMs)),
    completeMaterializationMedianMs: median(samples.map((sample) => sample.completeMaterializationMs)),
    serviceCpuMedianSeconds: median(samples.map((sample) => sample.components.service.cpuSeconds)),
    mongoCpuMedianSeconds: median(samples.map((sample) => sample.components.mongo.cpuSeconds)),
    servicePeakMemoryMaxBytes: Math.max(...samples.map((sample) => sample.components.service.lifetimePeakMemoryBytes)),
    mongoPeakMemoryMaxBytes: Math.max(...samples.map((sample) => sample.components.mongo.lifetimePeakMemoryBytes)),
    mongoStorageMedianBytes: median(samples.map((sample) => sample.storage['mongo-db'].allocatedBytes)),
    walInsertedMedianBytes: median(samples.map((sample) => sample.walInsertedBytes))
  })).sort((left, right) => left.completeMaterializationMedianMs - right.completeMaterializationMedianMs);
}

async function main() {
  const repeats = Number.parseInt(process.env.POWERSYNC_RESOURCE_CALIBRATION_REPEATS ?? '2', 10);
  const allocations = process.env.POWERSYNC_RESOURCE_CALIBRATION_REVERSE === '1'
    ? [...DEFAULT_ALLOCATIONS].reverse()
    : DEFAULT_ALLOCATIONS;
  const suffix = new Date().toISOString().replaceAll(/[:.]/g, '-');
  const root = path.resolve(process.env.POWERSYNC_RESOURCE_CALIBRATION_ROOT ?? path.join(defaultRoot, suffix));
  const invocationId = randomBytes(6).toString('hex');
  const gitCommit = capture('git', ['rev-parse', 'HEAD']);
  const gitDirty = capture('git', ['status', '--porcelain']) !== '';
  fs.mkdirSync(path.dirname(root), { recursive: true });
  fs.mkdirSync(root, { recursive: false });

  const manifest = {
    schemaVersion: 1,
    startedAt: new Date().toISOString(),
    invocationId,
    gitCommit,
    gitDirty,
    nodeVersion: process.version,
    totalBudget: { cpus: 4, memory: '8g' },
    serviceMemory: '2g',
    mongoMemory: '6g',
    mongoCacheGb: 2,
    status: 'running',
    runs: []
  };
  writeJson(path.join(root, 'calibration.json'), manifest);

  try {
    installSignalForwarding();
    await run('npm', ['--prefix', 'e2e/official-sdk', 'ci']);
    for (const item of calibrationSchedule(repeats, allocations)) {
      throwIfTerminating();
      const runRoot = path.join(root, `r${item.repeat}-${item.allocation.id}`);
      fs.mkdirSync(runRoot);
      await run(process.execPath, ['scripts/user_value_benchmark.mjs'], {
        env: calibrationEnvironment({
          allocation: item.allocation,
          artifactRoot: runRoot,
          invocationId
        })
      });
      const runDir = singleDirectory(runRoot);
      const results = JSON.parse(fs.readFileSync(path.join(runDir, 'results.json'), 'utf8'));
      manifest.runs.push(
        compactCalibrationResult(
          results,
          item.allocation,
          item.repeat,
          path.relative(root, runDir)
        )
      );
      writeJson(path.join(root, 'calibration.json'), manifest);
    }
    manifest.status = 'passed';
    manifest.finishedAt = new Date().toISOString();
    manifest.summary = summarizeCalibrationRuns(manifest.runs);
    writeJson(path.join(root, 'calibration.json'), manifest);
    process.stdout.write(`${path.join(root, 'calibration.json')}\n`);
  } catch (error) {
    manifest.status = terminationSignal == null ? 'failed' : 'interrupted';
    manifest.finishedAt = new Date().toISOString();
    manifest.error = error instanceof Error ? error.message : String(error);
    writeJson(path.join(root, 'calibration.json'), manifest);
    throw error;
  } finally {
    removeSignalForwarding();
  }
}

function assertCompleteInitialResources(initial, allocation, repeat) {
  const issues = [];
  for (const label of ['service', 'mongo']) {
    const component = initial?.components?.[label];
    if (component?.source !== 'linux-cgroup-v2') issues.push(`${label}.source=${component?.source ?? 'missing'}`);
    for (const field of [
      'cpuSeconds',
      'cgroupLifetimePeakMemoryBytes',
      'mainProcessLifetimePeakRssBytes',
      'blockReadBytes',
      'blockWriteBytes',
      'networkRxBytes',
      'networkTxBytes'
    ]) {
      if (!Number.isFinite(component?.[field])) issues.push(`${label}.${field}`);
    }
  }
  for (const label of ['mongo-db', 'mongo-config']) {
    for (const field of ['logicalBytes', 'allocatedBytes']) {
      if (!Number.isFinite(initial?.storage?.[label]?.[field])) issues.push(`${label}.${field}`);
    }
  }
  if (!Number.isFinite(initial?.wal?.insertedBytes)) issues.push('wal.insertedBytes');
  if (issues.length > 0) {
    throw new Error(`${allocation} repeat ${repeat} has incomplete initial resource evidence: ${issues.join(', ')}`);
  }
}

function median(values) {
  const sorted = values.map(Number).filter(Number.isFinite).sort((a, b) => a - b);
  if (sorted.length === 0) return null;
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
}

function singleDirectory(parent) {
  const directories = fs.readdirSync(parent, { withFileTypes: true }).filter((entry) => entry.isDirectory());
  if (directories.length !== 1) throw new Error(`expected one run directory below ${parent}, found ${directories.length}`);
  return path.join(parent, directories[0].name);
}

function writeJson(filePath, value) {
  const temporaryPath = `${filePath}.tmp-${process.pid}`;
  try {
    fs.writeFileSync(temporaryPath, `${JSON.stringify(value, null, 2)}\n`);
    fs.renameSync(temporaryPath, filePath);
  } finally {
    fs.rmSync(temporaryPath, { force: true });
  }
}

let activeChild = null;
let terminationSignal = null;
const signalHandlers = new Map();

function installSignalForwarding() {
  for (const signal of ['SIGINT', 'SIGTERM']) {
    const handler = () => {
      terminationSignal ??= signal;
      if (activeChild != null && activeChild.exitCode == null && activeChild.signalCode == null) {
        activeChild.kill(signal);
      }
    };
    signalHandlers.set(signal, handler);
    process.on(signal, handler);
  }
}

function removeSignalForwarding() {
  for (const [signal, handler] of signalHandlers) process.off(signal, handler);
  signalHandlers.clear();
}

function throwIfTerminating() {
  if (terminationSignal != null) throw new Error(`calibration interrupted by ${terminationSignal}`);
}

function run(command, args, { env = process.env } = {}) {
  throwIfTerminating();
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd: repoRoot, stdio: 'inherit', env });
    activeChild = child;
    let settled = false;
    const finish = (error) => {
      if (settled) return;
      settled = true;
      if (activeChild === child) activeChild = null;
      if (error == null) resolve();
      else reject(error);
    };
    child.once('error', finish);
    child.once('close', (code, signal) => {
      if (signal != null) {
        finish(new Error(`${command} ${args.join(' ')} terminated by ${signal}`));
      } else if (code !== 0) {
        finish(new Error(`${command} ${args.join(' ')} failed with exit code ${code}`));
      } else {
        finish(null);
      }
    });
  });
}

function capture(command, args) {
  const result = spawnSync(command, args, { cwd: repoRoot, encoding: 'utf8', env: process.env });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} failed with exit code ${result.status}`);
  return String(result.stdout ?? '').trim();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main();
