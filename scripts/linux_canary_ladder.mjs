#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const defaultRoot = path.join(repoRoot, 'tmp', 'linux-canary-ladder');

export const LADDER_PROFILES = Object.freeze([
  { profile: '250k', projectSamples: 200, timeoutMs: 1_800_000, mdbxMaxBytes: 4 * 1024 ** 3, minFreeGiB: 25 },
  { profile: '1m', projectSamples: 100, timeoutMs: 3_600_000, mdbxMaxBytes: 8 * 1024 ** 3, minFreeGiB: 50 },
  { profile: '2m', projectSamples: 100, timeoutMs: 7_200_000, mdbxMaxBytes: 16 * 1024 ** 3, minFreeGiB: 80 },
  { profile: '5m', projectSamples: 50, timeoutMs: 14_400_000, mdbxMaxBytes: 32 * 1024 ** 3, minFreeGiB: 150 }
]);

const pinnedImages = Object.freeze({
  POWERSYNC_OFFICIAL_IMAGE:
    'journeyapps/powersync-service@sha256:b6b22fa7d0d862f04bdff62846e656756d17bcf3dd6eca399a0633671051438b',
  POWERSYNC_USER_VALUE_MONGO_IMAGE:
    'mongo@sha256:d5b3ca8c3f3cdce78d44870dc0871b76d5235e9b2ad4ea6bea5d1fbff8027703',
  POWERSYNC_USER_VALUE_POSTGRES_IMAGE:
    'postgres@sha256:be01cf82fc7dbba824acf0a82e150b4b360f3ff93c6631d7844af431e841a95c'
});

export function canaryEnvironment(rung, artifactRoot, base = process.env) {
  return {
    ...base,
    ...pinnedImages,
    NODE_OPTIONS: appendNodeOption(base.NODE_OPTIONS, '--max-old-space-size=8192'),
    POWERSYNC_BENCHMARK_COMPOSE_PROJECT: `powersync_mdbx_ladder_${rung.profile}`,
    POWERSYNC_BENCHMARK_SKIP_TOOLING_INSTALL: '1',
    POWERSYNC_USER_VALUE_ARTIFACT_ROOT: artifactRoot,
    POWERSYNC_USER_VALUE_RUNTIME: 'symmetric-docker',
    POWERSYNC_USER_VALUE_RUST_IMAGE: base.POWERSYNC_USER_VALUE_RUST_IMAGE ?? 'powersync-mdbx:benchmark',
    POWERSYNC_USER_VALUE_RUST_IMAGE_PULL: '0',
    POWERSYNC_USER_VALUE_TARGET_CPUS: base.POWERSYNC_USER_VALUE_TARGET_CPUS ?? '4',
    POWERSYNC_USER_VALUE_TARGET_MEMORY: base.POWERSYNC_USER_VALUE_TARGET_MEMORY ?? '8g',
    POWERSYNC_USER_VALUE_SERVICE_CPUS: base.POWERSYNC_USER_VALUE_SERVICE_CPUS ?? '2.5',
    POWERSYNC_USER_VALUE_SERVICE_MEMORY: base.POWERSYNC_USER_VALUE_SERVICE_MEMORY ?? '5g',
    POWERSYNC_USER_VALUE_MONGO_CPUS: base.POWERSYNC_USER_VALUE_MONGO_CPUS ?? '1.5',
    POWERSYNC_USER_VALUE_MONGO_MEMORY: base.POWERSYNC_USER_VALUE_MONGO_MEMORY ?? '3g',
    POWERSYNC_USER_VALUE_MONGO_CACHE_GB: base.POWERSYNC_USER_VALUE_MONGO_CACHE_GB ?? '1',
    POWERSYNC_RUST_ALLOW_COMPARISON: '1',
    POWERSYNC_RUST_MAX_SYNC_READ_ENTRIES: '150000',
    POWERSYNC_RUST_MAX_SYNC_READ_BYTES: '134217728',
    POWERSYNC_RUST_MDBX_MAX_SIZE_BYTES: `${rung.mdbxMaxBytes}`,
    POWERSYNC_USER_VALUE_PROFILE: rung.profile,
    POWERSYNC_USER_VALUE_TARGETS: 'official,rust',
    POWERSYNC_USER_VALUE_PROCESSING_ONLY: '1',
    POWERSYNC_USER_VALUE_ACCESS_MODE: 'auth_perimeter',
    POWERSYNC_USER_VALUE_EQUIVALENCE_GATE: '1',
    POWERSYNC_USER_VALUE_CHURN_GATE: '1',
    POWERSYNC_USER_VALUE_CHURN_GATE_MODE: 'slot-lsn',
    POWERSYNC_USER_VALUE_INITIAL_READINESS: 'sync-protocol',
    POWERSYNC_USER_VALUE_PROJECT_BUCKET_SAMPLES: `${rung.projectSamples}`,
    POWERSYNC_USER_VALUE_CHURN_ROWS_PER_BUCKET: '10',
    POWERSYNC_USER_VALUE_LIFECYCLE_REPEATS: '0',
    POWERSYNC_USER_VALUE_BROWSER_ITERATIONS: '1',
    POWERSYNC_USER_VALUE_END_USER_REPEATS: '1',
    POWERSYNC_USER_VALUE_BUCKET_PROBE_BATCH_SIZE: '25',
    POWERSYNC_USER_VALUE_TIMEOUT_MS: `${rung.timeoutMs}`,
    POWERSYNC_USER_VALUE_WARMUP_PAIRS: '0',
    POWERSYNC_USER_VALUE_RETAIN_RAW_RECORDS: '1'
  };
}

function main() {
  assertCleanWorktree();
  assertLinuxDockerServer();
  run('npm', ['--prefix', 'e2e/official-sdk', 'ci']);

  const rustImage = process.env.POWERSYNC_USER_VALUE_RUST_IMAGE ?? 'powersync-mdbx:benchmark';
  if (process.env.POWERSYNC_LADDER_SKIP_BUILD !== '1') {
    run('docker', ['build', '-f', 'Dockerfile.benchmark', '-t', rustImage, '.']);
  }
  const rustImageId = capture('docker', ['image', 'inspect', '--format', '{{.Id}}', rustImage]);

  const suffix = new Date().toISOString().replaceAll(/[:.]/g, '-');
  const ladderRoot = path.resolve(process.env.POWERSYNC_LADDER_ARTIFACT_ROOT ?? path.join(defaultRoot, suffix));
  fs.mkdirSync(path.dirname(ladderRoot), { recursive: true });
  fs.mkdirSync(ladderRoot, { recursive: false });
  const manifestPath = path.join(ladderRoot, 'ladder.json');
  const manifest = {
    schemaVersion: 1,
    startedAt: new Date().toISOString(),
    gitCommit: capture('git', ['rev-parse', 'HEAD']),
    rustImage,
    rustImageId,
    dockerServer: capture('docker', [
      'info',
      '--format',
      'os={{.OSType}} arch={{.Architecture}} cpus={{.NCPU}} memory={{.MemTotal}}'
    ]),
    status: 'running',
    runs: []
  };
  writeManifest(manifestPath, manifest);

  try {
    for (const rung of LADDER_PROFILES) {
      assertFreeDisk(ladderRoot, rung.minFreeGiB, rung.profile);
      const artifactRoot = path.join(ladderRoot, rung.profile);
      fs.mkdirSync(artifactRoot);
      const startedAt = new Date().toISOString();
      const result = spawnSync(process.execPath, ['scripts/user_value_benchmark.mjs'], {
        cwd: repoRoot,
        env: canaryEnvironment(rung, artifactRoot),
        stdio: 'inherit'
      });
      const runDir = singleRunDirectory(artifactRoot);
      const entry = {
        ...rung,
        startedAt,
        finishedAt: new Date().toISOString(),
        exitCode: result.status,
        signal: result.signal,
        artifactDir: runDir == null ? null : path.relative(ladderRoot, runDir)
      };
      manifest.runs.push(entry);
      writeManifest(manifestPath, manifest);

      if (result.error) throw result.error;
      if (result.status !== 0) {
        throw new Error(`${rung.profile} canary failed with exit code ${result.status}${result.signal ? ` (${result.signal})` : ''}`);
      }
      assertRunArtifacts(runDir, rung.profile);
      entry.status = 'passed';
      writeManifest(manifestPath, manifest);
    }
    manifest.status = 'passed';
  } catch (error) {
    manifest.status = 'failed';
    manifest.error = error instanceof Error ? error.message : String(error);
    throw error;
  } finally {
    manifest.finishedAt = new Date().toISOString();
    writeManifest(manifestPath, manifest);
    process.stderr.write(`[linux-canary-ladder] manifest: ${manifestPath}\n`);
  }
}

function assertCleanWorktree() {
  const status = capture('git', ['status', '--porcelain']);
  if (status !== '') throw new Error('canary ladder requires a clean Git worktree');
}

function assertLinuxDockerServer() {
  const serverOs = capture('docker', ['info', '--format', '{{.OSType}}']);
  if (serverOs !== 'linux') throw new Error(`canary ladder requires a Linux Docker server, got ${serverOs || 'unknown'}`);
}

function assertFreeDisk(directory, minimumGiB, profile) {
  const stats = fs.statfsSync(directory);
  const freeGiB = Number(stats.bavail * stats.bsize) / 1024 ** 3;
  if (freeGiB < minimumGiB) {
    throw new Error(`${profile} canary requires at least ${minimumGiB} GiB free; ${freeGiB.toFixed(1)} GiB available`);
  }
}

function assertRunArtifacts(runDir, profile) {
  if (runDir == null) throw new Error(`${profile} canary did not create a run directory`);
  const resultsPath = path.join(runDir, 'results.json');
  const comparisonPath = path.join(runDir, 'compare.json');
  for (const required of [resultsPath, comparisonPath, path.join(runDir, 'summary.md')]) {
    if (!fs.existsSync(required)) throw new Error(`${profile} canary is missing ${required}`);
  }
  const results = JSON.parse(fs.readFileSync(resultsPath, 'utf8'));
  if (results.profile !== profile) throw new Error(`${profile} artifact reports profile ${results.profile}`);
  for (const target of ['official', 'rust']) {
    const runs = results.targets?.[target]?.endUser?.runs;
    if (!Array.isArray(runs) || runs.length !== 1) throw new Error(`${profile}/${target} must contain exactly one run`);
    if (runs[0]?.equivalence?.status !== 'passed' || runs[0]?.churn?.status !== 'passed') {
      throw new Error(`${profile}/${target} did not pass initial equivalence and churn gates`);
    }
    if (runs[0]?.resources?.status !== 'captured') {
      throw new Error(`${profile}/${target} did not capture resource evidence`);
    }
    const rawRecords = walkFiles(path.join(runDir, target)).filter((file) => file.endsWith('.json.gz'));
    for (const phase of ['initial', 'churn']) {
      if (!rawRecords.some((file) => path.basename(file).startsWith(`${phase}-protocol-records.r1.`))) {
        throw new Error(`${profile}/${target} is missing compressed ${phase} protocol records`);
      }
    }
  }
  const gzipPaths = walkFiles(runDir).filter((file) => file.endsWith('.gz'));
  for (const gzipPath of gzipPaths) {
    run('gzip', ['-t', gzipPath]);
  }
}

function singleRunDirectory(artifactRoot) {
  const directories = fs
    .readdirSync(artifactRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => path.join(artifactRoot, entry.name));
  if (directories.length > 1) throw new Error(`expected one run below ${artifactRoot}, found ${directories.length}`);
  return directories[0] ?? null;
}

function walkFiles(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const entryPath = path.join(directory, entry.name);
    return entry.isDirectory() ? walkFiles(entryPath) : [entryPath];
  });
}

function appendNodeOption(existing, option) {
  const value = existing?.trim();
  return value ? `${value} ${option}` : option;
}

function writeManifest(manifestPath, manifest) {
  const temporaryPath = `${manifestPath}.tmp`;
  fs.writeFileSync(temporaryPath, `${JSON.stringify(manifest, null, 2)}\n`);
  fs.renameSync(temporaryPath, manifestPath);
}

function run(command, args) {
  const result = spawnSync(command, args, { cwd: repoRoot, stdio: 'inherit' });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} failed with exit code ${result.status}`);
}

function capture(command, args) {
  const result = spawnSync(command, args, { cwd: repoRoot, encoding: 'utf8' });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} failed: ${result.stderr.trim()}`);
  return result.stdout.trim();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main();
