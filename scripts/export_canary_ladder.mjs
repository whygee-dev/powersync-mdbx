#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

export function buildCanarySummary(ladder, loadResults) {
  if (ladder?.status !== 'passed' || !Array.isArray(ladder.runs) || ladder.runs.length === 0) {
    throw new Error('only a completed, passing ladder can be exported');
  }
  const rungs = ladder.runs.map((run) => summarizeRung(run, loadResults(run.artifactDir)));
  const resourceEvidenceCaptured = rungs.every(
    (rung) =>
      rung.official.resourceEvidenceStatus === 'captured' &&
      rung.rust.resourceEvidenceStatus === 'captured'
  );
  const additionalBoundariesCaptured = rungs.every(
    (rung) =>
      rung.official.completeMaterialization != null &&
      rung.official.sourceSlotPosition != null &&
      rung.rust.completeMaterialization != null &&
      rung.rust.sourceSlotPosition != null
  );
  return {
    schemaVersion: 1,
    recordedAt: ladder.finishedAt,
    gitCommit: ladder.gitCommit,
    rustImageId: ladder.rustImageId,
    dockerServer: ladder.dockerServer,
    status: ladder.status,
    methodology: {
      measuredRunsPerTargetPerRung: 1,
      targetOrder: 'recorded in each rung',
      targetStores: 'empty at the start of each measured run',
      cacheControl: 'OS and PostgreSQL caches were not flushed',
      protocolBoundary:
        'first successful expected-state proof for one routed subscription through checkpoint_complete',
      additionalInitialBoundaries: additionalBoundariesCaptured
        ? 'target-specific complete materialization and replication-slot confirmed-flush position captured for both targets at every rung'
        : 'not collected by this harness revision',
      resourceEvidence: resourceEvidenceCaptured
        ? 'captured for both targets at every rung'
        : 'not collected by this harness revision'
    },
    rungs
  };
}

function summarizeRung(run, results) {
  if (run.status !== 'passed' || results.profile !== run.profile) {
    throw new Error(`invalid ${run.profile ?? 'unknown'} ladder artifact`);
  }
  const target = (label) => {
    const measured = results.targets?.[label]?.endUser?.runs?.[0];
    if (measured?.equivalence?.status !== 'passed' || measured?.churn?.status !== 'passed') {
      throw new Error(`${run.profile}/${label} did not pass both protocol gates`);
    }
    const completeMaterialization = compactBoundary(measured.readiness?.completeMaterialization, [
      'completionBoundary',
      'processingMs',
      'targetLsn'
    ]);
    const sourceSlotPosition = compactBoundary(measured.readiness?.sourceSlotPosition, [
      'method',
      'processingMs',
      'targetLsn',
      'confirmed_flush_lsn'
    ]);
    if ((completeMaterialization == null) !== (sourceSlotPosition == null)) {
      throw new Error(`${run.profile}/${label} has only one additional initial boundary`);
    }
    if (
      completeMaterialization != null &&
      completeMaterialization.targetLsn !== sourceSlotPosition.targetLsn
    ) {
      throw new Error(`${run.profile}/${label} additional initial boundaries use different source LSNs`);
    }
    return {
      protocolReadinessMs: measured.readiness?.processingMs ?? null,
      completeMaterialization,
      sourceSlotPosition,
      resourceEvidenceStatus: measured.resources?.status ?? 'not-collected',
      resources: summarizeResourceEvidence(measured.resources),
      initialEquivalence: summarizeGate(measured.equivalence),
      churn: summarizeGate(measured.churn)
    };
  };
  return {
    profile: run.profile,
    sourceTaskRows: results.methodology?.equivalence?.datasetTaskRows ?? null,
    routedBucketSamples: results.config?.projectBucketSampleCount ?? null,
    elapsedMs: Date.parse(run.finishedAt) - Date.parse(run.startedAt),
    targetResources: results.config?.targetResources ?? null,
    officialResourceSplit: {
      service: results.config?.officialServiceResources ?? null,
      mongo: results.config?.officialStorageResources ?? null
    },
    officialTuning: {
      mongoCacheGb: results.config?.officialMongoCacheGb ?? null,
      nodeOptions: results.config?.officialNodeOptions ?? null,
      reviewedByPowerSync: results.config?.officialTuningReviewed === true
    },
    storageControls: {
      classesAttested: results.config?.storageClassAttested === true,
      durabilityPoliciesAttested: results.config?.durabilityPolicyAttested === true,
      classes: results.config?.storageClasses ?? null,
      durabilityPolicies: results.config?.durabilityPolicies ?? null
    },
    imageInputs: results.config?.dockerImageInputs ?? null,
    rawValidationRecordsRetained: results.config?.retainRawValidationRecords === true,
    executionSchedule: results.config?.executionSchedule ?? [],
    official: target('official'),
    rust: target('rust')
  };
}

function summarizeResourceEvidence(resources) {
  const summarizeWindow = (window) => {
    if (window == null) return null;
    return {
      durationMs:
        Number.isFinite(Date.parse(window.finishedAt)) && Number.isFinite(Date.parse(window.startedAt))
          ? Date.parse(window.finishedAt) - Date.parse(window.startedAt)
          : null,
      walInsertedBytes: window.wal?.insertedBytes ?? null,
      components: Object.fromEntries(
        Object.entries(window.components ?? {}).map(([name, component]) => [
          name,
          {
            status: component.status ?? null,
            source: component.source ?? null,
            access: component.access ?? null,
            cpuSeconds: component.cpuSeconds ?? null,
            cgroupLifetimePeakMemoryBytes: component.cgroupLifetimePeakMemoryBytes ?? null,
            mainProcessLifetimePeakRssBytes: component.mainProcessLifetimePeakRssBytes ?? null,
            blockReadBytes: component.blockReadBytes ?? null,
            blockWriteBytes: component.blockWriteBytes ?? null,
            networkRxBytes: component.networkRxBytes ?? null,
            networkTxBytes: component.networkTxBytes ?? null
          }
        ])
      ),
      storageGrowth: Object.fromEntries(
        Object.entries(window.storage ?? {}).map(([name, storage]) => [
          name,
          {
            logicalBytes: storage.logicalBytes ?? null,
            allocatedBytes: storage.allocatedBytes ?? null,
            files: storage.files ?? null
          }
        ])
      )
    };
  };
  return {
    status: resources?.status ?? 'not-collected',
    initial: summarizeWindow(resources?.initial),
    total: summarizeWindow(resources?.total)
  };
}

function compactBoundary(boundary, fields) {
  if (boundary == null) return null;
  return Object.fromEntries(fields.map((field) => [field, boundary[field] ?? null]));
}

function summarizeGate(gate) {
  const buckets = gate?.buckets ?? [];
  return {
    status: gate?.status ?? null,
    buckets: buckets.length,
    puts: sum(buckets, 'puts'),
    removes: sum(buckets, 'removes')
  };
}

function sum(items, field) {
  return items.reduce((total, item) => total + (Number(item?.[field]) || 0), 0);
}

function main() {
  const [ladderDirArg, label] = process.argv.slice(2);
  if (!ladderDirArg || !/^[a-z0-9][a-z0-9-]*$/.test(label ?? '')) {
    throw new Error('usage: node scripts/export_canary_ladder.mjs <ladder-dir> <kebab-case-label>');
  }
  const ladderDir = path.resolve(repoRoot, ladderDirArg);
  const ladder = JSON.parse(fs.readFileSync(path.join(ladderDir, 'ladder.json'), 'utf8'));
  const summary = buildCanarySummary(ladder, (artifactDir) =>
    JSON.parse(fs.readFileSync(path.join(ladderDir, artifactDir, 'results.json'), 'utf8'))
  );
  const outputDir = path.join(repoRoot, 'docs', 'artifacts', label);
  fs.mkdirSync(outputDir, { recursive: true });
  fs.writeFileSync(path.join(outputDir, 'canary-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
  console.log(path.relative(repoRoot, path.join(outputDir, 'canary-summary.json')));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main();
