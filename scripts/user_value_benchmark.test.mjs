import assert from 'node:assert/strict';
import test from 'node:test';

import {
  activeSyncRulesStateMatches,
  assertCursorProgression,
  assertPublicationResourceEvidence,
  assertPublicRunPreflight,
  assertTargetProtocolParityAgainstOfficial,
  attachChurnExpectations,
  buildComparisons,
  buildChurnBucketSpecs,
  buildInitialReadinessSpec,
  buildReadinessSubscriptionSpec,
  buildPublicationReadiness,
  collectInitialReadinessBoundaries,
  compareDecimalCursors,
  createBenchmarkJwt,
  churnMutationSql,
  defaultProtocolReadinessAttempts,
  effectiveProjectBucketSampleCount,
  extractMutationTargetLsn,
  churnEquivalenceRequestBody,
  initialEquivalenceRequestBody,
  initialMaterializationDiagnosticsReached,
  initialReadinessSubject,
  isRetryableChurnProtocolConvergenceError,
  isRetryableProtocolReadinessError,
  normalizeDecimalCursor,
  observeRustChurnMetricsAfterPublicTiming,
  percentile,
  readinessAuthPerimeterRow,
  resetObservedBucketState,
  renderMarkdown,
  sumAvailable,
  summarizeSamples,
  syncRulesStateMatches
} from './user_value_benchmark.mjs';

test('protocol readiness attempts cannot expire before the overall timeout', () => {
  assert.equal(defaultProtocolReadinessAttempts(60_000, 300), 300);
  assert.equal(defaultProtocolReadinessAttempts(30 * 60_000, 300), 1_801);
  assert.equal(defaultProtocolReadinessAttempts(4 * 60 * 60_000, 300), 14_401);
});

test('churn protocol convergence retries stale views but not semantic corruption', () => {
  assert.equal(
    isRetryableChurnProtocolConvergenceError(new Error('churn bucket-a: PUT mismatch expected=20 actual=0')),
    true
  );
  assert.equal(
    isRetryableChurnProtocolConvergenceError(new Error('churn bucket-a request failed with 503: unavailable')),
    true
  );
  assert.equal(
    isRetryableChurnProtocolConvergenceError(new Error('churn bucket-a: checkpoint checksum 1 != expected 2')),
    true
  );
  assert.equal(
    isRetryableChurnProtocolConvergenceError(new Error('churn bucket-a: payload mismatch [{"field":"title"}]')),
    false
  );
  assert.equal(isRetryableChurnProtocolConvergenceError(new Error('churn bucket-a: unexpected PUT row-x')), false);
});

test('churn target LSN extraction ignores psql status output and requires one marker', () => {
  assert.equal(
    extractMutationTargetLsn('       ?column?\n-----------------------\n benchmark_target_lsn=0/1a2b \n(1 row)\n'),
    '0/1A2B'
  );
  assert.throws(() => extractMutationTargetLsn('COMMIT\n'), /returned 0 target LSN markers/);
  assert.throws(
    () => extractMutationTargetLsn('benchmark_target_lsn=0/1\nbenchmark_target_lsn=0/2\n'),
    /returned 2 target LSN markers/
  );
});

test('churn target LSN is captured after the mutation transaction commits', () => {
  const row = {
    id: 'task-1',
    title: 'title',
    summary: 'summary',
    projectId: 'project-1',
    organizationId: 'org-1'
  };
  const sql = churnMutationSql({ insertRows: [row], updateRows: [row], deleteRows: [row] });
  assert.ok(sql.indexOf('COMMIT;') < sql.indexOf("SELECT 'benchmark_target_lsn='"));
});

test('project bucket sample reporting distinguishes requested and effective counts', () => {
  assert.equal(
    effectiveProjectBucketSampleCount({ requested: 1_000, availableProjects: 250, enabled: true }),
    250
  );
  assert.equal(
    effectiveProjectBucketSampleCount({ requested: 0, availableProjects: 250, enabled: true }),
    1
  );
  assert.equal(
    effectiveProjectBucketSampleCount({ requested: 1_000, availableProjects: 250, enabled: false }),
    0
  );
});

test('cursor handling preserves decimal strings above MAX_SAFE_INTEGER and compares with BigInt', () => {
  const aboveMax = '9007199254740993123456789';
  assert.equal(normalizeDecimalCursor(aboveMax), aboveMax);
  assert.equal(compareDecimalCursors(aboveMax, '9007199254740993123456788'), 1);

  const state = { previousAfter: '9007199254740993123456787' };
  assertCursorProgression(
    state,
    {
      after: '9007199254740993123456788',
      next_after: aboveMax
    },
    'large cursor'
  );
  assert.equal(state.previousAfter, aboveMax);
  assert.throws(
    () => assertCursorProgression(state, { after: '9007199254740993123456788', next_after: aboveMax }, 'large cursor'),
    /data after cursor regressed/
  );
  assert.throws(() => normalizeDecimalCursor(Number.MAX_SAFE_INTEGER + 1), /safe integer/);
});

test('CLEAR resets observed bucket contents without discarding checksum recurrence state', () => {
  const state = {
    putIds: new Set(['before-clear']),
    seenPuts: new Set(['updated-before-clear']),
    seenRemoves: new Set(['removed-before-clear']),
    payloadMismatches: [{ objectId: 'before-clear' }],
    checksumSum: 123,
    checksumRecords: [{ op: 'PUT' }]
  };
  resetObservedBucketState(state);
  assert.deepEqual([...state.putIds], []);
  assert.deepEqual([...state.seenPuts], []);
  assert.deepEqual([...state.seenRemoves], []);
  assert.deepEqual(state.payloadMismatches, []);
  assert.equal(state.checksumSum, 123);
  assert.equal(state.checksumRecords.length, 1);
  assert.equal(state.resetObserved, true);
});

test('benchmark JWTs are minted with a fresh full lifetime for each request', () => {
  const first = decodeJwtPayload(createBenchmarkJwt({ nowSeconds: 1_000, ttlSeconds: 300 }));
  const second = decodeJwtPayload(createBenchmarkJwt({ nowSeconds: 1_200, ttlSeconds: 300 }));
  assert.equal(first.iat, 1_000);
  assert.equal(first.exp, 1_300);
  assert.equal(second.iat, 1_200);
  assert.equal(second.exp, 1_500);
});

test('public benchmark preflight accepts only a fully specified publication run', () => {
  const digest = `example.invalid/image@sha256:${'a'.repeat(64)}`;
  const valid = {
    platform: 'linux',
    targets: ['official', 'rust'],
    deploymentModel: 'symmetric-linux-containers',
    equalCpuMemoryLimits: true,
    sameStorageClass: true,
    samePostgresNetworkPath: true,
    imageInputs: { official: digest, mongo: digest, rust: digest, postgres: digest },
    readinessBoundary: 'sync-protocol-checkpoint-complete',
    measuredRepeats: 20,
    warmupPairs: 1,
    interleaveTargets: true,
    equivalenceGateEnabled: true,
    churnGateEnabled: true,
    churnGateMode: 'slot-lsn',
    rawValidationRecordsRetained: true,
    resourceEvidenceEnabled: true,
    appendOnlyArtifacts: true,
    officialTuningReviewed: true,
    gitDirty: false
  };
  assert.doesNotThrow(() => assertPublicRunPreflight(valid));
  assert.throws(
    () => assertPublicRunPreflight({ ...valid, measuredRepeats: 5, imageInputs: { official: 'image:latest' } }),
    /official image must be pinned.*mongo image must be pinned.*rust image must be pinned.*at least 20 measured paired repeats/s
  );
});

test('public benchmark preflight rejects a target resource budget mismatch', () => {
  const digest = `example.invalid/image@sha256:${'a'.repeat(64)}`;
  const base = {
    platform: 'linux',
    targets: ['official', 'rust'],
    deploymentModel: 'symmetric-linux-containers',
    equalCpuMemoryLimits: true,
    sameStorageClass: true,
    samePostgresNetworkPath: true,
    imageInputs: { official: digest, mongo: digest, rust: digest, postgres: digest },
    readinessBoundary: 'sync-protocol-checkpoint-complete',
    measuredRepeats: 20,
    warmupPairs: 1,
    interleaveTargets: true,
    equivalenceGateEnabled: true,
    churnGateEnabled: true,
    churnGateMode: 'slot-lsn',
    rawValidationRecordsRetained: true,
    resourceEvidenceEnabled: true,
    appendOnlyArtifacts: true,
    officialTuningReviewed: true,
    gitDirty: false
  };
  assert.throws(
    () => assertPublicRunPreflight({ ...base, equalCpuMemoryLimits: false }),
    /identical explicit CPU and memory limits/
  );
});

test('resource aggregation preserves unavailable counters instead of reporting zero', () => {
  assert.equal(sumAvailable([null, undefined]), null);
  assert.equal(sumAvailable([null, 1.25, 0.75]), 2);
});

test('publication resource evidence requires native Linux counters for every field', () => {
  const window = {
    components: {
      service: {
        status: 'captured',
        source: 'linux-cgroup-v2',
        cpuSeconds: 1,
        cgroupLifetimePeakMemoryBytes: 2,
        mainProcessLifetimePeakRssBytes: 3,
        blockReadBytes: 4,
        blockWriteBytes: 5,
        networkRxBytes: 6,
        networkTxBytes: 7
      }
    },
    storage: { mdbx: { logicalBytes: 8, allocatedBytes: 9 } },
    wal: { insertedBytes: 10 }
  };
  const complete = {
    target: 'rust',
    status: 'captured',
    windows: Object.fromEntries(
      ['initial', 'browser', 'equivalence', 'churn', 'total'].map((name) => [name, window])
    )
  };
  assert.doesNotThrow(() => assertPublicationResourceEvidence(complete));
  assert.throws(
    () =>
      assertPublicationResourceEvidence({
        ...complete,
        windows: { ...complete.windows, total: null }
      }),
    /total resource window was not captured/
  );
  assert.throws(
    () =>
      assertPublicationResourceEvidence({
        ...complete,
        windows: {
          ...complete.windows,
          initial: {
            ...complete.windows.initial,
            components: {
              service: {
                ...complete.windows.initial.components.service,
                source: 'docker-stats-fallback',
                cpuSeconds: null
              }
            }
          }
        }
      }),
    /not native Linux cgroup v2.*cpuSeconds is unavailable/s
  );
});

test('publication readiness headlines the common protocol boundary and keeps diagnostics secondary', () => {
  const targetLsn = '0/123';
  const readiness = buildPublicationReadiness({
    observableReady: {
      method: 'sync-protocol-checkpoint-complete',
      processingMs: 321,
      bucket: 'bucket-a'
    },
    completeMaterialization: {
      method: 'persisted-lsn',
      completionBoundary: 'rust-persisted-initial-snapshot-lsn',
      processingMs: 123,
      targetLsn
    },
    sourceSlotPosition: {
      method: 'replication-slot-confirmed-flush-lsn',
      processingMs: 140,
      targetLsn
    },
    targetLsn
  });
  assert.equal(readiness.method, 'sync-protocol-checkpoint-complete');
  assert.equal(readiness.processingMs, 321);
  assert.equal(readiness.completeMaterialization.processingMs, 123);
  assert.equal(readiness.sourceSlotPosition.processingMs, 140);
  assert.equal(readiness.targetSpecificDiagnostic.method, 'persisted-lsn');
  assert.equal(readiness.targetSpecificDiagnostic.processingMs, 123);
  assert.throws(
    () =>
      buildPublicationReadiness({
        observableReady: readiness.ready,
        completeMaterialization: readiness.completeMaterialization,
        sourceSlotPosition: { ...readiness.sourceSlotPosition, targetLsn: '0/124' },
        targetLsn
      }),
    /must use the captured fixture LSN/
  );
});

test('initial readiness observers start concurrently and retain all three boundaries', async () => {
  const started = [];
  let release;
  const barrier = new Promise((resolve) => {
    release = resolve;
  });
  const observer = (name, value) => async () => {
    started.push(name);
    await barrier;
    return value;
  };
  const pending = collectInitialReadinessBoundaries({
    observeProtocol: observer('protocol', { processingMs: 10 }),
    observeCompleteMaterialization: observer('materialization', { processingMs: 20 }),
    observeSlotPosition: observer('slot-position', { processingMs: 30 })
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(started, ['protocol', 'materialization', 'slot-position']);
  release();
  assert.deepEqual(await pending, {
    observableReady: { processingMs: 10 },
    completeMaterialization: { processingMs: 20 },
    sourceSlotPosition: { processingMs: 30 }
  });
});

test('complete materialization diagnostics require an explicit completion flag and source LSN', () => {
  const complete = {
    responseOk: true,
    initialReplicationDone: true,
    lsnReached: true,
    versionReached: true
  };
  assert.equal(initialMaterializationDiagnosticsReached(complete), true);
  assert.equal(
    initialMaterializationDiagnosticsReached({ ...complete, initialReplicationDone: false }),
    false
  );
  assert.equal(initialMaterializationDiagnosticsReached({ ...complete, lsnReached: false }), false);
  assert.equal(initialMaterializationDiagnosticsReached({ ...complete, versionReached: false }), false);
});

test('publication active-rule validation rejects different rule content', () => {
  assert.equal(syncRulesStateMatches({ content: 'bucket_definitions: []' }, 'bucket_definitions: []'), true);
  assert.equal(
    syncRulesStateMatches({ content: 'bucket_definitions: []' }, 'bucket_definitions:\n  tasks: {}'),
    false
  );
  assert.equal(
    activeSyncRulesStateMatches(
      { version: 7, content: 'bucket_definitions: []', slotName: 'powersync_7' },
      {
        expectedVersion: 7,
        expectedContent: 'bucket_definitions:\n  tasks: {}',
        expectedSlotName: 'powersync_7',
        requireExpectedContent: true
      }
    ),
    false
  );
});

test('common readiness spec is analytical and never scans initialTaskRows', () => {
  const source = buildInitialReadinessSpec.toString();
  assert.doesNotMatch(source, /initialTaskRows/);
  assert.match(source, /profile\.tasksPerProject/);
  assert.match(source, /buildReadinessSubscriptionSpec/);
});

test('auth-perimeter readiness uses one-project subscription and its dedicated JWT subject', () => {
  const accessRow = readinessAuthPerimeterRow();
  const spec = buildReadinessSubscriptionSpec({
    authPerimeter: true,
    projectIdValue: accessRow.project_id,
    expectedRows: []
  });
  const request = initialEquivalenceRequestBody([spec], 'readiness-test');
  assert.equal(spec.requestKind, 'subscription');
  assert.equal(spec.subscriptionMode, 'auth_perimeter');
  assert.deepEqual(spec.routeParameters, { project_id: accessRow.project_id });
  assert.deepEqual(request.buckets, []);
  assert.equal(request.streams.include_defaults, false);
  assert.deepEqual(request.streams.subscriptions, [
    {
      stream: 'tasks_by_auth_project',
      parameters: {},
      override_priority: 3
    }
  ]);
  assert.equal(initialReadinessSubject({ authPerimeter: true }), 'user-benchmark-readiness-probe');
  assert.notEqual(initialReadinessSubject({ authPerimeter: true }), initialReadinessSubject({ authPerimeter: false }));
  assert.equal(accessRow.user_id, initialReadinessSubject({ authPerimeter: true }));
  assert.equal(accessRow.project_id, spec.routeParameters.project_id);
});

test('subscription readiness requests exactly one routed project stream', () => {
  const spec = buildReadinessSubscriptionSpec({
    authPerimeter: false,
    projectIdValue: 'project-01-000001',
    expectedRows: []
  });
  const request = initialEquivalenceRequestBody([spec], 'readiness-test');
  assert.equal(spec.requestKind, 'subscription');
  assert.equal(spec.stream, 'tasks_by_project');
  assert.deepEqual(request.buckets, []);
  assert.deepEqual(request.streams.subscriptions, [
    {
      stream: 'tasks_by_project',
      parameters: { project_id: 'project-01-000001' },
      override_priority: 3
    }
  ]);
});

test('readiness retries transient availability failures but aborts deterministic protocol mismatches', () => {
  assert.equal(
    isRetryableProtocolReadinessError(new Error('equivalence readiness request failed with 503: starting')),
    true
  );
  assert.equal(isRetryableProtocolReadinessError(new Error('connect ECONNREFUSED 127.0.0.1')), true);
  assert.equal(
    isRetryableProtocolReadinessError(
      new Error('equivalence readiness: unexpected data bucket 1#tasks|0[]')
    ),
    false
  );
});

test('churn reuses enriched auth-perimeter specs and derives incremental and reset expectations', () => {
  const projectId = 'project-org-001-0001';
  const initialRows = [
    taskRow({ id: 'task-update', project_id: projectId, title: 'before update' }),
    taskRow({ id: 'task-delete', project_id: projectId, title: 'delete me' }),
    taskRow({ id: 'task-keep', project_id: projectId, title: 'keep me' })
  ];
  const verificationSpec = {
    requestKind: 'subscription',
    subscriptionMode: 'auth_perimeter',
    label: 'auth project 1',
    stream: 'tasks_by_auth_project',
    bucket: '1#tasks_by_auth_project|0["project-org-001-0001"]',
    routeParameters: { project_id: projectId },
    expectedRows: initialRows
  };
  const initialEquivalence = {
    buckets: [
      {
        bucket: verificationSpec.bucket,
        cursorAfter: '9007199254740993123456789',
        checkpointChecksum: 123,
        checkpointCount: 3
      }
    ]
  };
  Object.defineProperty(initialEquivalence, 'verificationSpecs', {
    value: [verificationSpec],
    enumerable: false
  });

  const [churnSpec] = buildChurnBucketSpecs(initialEquivalence);
  assert.equal(churnSpec.after, '9007199254740993123456789');
  assert.equal(churnSpec.subscriptionMode, 'auth_perimeter');
  assert.equal(churnSpec.expectedRows, initialRows);
  assert.doesNotMatch(JSON.stringify(initialEquivalence), /expectedRows/);

  const updated = taskRow({ id: 'task-update', project_id: projectId, title: 'after update' });
  const inserted = taskRow({ id: 'task-insert', project_id: projectId, title: 'inserted' });
  const [expected] = attachChurnExpectations([churnSpec], {
    insertRows: [inserted],
    updateRows: [updated],
    deleteRows: [initialRows[1]]
  });
  assert.deepEqual(expected.expectedPuts.map((row) => row.id), ['task-insert', 'task-update']);
  assert.deepEqual(expected.expectedRemoves, ['task-delete']);
  assert.deepEqual(
    expected.expectedSnapshotRows.map((row) => row.id).sort(),
    ['task-insert', 'task-keep', 'task-update']
  );
  assert.equal(expected.expectedSnapshotRows.find((row) => row.id === 'task-update').title, 'after update');

  const request = churnEquivalenceRequestBody([expected], 'churn-test');
  assert.deepEqual(request.buckets, [{ name: verificationSpec.bucket, after: '9007199254740993123456789' }]);
  assert.deepEqual(request.streams.subscriptions, [
    {
      stream: 'tasks_by_auth_project',
      parameters: {},
      override_priority: 3
    }
  ]);
});

test('buildChurnBucketSpecs fallback enriches expectedRows instead of returning undefined', () => {
  const [spec] = buildChurnBucketSpecs({
    buckets: [
      {
        bucket: '1#tasks|0[]',
        cursorAfter: '42',
        checkpointChecksum: 0,
        checkpointCount: 0
      }
    ]
  });
  assert.equal(spec.bucket, '1#tasks|0[]');
  assert.equal(spec.after, '42');
  assert.ok(Array.isArray(spec.expectedRows));
  assert.ok(spec.expectedRows.length > 0);
});

test('benchmark comparisons use the churn catch-up metric matching the configured gate', () => {
  const targets = benchmarkTargets();

  const slotLsnComparison = buildComparisons(targets, { churnGateMode: 'slot-lsn' })[0];
  assert.ok(slotLsnComparison.endUserExperience['churn.slotAckCatchupMs']);
  assert.equal(slotLsnComparison.endUserExperience['churn.replicationCatchupMs'], undefined);

  const targetSpecificComparison = buildComparisons(targets, { churnGateMode: 'target-specific' })[0];
  assert.ok(targetSpecificComparison.endUserExperience['churn.replicationCatchupMs']);
  assert.equal(targetSpecificComparison.endUserExperience['churn.slotAckCatchupMs'], undefined);
});

test('benchmark markdown renders slot-lsn side metrics and persisted-catchup issues', () => {
  const targets = benchmarkTargets();
  targets.rust.endUser.runs = [
    {
      repeat: 2,
      churn: {
        rustPersistedError: 'tail_ops_written did not advance'
      }
    }
  ];
  const config = {
    churnGateMode: 'slot-lsn',
    rustReplicationFeedback: {
      statusIntervalMs: null,
      statusSource: 'not-set-by-harness',
      idleWakeupIntervalMs: 250,
      idleWakeupSource: 'env-override',
      currentSourceDefaults: {
        statusIntervalMs: 1_000,
        idleWakeupIntervalMs: 1_000
      }
    }
  };

  const markdown = renderMarkdown({
    results: benchmarkResults({ targets, config }),
    comparisons: buildComparisons(targets, config)
  });

  assert.match(
    markdown,
    /rust replication feedback intervals: status=Rust binary default \(not set by harness; checked-in source default 1000ms\), idle=250ms \(env override\)/
  );
  assert.match(markdown, /Slot-LSN ack catch-up/);
  assert.match(markdown, /churn r2: Rust persisted catch-up observation unavailable: tail_ops_written did not advance/);

  const targetSpecificTargets = benchmarkTargets();
  const targetSpecificConfig = { churnGateMode: 'target-specific' };
  const targetSpecificMarkdown = renderMarkdown({
    results: benchmarkResults({ targets: targetSpecificTargets, config: targetSpecificConfig }),
    comparisons: buildComparisons(targetSpecificTargets, targetSpecificConfig)
  });

  assert.match(targetSpecificMarkdown, /Subsequent churn catch-up/);
  assert.doesNotMatch(targetSpecificMarkdown, /Rust persisted catch-up/);
});

test('rust persisted post-public-timing observation succeeds when tail ops reached expected delta', async () => {
  const observed = await observeRustChurnMetricsAfterPublicTiming({
    endpoint: 'http://127.0.0.1:1',
    baseline: metricsResponse({ tail_ops_written: 40 }, { last_persisted_end_lsn: '0/10' }),
    expectedTailOpsDelta: 3,
    fetchMetrics: async () => metricsResponse({ tail_ops_written: 43 }, { last_persisted_end_lsn: '0/20' })
  });

  assert.equal(observed.error, null);
  assert.deepEqual(observed.persistedReady, {
    method: 'rust-tail-ops-after-public-churn-timing',
    ok: true,
    statusCode: 200,
    baseline_tail_ops_written: 40,
    expected_tail_ops_written: 43,
    tail_ops_written: 43,
    last_persisted_end_lsn: '0/20',
    metrics: {
      tail_ops_written: 43
    }
  });
});

test('rust persisted post-public-timing observation records compact error when tail ops lag expected delta', async () => {
  const observed = await observeRustChurnMetricsAfterPublicTiming({
    endpoint: 'http://127.0.0.1:1',
    baseline: metricsResponse({ tail_ops_written: 40 }),
    expectedTailOpsDelta: 3,
    fetchMetrics: async () => metricsResponse({ tail_ops_written: 42 })
  });

  assert.equal(observed.persistedReady, null);
  assert.match(observed.error, /rust tail ops not observed after public churn timing/);
  assert.match(observed.error, /expected_tail_ops_written/);
});

test('rust persisted post-public-timing observation records compact error for non-ok metrics response', async () => {
  const observed = await observeRustChurnMetricsAfterPublicTiming({
    endpoint: 'http://127.0.0.1:1',
    baseline: metricsResponse({ tail_ops_written: 40 }),
    expectedTailOpsDelta: 3,
    fetchMetrics: async () => ({
      ok: false,
      statusCode: 503,
      body: {
        metrics: {
          tail_ops_written: 43
        }
      }
    })
  });

  assert.equal(observed.persistedReady, null);
  assert.match(observed.error, /rust tail ops not observed after public churn timing/);
  assert.match(observed.error, /"ok":false/);
  assert.match(observed.error, /"statusCode":503/);
});

test('rust persisted post-public-timing observation records compact error for missing fetched tail ops', async () => {
  const observed = await observeRustChurnMetricsAfterPublicTiming({
    endpoint: 'http://127.0.0.1:1',
    baseline: metricsResponse({ tail_ops_written: 40 }),
    expectedTailOpsDelta: 3,
    fetchMetrics: async () => metricsResponse({})
  });

  assert.equal(observed.persistedReady, null);
  assert.match(observed.error, /rust tail ops not observed after public churn timing/);
  assert.match(observed.error, /"tail_ops_written":null/);
});

test('rust persisted post-public-timing observation records compact error when metrics fetch throws', async () => {
  const observed = await observeRustChurnMetricsAfterPublicTiming({
    endpoint: 'http://127.0.0.1:1',
    baseline: metricsResponse({ tail_ops_written: 40 }),
    expectedTailOpsDelta: 3,
    fetchMetrics: async () => {
      throw new Error('metrics socket closed');
    }
  });

  assert.equal(observed.persistedReady, null);
  assert.match(observed.error, /metrics socket closed/);
});

test('rust persisted post-public-timing observation records compact error for invalid baseline metrics', async () => {
  const observed = await observeRustChurnMetricsAfterPublicTiming({
    endpoint: 'http://127.0.0.1:1',
    baseline: {
      ok: false,
      statusCode: 503,
      error: 'metrics endpoint unavailable'
    },
    expectedTailOpsDelta: 3,
    fetchMetrics: async () => {
      throw new Error('fetch should not run with invalid baseline');
    }
  });

  assert.equal(observed.persistedReady, null);
  assert.match(observed.error, /rust baseline metrics unavailable before churn/);
  assert.match(observed.error, /status=503/);
});

test('rust persisted post-public-timing observation records compact error for missing baseline tail ops', async () => {
  let fetchCalled = false;
  const observed = await observeRustChurnMetricsAfterPublicTiming({
    endpoint: 'http://127.0.0.1:1',
    baseline: metricsResponse({}),
    expectedTailOpsDelta: 3,
    fetchMetrics: async () => {
      fetchCalled = true;
      return metricsResponse({ tail_ops_written: 43 });
    }
  });

  assert.equal(fetchCalled, false);
  assert.equal(observed.persistedReady, null);
  assert.match(observed.error, /rust baseline metrics missing tail_ops_written before churn/);
});

test('protocol parity gate accepts identical official and candidate buckets', () => {
  const official = parityTarget('official', [parityBucket()]);
  const candidate = parityTarget('rust', [parityBucket()]);
  assert.doesNotThrow(() => assertTargetProtocolParityAgainstOfficial(official, candidate));
});

test('protocol parity gate throws when a single bucket checkpoint checksum differs', () => {
  const official = parityTarget('official', [parityBucket({ checkpointChecksum: 111 })]);
  const candidate = parityTarget('rust', [parityBucket({ checkpointChecksum: 222 })]);
  assert.throws(
    () => assertTargetProtocolParityAgainstOfficial(official, candidate),
    /initial\.checkpointChecksum mismatch/
  );
});

test('protocol parity gate throws when a single bucket PUT digest differs', () => {
  const official = parityTarget('official', [parityBucket()]);
  const candidate = parityTarget('rust', [parityBucket()]);
  candidate.endUser.runs[0].equivalence.buckets[0].entryChecksums.putDigest = 'pd-divergent';
  assert.throws(
    () => assertTargetProtocolParityAgainstOfficial(official, candidate),
    /initial\.putDigest mismatch official=pd-1 candidate=pd-divergent/
  );
});

test('protocol parity gate throws when the candidate is missing an official repeat', () => {
  const official = parityTarget('official', [parityBucket()]);
  const candidate = parityTarget('rust', [parityBucket()]);
  candidate.endUser.runs = [];
  assert.throws(
    () => assertTargetProtocolParityAgainstOfficial(official, candidate),
    /missing repeat 1/
  );
});

test('percentile interpolates linearly between sorted samples', () => {
  assert.equal(percentile([10, 20, 30, 40, 50], 0.5), 30);
  assert.equal(percentile([10, 20, 30, 40, 50], 0.95), 48);
  assert.equal(percentile([42], 0.95), 42);
  assert.equal(percentile([], 0.5), null);
});

test('summarizeSamples excludes partial samples from percentiles but keeps clean passes', () => {
  const samples = [
    { processingMs: 100 },
    { status: 'passed', processingMs: 200 },
    { status: 'partial', processingMs: 99_999, issues: ['readiness timed out'] },
    { status: 'failed', processingMs: 1, error: 'boom' }
  ];
  const summary = summarizeSamples(samples, ['processingMs']);
  assert.equal(summary.processingMs.count, 2);
  assert.equal(summary.processingMs.min, 100);
  assert.equal(summary.processingMs.max, 200);
  assert.equal(summary.health.failedCount, 1);
});

function benchmarkResults({ targets, config }) {
  return {
    profile: 'unit',
    generatedAt: '2026-06-14T00:00:00.000Z',
    config,
    methodology: {
      fairness: {
        interleavedTargets: true,
        endUserColdOpenReadiness: 'unit readiness',
        routedAccess: 'unit routed access'
      }
    },
    targets
  };
}

function benchmarkTargets() {
  return {
    official: benchmarkTarget('official', {
      replicationCatchupMs: metric(70),
      slotAckCatchupMs: metric(120),
      protocolProbeMs: metric(12),
      churnToProtocolVerifiedMs: metric(150)
    }),
    rust: benchmarkTarget('rust', {
      replicationCatchupMs: metric(30),
      slotAckCatchupMs: metric(90),
      protocolProbeMs: metric(10),
      churnToProtocolVerifiedMs: metric(110)
    })
  };
}

function benchmarkTarget(label, churn) {
  return {
    label,
    developerUsability: {},
    lifecycleScenarios: {},
    endUser: {
      summary: {
        processing: {
          processingMs: metric(label === 'official' ? 200 : 100)
        },
        coldOpen: {},
        warmReconnect: {},
        liveChangeToVisible: {},
        churn
      },
      runs: []
    },
    recovery: {}
  };
}

function metric(p50) {
  return {
    p50,
    p95: p50 + 10,
    count: 3
  };
}

function parityBucket(overrides = {}) {
  return {
    bucket: '1#tasks|0[]',
    expectedRows: 10,
    puts: 10,
    removes: 0,
    checkpointCount: 10,
    checkpointChecksum: 123456,
    entryChecksums: {
      clientOperationDigest: 'cod-1',
      putDigest: 'pd-1',
      semanticDigest: 'sd-1'
    },
    ...overrides
  };
}

function parityTarget(label, buckets) {
  return {
    label,
    endUser: {
      runs: [{ repeat: 1, equivalence: { buckets } }]
    }
  };
}

function metricsResponse(metrics, extra = {}) {
  return {
    ok: true,
    statusCode: 200,
    body: {
      metrics,
      ...extra
    }
  };
}

function decodeJwtPayload(token) {
  return JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString('utf8'));
}

function taskRow(overrides = {}) {
  return {
    id: 'task',
    org_id: 'org-001',
    project_id: 'project-org-001-0001',
    owner_id: 'user-target',
    title: 'task',
    status: 'todo',
    priority: 1,
    assignee_id: 'user-target',
    story_points: 1,
    updated_at: '2026-01-01T00:00:00Z',
    summary: 'task',
    ...overrides
  };
}
