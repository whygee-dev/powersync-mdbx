import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

import { expect, test } from 'playwright/test';

import {
  buildBenchmarkFixture,
  DEFAULT_PROFILE,
  TABLES,
  generatedTaskTitle
} from '../src/benchmark-fixture.mjs';

const config = loadConfig();
const fixture = buildBenchmarkFixture(config.profile);
const harnessBaseUrl = `http://127.0.0.1:${config.harnessPort}`;
const proxiedEndpoint = `${harnessBaseUrl}/powersync`;
const requestedScenarios = new Set(
  (process.env.POWERSYNC_BENCHMARK_SCENARIOS ?? 'all')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean)
);

test.describe.configure({ mode: 'serial' });

test('runs the browser benchmark scenarios for one PowerSync target', async ({ browser, page }) => {
  await installPageHooks(page);

  const result = {
    targetLabel: config.targetLabel,
    profile: config.profile,
    generatedAt: new Date().toISOString(),
    dataset: {
      expectedCounts: fixture.expectedCounts,
      targetOrgId: fixture.targetOrgId,
      concurrentClients: config.concurrentClients,
      iterations: config.iterations
    },
    scenarios: {
      coldInitialSync: { samples: [] },
      warmReconnect: { samples: [] },
      insertPropagation: { samples: [] },
      updatePropagation: { samples: [] },
      deletePropagation: { samples: [] },
      batchMixedPropagation: { samples: [] },
      concurrentColdStart: { runs: [] }
    }
  };

  for (let iteration = 1; iteration <= config.iterations; iteration += 1) {
    if (wantsScenario('coldInitialSync') || wantsScenario('warmReconnect')) {
      console.log(`[benchmark] coldInitialSync iteration=${iteration}`);
      const sharedDbFilename = `${config.targetLabel}-cold-${iteration}-${Date.now()}.db`;

      await gotoHarness(page);
      await resetSyncNetworkTelemetry(page);
      await openClient(page, sharedDbFilename);
      const coldConnected = await waitForConnected(page);
      const coldFirstSync = await waitForFirstSync(page);
      const coldSteady = await waitForCounts(page, fixture.expectedCounts);
      const coldTrace = await getTrace(page);
      const coldPhaseMetrics = coldSteady.phaseMetrics ?? (await getPhaseMetrics(page));
      await disconnect(page);
      const coldSyncRequests = await getSyncNetworkTelemetry(page);
      result.scenarios.coldInitialSync.samples.push({
        iteration,
        connectedMs: coldConnected.elapsedMs,
        firstSyncMs: coldFirstSync.elapsedMs,
        steadyMs: coldSteady.elapsedMs,
        counts: coldSteady.counts,
        phaseMetrics: coldPhaseMetrics,
        traceTail: coldTrace.slice(-25),
        syncRequests: coldSyncRequests
      });

      if (wantsScenario('warmReconnect')) {
        console.log(`[benchmark] warmReconnect iteration=${iteration}`);
        await gotoHarness(page);
        await resetSyncNetworkTelemetry(page);
        await openClient(page, sharedDbFilename);
        const warmConnected = await waitForConnected(page);
        const warmSteady = await waitForCounts(page, fixture.expectedCounts);
        const warmTrace = await getTrace(page);
        await disconnect(page);
        const warmSyncRequests = await getSyncNetworkTelemetry(page);
        result.scenarios.warmReconnect.samples.push({
          iteration,
          connectedMs: warmConnected.elapsedMs,
          steadyMs: warmSteady.elapsedMs,
          counts: warmSteady.counts,
          phaseMetrics: warmSteady.phaseMetrics,
          traceTail: warmTrace.slice(-12),
          syncRequests: warmSyncRequests
        });
      }
    }

    if (wantsScenario('insertPropagation')) {
      console.log(`[benchmark] insertPropagation iteration=${iteration}`);
      result.scenarios.insertPropagation.samples.push(await runInsertScenario(page, iteration));
    }
    if (wantsScenario('updatePropagation')) {
      console.log(`[benchmark] updatePropagation iteration=${iteration}`);
      result.scenarios.updatePropagation.samples.push(await runUpdateScenario(page, iteration));
    }
    if (wantsScenario('deletePropagation')) {
      console.log(`[benchmark] deletePropagation iteration=${iteration}`);
      result.scenarios.deletePropagation.samples.push(await runDeleteScenario(page, iteration));
    }
    if (wantsScenario('batchMixedPropagation')) {
      console.log(`[benchmark] batchMixedPropagation iteration=${iteration}`);
      result.scenarios.batchMixedPropagation.samples.push(await runBatchScenario(page, iteration));
    }
  }

  if (wantsScenario('concurrentColdStart')) {
    for (let run = 1; run <= Math.max(1, Math.min(2, config.iterations)); run += 1) {
      console.log(`[benchmark] concurrentColdStart run=${run}`);
      result.scenarios.concurrentColdStart.runs.push(await runConcurrentColdStart(browser, run));
    }
  }

  if (config.resultPath) {
    fs.mkdirSync(path.dirname(config.resultPath), { recursive: true });
    fs.writeFileSync(config.resultPath, JSON.stringify(result, null, 2));
  }

  if (wantsScenario('coldInitialSync')) {
    expect(result.scenarios.coldInitialSync.samples.length).toBe(config.iterations);
  }

  if (wantsScenario('concurrentColdStart')) {
    expect(result.scenarios.concurrentColdStart.runs.length).toBeGreaterThan(0);
  }
});

async function runInsertScenario(page, iteration) {
  const dbFilename = `${config.targetLabel}-insert-${iteration}-${Date.now()}.db`;
  const runtimeId = `${fixture.ids.insertTaskIdPrefix}-${config.targetLabel}-${iteration}`;
  const title = `${fixture.ids.insertTaskTitle} ${config.targetLabel} ${iteration}`;

  await gotoHarness(page);
  await openAndSync(page, dbFilename);

  const visibleMs = await page.evaluate(
    async ({ mutation, expectations, timeoutMs }) => {
      const start = performance.now();
      await window.psBenchmarkMutate(mutation);
      await window.__POWERSYNC_BENCHMARK__.waitForExpectations(expectations, timeoutMs);
      return performance.now() - start;
    },
    {
      mutation: { type: 'insert-task', runtimeId, title },
      expectations: [
        { type: 'count', table: TABLES.tasks, expected: fixture.expectedCounts[TABLES.tasks] + 1 },
        { type: 'field', table: TABLES.tasks, id: runtimeId, field: 'title', expected: title }
      ],
      timeoutMs: config.timeoutMs
    }
  );

  await page.evaluate(
    async ({ mutation }) => {
      await window.psBenchmarkMutate(mutation);
    },
    { mutation: { type: 'restore-insert-task', runtimeId } }
  );

  const trace = await getTrace(page);
  await disconnect(page);

  return {
    iteration,
    visibleMs,
    traceTail: trace.slice(-12)
  };
}

function wantsScenario(name) {
  return requestedScenarios.has('all') || requestedScenarios.has(name);
}

async function runUpdateScenario(page, iteration) {
  const dbFilename = `${config.targetLabel}-update-${iteration}-${Date.now()}.db`;
  const updatedTitle = `${fixture.ids.updateTaskUpdatedTitle} ${config.targetLabel} ${iteration}`;

  await gotoHarness(page);
  await openAndSync(page, dbFilename);

  const visibleMs = await page.evaluate(
    async ({ mutation, expectations, timeoutMs }) => {
      const start = performance.now();
      await window.psBenchmarkMutate(mutation);
      await window.__POWERSYNC_BENCHMARK__.waitForExpectations(expectations, timeoutMs);
      return performance.now() - start;
    },
    {
      mutation: { type: 'update-task', title: updatedTitle },
      expectations: [
        { type: 'field', table: TABLES.tasks, id: fixture.ids.updateTaskId, field: 'title', expected: updatedTitle }
      ],
      timeoutMs: config.timeoutMs
    }
  );

  await page.evaluate(
    async ({ mutation }) => {
      await window.psBenchmarkMutate(mutation);
    },
    { mutation: { type: 'restore-update-task' } }
  );

  await disconnect(page);
  return { iteration, visibleMs };
}

async function runDeleteScenario(page, iteration) {
  const dbFilename = `${config.targetLabel}-delete-${iteration}-${Date.now()}.db`;

  await gotoHarness(page);
  await openAndSync(page, dbFilename);

  const visibleMs = await page.evaluate(
    async ({ mutation, expectations, timeoutMs }) => {
      const start = performance.now();
      await window.psBenchmarkMutate(mutation);
      await window.__POWERSYNC_BENCHMARK__.waitForExpectations(expectations, timeoutMs);
      return performance.now() - start;
    },
    {
      mutation: { type: 'delete-task' },
      expectations: [
        { type: 'count', table: TABLES.tasks, expected: fixture.expectedCounts[TABLES.tasks] - 1 },
        { type: 'exists', table: TABLES.tasks, id: fixture.ids.deleteTaskId, expected: false }
      ],
      timeoutMs: config.timeoutMs
    }
  );

  await page.evaluate(
    async ({ mutation }) => {
      await window.psBenchmarkMutate(mutation);
    },
    { mutation: { type: 'restore-delete-task' } }
  );

  await disconnect(page);
  return { iteration, visibleMs };
}

async function runBatchScenario(page, iteration) {
  const dbFilename = `${config.targetLabel}-batch-${iteration}-${Date.now()}.db`;
  const batchTag = `${config.targetLabel}-${iteration}`;

  await gotoHarness(page);
  await openAndSync(page, dbFilename);

  const expectations = [
    {
      type: 'count',
      table: TABLES.tasks,
      expected:
        fixture.expectedCounts[TABLES.tasks] +
        fixture.profile.batchInsertCount -
        fixture.profile.batchDeleteCount
    },
    {
      type: 'field',
      table: TABLES.tasks,
      id: fixture.ids.batchUpdateIds[0],
      field: 'title',
      expected: batchUpdatedTitle(1)
    },
    {
      type: 'exists',
      table: TABLES.tasks,
      id: fixture.ids.batchDeleteRows[0].id,
      expected: false
    },
    {
      type: 'exists',
      table: TABLES.tasks,
      id: batchInsertedId(batchTag, 1),
      expected: true
    }
  ];

  const visibleMs = await page.evaluate(
    async ({ mutation, expectations, timeoutMs }) => {
      const start = performance.now();
      await window.psBenchmarkMutate(mutation);
      await window.__POWERSYNC_BENCHMARK__.waitForExpectations(expectations, timeoutMs);
      return performance.now() - start;
    },
    {
      mutation: { type: 'batch-mixed', batchTag },
      expectations,
      timeoutMs: config.timeoutMs
    }
  );

  await page.evaluate(
    async ({ mutation }) => {
      await window.psBenchmarkMutate(mutation);
    },
    { mutation: { type: 'restore-batch-mixed', batchTag } }
  );

  await disconnect(page);
  return { iteration, visibleMs };
}

async function runConcurrentColdStart(browser, run) {
  const contexts = [];

  try {
    for (let client = 1; client <= config.concurrentClients; client += 1) {
      const context = await browser.newContext();
      const page = await context.newPage();
      await installPageHooks(page);
      contexts.push({ context, page, client });
    }

    const samples = await Promise.all(
      contexts.map(async ({ page, client }) => {
        const dbFilename = `${config.targetLabel}-fanout-${run}-${client}-${Date.now()}.db`;
        await gotoHarness(page);
        await resetSyncNetworkTelemetry(page);
        await openAndSync(page, dbFilename);
        const trace = await getTrace(page);
        await disconnect(page);
        const syncRequests = await getSyncNetworkTelemetry(page);

        return {
          client,
          connectedMs: metricFromTrace(trace, (entry) => entry.type === 'status' && entry.status?.connected),
          firstSyncMs:
            metricFromTrace(trace, (entry) => entry.type === 'first-sync-complete') ??
            metricFromTrace(trace, (entry) => entry.type === 'status' && entry.status?.hasSynced) ??
            metricFromTrace(trace, (entry) => entry.type === 'expectations-met'),
          steadyMs: metricFromTrace(trace, (entry) => entry.type === 'expectations-met'),
          traceTail: trace.slice(-20),
          syncRequests
        };
      })
    );

    return { run, samples };
  } finally {
    await Promise.all(contexts.map(({ context }) => context.close()));
  }
}

async function resetBenchmarkData() {
  await runSql(resetBenchmarkSql());
}

async function installPageHooks(page) {
  const syncNetworkState = {
    nextRequestId: 1,
    requests: [],
    requestsByRequest: new Map(),
    pendingCaptures: new Set(),
    reset() {
      this.nextRequestId = 1;
      this.requests = [];
      this.requestsByRequest = new Map();
      this.pendingCaptures.clear();
    }
  };
  page.__benchmarkSyncNetworkState = syncNetworkState;

  page.on('worker', (worker) => {
    worker.on('console', async (message) => {
      const args = await Promise.all(
        message.args().map(async (arg) => {
          try {
            return await arg.jsonValue();
          } catch (_error) {
            return await arg.evaluate((value) => String(value)).catch(() => '<unserializable>');
          }
        })
      );
      console.log('[worker-console]', message.type(), message.text(), ...args);
    });
    worker.on('close', () => {
      console.log('[worker-close]', worker.url());
    });
  });

  page.on('console', async (message) => {
    const args = await Promise.all(
      message.args().map(async (arg) => {
        try {
          return await arg.jsonValue();
        } catch (_error) {
          return await arg.evaluate((value) => String(value)).catch(() => '<unserializable>');
        }
      })
    );

    console.log('[page-console]', message.type(), message.text(), ...args);
  });
  page.on('pageerror', (error) => {
    console.log('[page-error]', error.stack ?? error.message ?? String(error));
  });
  page.on('request', (request) => {
    if (request.url().includes('/sync/stream')) {
      const entry = {
        id: syncNetworkState.nextRequestId++,
        url: request.url(),
        method: request.method(),
        startedAt: new Date().toISOString(),
        resourceType: request.resourceType(),
        requestHeaders: pickRequestHeaders(request.headers()),
        requestBodySummary: summarizeSyncRequestBody(request.postData() ?? null),
        timing: null,
        response: null,
        failure: null,
        finishedAt: null
      };
      syncNetworkState.requests.push(entry);
      syncNetworkState.requestsByRequest.set(request, entry);
      console.log(
        '[sync-request]',
        request.method(),
        request.url(),
        'accept=',
        request.headers()['accept'] ?? '',
        request.postData() ?? ''
      );
    }
  });
  page.on('response', async (response) => {
    if (response.url().includes('/sync/stream')) {
      const entry = syncNetworkState.requestsByRequest.get(response.request()) ?? null;
      const capture = (async () => {
        let body = '';
        try {
          body = await response.text();
        } catch (_error) {}

        if (entry) {
          entry.response = {
            observedAt: new Date().toISOString(),
            status: response.status(),
            ok: response.ok(),
            headers: pickResponseHeaders(response.headers()),
            bodySummary: summarizeSyncResponseBody(body)
          };
        }

        console.log('[sync-response]', response.status(), response.url(), body.slice(0, 4000));
      })();
      syncNetworkState.pendingCaptures.add(capture);
      capture.finally(() => {
        syncNetworkState.pendingCaptures.delete(capture);
      });
    }
  });
  page.on('requestfinished', (request) => {
    if (!request.url().includes('/sync/stream')) return;
    const entry = syncNetworkState.requestsByRequest.get(request);
    if (!entry) return;
    entry.finishedAt = new Date().toISOString();
    entry.timing = sanitizeRequestTiming(request.timing?.() ?? null);
  });
  page.on('requestfailed', (request) => {
    if (!request.url().includes('/sync/stream')) return;
    const entry = syncNetworkState.requestsByRequest.get(request);
    if (!entry) return;
    entry.finishedAt = new Date().toISOString();
    entry.failure = request.failure()?.errorText ?? 'request failed';
    entry.timing = sanitizeRequestTiming(request.timing?.() ?? null);
  });

  await page.exposeFunction('psBenchmarkMutate', (mutation) => applyMutation(mutation));
}

async function gotoHarness(page) {
  await page.goto(`${harnessBaseUrl}/benchmark.html`, { waitUntil: 'networkidle' });
  await page.waitForFunction(() => Boolean(window.__POWERSYNC_BENCHMARK__));
}

async function openAndSync(page, dbFilename) {
  try {
    await openClient(page, dbFilename);
    await waitForConnected(page);
    await waitForCounts(page, fixture.expectedCounts);
  } catch (error) {
    const snapshot = await page
      .evaluate(() => window.__POWERSYNC_BENCHMARK__?.debugSnapshot?.().catch?.(() => null) ?? null)
      .catch(() => null);
    const syncRequests = await getSyncNetworkTelemetry(page, { waitForPending: false }).catch(() => []);
    throw new Error(`${error.message}\ndebug=${JSON.stringify(snapshot)}\nsync=${JSON.stringify(syncRequests)}`);
  }
}

async function openClient(page, dbFilename) {
  return await page.evaluate(
    (options) => window.__POWERSYNC_BENCHMARK__.open(options),
    {
      endpoint: proxiedEndpoint,
      token: config.token,
      dbFilename,
      debug: config.debug
    }
  );
}

async function waitForConnected(page) {
  return await page.evaluate((timeoutMs) => window.__POWERSYNC_BENCHMARK__.waitForConnected(timeoutMs), config.timeoutMs);
}

async function waitForFirstSync(page) {
  return await page.evaluate((timeoutMs) => window.__POWERSYNC_BENCHMARK__.waitForFirstSync(timeoutMs), config.timeoutMs);
}

async function waitForCounts(page, expectedCounts) {
  return await page.evaluate(
    ({ expectedCounts, timeoutMs }) => window.__POWERSYNC_BENCHMARK__.waitForCounts(expectedCounts, timeoutMs),
    { expectedCounts, timeoutMs: config.timeoutMs }
  );
}

async function getTrace(page) {
  return await page.evaluate(() => window.__POWERSYNC_BENCHMARK__.getTrace());
}

async function getPhaseMetrics(page) {
  return await page.evaluate(() => window.__POWERSYNC_BENCHMARK__.getPhaseMetrics());
}

async function getCounts(page) {
  return await page.evaluate(() => window.__POWERSYNC_BENCHMARK__.getCounts());
}

async function getSyncNetworkTelemetry(page, { waitForPending = true } = {}) {
  const state = page.__benchmarkSyncNetworkState;
  if (!state) return [];
  if (waitForPending && state.pendingCaptures.size > 0) {
    await Promise.allSettled([...state.pendingCaptures]);
  }

  return state.requests.map((request) => ({
    ...request,
    requestHeaders: request.requestHeaders ? { ...request.requestHeaders } : null,
    requestBodySummary: request.requestBodySummary ? { ...request.requestBodySummary } : null,
    timing: request.timing ? { ...request.timing } : null,
    response: request.response
      ? {
          ...request.response,
          headers: request.response.headers ? { ...request.response.headers } : null,
          bodySummary: request.response.bodySummary ? { ...request.response.bodySummary } : null
        }
      : null
  }));
}

async function resetSyncNetworkTelemetry(page) {
  page.__benchmarkSyncNetworkState?.reset?.();
}

async function disconnect(page) {
  await page.evaluate(() => window.__POWERSYNC_BENCHMARK__.disconnect({ close: true }));
}

function metricFromTrace(trace, predicate) {
  return trace.find(predicate)?.t ?? null;
}

function sanitizeRequestTiming(timing) {
  if (!timing || typeof timing !== 'object') return null;

  return Object.fromEntries(
    Object.entries(timing).map(([key, value]) => [key, Number.isFinite(value) ? Number(value.toFixed(3)) : null])
  );
}

function pickRequestHeaders(headers) {
  return pickHeaders(headers, ['accept', 'content-type']);
}

function pickResponseHeaders(headers) {
  return pickHeaders(headers, ['content-type', 'content-length', 'transfer-encoding', 'x-accel-buffering']);
}

function pickHeaders(headers, keys) {
  const selected = {};
  for (const key of keys) {
    if (headers[key] != null) {
      selected[key] = headers[key];
    }
  }
  return Object.keys(selected).length > 0 ? selected : null;
}

function summarizeSyncRequestBody(body) {
  if (!body) return null;

  try {
    const parsed = JSON.parse(body);
    return {
      clientId: parsed.client_id ?? null,
      binaryData: parsed.binary_data ?? null,
      rawData: parsed.raw_data ?? null,
      bucketCount: Array.isArray(parsed.buckets) ? parsed.buckets.length : null,
      hasBuckets: Array.isArray(parsed.buckets) ? parsed.buckets.length > 0 : null
    };
  } catch {
    return {
      parseError: true,
      bytes: Buffer.byteLength(body, 'utf8')
    };
  }
}

function summarizeSyncResponseBody(body) {
  const trimmed = String(body ?? '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);

  if (trimmed.length === 0) {
    return {
      bytes: 0,
      lineCount: 0,
      firstLineType: null,
      lastLineType: null,
      controlSequence: [],
      dataLineCount: 0,
      dataEntryCount: 0,
      dataOpsPerLine: null,
      parseErrors: 0
    };
  }

  let dataLineCount = 0;
  let dataEntryCount = 0;
  let parseErrors = 0;
  const lineTypes = [];
  const controlSequence = [];

  for (const line of trimmed) {
    let parsed;
    let type = 'unparseable';

    try {
      parsed = JSON.parse(line);
      if ('checkpoint' in parsed) type = 'checkpoint';
      else if ('checkpoint_diff' in parsed) type = 'checkpoint_diff';
      else if ('partial_checkpoint_complete' in parsed) type = 'partial_checkpoint_complete';
      else if ('checkpoint_complete' in parsed) type = 'checkpoint_complete';
      else if ('data' in parsed) type = 'data';
      else if ('token_expires_in' in parsed) type = 'keepalive';
      else type = 'unknown';
    } catch {
      parseErrors += 1;
    }

    lineTypes.push(type);
    if (type !== 'data') {
      controlSequence.push(type);
      continue;
    }

    dataLineCount += 1;
    const entries = parsed?.data?.data;
    if (Array.isArray(entries)) {
      dataEntryCount += entries.length;
    }
  }

  return {
    bytes: Buffer.byteLength(body, 'utf8'),
    lineCount: trimmed.length,
    firstLineType: lineTypes[0] ?? null,
    lastLineType: lineTypes[lineTypes.length - 1] ?? null,
    controlSequence,
    dataLineCount,
    dataEntryCount,
    dataOpsPerLine: dataLineCount > 0 ? Number((dataEntryCount / dataLineCount).toFixed(2)) : null,
    parseErrors
  };
}

function batchInsertedId(batchTag, index) {
  return `${fixture.ids.insertTaskIdPrefix}-${batchTag}-${String(index).padStart(4, '0')}`;
}

function batchUpdatedTitle(index) {
  return `Batch updated row ${String(index).padStart(4, '0')}`;
}

const UPDATE_MUTATION_TIMESTAMP = "TIMESTAMPTZ '2026-02-01T00:00:00Z'";
const UPDATE_RESTORE_TIMESTAMP = "TIMESTAMPTZ '2026-01-01T00:00:00Z'";

async function applyMutation(mutation) {
  switch (mutation.type) {
    case 'insert-task':
      return runSql(insertTaskSql(mutation.runtimeId, mutation.title));
    case 'restore-insert-task':
      return runSql(`DELETE FROM ${TABLES.tasks} WHERE id = '${escapeLiteral(mutation.runtimeId)}';`);
    case 'update-task':
      return runSql(`
        UPDATE ${TABLES.tasks}
        SET title = '${escapeLiteral(mutation.title)}', updated_at = ${UPDATE_MUTATION_TIMESTAMP}
        WHERE id = '${escapeLiteral(fixture.ids.updateTaskId)}';
      `);
    case 'restore-update-task':
      return runSql(`
        UPDATE ${TABLES.tasks}
        SET title = '${escapeLiteral(fixture.ids.updateTaskOriginalTitle)}', updated_at = ${UPDATE_RESTORE_TIMESTAMP}
        WHERE id = '${escapeLiteral(fixture.ids.updateTaskId)}';
      `);
    case 'delete-task':
      return runSql(`DELETE FROM ${TABLES.tasks} WHERE id = '${escapeLiteral(fixture.ids.deleteTaskId)}';`);
    case 'restore-delete-task':
      return runSql(insertTaskRowSql(fixture.ids.deleteTaskRow));
    case 'batch-mixed':
      return runSql(batchMixedSql(mutation.batchTag));
    case 'restore-batch-mixed':
      return runSql(restoreBatchMixedSql(mutation.batchTag));
    default:
      throw new Error(`Unknown mutation type: ${mutation.type}`);
  }
}

function insertTaskSql(runtimeId, title) {
  return insertTaskRowSql({
    id: runtimeId,
    org_id: fixture.targetOrgId,
    project_id: fixture.ids.primaryProjectId,
    title,
    status: 'todo',
    priority: 4,
    assignee_id: fixture.targetUserId,
    story_points: 3,
    updated_at: '2026-01-02T00:00:00Z',
    summary: `runtime-insert:${runtimeId}`,
    owner_id: fixture.targetUserId
  });
}

function resetBenchmarkSql() {
  return `
    DELETE FROM ${TABLES.tasks}
    WHERE id LIKE '${escapeLiteral(fixture.ids.insertTaskIdPrefix)}%';

    UPDATE ${TABLES.tasks}
    SET title = '${escapeLiteral(fixture.ids.updateTaskOriginalTitle)}', updated_at = ${UPDATE_RESTORE_TIMESTAMP}
    WHERE id = '${escapeLiteral(fixture.ids.updateTaskId)}';

    ${insertTaskRowSql(fixture.ids.deleteTaskRow)}

    UPDATE ${TABLES.tasks}
    SET title = CASE id
      ${fixture.ids.batchUpdateIds
        .map((id) => `WHEN '${escapeLiteral(id)}' THEN '${escapeLiteral(generatedTaskTitle(id))}'`)
        .join('\n      ')}
      ELSE title
    END,
    updated_at = ${UPDATE_RESTORE_TIMESTAMP}
    WHERE id IN (${fixture.ids.batchUpdateIds.map((id) => `'${escapeLiteral(id)}'`).join(', ')});

    ${fixture.ids.batchDeleteRows.map((row) => insertTaskRowSql(row)).join('\n')}
  `;
}

function insertTaskRowSql(row) {
  return `
    INSERT INTO ${TABLES.tasks}
      (id, org_id, project_id, title, status, priority, assignee_id, story_points, updated_at, summary, owner_id)
    VALUES
      (
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
      )
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
  `;
}

function batchMixedSql(batchTag) {
  const statements = ['BEGIN;'];

  for (let index = 1; index <= fixture.profile.batchInsertCount; index += 1) {
    statements.push(
      insertTaskSql(batchInsertedId(batchTag, index), `Batch inserted row ${String(index).padStart(4, '0')}`)
    );
  }

  fixture.ids.batchUpdateIds.forEach((id, index) => {
    statements.push(`
      UPDATE ${TABLES.tasks}
      SET title = '${escapeLiteral(batchUpdatedTitle(index + 1))}', updated_at = ${UPDATE_MUTATION_TIMESTAMP}
      WHERE id = '${escapeLiteral(id)}';
    `);
  });

  fixture.ids.batchDeleteRows.forEach((row) => {
    statements.push(`DELETE FROM ${TABLES.tasks} WHERE id = '${escapeLiteral(row.id)}';`);
  });

  statements.push('COMMIT;');
  return statements.join('\n');
}

function restoreBatchMixedSql(batchTag) {
  const statements = ['BEGIN;'];

  for (let index = 1; index <= fixture.profile.batchInsertCount; index += 1) {
    statements.push(`DELETE FROM ${TABLES.tasks} WHERE id = '${escapeLiteral(batchInsertedId(batchTag, index))}';`);
  }

  fixture.ids.batchUpdateIds.forEach((id) => {
    statements.push(`
      UPDATE ${TABLES.tasks}
      SET title = '${escapeLiteral(generatedTaskTitle(id))}', updated_at = ${UPDATE_RESTORE_TIMESTAMP}
      WHERE id = '${escapeLiteral(id)}';
    `);
  });

  fixture.ids.batchDeleteRows.forEach((row) => {
    statements.push(insertTaskRowSql(row));
  });

  statements.push('COMMIT;');
  return statements.join('\n');
}

function runSql(sql) {
  const result = spawnSync(
    'docker',
    [
      'exec',
      '-i',
      config.postgresContainer,
      'psql',
      '-U',
      'postgres',
      '-d',
      config.postgresDatabase,
      '-v',
      'ON_ERROR_STOP=1',
      '-f',
      '-'
    ],
    {
      encoding: 'utf8',
      input: sql
    }
  );

  if (result.status !== 0) {
    throw new Error(`Benchmark SQL failed: ${result.stderr || result.stdout}`);
  }
}

function escapeLiteral(value) {
  return String(value).replace(/'/g, "''");
}

function loadConfig() {
  const profile = process.env.POWERSYNC_BENCHMARK_PROFILE ?? DEFAULT_PROFILE;
  const profileFixture = buildBenchmarkFixture(profile);

  const requestedImplementation = process.env.POWERSYNC_BENCHMARK_CLIENT_IMPLEMENTATION;
  if (requestedImplementation && requestedImplementation !== 'rust') {
    throw new Error(
      `Unsupported POWERSYNC_BENCHMARK_CLIENT_IMPLEMENTATION=${requestedImplementation}; only rust is supported`
    );
  }

  return {
    targetLabel: requiredEnv('POWERSYNC_BENCHMARK_TARGET'),
    token: requiredEnv('POWERSYNC_TOKEN'),
    harnessPort: process.env.POWERSYNC_HARNESS_PORT ?? '4173',
    timeoutMs: Number(process.env.POWERSYNC_TIMEOUT_MS ?? profileFixture.profile.timeoutMs),
    iterations: Number(process.env.POWERSYNC_BENCHMARK_ITERATIONS ?? profileFixture.profile.iterations),
    concurrentClients: Number(
      process.env.POWERSYNC_BENCHMARK_CONCURRENT_CLIENTS ?? profileFixture.profile.concurrentClients
    ),
    profile,
    resultPath: process.env.POWERSYNC_BENCHMARK_RESULT_PATH,
    postgresContainer: process.env.POWERSYNC_POSTGRES_CONTAINER ?? 'powersync_benchmark-postgres-1',
    postgresDatabase: process.env.POWERSYNC_POSTGRES_DATABASE ?? 'powersync_benchmark_test',
    debug: process.env.POWERSYNC_DEBUG === '1'
  };
}

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }

  return value;
}
