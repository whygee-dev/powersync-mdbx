import { PowerSyncDatabase, Schema, Table, column } from '@powersync/web';
import {
  LogLevel,
  SyncClientImplementation,
  SyncStreamConnectionMethod,
  createBaseLogger,
  createLogger
} from '@powersync/common';
import { TABLES } from './benchmark-fixture.mjs';

const schema = new Schema({
  [TABLES.organizations]: new Table({
    id: column.text,
    name: column.text,
    plan: column.text,
    region: column.text,
    updated_at: column.text
  }),
  [TABLES.memberships]: new Table({
    id: column.text,
    org_id: column.text,
    user_id: column.text,
    role: column.text,
    display_name: column.text,
    email: column.text,
    updated_at: column.text
  }),
  [TABLES.projects]: new Table({
    id: column.text,
    org_id: column.text,
    code: column.text,
    name: column.text,
    status: column.text,
    priority: column.integer,
    owner_id: column.text,
    updated_at: column.text,
    summary: column.text
  }),
  [TABLES.tasks]: new Table({
    id: column.text,
    org_id: column.text,
    project_id: column.text,
    title: column.text,
    status: column.text,
    priority: column.integer,
    assignee_id: column.text,
    story_points: column.integer,
    updated_at: column.text,
    summary: column.text
  }),
  [TABLES.comments]: new Table({
    id: column.text,
    org_id: column.text,
    task_id: column.text,
    author_id: column.text,
    body: column.text,
    created_at: column.text,
    updated_at: column.text
  })
});

const statusEl = document.querySelector('#status');
const resultEl = document.querySelector('#result');
const state = {
  db: null,
  subscriptions: [],
  trace: [],
  openedAt: 0,
  config: null,
  phaseMetrics: createPhaseMetrics()
};

window.__POWERSYNC_BENCHMARK__ = {
  open,
  waitForConnected,
  waitForFirstSync,
  waitForCounts,
  waitForExpectations,
  getCounts,
  getRow,
  getStatus,
  getTrace,
  getPhaseMetrics,
  debugSnapshot,
  disconnect,
  reset,
  setResult
};

setStatus('ready');
setResult({ status: 'ready' });

async function reset() {
  await disconnect({ close: true }).catch(() => undefined);
  state.trace = [];
  state.openedAt = 0;
  state.config = null;
  state.phaseMetrics = createPhaseMetrics();
  setStatus('ready');
  setResult({ status: 'ready' });
  return { status: 'ready' };
}

async function open(config) {
  await reset();
  state.config = config;
  state.openedAt = performance.now();
  state.phaseMetrics = createPhaseMetrics();
  state.db = createDb(config);
  pushTrace({ type: 'open', dbFilename: config.dbFilename });
  state.phaseMetrics.createDbMs = elapsedSinceOpen();

  await state.db.waitForReady();
  state.phaseMetrics.waitForReadyMs = elapsedSinceOpen();
  pushTrace({ type: 'db-ready', elapsedMs: state.phaseMetrics.waitForReadyMs });

  pushTrace({ type: 'connect-called' });
  setStatus('connecting');

  await state.db.connect(
    {
      async fetchCredentials() {
        const call = (state.phaseMetrics.fetchCredentialsCallCount ?? 0) + 1;
        const startedAt = performance.now();
        const startedElapsedMs = elapsedSinceOpen();
        state.phaseMetrics.fetchCredentialsCallCount = call;
        state.phaseMetrics.firstFetchCredentialsStartMs ??= startedElapsedMs;
        pushTrace({ type: 'fetch-credentials-start', call, elapsedMs: startedElapsedMs });

        const credentials = { endpoint: config.endpoint, token: config.token };
        const durationMs = roundMs(performance.now() - startedAt);
        const resolvedElapsedMs = elapsedSinceOpen();
        state.phaseMetrics.lastFetchCredentialsDurationMs = durationMs;
        state.phaseMetrics.lastFetchCredentialsResolvedMs = resolvedElapsedMs;
        pushTrace({
          type: 'fetch-credentials-resolved',
          call,
          durationMs,
          elapsedMs: resolvedElapsedMs
        });
        return credentials;
      },
      async uploadData() {}
    },
    {
      connectionMethod: SyncStreamConnectionMethod.HTTP,
      clientImplementation: SyncClientImplementation.RUST
    }
  );
  state.phaseMetrics.connectResolvedMs = elapsedSinceOpen();
  pushTrace({ type: 'connect-resolved', elapsedMs: state.phaseMetrics.connectResolvedMs });

  state.subscriptions = [];

  return {
    openedAt: 0,
    dbFilename: config.dbFilename,
    clientImplementation: 'rust'
  };
}

async function waitForConnected(timeoutMs) {
  await ensureDb();
  const elapsedMs = await waitWithTimeout(
    timeoutMs,
    (signal) => state.db.waitForStatus((status) => status.connected, signal)
  );
  state.phaseMetrics.waitForConnectedMs = elapsedMs;
  pushTrace({ type: 'connected-wait-complete', elapsedMs });
  setStatus('connected');
  return { elapsedMs, status: serializeStatus(state.db.currentStatus) };
}

async function waitForFirstSync(timeoutMs) {
  await ensureDb();
  const elapsedMs = await waitWithTimeout(timeoutMs, (signal) => state.db.waitForFirstSync(signal));
  state.phaseMetrics.firstSyncResolvedMs = elapsedMs;
  pushTrace({ type: 'first-sync-complete' });
  setStatus('first-sync');
  return { elapsedMs, status: serializeStatus(state.db.currentStatus) };
}

async function waitForCounts(expectedCounts, timeoutMs = 60_000) {
  const elapsedMs = await waitForExpectations(
    Object.entries(expectedCounts).map(([table, expected]) => ({ type: 'count', table, expected })),
    timeoutMs
  );

  setStatus('steady');
  return {
    elapsedMs,
    counts: await getCounts(),
    phaseMetrics: getPhaseMetrics()
  };
}

async function waitForExpectations(expectations, timeoutMs = 60_000) {
  await ensureDb();
  const start = performance.now();
  const deadline = start + timeoutMs;
  const expectationPollIntervalMs = resolveExpectationPollIntervalMs();
  state.phaseMetrics.expectationPollIntervalMs = expectationPollIntervalMs;

  let lastSnapshot = null;
  let probeCount = 0;

  while (performance.now() < deadline) {
    probeCount += 1;
    const probeStartedAt = performance.now();
    lastSnapshot = await snapshotExpectations(expectations);
    const probeElapsedMs = performance.now() - probeStartedAt;
    state.phaseMetrics.expectationProbeCount = probeCount;
    state.phaseMetrics.lastExpectationProbeMs = roundMs(probeElapsedMs);

    if (lastSnapshot.ok) {
      const elapsedMs = elapsedSinceOpen();
      state.phaseMetrics.expectationsMetMs = elapsedMs;
      state.phaseMetrics.successfulExpectationProbeMs = roundMs(probeElapsedMs);
      pushTrace({
        type: 'expectations-met',
        elapsedMs,
        expectations,
        probeCount,
        successfulProbeMs: state.phaseMetrics.successfulExpectationProbeMs,
        expectationPollIntervalMs
      });
      return elapsedMs;
    }

    await delay(expectationPollIntervalMs);
  }

  throw new Error(
    `Timed out waiting for expectations: ${JSON.stringify(expectations)} actual=${JSON.stringify(lastSnapshot)}`
  );
}

async function snapshotExpectations(expectations) {
  const snapshot = { ok: true, checks: [] };

  for (const expectation of expectations) {
    if (expectation.type === 'count') {
      const actual = await countRows(expectation.table);
      snapshot.checks.push({ ...expectation, actual });
      snapshot.ok &&= actual === expectation.expected;
      continue;
    }

    if (expectation.type === 'field') {
      const row = await readRow(expectation.table, expectation.id);
      const actual = row?.[expectation.field] ?? null;
      snapshot.checks.push({ ...expectation, actual });
      snapshot.ok &&= actual === expectation.expected;
      continue;
    }

    if (expectation.type === 'exists') {
      const row = await readRow(expectation.table, expectation.id);
      const actual = Boolean(row);
      snapshot.checks.push({ ...expectation, actual });
      snapshot.ok &&= actual === expectation.expected;
      continue;
    }

    throw new Error(`Unknown expectation type: ${expectation.type}`);
  }

  return snapshot;
}

async function getCounts() {
  await ensureDb();
  const counts = {};

  for (const table of Object.values(TABLES)) {
    counts[table] = await countRows(table);
  }

  return counts;
}

async function getRow(table, id) {
  await ensureDb();
  return await readRow(table, id);
}

async function getStatus() {
  await ensureDb();
  return serializeStatus(state.db.currentStatus);
}

async function getTrace() {
  return state.trace.slice();
}

function getPhaseMetrics() {
  return { ...state.phaseMetrics };
}

async function debugSnapshot() {
  await ensureDb();

  const tablesResult = await state.db.execute(
    "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
  );
  const tableNames = (tablesResult?.rows?._array ?? []).map((row) => row.name);
  const counts = {};

  for (const table of tableNames.filter((name) => !name.startsWith('sqlite_'))) {
    try {
      const result = await state.db.execute(`SELECT count(*) AS count FROM "${table}"`);
      counts[table] = Number(firstRow(result)?.count ?? 0);
    } catch (error) {
      counts[table] = `error:${error.message ?? error}`;
    }
  }

  const sample = {};

  for (const table of [...Object.values(TABLES), 'ps_buckets', 'ps_oplog', 'ps_sync_state'].filter((value, index, arr) => arr.indexOf(value) === index)) {
    try {
      const result = await state.db.execute(`SELECT * FROM "${table}" LIMIT 5`);
      sample[table] = result?.rows?._array ?? [];
    } catch (error) {
      sample[table] = { error: error.message ?? String(error) };
    }
  }

  return {
    status: serializeStatus(state.db.currentStatus),
    tableNames,
    counts,
    sample,
    trace: state.trace.slice(-25)
  };
}

async function disconnect(options = {}) {
  if (!state.db) return { closed: true };

  const close = options.close ?? true;

  try {
    await Promise.all(
      state.subscriptions.map(async (subscription) => {
        if (!subscription?.unsubscribe) return;
        try {
          await subscription.unsubscribe();
        } catch (_error) {}
      })
    );
    state.subscriptions = [];
    await state.db.disconnect().catch(() => undefined);
    if (close) {
      await state.db.close({ disconnect: false }).catch(() => undefined);
    }
  } finally {
    state.db = null;
  }

  pushTrace({ type: 'disconnect', close });
  setStatus('disconnected');
  return { closed: true };
}

function createDb(config) {
  if (config.debug) {
    createBaseLogger().setLevel(LogLevel.TRACE);
  }

  const db = new PowerSyncDatabase({
    schema,
    database: {
      dbFilename: config.dbFilename
    },
    logger: config.debug ? createLogger(`benchmark-${config.dbFilename}`, { logLevel: LogLevel.TRACE }) : undefined,
    flags: {
      enableMultiTabs: false
    }
  });

  db.registerListener({
    statusChanged(status) {
      pushTrace({ type: 'status', status: serializeStatus(status) });
    },
    statusUpdated(update) {
      pushTrace({ type: 'status-updated', update: serializeStatus(update) });
    },
    initialized() {
      pushTrace({ type: 'initialized' });
    }
  });

  if (config.debug) {
    const adapter = db.bucketStorageAdapter;
    if (adapter?.control && !adapter.__benchmarkControlWrapped) {
      const originalControl = adapter.control.bind(adapter);
      adapter.control = async (op, payload) => {
        const response = await originalControl(op, payload);
        const payloadPreview =
          payload == null
            ? null
            : typeof payload === 'string'
              ? payload.slice(0, 200)
              : `<bytes:${payload.byteLength ?? payload.length ?? 0}>`;
        const responsePreview = typeof response === 'string' ? response.slice(0, 600) : String(response);

        console.log('[powersync-control]', op, payloadPreview, responsePreview);
        return response;
      };
      adapter.__benchmarkControlWrapped = true;
    }
  }

  return db;
}

async function countRows(table) {
  const result = await state.db.execute(`SELECT count(*) AS count FROM "${table}"`);
  const row = firstRow(result);
  return Number(row?.count ?? 0);
}

async function readRow(table, id) {
  try {
    const result = await state.db.execute(`SELECT * FROM "${table}" WHERE id = ? LIMIT 1`, [id]);
    return firstRow(result);
  } catch (error) {
    if (String(error?.message ?? error).includes('no such table')) {
      return null;
    }

    throw error;
  }
}

function firstRow(result) {
  return result?.rows?._array?.[0] ?? result?.rows?.[0] ?? null;
}

function createPhaseMetrics() {
  return {
    createDbMs: null,
    waitForReadyMs: null,
    firstFetchCredentialsStartMs: null,
    lastFetchCredentialsResolvedMs: null,
    lastFetchCredentialsDurationMs: null,
    fetchCredentialsCallCount: 0,
    connectResolvedMs: null,
    waitForConnectedMs: null,
    firstSyncResolvedMs: null,
    expectationsMetMs: null,
    lastExpectationProbeMs: null,
    successfulExpectationProbeMs: null,
    expectationProbeCount: 0,
    expectationPollIntervalMs: null
  };
}

function resolveExpectationPollIntervalMs() {
  const configured = Number(state.config?.expectationPollIntervalMs);
  if (Number.isFinite(configured) && configured > 0) {
    return Math.max(5, Math.floor(configured));
  }

  return 25;
}

function elapsedSinceOpen() {
  return roundMs(performance.now() - state.openedAt);
}

function roundMs(value) {
  return Number(value.toFixed(3));
}

function serializeStatus(status) {
  if (!status) return null;

  const raw = typeof status.toJSON === 'function' ? status.toJSON() : status;
  const dataFlow = status.dataFlowStatus ?? raw?.dataFlow ?? status.dataFlow ?? {};
  const downloadError = dataFlow.downloadError ?? null;
  const uploadError = dataFlow.uploadError ?? null;

  return {
    connected: Boolean(status.connected),
    connecting: Boolean(status.connecting),
    hasSynced: Boolean(status.hasSynced),
    lastSyncedAt: status.lastSyncedAt ? new Date(status.lastSyncedAt).toISOString() : null,
    downloading: Boolean(dataFlow.downloading),
    uploading: Boolean(dataFlow.uploading),
    downloadError:
      downloadError == null
        ? null
        : {
            name: downloadError.name ?? null,
            message: downloadError.message ?? String(downloadError),
            stack: downloadError.stack ?? null
          },
    uploadError:
      uploadError == null
        ? null
        : {
            name: uploadError.name ?? null,
            message: uploadError.message ?? String(uploadError),
            stack: uploadError.stack ?? null
          },
    raw: raw ?? null
  };
}

function pushTrace(entry) {
  state.trace.push({
    t: Number((performance.now() - state.openedAt).toFixed(3)),
    ...entry
  });

  if (state.trace.length > 250) {
    state.trace.shift();
  }
}

async function ensureDb() {
  if (!state.db) {
    throw new Error('Benchmark harness database is not open');
  }
}

async function waitWithTimeout(timeoutMs, callback) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('timeout')), timeoutMs);

  try {
    await callback(controller.signal);
    return elapsedSinceOpen();
  } finally {
    clearTimeout(timer);
  }
}

function setStatus(status) {
  statusEl.textContent = status;
  statusEl.dataset.status = status;
}

function setResult(payload) {
  resultEl.textContent = JSON.stringify(payload, null, 2);
  window.__POWERSYNC_BENCHMARK_RESULT__ = payload;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
