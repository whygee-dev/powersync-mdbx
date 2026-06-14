import { PowerSyncDatabase, Schema, Table, column } from '@powersync/web';
import { SyncClientImplementation, SyncStreamConnectionMethod } from '@powersync/common';

const params = new URLSearchParams(window.location.search);

const endpoint = requiredParam('endpoint');
const token = requiredParam('token');
const tableName = requiredParam('table');
const expectedName = params.get('expectedName') ?? 'Ada';
const dbFilename = params.get('dbFilename') ?? `powersync-web-e2e-${Date.now()}.db`;
const timeoutMs = Number(params.get('timeoutMs') ?? '30000');
const debug = params.get('debug') === '1';

const statusEl = document.querySelector('#status');
const resultEl = document.querySelector('#result');

const schema = new Schema({
  [tableName]: new Table({
    id: column.text,
    name: column.text
  })
});

const db = new PowerSyncDatabase({
  schema,
  database: {
    dbFilename
  },
  flags: {
    enableMultiTabs: false
  }
});

const connector = {
  async fetchCredentials() {
    return { endpoint, token };
  },
  async uploadData() {}
};

if (debug) {
  db.registerListener({
    statusChanged(status) {
      console.log('[powersync-web-e2e] statusChanged', status);
    },
    statusUpdated(update) {
      console.log('[powersync-web-e2e] statusUpdated', update);
    }
  });
}

boot().catch(async (error) => {
  await db.disconnect().catch(() => undefined);
  setResult({
    status: 'error',
    message: error instanceof Error ? error.message : String(error)
  });
});

async function boot() {
  setStatus('connecting');

  await db.connect(connector, {
    connectionMethod: SyncStreamConnectionMethod.HTTP,
    clientImplementation: SyncClientImplementation.RUST
  });

  setStatus('waiting');
  await waitForAccountName(expectedName, timeoutMs);

  const account = await readAccount();

  await db.disconnect().catch(() => undefined);

  setResult({
    status: 'ok',
    witness: 'official-web-sdk-connect-and-apply',
    clientImplementation: SyncClientImplementation.RUST,
    connectionMethod: SyncStreamConnectionMethod.HTTP,
    tableName,
    account
  });
}

async function waitForAccountName(name, timeout) {
  const deadline = Date.now() + timeout;

  while (Date.now() < deadline) {
    const account = await readAccount();
    if (account?.name === name) {
      return;
    }

    await delay(250);
  }

  throw new Error(`Timed out waiting for account name ${name}`);
}

async function readAccount() {
  try {
    const result = await db.execute(`SELECT * FROM "${tableName}" ORDER BY id LIMIT 1`);
    return result.rows?._array?.[0] ?? null;
  } catch (error) {
    if (String(error?.message ?? error).includes('no such table')) {
      return null;
    }

    throw error;
  }
}

function setStatus(status) {
  statusEl.textContent = status;
  statusEl.dataset.status = status;
}

function setResult(payload) {
  setStatus(payload.status);
  resultEl.textContent = JSON.stringify(payload, null, 2);
  window.__POWERSYNC_E2E_RESULT__ = payload;
}

function requiredParam(name) {
  const value = params.get(name);
  if (!value) {
    throw new Error(`Missing required query param: ${name}`);
  }

  return value;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
