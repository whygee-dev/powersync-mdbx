import { expect, test } from 'playwright/test';

test('official web SDK syncs against powersync_benchmark in a real browser', async ({ page }) => {
  const token = requiredEnv('POWERSYNC_TOKEN');
  const table = requiredEnv('POWERSYNC_TABLE');
  const expectedName = process.env.POWERSYNC_EXPECT_NAME ?? 'Ada';
  const timeoutMs = process.env.POWERSYNC_TIMEOUT_MS ?? '30000';
  const harnessPort = process.env.POWERSYNC_HARNESS_PORT ?? '4173';
  const dbFilename = `powersync-web-e2e-${Date.now()}.db`;

  const url = new URL(`http://127.0.0.1:${harnessPort}/`);
  url.searchParams.set('endpoint', `http://127.0.0.1:${harnessPort}/powersync`);
  url.searchParams.set('token', token);
  url.searchParams.set('table', table);
  url.searchParams.set('expectedName', expectedName);
  url.searchParams.set('timeoutMs', timeoutMs);
  url.searchParams.set('dbFilename', dbFilename);

  page.on('console', (message) => {
    console.log(`[browser] ${message.type()}: ${message.text()}`);
  });

  await page.goto(url.toString(), { waitUntil: 'networkidle' });

  const status = page.locator('#status');
  await expect(status).toHaveAttribute('data-status', 'ok', {
    timeout: Number(timeoutMs) + 15_000
  });

  const result = await page.evaluate(() => window.__POWERSYNC_E2E_RESULT__);

  expect(result.status).toBe('ok');
  expect(result.witness).toBe('official-web-sdk-connect-and-apply');
  expect(result.clientImplementation).toBe('rust');
  expect(result.connectionMethod).toBe('http');
  expect(result.account.name).toBe(expectedName);
});

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }

  return value;
}
