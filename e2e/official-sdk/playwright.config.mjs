import { defineConfig } from 'playwright/test';

const harnessPort = process.env.POWERSYNC_HARNESS_PORT ?? '4173';

export default defineConfig({
  testDir: './tests',
  timeout: Number(process.env.POWERSYNC_PLAYWRIGHT_TIMEOUT_MS ?? 600_000),
  fullyParallel: false,
  workers: 1,
  reporter: 'line',
  use: {
    headless: true,
    baseURL: `http://127.0.0.1:${harnessPort}`
  },
  webServer: {
    command: `npm run dev -- --host 127.0.0.1 --port ${harnessPort}`,
    port: Number(harnessPort),
    reuseExistingServer: false,
    stdout: 'pipe',
    stderr: 'pipe',
    timeout: Number(process.env.POWERSYNC_WEB_SERVER_TIMEOUT_MS ?? 60_000)
  }
});
