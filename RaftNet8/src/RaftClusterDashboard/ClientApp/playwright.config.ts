import { defineConfig } from '@playwright/test';
import path from 'node:path';
export default defineConfig({
  testDir: './tests', timeout: 180_000, workers: 1, fullyParallel: false,
  outputDir: process.env.RAFT_DASHBOARD_TEST_OUTPUT ?? path.join(process.env.RAFT_EXPERIMENT_ROOT ?? 'D:/Temp/RaftNet8', 'dashboard-browser-results-' + Date.now()),
  expect: { timeout: 15_000 },
  reporter: 'list', use: { browserName: 'chromium', viewport: { width: 1500, height: 1050 }, screenshot: 'only-on-failure', actionTimeout: 15_000, navigationTimeout: 15_000 },
});
