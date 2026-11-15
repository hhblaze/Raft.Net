# RaftClusterDashboard

Standalone net8.0 ASP.NET backend + React/TypeScript/Ant Design6/Vite client. See the [operations guide](../../docs/CLUSTER-DASHBOARD.md) and [human books](../../docs/human/tutorial/index.html).

```powershell
# From RaftNet8; this also builds UI into wwwroot:
dotnet publish src/RaftClusterDashboard -c Release -o D:/Temp/RaftNet8/dashboard-published
# Real independent-process local stand, with automatic private bootstrap:
./scripts/start-dashboard-stand.ps1 -Voters 3
./scripts/start-dashboard-stand.ps1 -Voters 5 -Secure -DashboardPort 7440
```

Production needs the .NET8 ASP.NET runtime, not Node. Building UI requires Node22.12+ or24+ and npm. The lock file pins actual dependencies. Dev mode: run ASP.NET on7400 and npm ci / npm run dev in ClientApp (Vite proxies API requests to the configured backend). The frontend handles RU/EN, dark/light and mobile layouts. The backend samples actual cluster data; it never uses synthetic stand values.

```powershell
# ClientApp directory
npm ci
npm test
npm run build
$env:PLAYWRIGHT_BROWSERS_PATH='D:/Temp/RaftNet8/playwright-browsers'
npx playwright install chromium
$env:RAFT_DASHBOARD_TEST_SETTINGS='D:/Temp/RaftNet8/owned-stand/browser-test-settings.json'
npx playwright test
```

The private test settings file contains Url and Password for an owned running stand. Never commit it. Browser tests sign in and inspect real observations, mutate entity policies, switch languages/themes and check responsive rendering. ASP.NET security/regression tests live in Raft.Tests, including token order/revocation, entity quotas, MFA/CSRF/roles, copy migration, cancellation after leader loss and diagnostic certificates without quorum. Dashboard accounts/data directory must be private and backed up with the Data Protection key ring.
