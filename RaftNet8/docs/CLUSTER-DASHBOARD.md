# RaftClusterDashboard

RaftClusterDashboard is a standalone .NET8 ASP.NET application with React19, TypeScript, Ant Design6 and Vite8. It serves its own built UI and connects to multiple clusters through a backend. It is never a voter or a RaftClient subscriber. Browser sessions do not create additional cluster collectors. See [source](../src/RaftClusterDashboard), [RU book](human/tutorial/book-ru.html#dashboard) and [English book](human/tutorial/book-en.html#dashboard).

## Local stand

PowerShell7, .NET8 runtime/SDK and Node.js22.12+ or24+ are needed to build. Production needs only the published ASP.NET application and .NET8 ASP.NET runtime. The pinned Node lock file is in ClientApp. All .NET projects target net8.0; DBreeze remains1.142.2026.1016-beta.

From RaftNet8:

```powershell
./scripts/start-dashboard-stand.ps1 -Voters 3
# Separate private-PKI stand; use other ports when the first stand is still running:
./scripts/start-dashboard-stand.ps1 -Voters 5 -Secure -DashboardPort 7440
```

The script creates an owned directory under D:\Temp\RaftNet8 and asks for the first dashboard administrator password. Use at least12 characters, including A–Z, a–z, a digit and a non-alphanumeric character. Identity validates it before cluster nodes start; a rejected interactive entry shows the missing requirements and prompts again. For unattended runs, supply RAFT_DASHBOARD_BOOTSTRAP_PASSWORD through the host environment. An invalid environment password fails immediately rather than waiting for input.

After creating the account, the script starts one process per voter plus two joining processes, bootstraps the root cluster credential, imports the connection and starts the dashboard. It stays attached; Ctrl+C stops only its own children. It retains the databases, logs, certificate files and process manifest. It never starts remote processes. Existing user data is not reused or removed. Ports6101+/7101+/7201+ must be available or be overridden together. In the browser sign in as admin. Use Add with n4/n5 for the3-voter stand, n6/n7 for5 voters; peer/client/management base ports increment by node number minus one.

HTTP/plain TCP is an explicit supported local profile. The default dashboard listens on127.0.0.1:7400; a node's management API defaults to loopback7201. Publishing externally requires an explicit host configuration. HTTP exposes passwords and bearer tokens to anyone able to observe that network: use the HTTPS profile across untrusted networks.

The secure lab creates an isolated CA, HTTPS server, nine ID-bound peer certificates and a separate dashboard diagnostic certificate. Nothing is installed in the system trust store. A human browser must explicitly trust the lab CA or use a browser profile configured for it. This laboratory CA expires in30 days; leaf certificates in14 days, with no CRL service. Production uses host-supplied certificates, private keys, CA/rotation/revocation policy. Do not copy lab keys into a deployment. Windows SChannel uses temporary OS-backed imported keys, released when their certificates are disposed. Secure profile requires Admin/Operator authenticator enrollment before management is enabled.

## Production hosting and accounts

```powershell
dotnet publish src/RaftClusterDashboard -c Release -o D:/Temp/RaftNet8/dashboard-published
# Run CLI setup while the dashboard process is stopped, with the same configuration and OS account:
dotnet D:/Temp/RaftNet8/dashboard-published/RaftClusterDashboard.dll bootstrap-admin `
  --username admin --Dashboard:DataDirectory D:/RaftDashboard
dotnet D:/Temp/RaftNet8/dashboard-published/RaftClusterDashboard.dll `
  --urls http://127.0.0.1:7400 --Dashboard:DataDirectory D:/RaftDashboard
```

bootstrap-admin reads RAFT_DASHBOARD_BOOTSTRAP_PASSWORD or stdin; never pass a password as a CLI argument. It refuses to run when an account exists. Use a private directory accessible only to the service account. On Windows the Data Protection key ring is protected with DPAPI for that account; on other platforms protect its persisted key ring using host filesystem/encryption controls. Back up keys with the dashboard database: losing them loses access to saved credentials and MFA keys. A different OS account cannot automatically decrypt DPAPI keys. Do not run two dashboard processes over one DBreeze directory.

CLI exit code2 means password validation failed: its requirements are written to stderr without the submitted password, and no account is created. A subsequent attempt can use the same data directory. Other bootstrap failures retain their original diagnostics and are not treated as password input errors.

ASP.NET Identity supplies password hashing, five-attempt lockout for15 minutes, TOTP and ten single-use recovery codes. Codes are stored hashed; authenticator keys are protected. SecureProfile requires TOTP for Admin/Operator. RequirePrivilegedMfa also enables this rule locally. Sessions last8 hours with sliding expiration, use HttpOnly/SameSiteStrict cookies and Secure in HTTPS mode. Every mutation, including login, requires the same-origin antiforgery cookie/header pair. Login/MFA attempts are limited to10/minute/IP. Changing account grants invalidates the security stamp; an existing SSE session is terminated too. Sensitive account/profile/token changes require authentication within5 minutes. Log out and sign in again to reauthenticate. Preserve recovery codes offline; there is no unauthenticated network password/MFA reset.

Viewer sees metadata for assigned clusters. Operator additionally changes nodes/entities. Admin manages profiles, tokens and local accounts. Admin covers all profiles; other roles use explicit ClusterIds. Payload preview is a separate CanReadPayload grant, including for Admin. Preview reads at most4096 bytes, displays escaped text/base64, and never executes payload HTML. Full data retrieval remains a RaftClusterClient operation requiring ReadPayload.

Use an HTTPS URL and Kestrel host certificate when Dashboard:SecureProfile=true. Invalid plaintext configuration fails startup. Reverse proxies must preserve the intended secure origin and cookie semantics; trusting forwarded headers is a host deployment decision, not enabled automatically. Token/secret-bearing API responses have no-store. Browser localStorage contains only language/theme/selected profile, never service credentials.

## Cluster host and bootstrap

RaftClusterNode defaults to cluster-v2. Configure ClientServer.Management with the node's separate management URL, original management endpoints and a host-generated random BootstrapSecret (32+ characters). SecureTransport=true requires HTTPS client/management origins and TlsRaftTransportSecurity for peers before any database is created. It defaults RequireClientTokens to true. Certificate material and server callbacks are supplied by the host, never journaled. DemoSecurity is a complete private-PKI host example; its explicit root trust still verifies HTTPS hostname and Raft peer-ID binding.

For the first administrative credential, call RaftClusterAdminClient.BootstrapAsync(hostSecret, stableOperationId) locally against the management seeds. Bootstrap is loopback-only, checks every voter's application/security capability and commits once. The returned opaque256-bit bearer secret is displayed once. No anonymous administration becomes available when ordinary client tokens are optional. A bootstrap retry with the same ID returns metadata with Secret=null. If that response was lost, use another already-authorized administrator to revoke/replace the token. A new empty cluster can instead be reprovisioned offline before storing user data; there is no unsafe quorum bypass or automatic extraction of a lost secret. Rotate the default30-day bootstrap credential before it expires. Avoid revoking/expiring the last usable administrative credential: recovery otherwise requires an application-specific offline procedure and is not a network API.

The bounded /capabilities protocol probe advertises only the configured node/group/application/security profile and routes, so a not-yet-joined node can be checked before it has replicated token state. It requires the group header and does not expose resources, ledger contents or management actions. That public protocol metadata is distinct from authenticated administrative state. Keep management listeners on the intended interface and network; HTTPS/mTLS validation and logged membership still determine which identities can participate.

Import a protected connection file through the local CLI or enter its secret in the Admin connection form. The CLI import file contains Name, GroupId, ManagementEndpoints, Credential and optional CertificateAlias. Never put that file in source control. At rest only the dashboard backend can decrypt its stored credential. Diagnostic certificate aliases resolve to host configuration:

```json
{
  "Dashboard": {
    "SecureProfile": true,
    "DataDirectory": "D:/RaftDashboard",
    "DiagnosticCertificates": {
      "production": {
        "Path": "D:/Secrets/dashboard-client.pfx",
        "Password": "supply-through-host-secret-configuration",
        "TrustedRootPath": "D:/Secrets/private-ca.cer"
      }
    }
  }
}
```

TrustedRootPath is optional; without it the backend uses platform trust. A configured private root is used only for that profile, with hostname/EKU/chain/time checks; this minimal private-root profile does not perform CRL/OCSP retrieval. If revocation checking is required, use the host trust infrastructure and rotate/revoke diagnostic thumbprints through deployment. Never disable certificate validation as a production shortcut. Browser-supplied aliases cannot open arbitrary certificate files.

## Tokens and ordered policies

RaftClusterClientOptions.AccessTokenProvider is called for initial discovery and every HTTP attempt/reconnect. One client and connection pool serves many entities. A security token is not a subscriber identity and never changes durable cursor ownership. Keep effects and cursor atomic in the subscriber's own database.

Tokens are rc2.ID.secret, with32 cryptographically random secret bytes. The cluster stores only ID, SHA256 hash, permissions, entity scopes, expiry, revocation and revision. All mutations require quorum, become ordered application entries and survive failover/snapshots. The ledger never stores the bearer secret. Minting requires ManageTokens for the whole group. Permissions are Discover, Publish, ReadMetadata, ReadPayload, CreateEntity, Observe, ManageEntities, ManageNodes and ManageTokens. Scopes are explicit names or ["*"]. Discover reveals routing for an entity-scoped token; cluster-wide diagnostics/administration needs a whole-group scope. Entity policies can be scoped to a name. Ordinary metadata/payload reads check permission before body work and again after ReadIndex. Writes check admission and again at their apply position. A revocation ordered earlier rejects a later write; previously ordered writes may complete. Expiry uses the leader's admission timestamp for deterministic replay, so synchronize host clocks; expiry is not a Byzantine clock guarantee.

Repeat a token-issue operation ID with the same grant to recover metadata after a lost response; a different grant conflicts. Secrets cannot be recovered. Revoke the unusable token and issue a new one. Publication idempotence binds the admitted credential identity and business content, while ignoring the admission timestamp: after an uncertain write, retry with the same request ID and credential; after credential rotation query the old outcome through application recovery rather than inventing a successful result. Cancellation/timeouts never imply noncommit.

Create entity and configure pause, maximum message size and logical retained-byte quota through the admin SDK/UI. They commit through Raft. Quotas are checked atomically with message ID advancement and payload storage; concurrent writers cannot overshoot. Lowering a quota below current usage retains history and blocks new excess writes. Standard accumulates all payloads; HighSpeedCache retains the last durable payload, so its quota applies to the replacement value. Both have bounded RAM caching; no memory-only message stream is added. No entity deletion, counter reset or mode conversion exists.

## Membership and operations

The admin SDK can start from a single authenticated follower seed. A not-leader response includes a bounded leader-address hint only after local Observe authorization; the SDK verifies the origin syntax and rejects HTTPS-to-HTTP downgrade. This is a routing hint, not a quorum-fenced state/resource observation. Status, payload access and management still require their normal authorization/barriers. RefreshDirectoryAsync publishes the current full management directory. A client offline through complete address turnover still needs a reachable seed/stable bootstrap.

Add requires an already-running JoiningNode with identical original bootstrap Members/InitialPeers, correct group, cluster-v2 and matching security profile. The backend checks advertised ID/peer/client/management routes before accepting intent. It does not launch the process. The durable operation registers routes, adds learner, catches it up and promotes it. Remove of the current leader uses existing automatic transfer; confirmed transfer failure never forces self-removal. Only one app administrative control operation occupies its persisted lane. Normal publications continue during catch-up.

The browser generates one operation ID per submitted dialog, reuses it for a retry and displays it for copying. Accepted operations live in cluster state, survive browser/dashboard/leader loss, and reappear in recent operations. Use GetOperationAsync(id) for older results. Explicit Cancel applies only to Add before final promotion has begun, journals cleanup of an operation-created learner/routes and frees the lane after cleanup. Too-late cancellation keeps the operation and reports its stage. Snapshot and manual transfer use the same durable administrative lane. Direct embedded RaftNode methods are privileged host APIs; expose the authenticated coordinator to remote administrators.

## Measurements and resource budgets

One backend collector polls every second, with at most4 clusters and8 node diagnostics requests concurrently. Management metadata/directory refresh every5 seconds. Each node samples process/host resources every second and scans its own folders every30 seconds, skipping reparse points and stopping after100000 files (unknown size if exceeded). Node diagnostic and management lanes admit8/16 concurrent requests, no unbounded waiting queue; excess gets429. The management body/header/connection limits are64KiB/16KiB/128. Data gateway bounds remain independent (default4 active requests,16,000,000-byte message,32MiB response/cache,128 entries/batch/cache,4096entities,1024 historical peer IDs/endpoints).

GetOverviewAsync combines status, directory, entity policies/counters, recent operations, the caller's credential expiry and the leader's cached diagnostics behind one authorized ReadIndex barrier. The collector uses it for its metadata refresh and reuses that leader diagnostic for the same poll. Other nodes retain their independent diagnostic authorization. This reduces control requests without changing sampling intervals, write durability or Raft batching timers. The response is an observation after a read barrier, not an atomic application snapshot: concurrent commits may advance individual counters while it is assembled.

Management routes are published as a bounded immutable view after administrative commit or snapshot restoration; routine diagnostic routing does not open a database transaction. Simultaneous management observations collect for2ms before starting one fresh ReadIndex. A request arriving after that read starts waits for the following fresh read, so no completed fence or read lease is reused. There is at most one in-flight and one waiting observation batch. Each caller still rechecks authorization after its barrier; cancelling one waiter does not cancel the others. BatchObservationReads=false disables this collection for diagnostics/comparison. It is a deployment setting and does not change proposal batching or the1s sampling interval. The backend SDK admits8 requests to match the global8-diagnostic lane and avoid splitting a five-node poll into4+1 waves.

Default dashboard bounds:32 profiles,128 observed nodes/profile (configurable up to1024),64 SSE clients (up to256),600 detailed points/profile (60..3600),7 days minute history (1..30),400000 total minute rows (1000..1000000),1024 accounts and10000 local audit rows. One queue slot per SSE client drops old notifications, not cluster messages. Slow browsers receive the latest observation. Persistent results, tokens, cluster audit, request deduplication and Standard history have no TTL; disk usage continues growing even for HighSpeedCache request metadata. Plan capacity and snapshots. Cluster metadata history is queried by prefix/index/page, never scanned for each publication.

Minute history expires at the earlier of its age and row budget; the total row budget is divided between saved profiles. The default400000 rows allow all32 profiles to retain a full week. A deliberately lower100000-row budget would retain about3125 minutes per profile at32 profiles; a configured row cap can shorten the chosen age retention. Pruning records bounds retained chart data; it does not promise that a DBreeze file immediately shrinks on the filesystem.

Publication throughput uses one current leader's replicated entity counters, never sums replicas. p99 merges compatible bucket counts; it is the bucket upper bound, not an average of percentiles. The publication histogram covers service quorum/apply latency and excludes discovery/full HTTP/client scheduling; benchmark reports include separate end-to-end samples. Core meter durations are reported as count/total and mean milliseconds; they are not mislabeled p99. Histograms reset on process epoch change. Gauges expose term/frontiers/peer lag separately.

Client API4xx/5xx is a cumulative application-level response counter since node telemetry startup, including admission429 and leader redirects409. The overview sums observed gateways; the node drawer shows its own count. It counts HTTP responses, not failed logical publications or replicated errors. TLS/protocol rejections before the application handler are outside this counter.

Process working set is deduplicated by HostId+PID, available physical host RAM and volume capacity by HostId+VolumeId when aggregating. Shared data roots across nodes are unsupported; physical node file bytes are distinct from logical retained payload bytes. HostId defaults to the host identity and should be stable/unique. Windows reports physical host RAM; Linux reads /proc/meminfo (host view, not an enforced container memory budget). A graph gap/— means no compatible measurement. Last observations remain stamped/stale after failures. A node ping never proves quorum. File sample age is shown independently. No-quorum diagnostics requires a separately trusted dashboard mTLS certificate thumbprint on every host; it cannot read messages or mutate anything. Without that certificate the dashboard keeps cached observations until a fenced read is possible.

Effective config displays host-supplied safe scalar limits. Core timers, transport security, trust roots, body/queue/cache budgets and resource intervals are deployment settings requiring restart. Tokens/entity policies change live. Entity activity uses successive committed catalog counters over the actual metadata sampling interval (5s by default); its first value is unknown. Certificate/private-key contents and credentials are excluded from reports. Diagnostic export contains only the metadata already visible to the signed-in user. Before switching an existing dashboard deployment to SecureProfile, edit every saved profile to HTTPS; startup rejects stored plaintext routes rather than forwarding a service credential to them.

Collection intervals are configurable deployment settings: node Management.SampleIntervalSeconds=1 (1..60), FileScanIntervalSeconds=30 (1..3600); dashboard PollIntervalSeconds=1 (1..60), DirectoryRefreshSeconds=5 (1..300). The UI uses the advertised sample interval when identifying stale observations. DetailedHistorySeconds bounds time as well as count. Increasing these intervals reduces monitoring frequency without changing Raft timers or client acknowledgment.

## Storage and copy migration

Core remains wire3/raft8-v4. Application snapshots/ledger have explicit cluster-v2 identity. RaftClusterNode wraps peer streams in ClusterApplicationTransport: a cluster-v2 connection preamble is exchanged outside core frames, inside configured TLS. Old/new application sessions are rejected before an RPC is handled. Custom embedded v2 message hosts must use the same wrapper. Data API remains /raft-cluster/v1 for existing optional-token clients; management is /raft-cluster/admin/v1. Keep compatibility cluster-v1 fixtures; never run v1/v2 applications together. New administrative bootstrap/Add also checks capability/version/profile. Offline migration is whole-group, not a rolling upgrade.

Stop every process and preserve an independent backup. For each original node root, use a new destination:

```powershell
dotnet run --project HumanTest/Raft.Cluster.Demo -c Release -- migrate-cluster-v2 `
  --source D:/OldCluster/n1 --destination D:/NewCluster/n1 --options D:/OldCluster/n1/node-options.json
```

Supply the node's original serialized RaftOptions (same node/group/bootstrap identity); if an older deployment did not save this file, export it from its host configuration. CopyNodeV1ToV2 takes read-only ownership of all source files, rejects a live engine, nested/existing destinations, reparse paths, pending installs and unsafe image placement. It copies and SHA256-verifies files, validates retained payload lengths, counters, checkpoints and completed snapshot images, rewrites only the copied application generations/images and updates the referenced snapshot digest. Core wire/storage are not converted. Source bytes remain unchanged. Converter bounds are32768 files,1MiB metadata per file; disk preflight3x source+64MiB. Images must use the standard node-root/images location. A failed destination is incomplete: retain it for diagnosis and retry in a new directory. Verify the returned report, start the new cluster as a group and test publication/ReadIndex/snapshot join before retiring source backups. Latest-cache historical byte counts start at currently retained bytes because discarded payload sizes cannot be reconstructed.

Each relevant DBreeze database uses one physical table with numeric ToIndex prefixes. Cluster prefixes separate entities, headers, payloads, requests, endpoints, service flags, policies, token hashes, operations, audit and operation index. The dashboard's separate database holds accounts, protected connection profiles, graph aggregates and local human audit. Cluster service state and application messages share one atomic checkpoint and snapshot boundary. Dashboard accounts/charts are backed up separately; they are not replicated by Raft. Local human audit links the signed-in user to the path and operation ID; replicated audit identifies the backend's cluster credential and operation ID.

TLS protects transport. This version does not encrypt message payloads, technical logs or snapshot files at rest; use host volume/backup encryption or application-encrypted payloads when required. Token secrets are absent from replicated storage, while dashboard credentials and MFA keys use Data Protection and account passwords use Identity hashes. These are separate protections with separate backup/key requirements.

## Reproducible checks

```powershell
$env:RAFT_EXPERIMENT_ROOT='D:/Temp/RaftNet8'
dotnet test Raft.Net8.sln -c Release
dotnet run --project HumanTest/Raft.Cluster.Demo -c Release -- dashboard-process-scenario --voters 3 --secure true --root D:/Temp/RaftNet8
dotnet run --project HumanTest/Raft.Cluster.Demo -c Release -- dashboard-process-scenario --voters 5 --root D:/Temp/RaftNet8
```

See [dashboard README](../src/RaftClusterDashboard/README.md) for UI tests, [validation evidence](VALIDATION.md) and the dedicated dashboard performance report for actual measurements. Dry runs establish functional execution, not capacity. TLS, native-token and backend-collector overhead are measured separately against the same HTTP message-service workload; embedded-core R18/e06/DotNext results are not reused as fresh comparisons.
