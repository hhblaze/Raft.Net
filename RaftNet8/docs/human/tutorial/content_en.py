from membership_chapters import membership_chapter, membership_state_chapter
"""English edition. HTML is authored here; C# comes from compiled examples."""
from build import (p, h, ul, ol, callout, table, code, excerpt, source_link,
                   path_tree, flow, quorum_lab, memory_lab, EXAMPLES)


def chapters():
    book = []
    add = lambda ident, title, body: book.append((ident, title, body))

    add("orientation", "What you are building", p(
        "Raft.Net8 is an <strong>embedded consensus library</strong>. You run a host process for each replica, give it a transport and a deterministic application, and submit commands to its elected leader. The library orders and replicates those commands. Your application decides what the bytes mean and stores the resulting business state.") + p(
        "The supported implementation is <code>RaftNet8/src/Raft</code>, targeting <strong>net8.0 only</strong>. Its assembly is <code>Raft.Net8</code> and its namespace is <code>Raft</code>. DBreeze is pinned to <code>1.142.2026.1016-beta</code>. That suffix is the upstream package version, not a setting to remove. The old Framework/Standard projects and <code>legacy-source/</code> are historical material with different contracts.") + p(
        "We will build a small replicated key/value service. Three processes will keep separate durable copies. A successful <code>put</code> means that a voter quorum has persisted the command and the leader has applied it. We will then restart a process, create a snapshot, and repeat the exercise with entirely volatile storage.") + callout(
        "The library is not your public client API", "There is no built-in HTTP/gRPC service, leader discovery service, authentication for business requests, automatic client forwarding, or retry policy. The tutorial console is a local control surface. A real host adds those parts around the library.") + h("How to use this book") + ol(
        "Run the existing human demo if you want to see the behavior immediately.",
        "Build the companion <code>examples/TutorialNode</code> and start three terminals.",
        "Read application, write and read semantics before adapting the code.",
        "Use the disk, memory, limits and recovery chapters before deploying persistent data.") + p(
        "All commands below start in the repository's <code>RaftNet8/</code> directory unless stated otherwise. The two editions have identical chapter anchors; the language switch preserves your current chapter. Everything needed to read this HTML is embedded. The adjacent C# project is needed only to run the examples."))

    add("consensus", "The cluster, the log and a quorum", p(
        "A <strong>voter</strong> participates in elections and commitment. A <strong>learner</strong> receives history and is excluded from quorum counting; it cannot campaign. The <strong>leader</strong> coordinates replication; followers receive its entries. A term is an election epoch, and a log index is an entry's position. A committed entry is part of the agreed history. An applied entry has also reached the local application.") + table(
        ["Voters", "Quorum", "Unavailable voters tolerated", "Meaning"], [
            ["1", "1", "0", "Useful for learning; no replica fault tolerance."],
            ["2", "2", "0", "Two copies but both are required for progress."],
            ["3", "2", "1", "A common small deployment."],
            ["5", "3", "2", "More failure tolerance and more replication work."]]) + p(
        "For a fixed active configuration, quorum is <code>voterCount / 2 + 1</code> using integer division. Count the local node only when it is a voter. Learners and merely provisioned <code>KnownPeers</code> do not increase or satisfy that quorum. During membership changes the active configuration comes from the latest configuration entry in the log, including an uncommitted entry; do not calculate operational membership from an old startup file.") + quorum_lab(False) + p(
        "When a three-voter group loses two voters, safe writes and fresh quorum-confirmed reads stop. A remaining process may still display old data. Calling that process a leader does not manufacture a majority. More replicas improve failure tolerance; they do not divide one group's writes into independent shards.") + flow(False) + p(
        "The implementation persists term/vote before dependent responses and log entries before replication acknowledgment. An elected leader appends a no-op, and its current-term commitment rule protects earlier history. You use these guarantees through the node API; your application must still make its own effects replay-safe."))

    add("setup", "Prepare .NET 8 and build", p(
        "Install a .NET 8 SDK. The solution's <code>global.json</code> selects the 8.0.400 feature band with latest-feature roll-forward within .NET 8; <code>dotnet --version</code> inside <code>RaftNet8/</code> should therefore report 8.0.x even if newer SDKs are installed. The source uses C# 12, nullable reference types and warnings as errors.") + code(
        "dotnet --version\ndotnet restore Raft.Net8.sln --locked-mode\ndotnet build Raft.Net8.sln -c Release --no-restore\ndotnet test tests/Raft.Tests -c Release --no-build --no-restore", "shell") + p(
        "The companion project is deliberately separate from the main solution. Build it once before starting multiple terminals; this avoids concurrent builds of the same output directory.") + code(
        "dotnet restore docs/human/tutorial/examples/TutorialNode/TutorialNode.csproj --locked-mode\ndotnet build docs/human/tutorial/examples/TutorialNode/TutorialNode.csproj -c Release --no-restore", "shell") + h("Reference the source project") + code(
        (EXAMPLES / "TutorialNode.csproj").read_text(encoding="utf-8"), "xml", "TutorialNode.csproj") + p(
        "The relative path above is correct for this companion project's location. For your own project, adjust the path to <code>src/Raft/Raft.csproj</code> and target <code>net8.0</code>. Central package management and lock files supply the exact DBreeze version. Do not assume that the assembly version in the source implies an already published NuGet release.") + callout(
        "Keep dependency and data compatibility separate", "Restoring a newer database package does not migrate a Raft database, an application schema, or a wire protocol. This book uses the pinned dependency and explicit wire3/v4 mode."))

    add("demo", "Watch the existing human demo", p(
        "The quickest experiment uses the existing human-test solution. Its three nodes live in one process, but communicate through real loopback TCP and use separate disk databases and snapshot directories.") + code(
        "dotnet run --project HumanTest/Raft.Demo -c Release --no-build", "shell") + code(
        "status\nset greeting hello from Raft\nget greeting\nload 1000\nsnapshot\nstatus", "text", "Enter into the demo console") + p(
        "Use <code>status</code> to identify the current leader, then isolate that actual ID. The literal node below is an example, not a promised election winner.") + code(
        "isolate n1\nstatus\nheal\nstop n2\nstart n2\nadd n4\npromote n4\ntransfer n4\nremove n1\nstatus\nexit", "text") + p(
        "The demo routes its <code>set</code>/<code>get</code> commands to the known leader; your own host must implement such routing. Its <code>get</code> performs ReadIndex before reading. The administration commands may report a transition gate while an election or configuration change settles. Wait for stable status and retry one operation at a time.") + code(
        "dotnet run --project HumanTest/Raft.Demo -c Release --no-build -- --scenario", "shell") + p(
        "The scripted scenario prints <code>SCENARIO PASS</code> on success. The interactive default reuses <code>.data/human-test-v4/</code>; scenarios normally create a fresh directory under <code>.data/scenarios/</code>. Use <code>--data &lt;new-directory&gt;</code> to start a fresh exercise without deleting old data. The tutorial application in the next chapter has different commands and directories.") + p(
        source_link("HumanTest/README.md", "Full human-demo instructions") + " · " + source_link("HumanTest/Raft.HumanTest.sln", "Human-test solution")))

    add("cluster-lab", "Start three independent C# processes", p(
        "Open three terminals at the same <code>RaftNet8/</code> directory after building the companion project. Run exactly one line in each terminal. The default endpoints are <code>127.0.0.1:6300</code>, <code>:6301</code> and <code>:6302</code>. The node order, common group and voter list are identical in all three processes.") + code(
        "# Terminal 1\ndotnet run --project docs/human/tutorial/examples/TutorialNode -c Release --no-build -- n1\n\n# Terminal 2\ndotnet run --project docs/human/tutorial/examples/TutorialNode -c Release --no-build -- n2\n\n# Terminal 3\ndotnet run --project docs/human/tutorial/examples/TutorialNode -c Release --no-build -- n3", "shell") + p(
        "Each process prints <code>STARTED</code> when its local runtime and listener start. That is not a quorum-readiness signal. Enter <code>status</code> until one process reports <code>role=Leader</code>. The sample uses 1–2 second election timers to give this disk-based exercise more room than the library's defaults. Send the following commands to the leader's terminal:") + code(
        "status\nput request-001 greeting hello from three processes\nget greeting\nput request-001 greeting hello from three processes\nlocal greeting\nsnapshot", "text") + p(
        "The second identical request commits another log entry but preserves the original business effect and <code>originalIndex</code>. <code>get</code> is quorum-confirmed; <code>local</code> is explicitly stale-capable. Reusing <code>request-001</code> with another key or value reports <code>CONFLICT</code> and leaves the first result intact. The companion explicitly uses UTF-8 stdin/stdout, including Windows pipes; keys and values may contain Cyrillic text.") + ol(
        "Stop the leader with <code>quit</code>. Wait for a new leader among the remaining two voters; write <code>request-002</code> there.",
        "Start the stopped process again with the same command and data directory. Observe catch-up with <code>status</code> and <code>local greeting</code>.",
        "Stop two of three processes. No remaining node can complete safe writes or ReadIndex. Restore a quorum without deleting databases.",
        "Quit all three, restart the same three commands, and read through the newly elected leader. Disk data and deduplication records should survive.") + callout(
        "Keep the lab's identity stable", "The default root is <code>.data/tutorial/&lt;node&gt;/</code>. Change all processes to a fresh common <code>--data</code> root for a new cluster. Use the same <code>--base-port</code> on all three if 6300–6302 are occupied. Never run two live processes with one node identity/data directory. Do not start this sample with directories owned by the existing human demo.", "warn") + p(
        "For a one-process exercise, add <code>--single --data .data/tutorial-single</code> to <code>n1</code>. Its single voter provides no replica failure tolerance. For different machines, replace the sample's loopback endpoint map with actual reachable addresses; the current CLI is deliberately a local lab."))

    add("bootstrap", "Construct options and recover a node", excerpt("TutorialSettings.cs", "options") + p(
        "These are <strong>sample overrides</strong>, not library defaults. A heartbeat is 50 ms, the election timeout is chosen in <code>[40,80)</code> 25-ms ticks (1–2 s), and the per-RPC deadline is 500 ms. The <code>Members</code> list contains n1/n2/n3, or only n1 in the separate single-voter exercise. The app/image format ID is <code>book-kv-v1</code>.") + table(
        ["Input", "Choose it this way", "What it does not do"], [
            ["<code>NodeId</code>", "Stable and unique per replica.", "Changing it cannot rename an existing database."],
            ["<code>GroupId</code>", "The same logical group on every member.", "It is not a secret or authentication credential."],
            ["<code>Members</code> / <code>Learners</code>", "Common bootstrap configuration for a new group.", "Options do not overwrite recovered membership."],
            ["<code>KnownPeers</code>", "Legacy bootstrap IDs; wire3 uses InitialPeers and the committed directory.", "It grants no voting rights."],
            ["PeerDescriptor / InitialPeers", "Canonical initial routes plus a local listen descriptor; new routes are replicated.", "A route never grants voting rights."],
            ["Paths", "Distinct stable log, application and image roots per node.", "A fresh path is not a safe replacement for lost committed history."]]) + excerpt("Program.cs", "startup") + p(
        "<code>CreateAsync</code> validates recovery and application checkpoints, completes pending snapshot-install recovery, and prepares the runtime. <code>StartAsync</code> starts participation. <code>tcp.Start(node.HandleAsync)</code> exposes the inbound RPC handler. A recovered application may need committed-log replay after startup; use a successful ReadIndex as an application-readiness fence on the leader.") + callout(
        "Choose wire3/v4 explicitly for new data", "<code>DBreezeLogLayout.UnifiedTableV4</code> is an explicit constructor argument. Constructors without it still select separate-table v2 for compatibility. Wire3 requires v4; opening v2/v3 data as v4 is rejected. There is no v4 converter."))

    add("application", "Implement deterministic business state", p(
        "Raft replicates command bytes, not arbitrary mutations made directly to your database. Generate request IDs, timestamps and other decisions before submitting, include them in the command, and make every replica interpret that command identically. A schema/version field makes unsupported commands visible instead of silently changing meaning.") + excerpt("BookState.cs", "encode") + p(
        "The gateway validates business input and encoded byte length before calling <code>ProposeAsync</code>. The library also enforces its own byte bound, but cannot validate your JSON schema. A malformed command discovered only during application has already entered the consensus history; the sample halts on that execution failure rather than pretending the caller merely supplied invalid input.") + excerpt("BookState.cs", "apply") + p(
        "The application stores values under <code>1.ToIndex(key)</code> and original request results under <code>2.ToIndex(requestId)</code> in one physical business table, <code>book/data</code>. The helper has a separate <code>book/checkpoint</code> table for its checked <code>(index, term)</code> checkpoint. Both tables participate in the same short DBreeze transaction. The helper reserves them before dependent reads, commits once, and only then publishes its applied frontier.") + p(
        "Do not call <code>Commit</code> inside the handler. Do not open a transaction on a captured live engine, await, call a remote service, use the clock/randomness, or mutate a live cache from this replay handler. Snapshot creation replays it against a separate generation. A direct write to a business table outside committed application bypasses replication and breaks replica equivalence.") + excerpt("BookState.cs", "disk-state") + p(
        "Built-in helpers automatically skip business effects for no-op/configuration entries but advance the checkpoint through them. If you implement <code>IStateMachine</code> yourself, inspect <code>entry.IsNoOp</code> and still advance <code>LastApplied</code>. A durable application should implement <code>ICheckpointStateMachine</code> and report the index/term matching its actual durable effects. <code>IBatchStateMachine</code> amortizes application transactions without changing log order.") + callout(
        "State-dependent business rejection is a result", "For insufficient funds, an ID collision or another valid business rejection, persist a deterministic result and advance the checkpoint. Throwing from Apply is a fail-stop signal, not a convenient way to return a normal business error. The sample keeps the first ID result and reports collisions after application.", "warn"))

    add("writes", "Write, retry and interpret the result", excerpt("Program.cs", "propose") + p(
        "<code>ProposalResult</code> contains the newly committed entry's <code>Index</code> and <code>Term</code>. It is not an application return value. The example reads its own durable request-result row after success to distinguish a first execution, a duplicate and a conflicting reuse of the ID. In a general service, persist the business result and expose a request-result query through the leader's read path.") + table(
        ["Outcome", "Meaning", "Caller response"], [
            ["Success", "Durable voter quorum and local leader application completed.", "Use the result; followers may still lag."],
            ["<code>NotLeaderException</code>", "This node cannot accept the leader operation; LeaderId may be null.", "Refresh routing; submit the same request identity and intent."],
            ["Cancellation / caller timeout", "Waiting stopped; a command may still be committed or later applied.", "Query/retry using the same request ID and payload."],
            ["<code>UncommittedLogLimitException</code>", "The uncommitted byte budget is exhausted.", "Bound callers and retry with backoff after quorum progress."],
            ["Storage/application failure", "Participation halts; persistence may have an uncertain outcome.", "Observe Completion, stop/dispose, preserve data and recover."]]) + p(
        "The library does not supply client deduplication. The sample's durable result record, business effect and checkpoint share one transaction. Retries therefore preserve the first business effect even if the original reply was lost. They still consume log entries and application work; deduplication is not a transport optimization.") + p(
        "Keep a request ID stable across retries and bind it to the same payload. Generate a new ID only for a new logical operation. The sample retains request results forever, so its database can grow even if the key count is constant. A production design needs bounded sessions/retention with a clearly defined retry horizon; deleting old results while clients can retry allows duplicate effects.") + callout(
        "External effects need another protocol", "A committed callback does not make email, payments or HTTP calls exactly-once. Persist an outbox record with the business effect, then use an external delivery worker and an idempotent receiver. Snapshot replay must not resend external actions.", "warn") + p(
        "Do not mutate or return pooled command memory while <code>ProposeAsync</code> is still using it. The node makes an owned copy after admission; keeping the input stable until completion is the simple safe calling rule. Request cancellation does not retract a command from the durable log."))

    add("reads", "Read current data safely", excerpt("Program.cs", "read") + p(
        "ReadIndex is a fence, not a returned database snapshot. The leader confirms a fresh voter-quorum context, requires current-term commitment, captures a committed index, and waits for local application through that index. Then read the <strong>same leader's concurrency-safe application</strong>. The read may include later applied writes, which is acceptable for a current read.") + p(
        "Do not obtain ReadIndex on n1 and then read n2's local database. Do not cache the fence and reuse it for unrelated future requests. <code>Status.Role == Leader</code>, a recent heartbeat, or <code>LastApplied == CommitIndex</code> is not a fresh quorum confirmation. During a partition, a stale former leader can still expose old local data.") + table(
        ["API", "Purpose", "Cost / boundary"], [
            ["<code>ReadIndexAsync</code>", "Fresh leader read fence; no new log entry.", "Needs a quorum and local application; wire2/3."],
            ["<code>ReadBarrierAsync</code>", "Compatibility read fence by committed no-op.", "Adds a durable log entry; available for wire1."],
            ["Application Get / local", "Read the node's currently applied state.", "May be stale; safe only when that is the explicit contract."],
            ["<code>Status</code>", "Published diagnostics.", "Not a data-read consistency guarantee."]]) + p(
        "The generation application's <code>Read</code> method protects the active engine during generation switches. Return materialized values or DTOs from its callback, not lazy DBreeze rows, enumerators, nested handles or engine references. A ReadIndex fence does not turn arbitrary multi-table application reads into a transactional snapshot; coordinate complex read consistency in your application.") + p(
        "There are no lease reads or automatic follower-to-leader forwarding. The host chooses the public query endpoint and handles leadership changes. A stale-read API can be useful, but should say so explicitly to its consumers."))

    add("disk", "Exactly what is stored on disk", path_tree() + p(
        "Every node owns its own copy. The sample's root is <code>.data/tutorial-v4/n1</code> (and n2/n3). It uses one DBreeze engine for the Raft log and another for the active business generation. The directory tree above is logical: DBreeze chooses physical file numbers, schemes, journals and rollback/helper files. Table names are not guaranteed filenames.") + table(
        ["Layer", "Persistent information", "Why it is needed"], [
            ["Raft identity", "Node/group identity, bootstrap identity and known members.", "Refuses accidental reuse as another replica/group."],
            ["Hard state/frontier", "Current term, vote, committed frontier and checked retained-log bounds/tail.", "Restart must not vote twice in a term or invent a different durable prefix."],
            ["Log", "Index, term, entry kind and command/configuration/service bytes, including uncommitted suffixes.", "Replication, conflict repair, restart and application replay."],
            ["Configuration index", "Persisted membership changes and recoverable configuration history.", "Recovery and rollback use logged membership, not edited startup lists."],
            ["Snapshot metadata/intent", "Included index/term, membership, image ID/digest; pending install intent when present.", "Binds a compacted prefix to an image and recovers interrupted installation."],
            ["Business generation", "Values, request-result rows and CRC-checked applied index/term.", "Application effects survive restart and duplicate requests."],
            ["Image repository", "Published archive/descriptor pairs, partial incoming files/progress and temporary builds.", "Catch-up after compaction, resumable transfer and local recovery."]]) + h("V4 is one primary table, not one file") + p(
        "<code>raft8/v4</code> holds the changing consensus data under ordered prefixes: <code>1.ToIndex(metadataKey)</code>, <code>2.ToIndex(logIndex)</code> and <code>3.ToIndex(configurationIndex)</code>, plus 4 for the service index and 5 for results. Hard state and the durable frontier share the checked RF4M manifest. Small guard records also exist in legacy metadata tables so incompatible readers fail closed. DBreeze system and rollback files remain. The word unified does not mean that a backup consists of one table file.") + p(
        "V2 uses <code>raft8/v2/meta</code>, <code>raft8/v2/log</code> and <code>raft8/v2/config</code>. V2/v3/v4 records have integrity checks; this is corruption detection, not encryption or Byzantine protection. The internal accounting overhead of 21 bytes per entry is a memory/wire-budget convention; the checked disk entry record adds 37 bytes before DBreeze's own key/index/journal overhead. Do not estimate filesystem capacity from payload size alone.") + h("What is not durable") + p(
        "Open sockets, pending caller tasks, read contexts, volatile leader role, in-flight RPCs and the bounded log cache are runtime state. The log cache accelerates access to durable entries; its 8 MiB default is not the entire database and is not a durability mode. After restart the runtime reconstructs participation and catches up from persisted state.") + callout(
        "No automatic log retention without snapshots", "With SnapshotIntervalEntries=0 and no manual snapshots, the log keeps growing. With snapshots, old log entries may be removed, but business data and deduplication rows remain in the application image. Snapshot retention is not application-data retention.", "warn"))

    add("memory", "In-memory mode and its real limits", p(
        "The modern implementation does have a memory log factory. It runs the DBreeze storage engine with <code>Storage = MEMORY</code>. For a completely volatile lab, the example also uses a separate memory DBreeze engine for business effects and checkpoint, and leaves <code>SnapshotDirectory</code> null.") + code(
        "var storage = DBreezeLogStorage.CreateInMemory(\n    options, DBreezeLogLayout.UnifiedTableV4);\nawait using var log = new RaftStorageAdapter(storage);", title="The memory log factory") + excerpt("BookState.cs", "memory-state") + code(
        "dotnet run --project docs/human/tutorial/examples/TutorialNode -c Release --no-build -- n1 --single --memory", "shell") + p(
        "Wait for Leader, put a value, then quit. Run the same command again and read after the new election: the value and request-result rows are gone. This mode does not persist Raft or business data to a database folder; TCP still uses sockets, the CLR still allocates memory, and the operating system can page process memory. It is not a promise that bytes never reach the machine's swap or diagnostics storage.") + table(
        ["Combination", "Restart behavior", "Appropriate use"], [
            ["Disk log + disk generation app + images", "Recover checked log/app state and install/replay images when needed.", "The book's durable path."],
            ["Memory log + memory app, no images", "Votes, terms, log, values, request IDs and checkpoints disappear.", "Disposable experiments and tests."],
            ["Disk log + fresh volatile app", "Can replay only if the whole needed prefix is retained, or a compatible snapshot app restores it.", "A custom explicitly designed application; not this CLI mode."],
            ["Memory log + durable app", "App can be ahead of the forgotten commit history and startup checks may reject it.", "Not a durable Raft configuration."]]) + callout(
        "Never promise crash-recovery durability from RAM voters", "Forgetting term, vote and log while rejoining under the same voter identity breaks Raft's stable-storage assumptions and can break safety. Do not restart a lost-memory voter into a surviving production cluster as though it were its old replica. Use disk-backed voters and the supported membership/replacement procedure. Treat an all-memory exercise as a disposable new cluster on each run.", "warn") + p(
        "Changing <code>LogCacheBytes</code> cannot switch storage to RAM. The built-in generation/snapshot implementation is disk-backed. The companion's memory mode intentionally has no snapshot support. Memory-mode business rows still grow with distinct values and request IDs until the process exits."))

    add("snapshots", "Snapshots, compaction and catch-up", p(
        "A snapshot represents application state through a committed, applied <code>(index, term)</code> plus the configuration at that fence. Once a complete image is durable and published, the log can compact older history. A far-behind follower then receives an image and continues with later entries instead of requiring every removed command.") + code(
        "using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));\nvar metadata = await node.CreateSnapshotAsync(deadline.Token);\nConsole.WriteLine($\"Image includes {metadata.LastIncludedIndex}/{metadata.LastIncludedTerm}\");", title="After node startup and at least one applied entry") + p(
        "Configure an absolute, exclusive <code>SnapshotDirectory</code> and an <code>ISnapshotStateMachine</code>. <code>DBreezeGenerationStateMachine</code> supplies that implementation for a separate absolute application root. Set <code>SnapshotIntervalEntries</code> above zero for automatic builds; zero is manual-only. The tutorial uses manual builds so the data lifecycle is visible.") + ol(
        "A build pins a committed/applied fence and replays the deterministic handler into an independent generation, seeded from the prior image when available.",
        "The generation is closed and archived. Images have SHA-256 digests; metadata/descriptors are checked.",
        "The durable image is published and compaction retains the configured log tail.",
        "Transfers use bounded CRC-checked chunks, durable incoming progress and resumable offsets. Installation uses an intent and an atomic generation pointer so restart can finish it.") + p(
        "This approach avoids assuming DBreeze provides a database-wide MVCC snapshot. The price is replay CPU, extra transactions, image I/O and temporary disk space. Plan for the live app, build/replay or prepared generations, archives and retained log at the same time. <code>MaxSnapshotBytes</code> and <code>MaxSnapshotUnpackedBytes</code> bound one archive/extraction; they do not cap total directory usage.") + p(
        "Cancellation stops the caller's wait; it need not cancel an already running snapshot build. Concurrent retention, reception or another build may make manual creation report busy. Published/transfer/prepared images can be pinned beyond the ordinary retention count. Never delete pinned files, modify generation pointers, or truncate the log manually.") + callout(
        "A snapshot is not a disaster-recovery reset button", "An image alone is not a supported force-new-cluster API. Keep the log, business generation and snapshot repository in a coherent backup, with the same identities and application format. A compacted log cannot rebuild a fresh app without its compatible image.", "warn"))

    add("membership", "Hot Add/Remove without restarting replicas", membership_chapter("en"))

    add("membership-state", "Membership storage, trust and limits", membership_state_chapter("en"))

    add("transfer", "Transfer leadership for maintenance", code(
        "await oldLeader.TransferLeadershipAsync(\"n2\", token);\n// Route to n2 through your host/client layer, then establish readiness:\nawait newLeader.ReadIndexAsync(token);", title="oldLeader/newLeader are local handles in their respective hosts") + p(
        "Transfer requires an eligible voter. The current leader stops accepting new proposals, reads and configuration changes, catches up the target, sends TimeoutNow and waits to observe the requested new leader. The operation succeeds on that observation or aborts within the election-timeout mechanism. An arbitrary learner cannot become the target until promoted.") + p(
        "Handle <code>LeadershipTransferInProgressException</code> as a transient routing condition. Cancellation does not retract a transfer request already sent. A successful transfer is not proof that your public load balancer has switched; route clients and issue a fresh ReadIndex on the new leader before stopping the old host.") + p(
        "For planned rolling maintenance, transfer away from the host you will stop, keep a voter quorum running, verify catch-up after restart, and only then move to another voter. The library does not choose a global maintenance schedule or move application-owned resources for you."))

    add("lifecycle", "Ownership, shutdown and fail-stop", table(
        ["Resource", "Owner", "Shutdown responsibility"], [
            ["RaftNode", "Your host", "DisposeAsync and observe Completion."],
            ["ILogStore after successful CreateAsync", "Node", "Node disposes it; keep a creation-failure guard."],
            ["Wrapped IRaftStorage", "RaftStorageAdapter", "Adapter closes storage/worker."],
            ["Engine made by a path constructor", "Storage", "Closed by the owning storage."],
            ["Engine supplied to storage/helper", "Caller unless explicitly transferred", "Dispose after node/app stop."],
            ["Transport, state machine, certificate, logger factory", "Caller", "Node does not take their ownership."]]) + excerpt("Program.cs", "shutdown") + p(
        "Stop inbound transport before disposing the node/store, then close the caller-owned application and engine. The companion scopes do this on normal exit and on errors. It guards the log even if creation fails; repeated adapter disposal is safe. Closing an engine while application or storage workers still use it causes failures.") + p(
        "Always observe <code>node.Completion</code>. It completes at shutdown and faults on persistence/application execution failure, preserving the original exception. The sample's input loop waits on both console input and Completion so a broken node is not left running silently while the console is idle.") + code(
        "var finished = await Task.WhenAny(hostStopping, node.Completion);\nif (finished == node.Completion)\n    await node.Completion; // Propagate the original fault to your supervisor.", title="Service-host pattern; hostStopping is supplied by the host") + callout(
        "Do not catch and continue after uncertain persistence", "After storage or application failure, stop participation, preserve the files, dispose and recover a new instance. Retrying an uncertain DB mutation on the failed instance can violate the durable-state contract. Exception type alone does not turn a failure during execution into invalid input.", "warn"))

    add("limits", "Buffer sizes and constants that matter", p(
        "These are the current <code>RaftOptions</code> defaults, not the lab's timer overrides. KiB means 1024 bytes and MiB means 1,048,576 bytes. Bounds are validated at construction and must also remain compatible with existing retained commands and every peer's decoder.") + table(
        ["Option", "Default", "Allowed / practical implication"], [
            ["MaxCommandBytes", "65,536 B (64 KiB)", "1..16 MiB. Includes the complete encoded application command, including JSON metadata."],
            ["MaxAppendEntries", "128", "1..4096 entries; count cap shared by proposal, replication and application batching."],
            ["MaxAppendBytes", "262,144 B (256 KiB)", "At least MaxCommandBytes + 21; at most 64 MiB. Application/replay and old wire1 budget."],
            ["MaxProposalBatchBytes", "null → 1,051,264 B", "Independent local durable proposal group; at least command + 21, at most 64 MiB and MaxUncommittedBytes."],
            ["MaxReplicationBytes", "null → 1,051,264 B", "Wire2 packet entry budget; explicit value fits MaxInFlightBytes − 64 and 64 MiB."],
            ["MaxPendingProposals / MaxPendingReads", "1024 / 1024", "Each 1..1,000,000. Limits admitted work, not arbitrary tasks waiting outside admission."],
            ["MaxInFlightMessages", "4 per peer", "1..256. Pipelining needs more than one; probing can still use fewer."],
            ["MaxInFlightBytes", "4 MiB per peer", "At least MaxAppendBytes + 64 and modern packet + 64; at most 1 GiB."],
            ["MaxUncommittedBytes", "64 MiB", "At least MaxAppendBytes; at most 1 TiB. A durable uncommitted suffix budget, not total log or RAM size."],
            ["LogCacheEntries / LogCacheBytes", "1024 / 8 MiB", "Both bounds apply; entry count zero disables the cache. Object/DBreeze overhead is additional."],
            ["Known peer IDs", "At most 1024", "Node/group/application IDs use 1..128 ASCII letters, digits, dash or underscore; voters/learners disjoint."],
            ["EnablePipelining / CoalesceCommitNotifications", "true / true", "Modern bounded flights and deferred idle commit-only notifications."]]) + h("Automatic byte budgets") + code(
        "entryAccounting = commandBytes + 21\nlocalGroup = min(4 * MaxAppendBytes + 21 * MaxAppendEntries,\n                 MaxUncommittedBytes, 64 MiB)\nwire2or3Packet = min(4 * MaxAppendBytes + 21 * MaxAppendEntries,\n                  MaxInFlightBytes - 64, 64 MiB)\nmaxFrame = max(wirePacket, SnapshotChunkBytes + (wire3 ? 900000 : 280000)) + 1024", "text", "Current automatic formulas; explicit nullable overrides replace the first two results") + p(
        "With defaults, the automatic packet/local limit is <strong>1,051,264 B</strong> (1 MiB + 2,688 B), and the internal frame-body limit is <strong>1,052,288 B</strong>; the TCP length prefix adds another 4 bytes. For wire1, wirePacket in the formula means MaxAppendBytes. The 21 bytes are entry accounting; the 64-byte per-flight allowance and frame allowances are different constants. A frame just above 1 MiB can rent a 2 MiB ArrayPool bucket. Do not equate frame length with exact managed heap consumption.") + p(
        "For example, an already encoded 16 KiB command accounts for 16,405 bytes. A default application batch fits 15 such entries, while a default local group or wire2/3 packet fits 64, subject to the count cap. With full 64 KiB encoded commands, those byte limits fit 3 and 16 entries respectively. JSON size includes its metadata and escaping; a 16 KiB value is not necessarily a 16 KiB command.") + h("Transport and worker bounds") + table(
        ["Implementation bound", "Current rule", "Default three-peer example"], [
            ["Inbound TCP connections", "max(4, endpointCount × 2)", "6 accepted concurrent connections for 3 configured endpoints."],
            ["Decoded handlers per connection", "Wire2: max(1, min(MaxInFlightMessages + 4, MaxInFlightBytes / MaxFrameBytes))", "Integer division gives 3, not 4 or 8."],
            ["Outgoing exchange capacity", "Wire2: MaxInFlightMessages + 4; wire1: 1", "8 exchanges; consensus replication still has its own 4-flight cap."],
            ["Storage adapter queue", "2 operations plus the executing operation", "One dedicated synchronous storage worker per adapter."],
            ["Application execution", "One ordered application worker per started node", "Slow application delays client completion."],
            ["Inbound frame-read deadline", "30 seconds", "Internal transport constant, not RpcTimeout."]]) + memory_lab(False) + p(
        "Those implementation constants describe this version; they are not all public tuning knobs. The process additionally owns DBreeze caches, socket/kernel buffers, CLR objects, business data and any host-owned queues. In-memory mode keeps its entire database beyond the log-cache budget. Limit your public request concurrency as well as the node's admitted work.") + h("Raise related limits together") + excerpt("IntegrationRecipes.cs", "larger-command") + p(
        "This is a 1 MiB command example, not a throughput recommendation. It keeps application batch ≥ command+21 and packet+64 ≤ peer window. Apply compatible limits on every receiver and plan for larger memory pools and persistent commands. Reducing MaxCommandBytes below already stored command lengths can make recovery reject the retained database.") + h("Snapshot bounds") + table(
        ["Option", "Default", "Contract"], [
            ["SnapshotDirectory / SnapshotIntervalEntries", "null / 0", "Absolute exclusive path; zero means manual only. An automatic interval requires a configured directory."],
            ["SnapshotRetainEntries", "128", "0..1,000,000; retained log tail, not business-record count."],
            ["SnapshotRetainedImages", "2", "2..64 newest images, plus pinned images."],
            ["SnapshotChunkBytes", "64 KiB", "1 KiB..4 MiB; also affects maximum frame size."],
            ["SnapshotBytesPerSecond", "16 MiB/s", "At least 1 KiB/s; per-node outbound snapshot throttle."],
            ["MaxSnapshotBytes", "64 GiB", "Archive size bound; at least one chunk."],
            ["MaxSnapshotUnpackedBytes", "128 GiB", "Extraction bound; at least MaxSnapshotBytes."]]))

    add("timing", "Election, batching and timeout budgets", table(
        ["Option", "Library default", "Relationship"], [
            ["TickInterval", "25 ms", "1 ms..10 s; logical clock cadence."],
            ["HeartbeatTicks", "1", "Heartbeat period = ticks × TickInterval; at least 1 tick."],
            ["ElectionMinTicks / ElectionMaxTicks", "12 / 24", "Random [min,max); 300 ms up to but excluding 600 ms by default; max ≤ 1,000,000 ticks."],
            ["RpcTimeout", "200 ms", "Positive and less than the minimum election period; within the runtime timer range."],
            ["ProposalBatchDelay", "2 ms", "0..1 s and at most half one heartbeat period."],
            ["BulkProposalBatchDelay", "null → up to 8 ms", "min(4 × small delay, one heartbeat, 1 s); explicit value must fit one heartbeat/1 s."],
            ["Single-voter collection cap", "1 ms", "The intentional proposal collection window is shortened for one voter."]]) + p(
        "Validation requires <code>ElectionMinTicks ≥ 10 × HeartbeatTicks</code> and <code>ElectionMaxTicks &gt; ElectionMinTicks</code>. These are minimum consistency checks, not proof your timing is operationally sound. Size deadlines from slow disk commits, network/TLS setup, CPU scheduling, GC pauses, application work and admitted backlog. Too-small timeouts create election churn; making them enormous increases failure-detection time.") + p(
        "The small/bulk windows collect proposals outside the consensus owner. Bulk delay applies to byte-limited groups while other admitted work is awaiting quorum/application; it shortens when all admitted work is already queued. A full count/byte group can flush immediately. The deadline starts at the oldest queued proposal and is not extended by every new arrival.") + p(
        "To remove intentional collection waits, set both <code>ProposalBatchDelay = TimeSpan.Zero</code> and <code>BulkProposalBatchDelay = TimeSpan.Zero</code>. Explicit nonzero bulk delay remains an override even if the small delay is zero. Measure the latency/transaction trade-off; one request at a time cannot exercise the same batching behavior as 64 concurrent callers.") + callout(
        "Commit notification lag is an explicit trade-off", "With modern wire2/3 and CoalesceCommitNotifications=true, an idle caught-up follower may learn the new commit index on the next data packet or scheduled heartbeat. Follower application can lag by that logical heartbeat plus I/O/network/owner delay. Leader quorum persistence and leader application still precede client success. Setting false restores an immediate send attempt; an existing flight can still delay that peer. ReadIndex on the leader remains the fresh-read contract.") + p(
        "A client deadline is independent of RpcTimeout and election timers. Choose it for admission + durable replication + leader application, not just one network trip. Do not treat a timeout as proof of rejection. The sample's 15-second console deadline is an exercise setting, not a library constant."))

    add("performance", "Use bounded concurrency and measure real work", excerpt("IntegrationRecipes.cs", "load") + p(
        "The recipe caps concurrently executing submissions at 64. It still requires your command producer, input bodies and public server queues to be bounded. Launching millions of tasks with Task.WhenAll merely moves the unbounded memory outside the Raft admission semaphore. Successful calls include leader application, so a slow deterministic handler directly limits useful throughput.") + ol(
        "Keep one DBreeze engine per physical directory and short synchronous transactions. Never await inside a transaction.",
        "Declare all write tables at helper construction. For custom read–modify–write transactions, reserve the full write set before dependent reads.",
        "Use ordered/prefixed keys and bounded ranges. Avoid full-table scans in application and maintenance hot paths.",
        "Separate command payload, application-batch, durable-group and replication limits; increasing one does not increase them all.",
        "Measure with the intended voter count, payload size, durable application work, disk and network. Record p50/p99 and allocation as well as commands per second.") + p(
        "The runtime has a dedicated storage thread per adapter and an application thread per started node, alongside async network/timer work and DBreeze activity. Hosting many groups in one process therefore has a thread/resource cost. The pure consensus reducer itself performs no I/O; that does not make synchronous storage flushes free.") + p(
        "The hot-membership report compares wire3/v4 with the immediately preceding HEAD, including full Add under load. Check throughput, p99 and allocations separately; a local disk/TCP result is not a cross-machine capacity promise. The earlier R18 competitor report describes the pre-membership version and is retained as historical evidence.") + p(
        source_link("benchmarks/results/hot-membership-2026-11-14.html", "Hot membership: HEAD comparison") + " · " + source_link("benchmarks/results/winner-r18-2026-11-13.html", "Historical R18 comparison") + " · " + source_link("benchmarks/README.md", "Benchmark methods and commands")))

    add("security", "Secure the peer network and your client API", p(
        "Plain TcpRaftTransport is appropriate only on a trusted lab/network. Group and peer IDs in an RPC are routing identifiers; they are not cryptographic authentication. A production host should restrict the network and use mutual TLS with a certificate-to-Raft-ID policy.") + excerpt("IntegrationRecipes.cs", "tls") + p(
        "The certificate is caller-owned and must contain the private key. The shown policy keeps platform chain, name and revocation checks and additionally pins each allowed identity to a trusted SHA-256 certificate hash. Configure those hashes out of band. The default TLS TargetHost is the peer's Raft ID, so its certificate SAN/name must validate for that ID, not merely for an unrelated IP address.") + p(
        "A private PKI may supply an explicit validation callback, but it must actually validate that PKI/name policy. Returning true for every certificate defeats authentication. Plan certificate rollover, trust changes and revocation availability across the entire group. TLS handshake work is subject to the transport's RPC timeout, so very slow validation can affect availability.") + p(
        "Peer mTLS does not authenticate your business clients. Add authorization, rate/body limits, request deadlines and request-ID policy to the public HTTP/gRPC/service layer. Command payloads and database files can contain business secrets; the library's CRCs and SHA-256 image digests detect corruption, not confidentiality or a malicious replica.") + callout(
        "Failure model", "This is crash/recovery Raft for non-Byzantine nodes. It does not tolerate a member intentionally lying, arbitrary operator edits to tables, or storage that acknowledges flushes without preserving the required data.", "warn"))

    add("diagnostics", "Observe frontiers, lag and failures", code(
        "var status = node.Status;\nConsole.WriteLine($\"{status.NodeId}: {status.Role}, term={status.Term}, \" +\n    $\"commit={status.CommitIndex}, applied={status.LastApplied}, tail={status.LastLogIndex}\");\nforeach (var peer in status.Peers)\n    Console.WriteLine($\"{peer.NodeId}: match={peer.MatchIndex}, \" +\n        $\"next={peer.NextIndex}, flights={peer.InFlightMessages}, bytes={peer.InFlightBytes}\");", title="Observe; do not use status as a read fence") + p(
        "Commit minus applied indicates local application backlog. Tail minus a peer's acknowledged MatchIndex indicates replication lag. Repeated term changes suggest timing, storage or connectivity trouble. Snapshot send/receive offsets explain why a behind follower has not yet rejoined normal replication. Status is a published diagnostic view; it is not an atomic application query.") + excerpt("IntegrationRecipes.cs", "logging") + table(
        ["Surface", "What to collect", "Units / scope"], [
            ["ILoggerFactory", "Roles, terms, configuration/images, faults and failed RPC attempts.", "Host supplies logging providers; payloads excluded."],
            ["Meter / ActivitySource", "<code>Raft.Net8</code>", "Tags include raft.group, raft.node, raft.peer/outcome."],
            ["Frontier gauges", "raft.commit.index, raft.applied.index, raft.replication.lag", "Entry indices or entry lag."],
            ["Queue/budget gauges", "raft.proposals.pending, raft.reads.pending, raft.uncommitted.bytes", "Counts and bytes."],
            ["Timing histograms", "raft.proposal.duration, raft.persistence.duration, raft.storage.commit.duration, raft.apply.duration", "Seconds. Commit timing includes DBreeze work/flush, not only a syscall."],
            ["Completion / failures", "Original exception chain and host exit/restart context.", "Stop participation on storage/application fault."]]) + p(
        "Register exporters/listeners in your host and preserve group/node dimensions. Do not synchronously block the hot path with a slow log sink or diagnostic callback. Diagnostic callback failures are isolated/countable and must not mask the original storage failure. Also collect process RSS/GC, CPU, disk free space, disk commit tails and network behavior outside the library."))

    add("hosting", "Move from a lab to a service", ol(
        "Use separate failure domains and disks for voters. Three processes on one laptop demonstrate protocol behavior, not machine-failure tolerance.",
        "Give each replica stable identity and persistent storage. Keep node ID/data together across service restarts and avoid simultaneously active clones.",
        "Provide reachable peer routes, firewall/TLS policy and compatible bounds on every member.",
        "Define a public leader-aware request layer, bounded admission, business validation and durable request results.",
        "Gate readiness on recovered healthy state and successful leader/quorum read or write behavior, not merely process startup.",
        "Connect Completion to a supervisor, close transport before storage, and practice restarts/restore before taking live traffic.") + p(
        "The companion binds only loopback and accepts local stdin commands. It deliberately has no production admin HTTP endpoint. Its <code>--base-port</code>, <code>--data</code>, <code>--single</code> and <code>--memory</code> switches are sample CLI features, not RaftOptions or library command-line flags.") + p(
        "For container/service hosting, use persistent volumes with storage semantics you have verified. Do not let every restarted container receive a fresh data directory while retaining its old voting identity. Separate liveness from readiness: a process waiting for quorum can be alive without being able to serve strong reads/writes. Repeatedly restarting it does not fix missing peers.") + p(
        "Run stable application/command formats on all replicas, including snapshot replay. A rollout that changes the interpretation of already committed commands can diverge business state even when the Raft log is correct. The library does not version/migrate your application handler for you."))

    add("recovery", "Back up, recover and replace replicas", h("Normal process restart") + p(
        "Stop/dispose cleanly when possible, keep the same node/group identity and paths, and start through CreateAsync. Recovery validates checksummed records, the durable frontier and application checkpoint, resolves pending image installation, and replays/catches up as required. A clean shutdown is useful, but the disk-backed path is also designed for abrupt process failure under the storage assumptions.") + h("Backup") + p(
        "The simplest operational baseline is a stopped, coherent backup of the node's log database, application root and snapshot repository. Close every engine before a plain directory copy. Preserve descriptors, generation pointers and identity. An ordinary live copy of a few table files is not automatically consistent. DBreeze backup facilities need a coordinated design with the separate application/image roots.") + h("One lost replica while quorum is healthy") + p(
        "Preserve failed files for investigation. Prefer supported replacement through provisioned new identity → learner → catch-up → promotion → removal, keeping a quorum available. Do not erase a current voter and reboot it empty under the same identity. Restoring a stopped replica requires a backup/history compatible with the still-running group.") + h("Whole-group disaster") + p(
        "Restore enough original identities from coherent backups containing the required committed history, then validate ReadIndex/write/read/catch-up before reopening clients. Backup age limits which acknowledged writes can be recovered. There is no safe-quorum-reset/force-new-cluster API that infers missing history from a stale single node.") + callout(
        "Checksums detect; they do not heal every loss", "A CRC mismatch or app/log checkpoint mismatch is a reason to stop and investigate/restore, not to edit the record until startup passes. Durability still relies on the OS/device honoring flushes. Software fault tests do not prove physical power-loss behavior.", "warn") + p(
        source_link("docs/OPERATIONS.md", "Detailed recovery, backup and migration runbook")))

    add("upgrades", "Keep disk layout, wire and app format separate", table(
        ["Version axis", "Current recommendation", "Upgrade rule"], [
            ["Runtime/API", "net8.0 project in RaftNet8/", "Historical Framework/Standard projects are not modern database inputs."],
            ["Disk layout", "Explicit UnifiedTableV4 + wire3 for fresh clusters; v2/v3 remain compatibility modes.", "No v2/v3→v4 converter. Historical v2→v3 copy is described below."],
            ["Wire", "Version 3 for new v4 clusters", "Mixed wire1/2/3 rejected; v4 uses wire3 only."],
            ["Wire2 limits", "Compatible MaxReplicationBytes/MaxCommandBytes/frame limits.", "Older wire2 receivers may reject the new larger defaults."],
            ["Application/image format", "Your stable versioned command handler and ApplicationId.", "An explicit application migration; the Raft tool cannot infer it."]]) + callout("Fresh v4 is a separate mode", "The copy command below is only for legacy v2→v3; it does not produce v4. Retain old data and use fresh directories for a new wire3 group.") + code(
        "dotnet run --project tools/Raft.Migrate -c Release -- --v2-to-v3 <source-db> <new-db> <group> <node> <initial-voter-list>", "shell", "From RaftNet8/ with the entire group and all engines stopped") + p(
        "The API is <code>RaftStorageMigration.CopyV2ToV3(source, destination, options)</code>. Use a non-existent destination outside the source. The tool verifies/copies/converts the database folder and seals the result; an incomplete target must not start. It copies application tables inside that DB folder unchanged, but not sibling application or snapshot roots. Copy those separately while stopped. Reparse/outside-table layouts are rejected by this copy method.") + p(
        "Keep the original node/group and recovered membership. Verify the migration manifest/frontiers, start a quorum, exercise reads/writes/catch-up/snapshots and another restart, then reopen clients. Once the new cluster accepts writes, the old folders are stale and are not a safe rollback.") + callout(
        "Larger wire2 defaults require compatible peers", "For a rolling binary upgrade with older wire2 nodes, explicitly set MaxReplicationBytes=262144 (or that cluster's older MaxAppendBytes) on every new node and retain it through the rollout. Raise limits only when all receivers are compatible; coordinate stop/configure/restart when send and receive limits cannot be changed independently. V2/v3 storage layouts can use the same wire2; no data conversion is needed merely for the frame-limit setting.", "warn") + p(
        "Wire1 requires pre-vote off, no snapshots and its old byte budget; modern ReadIndex, transfer and membership controls are unavailable. Compatibility fixtures do not make historical pre-.NET-8 databases automatically migratable. Use the runbook's separate v1→v2 procedure only for the supported <code>raft8-v1</code> format."))

    add("troubleshooting", "Recognize common failures", table(
        ["Symptom", "Likely explanation", "Useful action"], [
            ["No leader / endless campaigns", "No reachable voter quorum, wrong group/routes, incompatible settings or tiny timeouts.", "Check actual active membership, routes, TLS, disk tails and all status views."],
            ["Writes work but follower local read is old", "Follower apply/commit notification lag; local read is stale-capable.", "ReadIndex then query that leader, or explicitly accept stale reads."],
            ["NotLeader with no hint", "Election/partition or no known current leader.", "Refresh discovery; use a bounded retry policy with stable request ID."],
            ["Caller timeout", "Admission, replication or apply exceeded your deadline.", "Treat outcome as uncertain; query/retry the same intent."],
            ["UncommittedLogLimitException", "Quorum progress cannot drain uncommitted data.", "Restore quorum/progress, bound producers, back off."],
            ["Oversized command / frame", "Encoded payload or peers' byte limits differ.", "Count JSON metadata and configure related receiver/window bounds compatibly."],
            ["Identity/layout/checkpoint rejection on startup", "Wrong directory, wrong constructor, incomplete migration, or inconsistent app/log backup.", "Stop; inspect original exception and recover the correct coherent files."],
            ["File already open / owner.lock", "Another process owns the same engine/application/image root.", "Use one owner and separate per-node roots; do not delete a live owner's lock."],
            ["Snapshot creation busy", "Build, retention or receive/install in progress.", "Wait for progress and retry; distinguish a normal busy message from Completion fault."],
            ["Memory keeps growing", "Business/request-result rows, unbounded waiting callers, DBreeze caches or inactive snapshot policy.", "Measure categories; a log-cache bound alone is not a process RAM/disk bound."]]) + p(
        "Capture the full original exception, status/configuration, identity, runtime/dependency versions and disk/network context. Never log secret command bodies merely to diagnose replication. If Completion faults, stop the host's participation and preserve the data before attempting repair."))

    add("sources", "Companion source and reproducible exercises", p(
        "The adjacent C# project is the source of the embedded code excerpts. It references the real library and compiles all startup, application, TLS, bounded-load and membership recipes. The process program exercises writes, reads, original-result deduplication, snapshots and disposal. Administration recipes require a correctly provisioned host context; they are not hidden extra CLI commands.") + table(
        ["File", "Purpose"], [
            ['<a href="examples/TutorialNode/TutorialNode.csproj">TutorialNode.csproj</a>', "net8.0 source-project reference and inherited package policy."],
            ['<a href="examples/TutorialNode/TutorialSettings.cs">TutorialSettings.cs</a>', "Sample arguments, common voters, endpoints and timer overrides."],
            ['<a href="examples/TutorialNode/Program.cs">Program.cs</a>', "Startup, console API calls, Completion supervision and shutdown."],
            ['<a href="examples/TutorialNode/BookState.cs">BookState.cs</a>', "Versioned commands, deterministic/persistent deduplication, disk and RAM apps."],
            ['<a href="examples/TutorialNode/IntegrationRecipes.cs">IntegrationRecipes.cs</a>', "Compiled host-integration fragments; policy inputs come from your host."]]) + h("Check your understanding") + ol(
        "Why can one surviving node of a three-voter group display a value but not safely confirm a new write?",
        "What does ProposeAsync success say about the leader's app, and what does it leave open about follower application?",
        "Which three roots belong in the sample's coherent disk backup?",
        "What survives a memory-mode process exit, and why is restarting a forgotten RAM voter dangerous?",
        "Why does a retried request need the same ID and payload, and why can deduplication rows outgrow the value table?",
        "Which settings must change together for a 1 MiB command, and what happens to an older small-frame peer?") + p(
        "To repeat the companion's process-level checks after building it, run the verification script. It starts only its own lab processes and retains their logs/data under <code>.artifacts/human-tutorial/</code>. It checks leader failover, minority rejection, snapshots, durable restart/deduplication and memory reset; this is a functional exercise, not a capacity benchmark.") + code(
        "python -X utf8 -B docs/human/tutorial/verify_examples.py", "shell") + p(
        "To rebuild the HTML after changing prose or source excerpts, run the Python builder. Both scripts use only the Python standard library; reading the generated books does not require Python.") + code(
        "python -X utf8 -B docs/human/tutorial/build.py", "shell") + "<details><summary>Full application source: BookState.cs</summary>" + code(
        (EXAMPLES / "BookState.cs").read_text(encoding="utf-8"), title="BookState.cs") + "</details><details><summary>Full process source: Program.cs</summary>" + code(
        (EXAMPLES / "Program.cs").read_text(encoding="utf-8"), title="Program.cs") + "</details>")

    add("reference", "Glossary, boundaries and references", table(
        ["Term", "Meaning in this library"], [
            ["Term / index", "Election epoch / position of one ordered log entry."],
            ["Commit / apply", "Agreed durable history / that history executed by this local app."],
            ["Checkpoint", "Durable applied index and term corresponding atomically to business effects."],
            ["Voter / learner / known peer", "Counts for quorum / replicated non-voter / provisioned transport identity."],
            ["Snapshot / generation", "Immutable prefix image / one application database instance selected by an atomic pointer."],
            ["ReadIndex", "Fresh quorum context plus leader-local applied fence, without a new log entry."],
            ["Fail-stop", "Stop participation after uncertain storage/application execution failure."],
            ["Backpressure", "Bound admitted work and let callers wait/reject/retry instead of growing queues indefinitely."]]) + p(
        "Operational scope: crash/recovery within a non-Byzantine deployment, ordinarily one region. There is no joint consensus, lease read, automatic follower forwarding, public client protocol, built-in automatic deduplication, cross-region capacity guarantee or unsafe quorum-reset API. External side effects need an application protocol. Memory storage is a volatile lab capability, not a substitute for stable storage.") + ul(
        source_link("src/Raft/RaftOptions.cs", "RaftOptions — executable defaults and validation"),
        source_link("src/Raft/RaftNode.cs", "RaftNode — public lifecycle and leader APIs"),
        source_link("src/Raft/Storage/DBreezeLogStorage.cs", "DBreeze log storage — constructors and memory factory"),
        source_link("docs/CONFIGURATION.md", "Configuration and diagnostics reference"),
        source_link("docs/OPERATIONS.md", "Operational runbook and migrations"),
        source_link("docs/ARCHITECTURE.md", "Architecture and storage formats"),
        source_link("docs/VALIDATION.md", "Executed validation and its limits"),
        '<a href="../../../../_docs/raft_skill.md">Repository integration skill</a>',
        '<a href="../../../../_docs/llm/skill/DBreeze_skill.md">Preserved DBreeze transaction reference</a>',
        '<a href="https://raft.github.io/raft.pdf">Ongaro and Ousterhout: Raft extended paper</a> — conceptual background; this implementation\'s public contracts are in its source and runbooks.',
        '<a href="https://github.com/hhblaze/DBreeze">DBreeze upstream</a> — embedded database project.') + p(
        "This edition describes the checked-in .NET 8 API and defaults as of 14 November 2026. Sample-specific timing, paths and command syntax are identified separately. Recheck source/upgrade notes before combining it with another binary version."))

    return book
