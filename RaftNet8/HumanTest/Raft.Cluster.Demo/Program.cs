using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using DBreeze;
using DBreeze.Utils;
using Raft;
using Raft.Cluster;
using Raft.Cluster.Client;
using Raft.Cluster.Demo;
using Raft.Cluster.Server;

var command = args.FirstOrDefault() ?? "help";
string Option(string name, string fallback) { var index = Array.IndexOf(args, "--" + name); return index >= 0 && index + 1 < args.Length ? args[index + 1] : fallback; }
int Integer(string name, int fallback) => int.Parse(Option(name, fallback.ToString(System.Globalization.CultureInfo.InvariantCulture)), System.Globalization.CultureInfo.InvariantCulture);
var root = Path.GetFullPath(Option("root", Environment.GetEnvironmentVariable("RAFT_EXPERIMENT_ROOT") ?? Path.Combine(Path.GetTempPath(), "RaftNet8")));
var group = Option("group", "movies");
var voters = Integer("voters", 3);
using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) => { eventArgs.Cancel = true; stop.Cancel(); };
try
{
    if (command == "dashboard-process-scenario")
        await DashboardProcessScenario.RunAsync(root, voters, Option("secure", "false") == "true", stop.Token);
    else if (command == "create-lab-pki")
    {
        DemoSecurity.Generate(Path.GetFullPath(Option("pki", Path.Combine(root, "pki"))));
        Console.WriteLine("Local laboratory certificates created; no system trust settings changed. Protect the output directory.");
    }
    else if (command == "bootstrap-stand")
    {
        using var security = args.Contains("--pki") ? new DemoSecurity(Option("pki", "")) : null;
        using var http = security?.HttpClient();
        var endpoints = Enumerable.Range(0, voters).Select(index => new Uri($"{(security is null ? "http" : "https")}://127.0.0.1:{Integer("management-base-port", 7201) + index}/")).ToArray();
        await using var admin = new RaftClusterAdminClient(new() { GroupId = group, SeedEndpoints = endpoints, HttpClient = http, OperationTimeout = TimeSpan.FromMinutes(2) });
        var secret = await File.ReadAllTextAsync(Path.Combine(root, "cluster-bootstrap.secret"), stop.Token);
        var issued = await admin.BootstrapAsync(secret, Guid.Parse(Option("operation-id", Guid.NewGuid().ToString("D"))), stop.Token);
        if (issued.Secret is null) throw new InvalidOperationException("Bootstrap was already committed; its secret cannot be retrieved again.");
        var path = Path.Combine(root, "dashboard-connection.json");
        await File.WriteAllBytesAsync(path, JsonSerializer.SerializeToUtf8Bytes(new { Name = "Raft / process stand", GroupId = group,
            ManagementEndpoints = endpoints.Select(endpoint => endpoint.AbsoluteUri).ToArray(), Credential = issued.Secret,
            CertificateAlias = security is null ? null : "lab" }), stop.Token);
        Console.WriteLine("DASHBOARD CONNECTION FILE " + path);
    }
    else if (command == "dashboard-stand")
    {
        var run = Path.Combine(root, "dashboard-stand-" + DateTime.UtcNow.ToString("yyyyMMdd-HHmmss") + "-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(run);
        var bootstrapSecret = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
        await using var cluster = new DemoCluster(run, voters, fixedPorts: true, groupId: group,
            peerBasePort: Integer("peer-port", 6101), clientBasePort: Integer("client-port", 7101), managementBasePort: Integer("management-port", 7201))
            { ManagementEnabled = true, BootstrapSecret = bootstrapSecret, RequireClientTokens = Option("tokens", "false") == "true" };
        await cluster.StartAsync(stop.Token);
        await cluster.LeaderAsync(stop.Token);
        await using var bootstrap = new RaftClusterAdminClient(new()
        {
            GroupId = group, SeedEndpoints = Enumerable.Range(1, voters).Select(index => cluster.ManagementEndpoint("n" + index).Address).ToArray()
        });
        var grant = await bootstrap.BootstrapAsync(bootstrapSecret, Guid.NewGuid(), stop.Token);
        var connectionPath = Path.Combine(run, "dashboard-connection.json");
        await File.WriteAllBytesAsync(connectionPath, JsonSerializer.SerializeToUtf8Bytes(new
        {
            Name = "Raft / local", GroupId = group,
            ManagementEndpoints = Enumerable.Range(1, voters).Select(index => cluster.ManagementEndpoint("n" + index).Address.AbsoluteUri).ToArray(),
            Credential = grant.Secret
        }, new JsonSerializerOptions { WriteIndented = true }), stop.Token);
        for (var index = voters + 1; index <= Math.Min(voters + 2, 9); index++) await cluster.StartNodeAsync("n" + index, stop.Token);
        Console.WriteLine($"DASHBOARD CONNECTION FILE {connectionPath}");
        Console.WriteLine("Already-running joining nodes are ready for Add. No synthetic messages are generated.");
        while (!stop.IsCancellationRequested) await Task.Delay(1000, stop.Token);
    }
    else if (command == "migrate-cluster-v2")
    {
        var source = Path.GetFullPath(Option("source", ""));
        var destination = Path.GetFullPath(Option("destination", ""));
        var optionsFile = Option("options", Path.Combine(source, "node-options.json"));
        var settings = JsonSerializer.Deserialize<RaftOptions>(await File.ReadAllBytesAsync(optionsFile, stop.Token))
            ?? throw new ArgumentException("Missing original RaftOptions.");
        Console.WriteLine(JsonSerializer.Serialize(ClusterStorageMigration.CopyNodeV1ToV2(source, destination, settings, stop.Token)));
    }
    else if (command == "node")
    {
        var id = args.Length > 1 ? args[1] : throw new ArgumentException("node n1 --root ...");
        using var security = args.Contains("--pki") ? new DemoSecurity(Option("pki", "")) : null;
        var managementEnabled = Option("management", "false") == "true";
        string? bootstrapSecret = null;
        if (managementEnabled)
        {
            Directory.CreateDirectory(root);
            var secretFile = Path.GetFullPath(Option("bootstrap-file", Path.Combine(root, "cluster-bootstrap.secret")));
            if (!File.Exists(secretFile))
            {
                var temporary = Path.Combine(Path.GetDirectoryName(secretFile)!, ".bootstrap-" + Guid.NewGuid().ToString("N"));
                await File.WriteAllTextAsync(temporary, Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)), stop.Token);
                try { File.Move(temporary, secretFile); }
                catch (IOException) when (File.Exists(secretFile)) { File.Delete(temporary); }
            }
            if (new FileInfo(secretFile).Length > 256) throw new ArgumentException("Bootstrap secret file exceeds256 bytes.");
            bootstrapSecret = await File.ReadAllTextAsync(secretFile, stop.Token);
        }
        await using var cluster = new DemoCluster(root, voters, fixedPorts: true, groupId: group,
            peerBasePort: Integer("peer-base-port", 6101), clientBasePort: Integer("client-base-port", 7101), managementBasePort: Integer("management-base-port", 7201), secure: security is not null)
            { ManagementEnabled = managementEnabled, RequireClientTokens = Option("tokens", "false") == "true", BootstrapSecret = bootstrapSecret, Security = security };
        var host = await cluster.StartNodeAsync(id, stop.Token);
        await File.WriteAllBytesAsync(Path.Combine(root, id, "node-options.json"), JsonSerializer.SerializeToUtf8Bytes(host.Options, new JsonSerializerOptions { WriteIndented = true }), stop.Token);
        Console.WriteLine($"NODE {id} peer={cluster.Peer(id).Port} client={cluster.Endpoint(id).Address} data={root}");
        if (args.Contains("--daemon"))
        {
            if (args.Contains("--control-stdin"))
                while (await Console.In.ReadLineAsync(stop.Token) is { } control && control != "quit") { }
            else await Task.Delay(Timeout.InfiniteTimeSpan, stop.Token);
            return;
        }
        Console.WriteLine("Commands: status | add n4 | remove n1 | snapshot | quit. Run the new process before add.");
        while (!stop.IsCancellationRequested)
        {
            var line = await Console.In.ReadLineAsync(stop.Token);
            if (line is null || line == "quit") break;
            try
            {
                var words = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (words.Length == 0) continue;
                if (words[0] == "status") Console.WriteLine(JsonSerializer.Serialize(host.Node.Status));
                else if (words[0] == "snapshot") Console.WriteLine(JsonSerializer.Serialize(await host.Node.CreateSnapshotAsync(stop.Token)));
                else if (words[0] == "add" && words.Length >= 2)
                {
                    var operationId = words.Length >= 3 ? Guid.Parse(words[2]) : Guid.NewGuid(); Console.WriteLine($"OPERATION {operationId}");
                    Console.WriteLine(JsonSerializer.Serialize(await host.Server.AddNodeAsync(cluster.Peer(words[1]), cluster.Endpoint(words[1]), operationId, stop.Token)));
                }
                else if (words[0] == "remove" && words.Length >= 2)
                {
                    var operationId = words.Length >= 3 ? Guid.Parse(words[2]) : Guid.NewGuid();
                    Console.WriteLine($"OPERATION {operationId}; retry remove {words[1]} {operationId} on the next leader if redirected.");
                    Console.WriteLine(JsonSerializer.Serialize(await host.Node.RemoveNodeAsync(words[1], operationId, stop.Token)));
                }
            }
            catch (Exception error) { Console.WriteLine($"ERROR {error.GetType().Name}: {error.Message}"); }
        }
    }
    else if (command is "scenario" or "stress-file")
    {
        var run = Path.Combine(root, command + "-" + DateTime.UtcNow.ToString("yyyyMMdd-HHmmss") + "-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(run);
        var fileBytes = command == "stress-file" ? checked((long)Integer("gib", 80) * 1024 * 1024 * 1024) : 4L * 1024 * 1024 + 123;
        var drive = new DriveInfo(Path.GetPathRoot(run)!);
        var required = checked(fileBytes * (2L * voters + 2) + 16L * 1024 * 1024 * 1024);
        if (fileBytes < 1 || drive.AvailableFreeSpace < required) throw new IOException($"Need at least {required} free bytes, have {drive.AvailableFreeSpace}.");
        Console.WriteLine($"DATA {run}; bytes={fileBytes}; voters={voters}. Data is retained after this experiment.");
        var input = Path.Combine(run, "source.bin");
        await using (var file = new FileStream(input, FileMode.CreateNew, FileAccess.Write, FileShare.None, 64 * 1024, FileOptions.Asynchronous))
        {
            var buffer = RandomNumberGenerator.GetBytes(64 * 1024);
            for (long written = 0; written < fileBytes; written += Math.Min(buffer.Length, fileBytes - written))
                await file.WriteAsync(buffer.AsMemory(0, (int)Math.Min(buffer.Length, fileBytes - written)), stop.Token);
        }
        await using var cluster = new DemoCluster(run, voters, groupId: group);
        await cluster.StartAsync(stop.Token); var leader = await cluster.LeaderAsync(stop.Token);
        await using var client = cluster.Client();
        client.TopologyChanged += topology => Console.WriteLine($"TOPOLOGY term={topology.Term} leader={topology.LeaderId} nodes={string.Join(',', topology.Endpoints.Select(endpoint => endpoint.NodeId))}");
        using var localDatabase = new DBreezeEngine(Path.Combine(run, "subscriber"));
        using var subscriptions = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);
        var idsDelivered = 0;
        // book:start client-cursor
        bool SaveCursor(string entity, long id, ReadOnlyMemory<byte> data)
        {
            using var tx = localDatabase.GetTransaction();
            tx.SynchronizeTables("consumer");
            var cursorKey = 2.ToIndex(entity);
            var previous = tx.Select<byte[], long>("consumer", cursorKey);
            if (!previous.Exists || id > previous.Value)
            {
                // Business effects and the cursor share the same local transaction.
                tx.Insert<byte[], byte[]>("consumer", 1.ToIndex(entity, id), data.ToArray());
                tx.Insert<byte[], long>("consumer", cursorKey, id);
                tx.Commit();
            }
            return true;
        }
        long ReadCursor(string entity)
        {
            using var tx = localDatabase.GetTransaction();
            var row = tx.Select<byte[], long>("consumer", 2.ToIndex(entity));
            return row.Exists ? row.Value : 0;
        }
        var idSubscription = client.SubscribeIdsAsync("chunks", ReadCursor("chunks"), (entity, id) =>
        {
            SaveCursor(entity, id, ReadOnlyMemory<byte>.Empty);
            Interlocked.Increment(ref idsDelivered);
            Console.WriteLine($"ID ONLY committed {entity}/{id}");
            return true;
        }, subscriptions.Token);
        var metadataSubscription = client.SubscribeAsync("movies", ReadCursor("movies"), SaveCursor, subscriptions.Token);
        // book:end client-cursor
        var chunkBytes = command == "scenario" ? 1024 * 1024 : ClusterLimits.MaxPayloadBytes;
        var manifestId = await FileMessages.UploadAsync(client, input, "chunks", "movies", chunkBytes, stop.Token);
        var expectedChunks = (fileBytes + chunkBytes - 1) / chunkBytes;
        using (var deliveryDeadline = CancellationTokenSource.CreateLinkedTokenSource(stop.Token))
        {
            deliveryDeadline.CancelAfter(TimeSpan.FromSeconds(60));
            while (Volatile.Read(ref idsDelivered) < expectedChunks || ReadCursor("movies") < manifestId)
                await Task.Delay(50, deliveryDeadline.Token);
        }
        if (command == "scenario")
        {
            await client.CreateEntityAsync("status", EntityMode.HighSpeedCache, cancellationToken: stop.Token);
            await client.AddLogEntryAsync(Encoding.UTF8.GetBytes("old"), "status", cancellationToken: stop.Token);
            var latest = await client.AddLogEntryAsync(Encoding.UTF8.GetBytes("current"), "status", cancellationToken: stop.Token);
            if ((await client.ReadBatchAsync("status", 0, cancellationToken: stop.Token)).Messages.Single().MessageId != latest) throw new InvalidDataException("Cache failed.");
            await leader.Node.CreateSnapshotAsync(stop.Token);
            var joining = "n" + (voters + 1);
            await cluster.StartNodeAsync(joining, stop.Token);
            var added = await leader.Server.AddNodeAsync(cluster.Peer(joining), cluster.Endpoint(joining), Guid.NewGuid(), stop.Token);
            if (added.Stage != MembershipOperationStage.Completed) throw new InvalidOperationException(added.Error);
            var removedId = leader.Node.Status.NodeId; var operationId = Guid.NewGuid();
            while (true)
            {
                try
                {
                    var removed = await (await cluster.LeaderAsync(stop.Token)).Node.RemoveNodeAsync(removedId, operationId, stop.Token);
                    if (removed.Stage != MembershipOperationStage.Completed) throw new InvalidOperationException(removed.Error);
                    break;
                }
                catch (NotLeaderException) { await Task.Delay(50, stop.Token); }
            }
            await cluster.StopAsync(removedId);
        }
        await FileMessages.DownloadAsync(client, "movies", manifestId, Path.Combine(run, "download.bin"), stop.Token);
        subscriptions.Cancel();
        foreach (var subscription in new[] { idSubscription, metadataSubscription })
            try { await subscription; } catch (OperationCanceledException) { }
        var result = new { Passed = true, FileBytes = fileBytes, Chunks = expectedChunks, ManifestId = manifestId, IdsDelivered = idsDelivered, DataRoot = run };
        await File.WriteAllTextAsync(Path.Combine(run, "result.json"), JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true }), stop.Token);
        Console.WriteLine(JsonSerializer.Serialize(result));
    }
    else if (command == "benchmark")
    {
        await ClientMeasurements.RunAsync(root, Integer("rounds", 5), Integer("requests", 512), stop.Token,
            management: Option("management", "false") == "true", nativeTokens: Option("tokens", "false") == "true",
            dashboardAssembly: Option("dashboard", "") is { Length: > 0 } dashboard ? Path.GetFullPath(dashboard) : null,
            telemetry: Option("telemetry", "true") == "true", tls: Option("tls", "false") == "true",
            onlyVoters: Integer("only-voters", 0), onlyPayload: Integer("only-payload", 0), diagnose: Option("diagnose", "false") == "true",
            batchObservationReads: Option("batch-observation-reads", "true") == "true");
        if (Option("trace-hold", "false") == "true") await Console.In.ReadLineAsync();
    }
    else if (command is "publish" or "subscribe" or "upload" or "download")
    {
        using var security = args.Contains("--pki") ? new DemoSecurity(Option("pki", "")) : null;
        using var http = security?.HttpClient();
        var seeds = Option("seeds", string.Join(',', Enumerable.Range(7101, 3).Select(port => $"{(security is null ? "http" : "https")}://127.0.0.1:{port}/"))).Split(',').Select(value => new Uri(value)).ToArray();
        string? accessToken = null;
        if (Option("token-file", "") is { Length: > 0 } tokenFile)
        {
            if (new FileInfo(tokenFile).Length > 512) throw new ArgumentException("Token file exceeds512 bytes.");
            accessToken = (await File.ReadAllTextAsync(tokenFile, stop.Token)).Trim();
        }
        await using var client = new RaftClusterClient(new() { SeedEndpoints = seeds, GroupId = group, HttpClient = http,
            AccessTokenProvider = _ => ValueTask.FromResult(accessToken ?? Environment.GetEnvironmentVariable("RAFT_CLUSTER_ACCESS_TOKEN")) });
        client.TopologyChanged += topology => Console.WriteLine($"TOPOLOGY {JsonSerializer.Serialize(topology)}");
        var entity = Option("entity", "events");
        if (command == "publish") Console.WriteLine(await client.AddLogEntryAsync(Encoding.UTF8.GetBytes(Option("text", "Hello")), entity, cancellationToken: stop.Token));
        else if (command == "subscribe")
        {
            var after = long.Parse(Option("after", "0"), System.Globalization.CultureInfo.InvariantCulture);
            if (args.Contains("--ids")) await client.SubscribeIdsAsync(entity, after, (name, id) => { Console.WriteLine($"COMMITTED {name}/{id}"); return true; }, stop.Token);
            else await client.SubscribeAsync(entity, after, (name, id, data) => { Console.WriteLine($"COMMITTED {name}/{id} {Encoding.UTF8.GetString(data.Span)}"); return true; }, stop.Token);
        }
        else if (command == "upload") Console.WriteLine(await FileMessages.UploadAsync(client, Option("file", "source.bin"), Option("chunk-entity", "chunks"), Option("metadata-entity", "movies"), Integer("chunk-bytes", ClusterLimits.MaxPayloadBytes), stop.Token));
        else await FileMessages.DownloadAsync(client, Option("metadata-entity", "movies"), long.Parse(Option("manifest", "1"), System.Globalization.CultureInfo.InvariantCulture), Option("file", "download.bin"), stop.Token);
    }
    else Console.WriteLine("Raft.Cluster.Demo: scenario | node n1 [--daemon --management true --tokens true --pki PATH] | dashboard-stand | dashboard-process-scenario [--secure true] | create-lab-pki --pki NEW_PATH | bootstrap-stand | migrate-cluster-v2 --source OLD --destination NEW --options JSON | publish | subscribe [--ids] [--after ID] | upload --file PATH | download --manifest ID --file NEW_PATH | benchmark [--management true --dashboard DLL --tokens true --tls true] | stress-file --gib 80. Use --root ABSOLUTE_PATH, --voters 3/5, --group movies, --seeds HTTP_ORIGINS.");
}
catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
