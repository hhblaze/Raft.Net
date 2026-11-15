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
    if (command == "node")
    {
        var id = args.Length > 1 ? args[1] : throw new ArgumentException("node n1 --root ...");
        await using var cluster = new DemoCluster(root, voters, fixedPorts: true, groupId: group,
            peerBasePort: Integer("peer-base-port", 6101), clientBasePort: Integer("client-base-port", 7101));
        var host = await cluster.StartNodeAsync(id, stop.Token);
        Console.WriteLine($"NODE {id} peer={cluster.Peer(id).Port} client={cluster.Endpoint(id).Address} data={root}");
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
        await ClientMeasurements.RunAsync(root, Integer("rounds", 5), Integer("requests", 512), stop.Token);
    }
    else if (command is "publish" or "subscribe" or "upload" or "download")
    {
        var seeds = Option("seeds", "http://127.0.0.1:7101/,http://127.0.0.1:7102/,http://127.0.0.1:7103/").Split(',').Select(value => new Uri(value)).ToArray();
        await using var client = new RaftClusterClient(new() { SeedEndpoints = seeds, GroupId = group });
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
    else Console.WriteLine("Raft.Cluster.Demo: scenario | node n1 | publish | subscribe [--ids] [--after ID] | upload --file PATH | download --manifest ID --file NEW_PATH | benchmark | stress-file --gib 80. Use --root ABSOLUTE_PATH, --voters 3/5, --group movies, --seeds HTTP_ORIGINS.");
}
catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
