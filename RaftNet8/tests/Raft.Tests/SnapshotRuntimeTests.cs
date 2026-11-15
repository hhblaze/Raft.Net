using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

internal sealed class SnapshotTestMachine(string path) : IRaftBatchStateMachine, ISnapshotStateMachine, IDisposable
{
    private static readonly byte[] Padding = Enumerable.Range(0, 16 * 1024).Select(i => (byte)(i % 251)).ToArray();
    public readonly DBreezeGenerationStateMachine Database = new(path, "padded-counter-v1", "checkpoint", ["counter"], (tx, entry) =>
    {
        SnapshotStorageTests.ApplyCounter(tx, entry);
        // Force a genuinely multi-chunk image; a one-key counter alone occupies less than 4 KiB.
        if (!tx.Select<string, byte[]>("counter", "padding").Exists) tx.Insert<string, byte[]>("counter", "padding", Padding);
    });
    public Func<CancellationToken, Task>? BeforeBuild;
    public long LastApplied => Database.LastApplied;
    public AppliedCheckpoint Checkpoint => Database.Checkpoint;
    public ValueTask ApplyAsync(LogEntry entry, CancellationToken token) => Database.ApplyAsync(entry, token);
    public ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken token) => Database.ApplyBatchAsync(entries, token);
    public async ValueTask CreateSnapshotAsync(SnapshotBuildContext context, string outputPath, CancellationToken token)
    {
        if (BeforeBuild is { } before) await before(token);
        await Database.CreateSnapshotAsync(context, outputPath, token);
    }
    public ValueTask PrepareSnapshotAsync(SnapshotImage image, long maxBytes, CancellationToken token) => Database.PrepareSnapshotAsync(image, maxBytes, token);
    public ValueTask InstallSnapshotAsync(SnapshotImage image, CancellationToken token) => Database.InstallSnapshotAsync(image, token);
    public long Value => SnapshotStorageTests.Value(Database);
    public void Dispose() => Database.Dispose();
}

internal sealed class SnapshotTestCluster : IAsyncDisposable
{
    private sealed class Transport(string from, SnapshotTestCluster cluster) : IRaftTransport
    {
        public async ValueTask<RaftRpc> SendAsync(string peer, RaftRpc request, CancellationToken token)
        {
            if (cluster.BeforeSend is { } before) await before(from, peer, request, token);
            if (!cluster.Nodes.TryGetValue(peer, out var node)) throw new IOException("Stopped.");
            if (request is SnapshotChunkRequest chunk) cluster.Chunks.Enqueue((from, peer, chunk.Offset, chunk.StorageId));
            var response = await node.HandleAsync(from, request, token);
            if (response is SnapshotChunkResponse { Success: true, Complete: false } chunkReply)
                cluster.AcknowledgedChunks.Enqueue((peer, chunkReply.NextOffset));
            return response;
        }
    }
    public readonly ConcurrentDictionary<string, RaftNode> Nodes = new();
    public readonly Dictionary<string, SnapshotTestMachine> Machines = new();
    public readonly Dictionary<string, DBreezeLogStorage> Stores = new();
    public readonly ConcurrentQueue<(string From, string To, long Offset, string Id)> Chunks = new();
    public readonly ConcurrentQueue<(string To, long NextOffset)> AcknowledgedChunks = new();
    public Func<string, string, RaftRpc, CancellationToken, ValueTask>? BeforeSend;
    public readonly Dictionary<string, RaftOptions> Options = new();
    private readonly Dictionary<string, TestDirectory> _directories = new();
    private readonly Dictionary<string, IPEndPoint> _endpoints = new();
    private readonly Dictionary<string, TcpRaftTransport> _tcp = new();
    private readonly bool _useTcp;

    public SnapshotTestCluster(int count = 3, bool tcp = false, long interval = 0, long bandwidth = 16 * 1024 * 1024)
    {
        _useTcp = tcp;
        var ids = Enumerable.Range(1, count).Select(i => $"n{i}").ToArray();
        foreach (var id in ids)
        {
            _directories[id] = new TestDirectory();
            Options[id] = new() { NodeId = id, Members = ids, SnapshotDirectory = Path.Combine(_directories[id].Path, "snapshots"),
                SnapshotChunkBytes = 4096, SnapshotRetainEntries = 3, SnapshotIntervalEntries = interval,
                SnapshotBytesPerSecond = bandwidth,
                TickInterval = TimeSpan.FromMilliseconds(20), ElectionMinTicks = 100, ElectionMaxTicks = 150, RpcTimeout = TimeSpan.FromSeconds(1) };
        }
        var listeners = ids.Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
        try
        {
            for (var i = 0; i < ids.Length; i++) { listeners[i].Start(); _endpoints[ids[i]] = (IPEndPoint)listeners[i].LocalEndpoint; }
        }
        finally { foreach (var listener in listeners) listener.Stop(); }
    }
    public string ApplicationPath(string id) => Path.Combine(_directories[id].Path, "application");
    public async Task StartAsync() { foreach (var id in Options.Keys) await StartAsync(id); }
    public async Task StartAsync(string id)
    {
        var machine = new SnapshotTestMachine(ApplicationPath(id));
        Machines[id] = machine;
        var store = new DBreezeLogStorage(Path.Combine(_directories[id].Path, "log"), Options[id]);
        Stores[id] = store;
        IRaftTransport transport;
        if (_useTcp) { _tcp[id] = new TcpRaftTransport(Options[id], _endpoints); transport = _tcp[id]; }
        else transport = new Transport(id, this);
        var log = new RaftStorageAdapter(store);
        RaftNode node;
        try { node = await RaftNode.CreateAsync(Options[id], log, transport, machine); }
        catch { await log.DisposeAsync(); machine.Dispose(); throw; }
        Nodes[id] = node;
        await node.StartAsync();
        if (_useTcp) _tcp[id].Start(node.HandleAsync);
    }
    public async Task StopAsync(string id)
    {
        if (_tcp.Remove(id, out var tcp)) await tcp.DisposeAsync();
        if (Nodes.TryRemove(id, out var node)) await node.DisposeAsync();
        Machines[id].Dispose();
    }
    public async Task<RaftNode> LeaderAsync()
    {
        RaftNode? leader = null;
        await RuntimeCluster.EventuallyAsync(() =>
        {
            var leaders = Nodes.Values.Where(n => n.Status.Role == RaftRole.Leader && n.Status.CommitIndex > 0).ToArray();
            if (leaders.Length != 1) return false;
            leader = leaders[0];
            return true;
        });
        return leader!;
    }
    public static Task<ProposalResult[]> WriteAsync(RaftNode leader, int count) =>
        Task.WhenAll(Enumerable.Range(0, count).Select(_ => leader.ProposeAsync(BitConverter.GetBytes(1))));
    public async Task WaitForSnapshotPrefixAsync(string target)
    {
        try { await RuntimeCluster.EventuallyAsync(() => AcknowledgedChunks.Any(c => c.To == target && c.NextOffset >= 4096)); }
        catch (TimeoutException error)
        {
            throw new InvalidOperationException(string.Join("\n", Nodes.Values.Select(n => n.Status + " peers=" +
                string.Join(",", n.Status.Peers.Select(p => p.ToString())))) + "\nchunks=" + string.Join(",", Chunks.Take(20)) +
                "\nacks=" + string.Join(",", AcknowledgedChunks.Take(20)), error);
        }
    }
    public async ValueTask DisposeAsync()
    {
        foreach (var id in Nodes.Keys) await StopAsync(id);
        foreach (var directory in _directories.Values) directory.Dispose();
    }
}

public sealed class SnapshotRuntimeTests
{
    [Fact]
    public async Task Creation_does_not_block_live_apply_and_restart_restores_a_missing_application_from_compacted_log()
    {
        await using var cluster = new SnapshotTestCluster(1);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        await SnapshotTestCluster.WriteAsync(leader, 50);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.Machines["n1"].BeforeBuild = async token => { entered.TrySetResult(); await release.Task.WaitAsync(token); };
        var snapshot = leader.CreateSnapshotAsync();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            await SnapshotTestCluster.WriteAsync(leader, 20).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(70, cluster.Machines["n1"].Value);
        }
        finally { release.TrySetResult(); }
        var metadata = await snapshot.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(51, metadata.LastIncludedIndex);
        Assert.Equal(49, cluster.Stores["n1"].Recover().FirstIndex);
        Assert.Null(leader.Status.Fault);
        await cluster.StopAsync("n1");
        // Simulate loss of the application pointer. Its old generation remains untouched; recovery uses the immutable image.
        File.Delete(Path.Combine(cluster.ApplicationPath("n1"), "current"));
        await cluster.StartAsync("n1");
        var replacement = await cluster.LeaderAsync();
        await replacement.ReadIndexAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(70, cluster.Machines["n1"].Value);
        Assert.True(replacement.Status.LastApplied >= 71);
        Assert.Null(replacement.Status.Fault);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    public async Task Far_behind_disk_TCP_follower_installs_snapshot_then_replays_the_suffix(int count)
    {
        await using var cluster = new SnapshotTestCluster(count, tcp: true);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var target = cluster.Nodes.Keys.First(id => id != leader.Status.NodeId);
        await cluster.StopAsync(target);
        await SnapshotTestCluster.WriteAsync(leader, 100);
        var metadata = await leader.CreateSnapshotAsync().WaitAsync(TimeSpan.FromSeconds(10));
        var writes = await SnapshotTestCluster.WriteAsync(leader, 50);
        await cluster.StartAsync(target);
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes[target].Status.LastApplied >= writes.Max(w => w.Index), 15000);
        Assert.Equal(150, cluster.Machines[target].Value);
        Assert.Equal(metadata.StorageId, cluster.Nodes[target].Status.Snapshot!.StorageId);
        Assert.All(cluster.Nodes.Values, node => Assert.Null(node.Status.Fault));
        await cluster.StopAsync(target);
        await cluster.StartAsync(target);
        Assert.Equal(150, cluster.Machines[target].Value);
    }

    [Fact]
    public async Task Interrupted_transfer_resumes_durable_chunks_after_receiver_restart()
    {
        await using var cluster = new SnapshotTestCluster();
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var target = cluster.Nodes.Keys.First(id => id != leader.Status.NodeId);
        await cluster.StopAsync(target);
        await SnapshotTestCluster.WriteAsync(leader, 100);
        var metadata = await leader.CreateSnapshotAsync();
        var blocked = 1;
        cluster.BeforeSend = (_, peer, request, _) =>
        {
            if (peer == target && request is SnapshotChunkRequest { Offset: > 0 } && Volatile.Read(ref blocked) != 0)
                throw new IOException("Interrupted snapshot.");
            return ValueTask.CompletedTask;
        };
        await cluster.StartAsync(target);
        await cluster.WaitForSnapshotPrefixAsync(target);
        await cluster.StopAsync(target);
        var before = cluster.Chunks.Count;
        await cluster.StartAsync(target);
        Interlocked.Exchange(ref blocked, 0);
        await RuntimeCluster.EventuallyAsync(() => cluster.Machines[target].Value == 100, 15000);
        Assert.Contains(cluster.Chunks.Skip(before), chunk => chunk.To == target && chunk.Offset > 0 && chunk.Id == metadata.StorageId);
        Assert.All(cluster.Nodes.Values, node => Assert.Null(node.Status.Fault));
    }

    [Fact]
    public async Task Automatic_threshold_publishes_a_durable_image_and_compacts()
    {
        await using var cluster = new SnapshotTestCluster(1, interval: 20);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        await SnapshotTestCluster.WriteAsync(leader, 50);
        await RuntimeCluster.EventuallyAsync(() => leader.Status.Snapshot is { LastIncludedIndex: >= 20 });
        Assert.True(cluster.Stores["n1"].Recover().FirstIndex > 1);
        Assert.Equal(50, cluster.Machines["n1"].Value);
        Assert.Null(leader.Status.Fault);
    }

    [Fact]
    public async Task A_new_leader_can_replace_an_incomplete_older_image_without_changing_live_state()
    {
        await using var cluster = new SnapshotTestCluster();
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var target = cluster.Nodes.Keys.First(id => id != leader.Status.NodeId);
        var successor = cluster.Nodes.Keys.Single(id => id != leader.Status.NodeId && id != target);
        await cluster.StopAsync(target);
        await SnapshotTestCluster.WriteAsync(leader, 100);
        var oldImage = await leader.CreateSnapshotAsync();
        var block = 1;
        cluster.BeforeSend = (_, peer, request, _) =>
        {
            if (peer == target && Volatile.Read(ref block) != 0 &&
                (request is SnapshotChunkRequest { Offset: > 0 } || request is AppendRequest { Entries.Count: > 0 }))
                throw new IOException("Transfer paused.");
            return ValueTask.CompletedTask;
        };
        await cluster.StartAsync(target);
        await cluster.WaitForSnapshotPrefixAsync(target);
        Assert.Equal(0, cluster.Machines[target].Value);
        await leader.TransferLeadershipAsync(successor).WaitAsync(TimeSpan.FromSeconds(10));
        var replacement = cluster.Nodes[successor];
        await replacement.ReadIndexAsync();
        var newImage = await replacement.CreateSnapshotAsync();
        Assert.NotEqual(oldImage.StorageId, newImage.StorageId);
        Interlocked.Exchange(ref block, 0);
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes[target].Status.Snapshot?.StorageId == newImage.StorageId, 15000);
        Assert.Equal(100, cluster.Machines[target].Value);
        Assert.All(cluster.Nodes.Values, node => Assert.Null(node.Status.Fault));
    }

    [Fact]
    public async Task Throttled_snapshot_keeps_contact_and_quorum_while_a_chunk_waits_longer_than_an_election_timeout()
    {
        await using var cluster = new SnapshotTestCluster(bandwidth: 1024);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var target = cluster.Nodes.Keys.First(id => id != leader.Status.NodeId);
        var other = cluster.Nodes.Keys.Single(id => id != leader.Status.NodeId && id != target);
        await cluster.StopAsync(target);
        await SnapshotTestCluster.WriteAsync(leader, 50);
        await leader.CreateSnapshotAsync();
        await cluster.StopAsync(other);
        await cluster.StartAsync(target);
        await cluster.WaitForSnapshotPrefixAsync(target);
        var term = leader.Status.Term;
        await Task.Delay(1200);
        Assert.Equal(RaftRole.Leader, leader.Status.Role);
        Assert.Equal(term, cluster.Nodes[target].Status.Term);
        Assert.Equal(RaftRole.Follower, cluster.Nodes[target].Status.Role);
        Assert.Equal(ReplicationMode.Snapshot, leader.Status.Peers.Single(p => p.NodeId == target).Mode);
        Assert.Single(cluster.Chunks, c => c.To == target); // next 4 KiB chunk is throttled for four seconds
        Assert.All(cluster.Nodes.Values, node => Assert.Null(node.Status.Fault));
    }
}
