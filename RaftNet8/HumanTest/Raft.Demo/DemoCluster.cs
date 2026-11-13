using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Demo;

internal static class DemoConsole
{
    private static readonly object Gate = new();
    public static void Write(string text) { lock (Gate) Console.WriteLine(text); }
}

internal sealed class DemoCluster : IAsyncDisposable
{
    internal sealed record Member(TcpRaftTransport Tcp, RaftNode Node, SnapshotKeyValueMachine Machine);
    private sealed class PartitionTransport(string from, DemoCluster cluster, TcpRaftTransport tcp) : IRaftTransport
    {
        public async ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken)
        {
            if (!cluster.Connected(from, peerId)) throw new IOException("Demo network partition.");
            var data = request is AppendRequest { Entries.Count: > 0 };
            var link = (from, peerId);
            if (data) cluster._dataFlights.AddOrUpdate(link, 1, static (_, count) => count + 1);
            try { return await tcp.SendAsync(peerId, request, cancellationToken); }
            finally { if (data) cluster._dataFlights.AddOrUpdate(link, 0, static (_, count) => count - 1); }
        }
    }

    private static readonly string[] Ids = { "n1", "n2", "n3" };
    private static readonly string[] KnownIds = { "n1", "n2", "n3", "n4" };
    private readonly string _data;
    private readonly DBreezeLogLayout _layout;
    private readonly Dictionary<string, IPEndPoint> _endpoints = new();
    private readonly ConcurrentDictionary<string, Member> _members = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _isolated = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<(string From, string To), int> _dataFlights = new();
    private readonly CancellationTokenSource _watchStop = new();
    private Task? _watch;
    public bool TraceCommands { get; set; } = true;
    public IReadOnlyDictionary<string, Member> Members => _members;

    public DemoCluster(string data, int basePort = 0)
    {
        _data = Path.GetFullPath(data);
        Directory.CreateDirectory(_data);
        var marker = Path.Combine(_data, "demo-format");
        if (!File.Exists(marker))
        {
            if (Directory.EnumerateFileSystemEntries(_data).Any())
                throw new InvalidDataException("Use an empty directory for a new demo; migrate old data offline before opening it.");
            File.WriteAllText(marker, "raft-demo-v3");
        }
        _layout = File.ReadAllText(marker) switch
        {
            "raft-demo-v2" => DBreezeLogLayout.SeparateTablesV2,
            "raft-demo-v3" => DBreezeLogLayout.UnifiedTableV3,
            _ => throw new InvalidDataException("Unknown demo data version.")
        };
        if (basePort != 0)
        {
            if (basePort is < 1024 or > 65532) throw new ArgumentOutOfRangeException(nameof(basePort));
            for (var i = 0; i < KnownIds.Length; i++) _endpoints.Add(KnownIds[i], new(IPAddress.Loopback, basePort + i));
        }
        else
        {
            var reservations = KnownIds.Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
            try
            {
                for (var i = 0; i < KnownIds.Length; i++)
                {
                    reservations[i].Start();
                    _endpoints.Add(KnownIds[i], (IPEndPoint)reservations[i].LocalEndpoint);
                }
            }
            finally { foreach (var reservation in reservations) reservation.Stop(); }
        }
    }

    public async Task StartAsync()
    {
        foreach (var id in Ids) await StartMemberAsync(id);
        if (Directory.Exists(Path.Combine(_data, "n4"))) await StartMemberAsync("n4");
        _watch = WatchAsync();
    }

    public async Task StartMemberAsync(string id)
    {
        if (!KnownIds.Contains(id)) throw new ArgumentException("Choose n1, n2, n3, or n4.");
        if (_members.ContainsKey(id)) throw new InvalidOperationException($"{id} is already running.");
        var root = Path.Combine(_data, id);
        var options = new RaftOptions { NodeId = id, GroupId = "human_test", Members = Ids,
            Learners = id == "n4" ? [id] : [], KnownPeers = KnownIds,
            SnapshotDirectory = Path.Combine(root, "snapshots"), SnapshotRetainEntries = 8 };
        var tcp = new TcpRaftTransport(options, _endpoints);
        RaftNode? node = null;
        SnapshotKeyValueMachine? machine = null;
        RaftStorageAdapter? log = null;
        try
        {
            machine = new SnapshotKeyValueMachine(Path.Combine(root, "application"), id,
                message => { if (TraceCommands) DemoConsole.Write(message); });
            log = new RaftStorageAdapter(new DBreezeLogStorage(Path.Combine(root, "log"), options, _layout));
            node = await RaftNode.CreateAsync(options, log, new PartitionTransport(id, this, tcp), machine);
            var member = new Member(tcp, node, machine);
            if (!_members.TryAdd(id, member)) throw new InvalidOperationException("Member already exists.");
            await node.StartAsync();
            tcp.Start((from, request, token) =>
            {
                if (!Connected(from, id)) throw new IOException("Demo network partition.");
                return node.HandleAsync(from, request, token);
            });
            DemoConsole.Write($"START {id} tcp={_endpoints[id]} checkpoint={machine.LastApplied}");
        }
        catch
        {
            _members.TryRemove(id, out _);
            await tcp.DisposeAsync();
            if (node is not null) await node.DisposeAsync();
            else if (log is not null) await log.DisposeAsync();
            machine?.Dispose();
            throw;
        }
    }

    public async Task StopMemberAsync(string id)
    {
        if (!_members.TryRemove(id, out var member)) throw new InvalidOperationException($"{id} is stopped.");
        await member.Tcp.DisposeAsync();
        await member.Node.DisposeAsync();
        member.Machine.Dispose();
        DemoConsole.Write($"STOP {id}");
    }

    private bool Connected(string from, string to) => !_isolated.ContainsKey(from) && !_isolated.ContainsKey(to);
    public void Isolate(string id)
    {
        if (!KnownIds.Contains(id)) throw new ArgumentException("Choose n1, n2, n3, or n4.");
        _isolated[id] = 0;
        DemoConsole.Write($"ISOLATE {id}: traffic in both directions is dropped.");
    }
    public void Heal()
    {
        _isolated.Clear();
        DemoConsole.Write("HEAL: all links are connected.");
    }

    public async Task<Member> LeaderAsync(CancellationToken token, string? excluding = null)
    {
        var timer = Stopwatch.StartNew();
        while (timer.Elapsed < TimeSpan.FromSeconds(15))
        {
            token.ThrowIfCancellationRequested();
            var leader = _members.Where(pair => pair.Key != excluding && !_isolated.ContainsKey(pair.Key) &&
                    pair.Value.Node.Status.Role == RaftRole.Leader && pair.Value.Node.Status.Fault is null)
                .OrderByDescending(pair => pair.Value.Node.Status.Term).Select(pair => pair.Value).FirstOrDefault();
            if (leader is not null) return leader;
            await Task.Delay(25, token);
        }
        throw new TimeoutException("No leader with a reachable quorum. Start/heal at least two nodes.");
    }

    public async Task<ProposalResult> SetAsync(string key, string value, CancellationToken token)
    {
        // The same request ID is reused across retries; the state machine deduplicates it durably.
        var bytes = SnapshotKeyValueMachine.Encode(key, value);
        for (var attempt = 0; ; attempt++)
        {
            var leader = await LeaderAsync(token);
            try { return await leader.Node.ProposeAsync(bytes, token); }
            catch (NotLeaderException) when (attempt < 3) { await Task.Delay(50, token); }
        }
    }

    public async Task<string?> GetAsync(string key, CancellationToken token)
    {
        var leader = await LeaderAsync(token);
        await leader.Node.ReadIndexAsync(token);
        return leader.Machine.Get(key);
    }

    public async Task WaitAppliedAsync(long index, CancellationToken token)
    {
        var timer = Stopwatch.StartNew();
        while (_members.Values.Any(member => member.Node.Status.Role != RaftRole.Removed && member.Node.Status.LastApplied < index))
        {
            token.ThrowIfCancellationRequested();
            if (timer.Elapsed > TimeSpan.FromSeconds(15)) throw new TimeoutException("Members have not caught up.");
            await Task.Delay(25, token);
        }
    }

    public void ShowStatus()
    {
        var rows = new List<string> { "node role       term leader commit applied log   snap  keys link" };
        foreach (var id in KnownIds)
        {
            if (!_members.TryGetValue(id, out var member)) { rows.Add($"{id}   STOPPED"); continue; }
            var status = member.Node.Status;
            rows.Add($"{id,-4} {status.Role,-10} {status.Term,4} {status.LeaderId ?? "?",-6} {status.CommitIndex,6} {status.LastApplied,7} {status.LastLogIndex,5} {status.Snapshot?.LastIncludedIndex ?? 0,5} {member.Machine.Count,4} {(_isolated.ContainsKey(id) ? "isolated" : "up")}");
            if (status.Role == RaftRole.Leader)
            {
                rows.Add($"     voters={string.Join(',', status.Configuration!.Voters)} learners={string.Join(',', status.Configuration.Learners)}");
                foreach (var peer in status.Peers)
                    rows.Add($"     {peer.NodeId}: match={peer.MatchIndex} next={peer.NextIndex} mode={peer.Mode} inflight={peer.InFlightMessages}/{peer.InFlightBytes}B");
            }
            if (status.Fault is not null) rows.Add($"     FAULT: {status.Fault.Message}");
        }
        DemoConsole.Write(string.Join(Environment.NewLine, rows));
    }

    public async Task LoadAsync(int count, CancellationToken token)
    {
        if (count is < 1 or > 1_000_000) throw new ArgumentOutOfRangeException(nameof(count), "Choose 1..1000000 commands.");
        TraceCommands = false;
        try
        {
            var timer = Stopwatch.StartNew();
            await Parallel.ForEachAsync(Enumerable.Range(0, count), new ParallelOptions { MaxDegreeOfParallelism = 64, CancellationToken = token },
                async (i, ct) => { await SetAsync($"load-{i % 8}", i.ToString(System.Globalization.CultureInfo.InvariantCulture), ct); });
            timer.Stop();
            var leader = await LeaderAsync(token);
            var fence = await leader.Node.ReadIndexAsync(token);
            // Let connected followers finish while per-command tracing is still suppressed.
            while (_members.Any(pair => !_isolated.ContainsKey(pair.Key) && pair.Value.Node.Status.Role != RaftRole.Removed && pair.Value.Node.Status.LastApplied < fence.Index))
                await Task.Delay(25, token);
            DemoConsole.Write($"LOAD {count} commands committed and applied on the leader in {timer.Elapsed.TotalSeconds:F3}s ({count / timer.Elapsed.TotalSeconds:F0} commands/s). Demo timing; use benchmarks for measurement.");
        }
        finally { TraceCommands = true; }
    }

    public async Task TransferAsync(string target, CancellationToken token) =>
        await (await LeaderAsync(token)).Node.TransferLeadershipAsync(target, token);
    public async Task AddAsync(string target, CancellationToken token)
    {
        if (!_members.ContainsKey(target)) await StartMemberAsync(target);
        var leader = await LeaderAsync(token);
        await leader.Node.ReadIndexAsync(token);
        await leader.Node.AddLearnerAsync(target, token);
    }
    public async Task PromoteAsync(string target, CancellationToken token)
    {
        var leader = await LeaderAsync(token);
        await leader.Node.ReadIndexAsync(token);
        while (!leader.Node.Status.Peers.Any(p => p.NodeId == target && p.MatchIndex >= leader.Node.Status.LastLogIndex))
            await Task.Delay(25, token);
        await leader.Node.PromoteToVoterAsync(target, token);
    }
    public async Task RemoveAsync(string target, CancellationToken token)
    {
        var leader = await LeaderAsync(token);
        await leader.Node.ReadIndexAsync(token);
        await leader.Node.RemoveNodeAsync(target, token);
    }
    public async Task SnapshotAsync(CancellationToken token)
    {
        var leader = await LeaderAsync(token);
        var snapshot = await leader.Node.CreateSnapshotAsync(token);
        DemoConsole.Write($"SNAPSHOT {leader.Node.Status.NodeId} index={snapshot.LastIncludedIndex} term={snapshot.LastIncludedTerm}");
    }

    public async Task RunScenarioAsync(CancellationToken token)
    {
        DemoConsole.Write("SCENARIO: real TCP, three disk-backed DBreeze nodes, quorum loss, failover, and restart.");
        var first = await LeaderAsync(token);
        await SetAsync("message", "hello-raft", token);
        var one = await SetAsync("counter", "1", token);
        await WaitAppliedAsync(one.Index, token);
        ShowStatus();
        var isolatedId = first.Node.Status.NodeId;
        Isolate(isolatedId);
        try
        {
            await first.Node.ProposeAsync(KeyValueMachine.Encode("orphan", "must-not-commit"), token);
            throw new InvalidOperationException("An isolated leader committed without quorum.");
        }
        catch (NotLeaderException) { DemoConsole.Write("PASS: isolated leader could not commit and stepped down."); }
        var replacement = await LeaderAsync(token, isolatedId);
        DemoConsole.Write($"NEW LEADER {replacement.Node.Status.NodeId}, term={replacement.Node.Status.Term}");
        await SetAsync("counter", "2", token);
        Heal();
        var fence = await replacement.Node.ReadIndexAsync(token);
        await WaitAppliedAsync(fence.Index, token);
        if (_members.Values.Any(member => member.Machine.Get("orphan") is not null)) throw new InvalidOperationException("Orphan command was applied.");
        var stoppedId = replacement.Node.Status.NodeId;
        await StopMemberAsync(stoppedId);
        await LeaderAsync(token);
        await SetAsync("counter", "3", token);
        await StartMemberAsync(stoppedId);
        await LoadAsync(100, token);
        var leader = await LeaderAsync(token);
        fence = await leader.Node.ReadIndexAsync(token);
        await WaitAppliedAsync(fence.Index, token);
        if (_members.Count != 3 || _members.Values.Any(member => member.Machine.Get("counter") != "3" || member.Machine.Get("message") != "hello-raft"))
            throw new InvalidOperationException("State machines disagree after recovery.");
        // The stopped follower falls behind the compacted prefix, then installs the image over TCP.
        var lagging = _members.Keys.First(id => id != leader.Node.Status.NodeId);
        await StopMemberAsync(lagging);
        await LoadAsync(64, token);
        await SnapshotAsync(token);
        var imageIndex = leader.Node.Status.Snapshot!.LastIncludedIndex;
        // Previously issued RPCs own their payloads even after log compaction.
        // Let them finish while the follower is stopped; otherwise reconnect can
        // legitimately replay that data and bypass the image this scenario tests.
        while (leader.Node.Status.Peers.Single(peer => peer.NodeId == lagging).Mode != ReplicationMode.Snapshot ||
            _dataFlights.Any(link => link.Key.To == lagging && link.Value != 0))
            await Task.Delay(25, token);
        DemoConsole.Write($"PASS: {lagging} requires a snapshot; outstanding data appends have drained.");
        await StartMemberAsync(lagging);
        while ((_members[lagging].Node.Status.Snapshot?.LastIncludedIndex ?? 0) < imageIndex) await Task.Delay(25, token);
        await AddAsync("n4", token);
        await PromoteAsync("n4", token);
        await TransferAsync("n4", token);
        while (_members["n4"].Node.Status.Role != RaftRole.Leader) await Task.Delay(25, token);
        await RemoveAsync("n1", token);
        while (_members["n1"].Node.Status.Role != RaftRole.Removed) await Task.Delay(25, token);
        await StopMemberAsync("n1");
        await StartMemberAsync("n1");
        if (_members["n1"].Node.Status.Role != RaftRole.Removed) throw new InvalidOperationException("Removed node campaigned after restart.");
        var final = await SetAsync("counter", "4", token);
        await WaitAppliedAsync(final.Index, token);
        if (await GetAsync("counter", token) != "4") throw new InvalidOperationException("ReadIndex returned a stale value.");
        ShowStatus();
        DemoConsole.Write("SCENARIO PASS: quorum safety, failover/restart, ReadIndex, snapshot catch-up, learner promotion, transfer, and durable removal verified.");
    }

    private async Task WatchAsync()
    {
        var previous = new Dictionary<string, (RaftRole, long)>();
        try
        {
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(200));
            while (await timer.WaitForNextTickAsync(_watchStop.Token))
            {
                foreach (var (id, member) in _members)
                {
                    var status = member.Node.Status;
                    var now = (status.Role, status.Term);
                    if (!previous.TryGetValue(id, out var old) || old != now)
                    {
                        previous[id] = now;
                        DemoConsole.Write($"STATE {id}: {status.Role} term={status.Term} leader={status.LeaderId ?? "?"}");
                    }
                }
            }
        }
        catch (OperationCanceledException) when (_watchStop.IsCancellationRequested) { }
    }

    public async ValueTask DisposeAsync()
    {
        _watchStop.Cancel();
        if (_watch is not null) await _watch;
        foreach (var id in _members.Keys.ToArray()) await StopMemberAsync(id);
        _watchStop.Dispose();
    }
}
