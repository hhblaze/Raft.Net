using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using DBreeze;
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
    internal sealed record Member(DBreezeEngine Engine, TcpRaftTransport Tcp, RaftNode Node, KeyValueMachine Machine);
    private sealed class PartitionTransport(string from, DemoCluster cluster, TcpRaftTransport tcp) : IRaftTransport
    {
        public ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken)
        {
            if (!cluster.Connected(from, peerId)) throw new IOException("Demo network partition.");
            return tcp.SendAsync(peerId, request, cancellationToken);
        }
    }

    private static readonly string[] Ids = { "n1", "n2", "n3" };
    private readonly string _data;
    private readonly Dictionary<string, IPEndPoint> _endpoints = new();
    private readonly ConcurrentDictionary<string, Member> _members = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _isolated = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _watchStop = new();
    private Task? _watch;
    public bool TraceCommands { get; set; } = true;
    public IReadOnlyDictionary<string, Member> Members => _members;

    public DemoCluster(string data, int basePort = 0)
    {
        _data = Path.GetFullPath(data);
        Directory.CreateDirectory(_data);
        if (basePort != 0)
        {
            if (basePort is < 1024 or > 65533) throw new ArgumentOutOfRangeException(nameof(basePort));
            for (var i = 0; i < Ids.Length; i++) _endpoints.Add(Ids[i], new(IPAddress.Loopback, basePort + i));
        }
        else
        {
            var reservations = Ids.Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
            try
            {
                for (var i = 0; i < Ids.Length; i++)
                {
                    reservations[i].Start();
                    _endpoints.Add(Ids[i], (IPEndPoint)reservations[i].LocalEndpoint);
                }
            }
            finally { foreach (var reservation in reservations) reservation.Stop(); }
        }
    }

    public async Task StartAsync()
    {
        foreach (var id in Ids) await StartMemberAsync(id);
        _watch = WatchAsync();
    }

    public async Task StartMemberAsync(string id)
    {
        if (!Ids.Contains(id)) throw new ArgumentException("Choose n1, n2, or n3.");
        if (_members.ContainsKey(id)) throw new InvalidOperationException($"{id} is already running.");
        var options = new RaftOptions { NodeId = id, GroupId = "human_test", Members = Ids };
        var engine = new DBreezeEngine(Path.Combine(_data, id));
        var tcp = new TcpRaftTransport(options, _endpoints);
        RaftNode? node = null;
        try
        {
            var machine = new KeyValueMachine(engine, id, message => { if (TraceCommands) DemoConsole.Write(message); });
            var storage = new DBreezeRaftStorage(engine, options);
            node = new RaftNode(options, storage, new PartitionTransport(id, this, tcp), machine);
            var member = new Member(engine, tcp, node, machine);
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
            engine.Dispose();
            throw;
        }
    }

    public async Task StopMemberAsync(string id)
    {
        if (!_members.TryRemove(id, out var member)) throw new InvalidOperationException($"{id} is stopped.");
        await member.Tcp.DisposeAsync();
        await member.Node.DisposeAsync();
        member.Engine.Dispose();
        DemoConsole.Write($"STOP {id}");
    }

    private bool Connected(string from, string to) => !_isolated.ContainsKey(from) && !_isolated.ContainsKey(to);
    public void Isolate(string id)
    {
        if (!Ids.Contains(id)) throw new ArgumentException("Choose n1, n2, or n3.");
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
        var bytes = KeyValueMachine.Encode(key, value);
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
        await leader.Node.ReadBarrierAsync(token);
        return leader.Machine.Get(key);
    }

    public async Task WaitAppliedAsync(long index, CancellationToken token)
    {
        var timer = Stopwatch.StartNew();
        while (_members.Values.Any(member => member.Node.Status.LastApplied < index))
        {
            token.ThrowIfCancellationRequested();
            if (timer.Elapsed > TimeSpan.FromSeconds(15)) throw new TimeoutException("Members have not caught up.");
            await Task.Delay(25, token);
        }
    }

    public void ShowStatus()
    {
        var rows = new List<string> { "node role       term leader commit applied log   keys link" };
        foreach (var id in Ids)
        {
            if (!_members.TryGetValue(id, out var member)) { rows.Add($"{id}   STOPPED"); continue; }
            var status = member.Node.Status;
            rows.Add($"{id,-4} {status.Role,-10} {status.Term,4} {status.LeaderId ?? "?",-6} {status.CommitIndex,6} {status.LastApplied,7} {status.LastLogIndex,5} {member.Machine.Count,4} {(_isolated.ContainsKey(id) ? "isolated" : "up")}");
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
            var fence = await leader.Node.ReadBarrierAsync(token);
            // Let connected followers finish while per-command tracing is still suppressed.
            while (_members.Any(pair => !_isolated.ContainsKey(pair.Key) && pair.Value.Node.Status.LastApplied < fence.Index))
                await Task.Delay(25, token);
            DemoConsole.Write($"LOAD {count} commands committed and applied on the leader in {timer.Elapsed.TotalSeconds:F3}s ({count / timer.Elapsed.TotalSeconds:F0} commands/s). Demo timing; use benchmarks for measurement.");
        }
        finally { TraceCommands = true; }
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
        var fence = await replacement.Node.ReadBarrierAsync(token);
        await WaitAppliedAsync(fence.Index, token);
        if (_members.Values.Any(member => member.Machine.Get("orphan") is not null)) throw new InvalidOperationException("Orphan command was applied.");
        var stoppedId = replacement.Node.Status.NodeId;
        await StopMemberAsync(stoppedId);
        await LeaderAsync(token);
        await SetAsync("counter", "3", token);
        await StartMemberAsync(stoppedId);
        await LoadAsync(100, token);
        var leader = await LeaderAsync(token);
        fence = await leader.Node.ReadBarrierAsync(token);
        await WaitAppliedAsync(fence.Index, token);
        if (_members.Count != 3 || _members.Values.Any(member => member.Machine.Get("counter") != "3" || member.Machine.Get("message") != "hello-raft"))
            throw new InvalidOperationException("State machines disagree after recovery.");
        ShowStatus();
        DemoConsole.Write("SCENARIO PASS: quorum safety, ordered replication, failover, durable application checkpoints, and rejoin verified.");
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
