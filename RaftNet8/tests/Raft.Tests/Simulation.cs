using Raft.Core;
using Raft.Storage;

namespace Raft.Tests;

internal sealed class SimCluster : IDisposable
{
    internal sealed record Packet(string From, string To, RaftRpc Message, RaftRpc Original);
    public Dictionary<string, RaftCore> Nodes { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, DBreezeRaftStorage> Stores { get; } = new(StringComparer.Ordinal);
    public List<Packet> Network { get; } = new();
    public HashSet<string> Offline { get; } = new(StringComparer.Ordinal);
    private readonly Dictionary<string, RaftOptions> _options = new();
    private readonly HashSet<(string, string)> _blocked = new();
    private readonly Dictionary<long, LogEntry> _committed = new();
    private readonly Dictionary<long, string> _leaders = new();
    private readonly Dictionary<string, (long Term, long Commit)> _previous = new();
    private readonly Random _random;
    public double DropRate { get; set; }
    public double DuplicateRate { get; set; }
    public bool Reorder { get; set; }

    public SimCluster(int size = 3, int seed = 1, int maxAppendEntries = 16)
    {
        _random = new Random(seed);
        var members = Enumerable.Range(1, size).Select(i => $"n{i}").ToArray();
        foreach (var id in members)
        {
            var options = new RaftOptions { NodeId = id, Members = members, MaxAppendEntries = maxAppendEntries };
            _options.Add(id, options);
            var store = DBreezeRaftStorage.CreateInMemory(options);
            Stores.Add(id, store);
            Nodes.Add(id, new RaftCore(options, store, new Random(_random.Next())));
        }
    }

    public IEnumerable<string> Leaders => Nodes.Where(pair => pair.Value.Role == RaftRole.Leader && !Offline.Contains(pair.Key)).Select(pair => pair.Key);
    public string Leader => Assert.Single(Leaders);

    public void Partition(params string[][] components)
    {
        _blocked.Clear();
        foreach (var a in Nodes.Keys)
            foreach (var b in Nodes.Keys)
                if (a != b && !components.Any(group => group.Contains(a) && group.Contains(b))) _blocked.Add((a, b));
    }

    public void Heal() => _blocked.Clear();

    public void Restart(string id)
    {
        Nodes[id] = new RaftCore(_options[id], Stores[id], new Random(_random.Next()));
        Offline.Remove(id);
    }

    public long Propose(string leader, byte[] command) => Nodes[leader].Propose(new[] { new ProposedCommand(command) });

    public void Collect()
    {
        foreach (var (from, node) in Nodes)
            while (node.TryDequeue(out var rpc)) Network.Add(new(from, rpc.PeerId, rpc.Request, rpc.Request));
    }

    public void Tick(int count = 1, bool pump = true)
    {
        for (var tick = 0; tick < count; tick++)
        {
            foreach (var (id, node) in Nodes) if (!Offline.Contains(id)) node.Tick();
            AssertSafety();
            Collect();
            if (pump) Pump();
        }
    }

    public void Pump(int budget = 10000)
    {
        Collect();
        var delivered = 0;
        while (Network.Count > 0 && delivered++ < budget)
        {
            var index = Reorder ? _random.Next(Network.Count) : 0;
            var packet = Network[index];
            Network.RemoveAt(index);
            if (Offline.Contains(packet.From) || Offline.Contains(packet.To) || _blocked.Contains((packet.From, packet.To)) ||
                _random.NextDouble() < DropRate)
            {
                var sender = packet.Message is VoteRequest or AppendRequest ? packet.From : packet.To;
                var peer = sender == packet.From ? packet.To : packet.From;
                Nodes[sender].ReportUnreachable(peer, packet.Original);
                continue;
            }
            Deliver(packet);
            if (_random.NextDouble() < DuplicateRate) Deliver(packet);
            AssertSafety();
            Collect();
        }
        if (budget == 10000) Assert.True(Network.Count == 0, "Message loop failed to quiesce.");
    }

    private void Deliver(Packet packet)
    {
        if (packet.Message is VoteRequest or AppendRequest)
        {
            var response = Nodes[packet.To].HandleRequest(packet.From, packet.Message);
            Network.Add(new(packet.To, packet.From, response, packet.Message));
        }
        else Nodes[packet.To].HandleResponse(packet.From, packet.Message);
    }

    public void AssertSafety()
    {
        foreach (var (id, node) in Nodes)
        {
            if (_previous.TryGetValue(id, out var previous))
            {
                Assert.True(node.Term >= previous.Term, $"Term regressed on {id}.");
                Assert.True(node.CommitIndex >= previous.Commit, $"Commit regressed on {id}.");
            }
            _previous[id] = (node.Term, node.CommitIndex);
            Assert.InRange(node.CommitIndex, 0, node.LastIndex);
            if (node.Role == RaftRole.Leader)
            {
                if (_leaders.TryGetValue(node.Term, out var leader)) Assert.Equal(leader, id);
                else
                {
                    _leaders.Add(node.Term, id);
                    foreach (var (index, entry) in _committed)
                    {
                        Assert.True(node.LastIndex >= index, $"Leader completeness failed on {id}, index {index}.");
                        SameEntry(entry, Stores[id].Read(index, 1, 1024 * 1024)[0]);
                    }
                }
            }
            for (long index = 1; index <= node.CommitIndex; index++)
            {
                var entry = Stores[id].Read(index, 1, 1024 * 1024)[0];
                if (_committed.TryGetValue(index, out var committed)) SameEntry(committed, entry);
                else _committed.Add(index, entry);
            }
        }
    }

    public static void SameEntry(LogEntry expected, LogEntry actual)
    {
        Assert.Equal(expected.Index, actual.Index);
        Assert.Equal(expected.Term, actual.Term);
        Assert.Equal(expected.IsNoOp, actual.IsNoOp);
        Assert.Equal(expected.Command.ToArray(), actual.Command.ToArray());
    }

    public void Dispose()
    {
        foreach (var store in Stores.Values) store.Dispose();
    }
}

internal sealed class TestDirectory : IDisposable
{
    public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "raftnet8-tests", Guid.NewGuid().ToString("N"));
    public TestDirectory() => Directory.CreateDirectory(Path);
    public void Dispose()
    {
        var absolute = System.IO.Path.GetFullPath(Path);
        var parent = System.IO.Path.GetFullPath(System.IO.Path.Combine(System.IO.Path.GetTempPath(), "raftnet8-tests")) + System.IO.Path.DirectorySeparatorChar;
        if (!absolute.StartsWith(parent, StringComparison.OrdinalIgnoreCase)) throw new InvalidOperationException("Unexpected test path.");
        if (Directory.Exists(absolute)) Directory.Delete(absolute, true);
    }
}
