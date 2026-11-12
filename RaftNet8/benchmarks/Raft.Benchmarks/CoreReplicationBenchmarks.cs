using BenchmarkDotNet.Attributes;
using Raft.Core;
using Raft.Storage;

namespace Raft.Benchmarks;

/// <summary>Consensus/storage path with deterministic in-process delivery; excludes sockets and application I/O.</summary>
[MemoryDiagnoser]
public class CoreReplicationBenchmarks
{
    [Params(1, 32, 128)] public int BatchSize { get; set; }
    private readonly Dictionary<string, RaftCore> _nodes = new();
    private readonly List<DBreezeRaftStorage> _stores = new();
    private ProposedCommand[] _commands = null!;

    [IterationSetup]
    public void Setup()
    {
        var members = new[] { "n1", "n2", "n3" };
        foreach (var id in members)
        {
            var options = new RaftOptions { NodeId = id, Members = members };
            var store = DBreezeRaftStorage.CreateInMemory(options);
            _stores.Add(store);
            _nodes.Add(id, new RaftCore(options, store));
        }
        _commands = Enumerable.Range(0, BatchSize).Select(_ => new ProposedCommand(new byte[256])).ToArray();
        _nodes["n1"].Campaign();
        Deliver();
    }

    [Benchmark(OperationsPerInvoke = 64)]
    public long ReplicateBatch()
    {
        for (var i = 0; i < 64; i++) { _nodes["n1"].Propose(_commands); Deliver(); }
        return _nodes["n1"].CommitIndex;
    }

    private void Deliver()
    {
        bool more;
        do
        {
            more = false;
            foreach (var (from, node) in _nodes)
            {
                while (node.TryDequeue(out var rpc))
                {
                    more = true;
                    var response = _nodes[rpc.PeerId].HandleRequest(from, rpc.Request);
                    node.HandleResponse(rpc.PeerId, response);
                }
            }
        } while (more);
    }

    [IterationCleanup]
    public void Cleanup()
    {
        foreach (var store in _stores) store.Dispose();
        _stores.Clear();
        _nodes.Clear();
    }
}
