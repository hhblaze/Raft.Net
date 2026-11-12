using System.Net;
using System.Net.Sockets;
using BenchmarkDotNet.Attributes;
using Raft.Transport;

namespace Raft.Benchmarks;

/// <summary>256 concurrent proposals through real TCP, completed after quorum persistence and leader application.</summary>
[MemoryDiagnoser]
public class TcpLoadBenchmarks
{
    private sealed class CounterMachine : IRaftStateMachine
    {
        private long _lastApplied;
        public long LastApplied => Interlocked.Read(ref _lastApplied);
        public ValueTask ApplyAsync(LogEntry entry, CancellationToken cancellationToken)
        {
            Interlocked.Exchange(ref _lastApplied, entry.Index);
            return ValueTask.CompletedTask;
        }
    }

    [Params(false, true)] public bool Disk { get; set; }
    private readonly List<BenchmarkStorage> _storage = new();
    private readonly List<TcpRaftTransport> _transports = new();
    private readonly List<RaftNode> _nodes = new();
    private RaftNode _leader = null!;
    private readonly byte[] _command = new byte[256];

    [IterationSetup] public void Setup() => StartAsync().GetAwaiter().GetResult();

    private async Task StartAsync()
    {
        var members = new[] { "n1", "n2", "n3" };
        var reservations = members.Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
        var endpoints = new Dictionary<string, IPEndPoint>();
        try
        {
            for (var i = 0; i < members.Length; i++)
            {
                reservations[i].Start();
                endpoints.Add(members[i], (IPEndPoint)reservations[i].LocalEndpoint);
            }
        }
        finally { foreach (var reservation in reservations) reservation.Stop(); }
        foreach (var id in members)
        {
            var options = new RaftOptions { NodeId = id, Members = members };
            var scope = new BenchmarkStorage(Disk, options);
            _storage.Add(scope);
            var tcp = new TcpRaftTransport(options, endpoints);
            _transports.Add(tcp);
            var node = new RaftNode(options, scope.Store, tcp, new CounterMachine());
            _nodes.Add(node);
            await node.StartAsync();
            tcp.Start(node.HandleAsync);
        }
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (true)
        {
            var leader = _nodes.FirstOrDefault(n => n.Status.Role == RaftRole.Leader && n.Status.CommitIndex > 0);
            if (leader is not null) { _leader = leader; break; }
            await Task.Delay(10, timeout.Token);
        }
        await _leader.ReadBarrierAsync(timeout.Token);
    }

    [Benchmark(OperationsPerInvoke = 256)]
    public async Task<long> Replicate256()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var work = new Task<ProposalResult>[256];
        for (var i = 0; i < work.Length; i++) work[i] = _leader.ProposeAsync(_command, timeout.Token);
        var results = await Task.WhenAll(work);
        return results[^1].Index;
    }

    [IterationCleanup] public void Cleanup() => StopAsync().GetAwaiter().GetResult();
    private async Task StopAsync()
    {
        foreach (var tcp in _transports) await tcp.DisposeAsync();
        foreach (var node in _nodes) await node.DisposeAsync();
        foreach (var storage in _storage) storage.Dispose();
        _nodes.Clear(); _transports.Clear(); _storage.Clear();
    }
}
