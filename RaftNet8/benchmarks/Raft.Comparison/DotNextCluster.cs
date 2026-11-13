using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using DBreeze;
using DotNext.Net.Cluster.Consensus.Raft;
using NativeCluster = DotNext.Net.Cluster.Consensus.Raft.RaftCluster;

namespace Raft.Benchmarks;

internal sealed class DotNextCluster(int nodes, int payloadBytes, bool batched, string parent) : IAsyncDisposable
{
    private sealed record Node(NativeCluster Cluster, DotNextAuditTrail Log, DBreezeEngine Engine);
    private readonly List<Node> _nodes = [];
    private readonly List<Task> _replication = [];
    private readonly List<Task> _completions = [];
    private readonly SemaphoreSlim _admission = new(1024, 1024);
    private readonly string _root = Path.Combine(parent, Guid.NewGuid().ToString("N"));
    private readonly byte[] _payload = new byte[payloadBytes];
    private readonly Channel<TaskCompletionSource> _proposals = Channel.CreateBounded<TaskCompletionSource>(
        new BoundedChannelOptions(1024) { SingleReader = true, SingleWriter = false });
    private readonly CancellationTokenSource _stop = new();
    private Node? _leader;
    private Task _worker = Task.CompletedTask;
    private long _completed;
    private Exception? _failure;
    public string DataDirectory => _root;
    public object Diagnostics => new { NativeWal = "MemoryBasedStateMachine AutoFlush + IntegrityCheck; OnCommit cache eviction; no compaction",
        Configuration = "fixed prepopulated in-memory endpoints; not a membership-recovery benchmark",
        AppendBatches = _nodes.Sum(n => n.Log.AppendBatches), AppendedEntries = _nodes.Sum(n => n.Log.AppendedEntries),
        ApplyBatches = _nodes.Sum(n => n.Log.ApplyBatches), AppliedCommands = _nodes.Sum(n => n.Log.AppliedCommands),
        MaxApplyBatch = _nodes.Max(n => n.Log.MaxObservedApplyBatch) };

    public async Task StartAsync(CancellationToken token)
    {
        Directory.CreateDirectory(_root);
        var listeners = Enumerable.Range(0, nodes).Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
        IPEndPoint[] endpoints;
        try
        {
            foreach (var listener in listeners) listener.Start();
            endpoints = listeners.Select(l => (IPEndPoint)l.LocalEndpoint).ToArray();
        }
        finally { foreach (var listener in listeners) listener.Stop(); }
        foreach (var endpoint in endpoints)
        {
            var index = _nodes.Count + 1;
            var directory = Path.Combine(_root, "n" + index);
            var engine = new DBreezeEngine(directory);
            var log = new DotNextAuditTrail(Path.Combine(directory, "native-wal"), new ComparisonMachine(engine), batched ? 128 : 1, payloadBytes);
            var config = new NativeCluster.TcpConfiguration(endpoint)
            {
                ColdStart = false,
                LowerElectionTimeout = 5000, UpperElectionTimeout = 6000,
                HeartbeatThreshold = .005, // 25..30 ms for the sampled election timeout
                RequestTimeout = TimeSpan.FromSeconds(2),
                TransmissionBlockSize = 65535
            };
            var builder = config.UseInMemoryConfigurationStorage().CreateActiveConfigurationBuilder();
            foreach (var member in endpoints) builder.Add(member);
            builder.Build();
            _nodes.Add(new(new NativeCluster(config) { AuditTrail = log }, log, engine));
        }
        await Task.WhenAll(_nodes.Select(n => n.Cluster.StartAsync(token)));
        while ((_leader = _nodes.FirstOrDefault(n => n.Cluster.Leader is { IsRemote: false })) is null) await Task.Delay(10, token);
        await ((IRaftCluster)_leader.Cluster).ApplyReadBarrierAsync(token);
        _worker = Task.Run(ProcessAsync);
    }

    public async Task ProposeAsync(CancellationToken token)
    {
        await _admission.WaitAsync(token);
        try
        {
            if (_failure is { } error) throw new InvalidOperationException("DotNext proposal worker failed.", error);
            var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await _proposals.Writer.WriteAsync(completion, token);
            await completion.Task.WaitAsync(token);
            Interlocked.Increment(ref _completed);
        }
        finally { _admission.Release(); }
    }

    private async Task ProcessAsync()
    {
        var active = new List<TaskCompletionSource>(128);
        try
        {
            while (await _proposals.Reader.WaitToReadAsync(_stop.Token))
            {
                active.Clear();
                active.Add(await _proposals.Reader.ReadAsync(_stop.Token));
                if (batched) await Task.Delay(1, _stop.Token);
                var count = batched ? Math.Min(128, 256 * 1024 / (payloadBytes + 21)) : 1;
                while (active.Count < count && _proposals.Reader.TryRead(out var next)) active.Add(next);
                var leader = _leader!;
                var term = ((IRaftCluster)leader.Cluster).Term;
                if (leader.Cluster.Leader is not { IsRemote: false }) throw new InvalidOperationException("Benchmark leadership changed.");
                var entries = new BinaryLogEntry[active.Count];
                for (var i = 0; i < entries.Length; i++) entries[i] = new() { Term = term, Content = _payload };
                var last = await leader.Log.AppendBatchAsync(entries, _stop.Token);
                // Wake native replication. Its public force waiter waits for every reachable peer;
                // client completion below independently waits only for the applied committed prefix.
                _replication.RemoveAll(task => task.IsCompletedSuccessfully);
                _replication.Add(leader.Cluster.ForceReplicationAsync(_stop.Token).AsTask());
                _completions.RemoveAll(task => task.IsCompletedSuccessfully);
                _completions.Add(CompleteAsync(leader, last, term, active.ToArray()));
            }
        }
        catch (Exception error)
        {
            _failure = error;
            foreach (var completion in active) completion.TrySetException(error);
            _proposals.Writer.TryComplete(error);
            while (_proposals.Reader.TryRead(out var completion)) completion.TrySetException(error);
        }
    }

    private async Task CompleteAsync(Node leader, long last, long term, TaskCompletionSource[] clients)
    {
        try
        {
            await leader.Log.WaitForApplyAsync(last, _stop.Token);
            if (((IRaftCluster)leader.Cluster).Term != term) throw new InvalidOperationException("Benchmark term changed.");
            foreach (var completion in clients) completion.TrySetResult();
        }
        catch (Exception error)
        {
            _failure = error;
            foreach (var completion in clients) completion.TrySetException(error);
        }
    }

    public async Task ValidateAsync(CancellationToken token)
    {
        if (_failure is { } error) throw new InvalidOperationException("DotNext benchmark failed.", error);
        await _leader!.Cluster.ForceReplicationAsync(token);
        var through = _leader.Log.BusinessIndex;
        while (_nodes.Any(n => n.Log.BusinessIndex < through)) await Task.Delay(10, token);
        foreach (var node in _nodes)
        {
            if (node.Log.Failure is { } failure) throw new InvalidOperationException("DotNext application failed.", failure);
            node.Log.Machine.Validate(Interlocked.Read(ref _completed));
        }
        await Task.WhenAll(_replication);
        _replication.Clear();
        await Task.WhenAll(_completions);
        _completions.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        _proposals.Writer.TryComplete();
        _stop.Cancel();
        await _worker;
        await Task.WhenAll(_completions);
        try { await Task.WhenAll(_replication); } catch (OperationCanceledException) { }
        foreach (var node in _nodes) await node.Cluster.StopAsync(CancellationToken.None);
        foreach (var node in _nodes)
        {
            await node.Cluster.DisposeAsync();
            node.Log.Dispose();
            node.Engine.Dispose();
        }
        _stop.Dispose();
        _admission.Dispose();
    }
}
