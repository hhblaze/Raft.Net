extern alias baseline;

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using DBreeze;
using Raft.Storage;
using Raft.Transport;
using Old = baseline::Raft;

namespace Raft.Benchmarks;

public enum ClusterVersion { Baseline, Current }
public enum ClusterMode { Unbatched, Batched, Pipeline, UnbatchedPipeline }
public sealed record ClusterCase(ClusterVersion Version, int Nodes, int PayloadBytes, ClusterMode Mode)
{
    public override string ToString() => $"{Version}-{Nodes}n-{PayloadBytes}B-{Mode}";
    public static IEnumerable<ClusterCase> All()
    {
        foreach (var nodes in new[] { 1, 3, 5 })
        foreach (var bytes in new[] { 64, 16 * 1024 })
        {
            yield return new(ClusterVersion.Baseline, nodes, bytes, ClusterMode.Unbatched);
            yield return new(ClusterVersion.Baseline, nodes, bytes, ClusterMode.Batched);
            yield return new(ClusterVersion.Current, nodes, bytes, ClusterMode.Unbatched);
            yield return new(ClusterVersion.Current, nodes, bytes, ClusterMode.Batched);
            yield return new(ClusterVersion.Current, nodes, bytes, ClusterMode.Pipeline);
            yield return new(ClusterVersion.Current, nodes, bytes, ClusterMode.UnbatchedPipeline);
        }
    }
}

/// <summary>Same durable application transaction/checkpoint implementation for both consensus versions.</summary>
internal sealed class ComparisonMachine(DBreezeEngine engine, Action<int>? appliedBatch = null) : IRaftBatchStateMachine, Old.IRaftBatchStateMachine
{
    private readonly record struct Item(long Index, long Term, bool NoOp, ReadOnlyMemory<byte> Bytes);
    private long _applied, _commands;
    public long LastApplied => Interlocked.Read(ref _applied);
    public long Commands => Interlocked.Read(ref _commands);
    public ValueTask ApplyAsync(LogEntry entry, CancellationToken token) => ApplyBatchAsync([entry], token);
    public ValueTask ApplyAsync(Old.LogEntry entry, CancellationToken token) => ApplyBatchAsync([entry], token);
    public ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken token) =>
        Apply(entries, static e => new(e.Index, e.Term, e.IsNoOp, e.Command), token);
    public ValueTask ApplyBatchAsync(IReadOnlyList<Old.LogEntry> entries, CancellationToken token) =>
        Apply(entries, static e => new(e.Index, e.Term, e.IsNoOp, e.Command), token);
    private ValueTask Apply<T>(IReadOnlyList<T> entries, Func<T, Item> item, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        using var tx = engine.GetTransaction();
        tx.SynchronizeTables("bench/values", "bench/checkpoint");
        var count = Commands;
        var last = new AppliedCheckpoint(LastApplied, 0);
        foreach (var entry in entries)
        {
            var value = item(entry);
            if (value.Index != last.Index + 1) throw new InvalidDataException("Non-contiguous application.");
            if (!value.NoOp)
            {
                // Fixed ring of keys keeps business table size comparable while the Raft log grows.
                tx.Insert<long, byte[]>("bench/values", value.Index % 256, value.Bytes.ToArray());
                count++;
            }
            last = new(value.Index, value.Term);
        }
        tx.Insert<string, byte[]>("bench/checkpoint", "applied", DBreezeStateMachine.EncodeCheckpoint(last));
        tx.Insert<string, long>("bench/checkpoint", "commands", count);
        tx.Commit();
        Interlocked.Exchange(ref _commands, count);
        Interlocked.Exchange(ref _applied, last.Index);
        appliedBatch?.Invoke(entries.Count);
        return ValueTask.CompletedTask;
    }
    public void Validate(long commands)
    {
        using var tx = engine.GetTransaction();
        var checkpoint = DBreezeStateMachine.DecodeCheckpoint(tx.Select<string, byte[]>("bench/checkpoint", "applied").Value);
        if (checkpoint.Index != LastApplied || checkpoint.Term < 1 || Commands != commands ||
            tx.Select<string, long>("bench/checkpoint", "commands").Value != commands)
            throw new InvalidDataException("Benchmark application/checkpoint disagrees with completed proposals.");
    }
}

internal sealed class ComparisonCluster : IAsyncDisposable
{
    internal bool RetainDataForValidation { get; init; }
    internal string DataDirectory => _root;
    internal Func<string, ILogStore, ILogStore>? DecorateLogStore { get; init; }
    internal Func<string, TcpRaftTransport, ITransport>? DecorateTransport { get; init; }
    internal Action<string, int>? AppliedBatch { get; init; }
    internal bool UseUnifiedLogLayout { get; init; }
    internal bool UseHotMembership { get; init; }
    internal int? CurrentProposalBatchBytes { get; init; }
    private readonly string _parent = Path.GetFullPath(Environment.GetEnvironmentVariable("RAFT_BENCHMARK_ROOT") ??
        Path.Combine(".artifacts", "autonomous", "benchmarks", "data"));
    private readonly string _root;
    private readonly List<DBreezeEngine> _engines = [];
    private readonly List<ComparisonMachine> _machines = [];
    private readonly List<RaftNode> _nodes = [];
    private readonly List<Old.RaftNode> _oldNodes = [];
    private readonly List<TcpRaftTransport> _tcp = [];
    private readonly List<Old.Transport.TcpRaftTransport> _oldTcp = [];
    private RaftNode? _leader;
    private Old.RaftNode? _oldLeader;
    private readonly byte[] _payload;
    private long _completed;
    private RaftOptions? _hotTemplate;
    private PeerDescriptor? _joiningPeer;
    public ClusterCase Case { get; }
    public ComparisonCluster(ClusterCase configuration)
    {
        Case = configuration;
        _payload = new byte[configuration.PayloadBytes];
        _root = Path.Combine(_parent, Guid.NewGuid().ToString("N"));
    }

    public async Task StartAsync()
    {
        Directory.CreateDirectory(_root);
        var members = Enumerable.Range(1, Case.Nodes).Select(i => "n" + i).ToArray();
        var listeners = members.Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
        var endpoints = new Dictionary<string, IPEndPoint>();
        try
        {
            for (var i = 0; i < members.Length; i++)
            {
                listeners[i].Start();
                endpoints.Add(members[i], (IPEndPoint)listeners[i].LocalEndpoint);
            }
        }
        finally { foreach (var listener in listeners) listener.Stop(); }
        foreach (var id in members)
        {
            var engine = new DBreezeEngine(Path.Combine(_root, id));
            _engines.Add(engine);
            var machine = new ComparisonMachine(engine, AppliedBatch is null ? null : count => AppliedBatch(id, count));
            _machines.Add(machine);
            var batch = Case.Mode is ClusterMode.Unbatched or ClusterMode.UnbatchedPipeline ? 1 : 128;
            if (Case.Version == ClusterVersion.Current)
            {
                var options = new RaftOptions { NodeId = id, Members = members, HeartbeatTicks = 1,
                    ElectionMinTicks = 200, ElectionMaxTicks = 240, RpcTimeout = TimeSpan.FromSeconds(2),
                    MaxAppendEntries = batch, EnablePipelining = Case.Mode is ClusterMode.Pipeline or ClusterMode.UnbatchedPipeline,
                    MaxProposalBatchBytes = CurrentProposalBatchBytes,
                    ProposalBatchDelay = batch == 1 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(2) };
                if (UseHotMembership)
                {
                    options = options with { WireVersion = 3, InitialPeers = endpoints.Select(pair =>
                        new PeerDescriptor(pair.Key, pair.Value.Address.ToString(), pair.Value.Port)).ToArray() };
                    _hotTemplate = options;
                }
                var tcp = new TcpRaftTransport(options, endpoints);
                _tcp.Add(tcp);
                ILogStore log = new RaftStorageAdapter(new DBreezeLogStorage(engine, options,
                    UseHotMembership ? DBreezeLogLayout.UnifiedTableV4 :
                        UseUnifiedLogLayout ? DBreezeLogLayout.UnifiedTableV3 : DBreezeLogLayout.SeparateTablesV2));
                if (DecorateLogStore is not null) log = DecorateLogStore(id, log);
                var node = await RaftNode.CreateAsync(options, log, DecorateTransport?.Invoke(id, tcp) ?? tcp, machine);
                _nodes.Add(node);
                await node.StartAsync();
                tcp.Start(node.HandleAsync);
            }
            else
            {
                var options = new Old.RaftOptions { NodeId = id, Members = members, HeartbeatTicks = 1, MaxAppendEntries = batch,
                    ElectionMinTicks = 200, ElectionMaxTicks = 240, RpcTimeout = TimeSpan.FromSeconds(2) };
                var tcp = new Old.Transport.TcpRaftTransport(options, endpoints);
                _oldTcp.Add(tcp);
                var node = new Old.RaftNode(options, new Old.Storage.DBreezeRaftStorage(engine, options), tcp, machine);
                _oldNodes.Add(node);
                await node.StartAsync();
                tcp.Start(node.HandleAsync);
            }
        }
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (true)
        {
            _leader = _nodes.FirstOrDefault(n => n.Status.Role == RaftRole.Leader && n.Status.CommitIndex > 0);
            _oldLeader = _oldNodes.FirstOrDefault(n => n.Status.Role == Old.RaftRole.Leader && n.Status.CommitIndex > 0);
            if (_leader is not null) { await _leader.ReadBarrierAsync(timeout.Token); break; }
            if (_oldLeader is not null) { await _oldLeader.ReadBarrierAsync(timeout.Token); break; }
            await Task.Delay(10, timeout.Token);
        }
    }
    internal async Task PrepareJoiningPeerAsync()
    {
        if (!UseHotMembership || _hotTemplate is null || _leader is null) throw new InvalidOperationException("Start a hot membership cluster first.");
        var id = "n" + (Case.Nodes + 1);
        var reservation = new TcpListener(IPAddress.Loopback, 0);
        reservation.Start();
        var endpoint = (IPEndPoint)reservation.LocalEndpoint;
        reservation.Stop();
        _joiningPeer = new(id, endpoint.Address.ToString(), endpoint.Port);
        var options = _hotTemplate with { NodeId = id, JoiningNode = true };
        var engine = new DBreezeEngine(Path.Combine(_root, id));
        _engines.Add(engine);
        var machine = new ComparisonMachine(engine);
        _machines.Add(machine);
        var tcp = new TcpRaftTransport(options, _joiningPeer);
        _tcp.Add(tcp);
        var log = new RaftStorageAdapter(new DBreezeLogStorage(engine, options, DBreezeLogLayout.UnifiedTableV4));
        var node = await RaftNode.CreateAsync(options, log, tcp, machine);
        _nodes.Add(node);
        await node.StartAsync();
        tcp.Start(node.HandleAsync);
    }

    internal Task<MembershipOperationStatus> AddJoiningPeerAsync(CancellationToken token) =>
        _leader!.AddNodeAsync(_joiningPeer ?? throw new InvalidOperationException("Prepare a new ID first."), Guid.NewGuid(), token);
    public async Task ProposeAsync(CancellationToken token)
    {
        if (_leader is not null) await _leader.ProposeAsync(_payload, token);
        else await _oldLeader!.ProposeAsync(_payload, token);
        Interlocked.Increment(ref _completed);
    }
    public async Task RunAsync(int operations, int concurrency, double[]? latencies = null)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var issued = -1;
        var workers = Enumerable.Range(0, Math.Min(operations, concurrency)).Select(async _ =>
        {
            while (true)
            {
                var index = Interlocked.Increment(ref issued);
                if (index >= operations) return;
                var start = latencies is null ? 0 : Stopwatch.GetTimestamp();
                await ProposeAsync(timeout.Token);
                if (latencies is not null) latencies[index] = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
            }
        });
        await Task.WhenAll(workers);
    }
    public async Task ValidateAsync()
    {
        var through = _leader?.Status.LastApplied ?? _oldLeader!.Status.LastApplied;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (_machines.Any(machine => machine.LastApplied < through)) await Task.Delay(10, timeout.Token);
        foreach (var node in _nodes) if (node.Status.Fault is { } fault) throw new InvalidOperationException("Current node faulted.", fault);
        foreach (var node in _oldNodes) if (node.Status.Fault is { } fault) throw new InvalidOperationException("Baseline node faulted.", fault);
        foreach (var machine in _machines) machine.Validate(Interlocked.Read(ref _completed));
    }
    public async ValueTask DisposeAsync()
    {
        foreach (var tcp in _tcp) await tcp.DisposeAsync();
        foreach (var tcp in _oldTcp) await tcp.DisposeAsync();
        foreach (var node in _nodes) await node.DisposeAsync();
        foreach (var node in _oldNodes) await node.DisposeAsync();
        foreach (var engine in _engines) engine.Dispose();
        var parent = _parent.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
        var target = Path.GetFullPath(_root);
        if (!target.StartsWith(parent, StringComparison.OrdinalIgnoreCase)) throw new InvalidOperationException("Unexpected benchmark path.");
        if (!RetainDataForValidation && Directory.Exists(target)) Directory.Delete(target, recursive: true);
    }
}
