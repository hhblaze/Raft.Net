using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Raft.Storage;
using Raft.Transport;
using Microsoft.Extensions.Logging;

namespace Raft.Tests;

internal sealed class TestStateMachine : IRaftStateMachine
{
    private long _lastApplied;
    private readonly List<LogEntry> _entries = new();
    public Func<LogEntry, CancellationToken, ValueTask>? BeforeApply { get; set; }
    public long LastApplied => Interlocked.Read(ref _lastApplied);
    public LogEntry[] Entries { get { lock (_entries) return _entries.ToArray(); } }
    public async ValueTask ApplyAsync(LogEntry entry, CancellationToken cancellationToken)
    {
        if (BeforeApply is { } before) await before(entry, cancellationToken);
        Assert.Equal(LastApplied + 1, entry.Index);
        lock (_entries) _entries.Add(entry);
        Interlocked.Exchange(ref _lastApplied, entry.Index);
    }
}

internal sealed class RuntimeCluster : IAsyncDisposable
{
    private sealed class MemoryTransport(string from, RuntimeCluster cluster) : IRaftTransport
    {
        public ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken)
        {
            if (cluster.IsBlocked(from, peerId) || !cluster.Nodes.TryGetValue(peerId, out var node)) throw new IOException("Partitioned or stopped.");
            return node.HandleAsync(from, request, cancellationToken);
        }
    }

    public ConcurrentDictionary<string, RaftNode> Nodes { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, TestStateMachine> Machines { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, TcpRaftTransport> Tcp { get; } = new(StringComparer.Ordinal);
    public ConcurrentDictionary<string, IRaftStorage> Stores { get; } = new(StringComparer.Ordinal);
    private readonly Dictionary<string, RaftOptions> _options = new();
    private readonly Dictionary<string, IPEndPoint> _endpoints = new();
    private readonly HashSet<(string, string)> _blocked = new();
    private readonly List<TestDirectory> _directories = new();
    private readonly Dictionary<string, string> _paths = new();
    private readonly bool _disk, _tcp, _modern;
    private readonly ILoggerFactory? _loggerFactory;

    public RuntimeCluster(int count = 3, bool disk = false, bool tcp = false, int pending = 128, bool modern = false, int spares = 0,
        Func<RaftOptions, RaftOptions>? configure = null, ILoggerFactory? loggerFactory = null)
    {
        _disk = disk;
        _tcp = tcp;
        _modern = modern;
        _loggerFactory = loggerFactory;
        var members = Enumerable.Range(1, count).Select(i => $"n{i}").ToArray();
        var all = Enumerable.Range(1, count + spares).Select(i => $"n{i}").ToArray();
        foreach (var id in all)
        {
            var options = new RaftOptions
            {
                NodeId = id, Members = members, Learners = members.Contains(id) ? [] : [id], KnownPeers = all,
                TickInterval = TimeSpan.FromMilliseconds(10),
                // Native coverage and concurrent real disk commits are not a 150ms failure detector.
                // Fast virtual/in-memory runtime cases retain the short deadlines; disk/TCP cases
                // exercise the same protocol with room for measured filesystem/scheduler tails.
                ElectionMinTicks = disk ? 150 : 15, ElectionMaxTicks = disk ? 250 : 30,
                RpcTimeout = TimeSpan.FromMilliseconds(disk ? 750 : 100),
                MaxPendingProposals = pending
            };
            _options.Add(id, configure is null ? options : configure(options));
            if (disk)
            {
                var directory = new TestDirectory();
                _directories.Add(directory);
                _paths.Add(id, directory.Path);
            }
        }
        if (tcp)
        {
            // Reserve distinct OS-assigned ports together, then hand them to the transports.
            var listeners = all.Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
            try
            {
                for (var i = 0; i < listeners.Length; i++)
                {
                    listeners[i].Start();
                    _endpoints.Add(all[i], (IPEndPoint)listeners[i].LocalEndpoint);
                }
            }
            finally { foreach (var listener in listeners) listener.Stop(); }
        }
    }

    public async Task StartAsync()
    {
        foreach (var id in _options.Keys) await StartNodeAsync(id);
    }

    private async Task StartNodeAsync(string id)
    {
        var options = _options[id];
        var machine = new TestStateMachine();
        Machines[id] = machine;
        IRaftStorage storage = _modern
            ? _disk ? new DBreezeLogStorage(_paths[id], options) : DBreezeLogStorage.CreateInMemory(options)
            : _disk ? new DBreezeRaftStorage(_paths[id], options) : DBreezeRaftStorage.CreateInMemory(options);
        Stores[id] = storage;
        IRaftTransport transport;
        if (_tcp)
        {
            var tcp = new TcpRaftTransport(options, _endpoints);
            Tcp.Add(id, tcp);
            transport = tcp;
        }
        else transport = new MemoryTransport(id, this);
        var node = new RaftNode(options, storage, transport, machine, _loggerFactory);
        Assert.True(Nodes.TryAdd(id, node));
        await node.StartAsync();
        if (_tcp) Tcp[id].Start(node.HandleAsync);
    }

    public async Task<RaftNode> LeaderAsync(string? excluding = null)
    {
        RaftNode? result = null;
        await EventuallyAsync(() =>
        {
            var leaders = Nodes.Where(pair => pair.Key != excluding && pair.Value.Status.Role == RaftRole.Leader).Select(pair => pair.Value).ToArray();
            if (leaders.Length != 1) return false;
            result = leaders[0];
            return result.Status.CommitIndex > 0;
        });
        return result!;
    }

    public void Isolate(string id)
    {
        lock (_blocked)
            foreach (var other in _options.Keys) if (other != id) { _blocked.Add((id, other)); _blocked.Add((other, id)); }
    }
    private bool IsBlocked(string from, string to) { lock (_blocked) return _blocked.Contains((from, to)); }
    public void Heal() { lock (_blocked) _blocked.Clear(); }

    public async Task StopAsync(string id)
    {
        if (Tcp.Remove(id, out var tcp)) await tcp.DisposeAsync();
        if (Nodes.TryRemove(id, out var node)) await node.DisposeAsync();
    }

    public async Task RestartAsync(string id)
    {
        if (!_disk) throw new InvalidOperationException("Restart requires durable storage.");
        await StopAsync(id);
        await StartNodeAsync(id);
    }

    public static async Task EventuallyAsync(Func<bool> predicate, int timeoutMs = 10000)
    {
        var timer = Stopwatch.StartNew();
        while (!predicate())
        {
            if (timer.ElapsedMilliseconds > timeoutMs) throw new TimeoutException("Cluster condition was not reached.");
            await Task.Delay(10);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var id in Nodes.Keys.ToArray()) await StopAsync(id);
        foreach (var directory in _directories) directory.Dispose();
    }
}
