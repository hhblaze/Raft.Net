using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class ReplicationBudgetTests
{
    private sealed class Clock : IClock, ITicker
    {
        private readonly Channel<bool> _ticks = Channel.CreateBounded<bool>(1);
        public ITicker CreateTicker(TimeSpan interval) => this;
        public ValueTask<bool> WaitForNextTickAsync(CancellationToken token) => _ticks.Reader.ReadAsync(token);
        public ValueTask DisposeAsync() { _ticks.Writer.TryComplete(); return ValueTask.CompletedTask; }
    }
    private sealed class NoNetwork : ITransport
    {
        public ValueTask<RaftRpc> SendAsync(string peer, RaftRpc request, CancellationToken token) =>
            throw new InvalidOperationException("The controlled follower never sends RPCs.");
    }
    private sealed class Machine(int bytes) : IBatchStateMachine
    {
        private long _lastApplied;
        public long LastApplied => Interlocked.Read(ref _lastApplied);
        internal readonly ConcurrentQueue<int> Batches = new();
        public ValueTask ApplyAsync(LogEntry entry, CancellationToken token) => ApplyBatchAsync([entry], token);
        public ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken token)
        {
            Assert.InRange(entries.Sum(e => e.Command.Length + LogEntry.EncodedOverhead), 1, bytes);
            foreach (var entry in entries)
            {
                Assert.Equal(LastApplied + 1, entry.Index);
                Interlocked.Exchange(ref _lastApplied, entry.Index);
            }
            Batches.Enqueue(entries.Count);
            return ValueTask.CompletedTask;
        }
    }
    private static RaftOptions Options => new() { NodeId = "n2", Members = ["n1", "n2", "n3"],
        MaxCommandBytes = 128, MaxAppendBytes = 149, MaxReplicationBytes = 596, MaxAppendEntries = 8 };
    private static AppendRequest Append(int count) => new(1, 9, 0, 0, count,
        Enumerable.Range(1, count).Select(i => new LogEntry(i, 1, new byte[128])).ToArray());

    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3)]
    public async Task A_larger_wire_packet_is_durable_but_application_still_uses_its_smaller_budget(DBreezeLogLayout layout)
    {
        var options = Options;
        var backend = DBreezeLogStorage.CreateInMemory(options, layout);
        var machine = new Machine(options.MaxAppendBytes);
        await using var node = await RaftNode.CreateAsync(options, new RaftStorageAdapter(backend), new NoNetwork(), machine, clock: new Clock());
        await node.StartAsync();
        using var frame = RaftWireCodec.Encode("default", "n1", Append(3), options.MaxFrameBytes, 2, 1);
        var decoded = RaftWireCodec.Decode(frame.Memory.Span[4..], options);
        var reply = Assert.IsType<AppendResponse>(await node.HandleAsync(decoded.From, decoded.Message));
        Assert.True(reply.Success); Assert.Equal(3, reply.MatchIndex);
        Assert.Equal(3, backend.LastIndex); Assert.Equal(3, backend.HardState.CommitIndex);
        await RuntimeCluster.EventuallyAsync(() => machine.LastApplied == 3);
        Assert.Equal(new[] { 1, 1, 1 }, machine.Batches);
        await Assert.ThrowsAsync<ArgumentException>(() => node.HandleAsync("n1", Append(5)).AsTask());
        Assert.Equal(3, backend.LastIndex); Assert.False(node.Completion.IsCompleted);
        using var oversized = RaftWireCodec.Encode("default", "n1", Append(5), options.MaxFrameBytes, 2, 2);
        Assert.Throws<InvalidDataException>(() => RaftWireCodec.Decode(oversized.Memory.Span[4..], options));
    }

    [Fact]
    public async Task A_real_TCP_exchange_accepts_a_packet_larger_than_the_application_transaction()
    {
        var options = Options;
        var machine = new Machine(options.MaxAppendBytes);
        await using var node = await RaftNode.CreateAsync(options,
            new RaftStorageAdapter(DBreezeLogStorage.CreateInMemory(options, DBreezeLogLayout.UnifiedTableV3)),
            new NoNetwork(), machine, clock: new Clock());
        await node.StartAsync();
        var endpoints = options.Members.ToDictionary(id => id, _ => new IPEndPoint(IPAddress.Loopback, 0));
        await using var server = new TcpRaftTransport(options, endpoints);
        server.Start(node.HandleAsync);
        endpoints["n2"] = server.LocalEndPoint;
        await using var client = new TcpRaftTransport(options with { NodeId = "n1" }, endpoints);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var reply = Assert.IsType<AppendResponse>(await client.SendAsync("n2", Append(4), timeout.Token));
        Assert.True(reply.Success); Assert.Equal(4, reply.MatchIndex);
        await RuntimeCluster.EventuallyAsync(() => machine.LastApplied == 4);
        Assert.Equal(new[] { 1, 1, 1, 1 }, machine.Batches);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(148)]
    [InlineData(4 * 1024 * 1024)]
    [InlineData(64 * 1024 * 1024 + 1)]
    public void An_explicit_replication_budget_must_fit_a_command_and_the_peer_window(int bytes) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => (Options with { MaxReplicationBytes = bytes }).Freeze());

    [Fact]
    public void Wire1_preserves_its_old_packet_budget_and_rejects_the_modern_override()
    {
        var legacy = (Options with { WireVersion = 1, EnablePreVote = false, MaxReplicationBytes = null }).Freeze();
        using var frame = RaftWireCodec.Encode("default", "n1", Append(3), legacy.MaxFrameBytes);
        Assert.Throws<InvalidDataException>(() => RaftWireCodec.Decode(frame.Memory.Span[4..], legacy));
        Assert.Throws<ArgumentOutOfRangeException>(() => (legacy with { MaxReplicationBytes = 596 }).Freeze());
    }
}
