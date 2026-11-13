using System.Buffers;
using DBreeze;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class CompatibilityTests
{
    [Fact]
    public async Task Wire_v1_rejects_new_control_APIs_clearly_and_keeps_legacy_writes_and_barriers_working()
    {
        await using var cluster = new RuntimeCluster(tcp: true, configure: options => options with
        {
            WireVersion = 1, EnablePreVote = false, ElectionMinTicks = 50, ElectionMaxTicks = 80,
            RpcTimeout = TimeSpan.FromMilliseconds(200)
        });
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        await leader.ProposeAsync("before"u8.ToArray());
        await Assert.ThrowsAsync<NotSupportedException>(() => leader.ReadIndexAsync());
        await Assert.ThrowsAsync<NotSupportedException>(() => leader.TransferLeadershipAsync(cluster.Nodes.Keys.First(id => id != leader.Status.NodeId)));
        var write = await leader.ProposeAsync("after"u8.ToArray());
        Assert.True((await leader.ReadBarrierAsync()).Index > write.Index);
        Assert.All(cluster.Nodes.Values, node => Assert.Null(node.Status.Fault));
    }

    private sealed class ExplicitLegacyTransport : IRaftTransport
    {
        ValueTask<RaftRpc> IRaftTransport.SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken) =>
            new(new VoteResponse(request.Term, true));
    }

    private sealed class ExplicitLegacyMachine : IRaftBatchStateMachine
    {
        private long _applied;
        long IRaftStateMachine.LastApplied => _applied;
        ValueTask IRaftStateMachine.ApplyAsync(LogEntry entry, CancellationToken cancellationToken)
        { _applied = entry.Index; return ValueTask.CompletedTask; }
        ValueTask IRaftBatchStateMachine.ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken cancellationToken)
        { _applied = entries[^1].Index; return ValueTask.CompletedTask; }
    }

    [Fact]
    public async Task Explicit_legacy_implementations_work_through_the_new_contracts()
    {
        ITransport transport = new ExplicitLegacyTransport();
        Assert.Equal(new VoteResponse(3, true), await transport.SendAsync("n2", new VoteRequest(3, 0, 0), default));
        IBatchStateMachine machine = new ExplicitLegacyMachine();
        await machine.ApplyAsync(new LogEntry(1, 3, "a"u8), default);
        Assert.Equal(1, machine.LastApplied);
        await machine.ApplyBatchAsync([new LogEntry(2, 3, "b"u8), new LogEntry(3, 3, "c"u8)], default);
        Assert.Equal(3, machine.LastApplied);
    }

    // Captured from the unmodified v1 encoder before introducing IBufferWriter.
    public static TheoryData<RaftRpc, string> Frames => new()
    {
        { new VoteRequest(7, 9, 6), "2500000001010100000067020000006E31070000000000000009000000000000000600000000000000" },
        { new VoteResponse(7, true), "1600000001020100000067020000006E31070000000000000001" },
        { new AppendRequest(7, 42, 9, 6, 9, [new LogEntry(10, 7, new byte[] { 0, 127, 255 })]),
            "5100000001030100000067020000006E3107000000000000002A00000000000000090000000000000006000000000000000900000000000000010000000A0000000000000007000000000000000003000000007FFF" },
        { new AppendResponse(7, 42, false, 0, 3, 2),
            "3600000001040100000067020000006E3107000000000000002A0000000000000000000000000000000003000000000000000200000000000000" },
        { new AppendRequest(7, 43, 9, 6, 9, Array.Empty<LogEntry>()),
            "3900000001030100000067020000006E3107000000000000002B0000000000000009000000000000000600000000000000090000000000000000000000" }
    };

    [Theory]
    [MemberData(nameof(Frames))]
    public void Version_one_frames_remain_byte_compatible(RaftRpc message, string expected)
    {
        using var frame = RaftWireCodec.Encode("g", "n1", message, 1024);
        Assert.Equal(expected, Convert.ToHexString(frame.Memory.Span));
        var writer = new ArrayBufferWriter<byte>();
        RaftWireCodec.Encode(writer, "g", "n1", message, 1024);
        Assert.Equal(expected, Convert.ToHexString(writer.WrittenSpan));
    }

    [Fact]
    public void Version_one_database_values_and_identity_remain_compatible()
    {
        var options = new RaftOptions { NodeId = "n1", GroupId = "g", Members = ["n3", "n1", "n2"] };
        using var engine = new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
        using var storage = new DBreezeRaftStorage(engine, options);
        storage.SaveHardState(new HardState(7, "n2"));
        storage.Append([new LogEntry(1, 7, new byte[] { 0, 127, 255 })]);
        storage.SaveHardState(new HardState(7, "n2", 1));
        using var transaction = engine.GetTransaction();
        Assert.Equal("raft8-v1\ng\nn1\nn1\nn2\nn3", System.Text.Encoding.UTF8.GetString(
            transaction.Select<string, byte[]>("raft8/meta", "identity").Value));
        Assert.Equal("07000000000000000100000000000000020000006E32", Convert.ToHexString(
            transaction.Select<string, byte[]>("raft8/meta", "state").Value));
        Assert.Equal("070000000000000000007FFF", Convert.ToHexString(
            transaction.Select<long, byte[]>("raft8/log", 1).Value));
    }
}
