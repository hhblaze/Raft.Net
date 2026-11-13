using Raft.Transport;

namespace Raft.Tests;

public sealed class ReplicationDefaultFrameTests
{
    [Fact]
    public void The_default_wire_budget_includes_metadata_for_a_full_local_payload_group()
    {
        var options = new RaftOptions { NodeId = "n2", Members = ["n1", "n2", "n3"] }.Freeze();
        const int payloadBytes = 16 * 1024;
        var count = 4 * options.MaxAppendBytes / payloadBytes;
        var request = new AppendRequest(1, 7, 0, 0, count,
            Enumerable.Range(1, count).Select(i => new LogEntry(i, 1, new byte[payloadBytes])).ToArray());
        using var frame = RaftWireCodec.Encode("default", "n1", request, options.MaxFrameBytes, 2, 1);
        var decoded = Assert.IsType<AppendRequest>(RaftWireCodec.Decode(frame.Memory.Span[4..], options).Message);
        Assert.Equal(count, decoded.Entries.Count);
        Assert.All(decoded.Entries, entry => Assert.Equal(payloadBytes, entry.Command.Length));
        Assert.Equal(request.Entries[^1].Index, decoded.Entries[^1].Index);
    }
}
