using System.Buffers.Binary;
using System.Collections.Immutable;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class WireV2Tests
{
    private static RaftOptions Options => new() { NodeId = "n1", Members = ["n1", "n2", "n3"] };

    [Fact]
    public void V2_roundtrips_new_message_kinds_read_contexts_configuration_and_snapshot_chunks()
    {
        var snapshot = new SnapshotMetadata(100, 7, ["n1", "n2", "n3"], ["n4"], "snap100", new byte[32].ToImmutableArray());
        var bytes = "snapshot-image"u8.ToArray();
        RaftRpc[] messages =
        [
            new VoteRequest(8, 100, 7), new VoteResponse(8, true),
            new AppendRequest(8, 2, 100, 7, 100, [new LogEntry(101, 8, new ClusterConfiguration(["n1", "n2", "n3"], ["n4"]).Encode().AsSpan(), LogEntryKind.Configuration)])
                { ReadContext = 99 },
            new AppendResponse(8, 2, true, 101) { ReadContext = 99 },
            new PreVoteRequest(9, 12, 100, 7), new PreVoteResponse(8, 12, 9, true),
            new TimeoutNowRequest(8, 15), new TimeoutNowResponse(8, 15, true),
            new SnapshotChunkRequest(8, 16, "snap100", 0, bytes.Length, Crc32C.Compute(bytes), true, snapshot, bytes),
            new SnapshotChunkResponse(8, 16, "snap100", bytes.Length, true, true)
        ];
        foreach (var message in messages)
        {
            using var frame = RaftWireCodec.Encode("g", "n1", message, Options.MaxFrameBytes, 2, 77);
            Assert.Equal(frame.Memory.Length - 4, BinaryPrimitives.ReadInt32LittleEndian(frame.Memory.Span));
            var decoded = RaftWireCodec.Decode(frame.Memory.Span[4..], Options);
            Assert.Equal(2, decoded.Version);
            Assert.Equal(77, decoded.ExchangeId);
            Assert.Equal(message.GetType(), decoded.Message.GetType());
            Assert.Equal(message.Term, decoded.Message.Term);
            if (message is AppendRequest append)
            {
                var actual = Assert.IsType<AppendRequest>(decoded.Message);
                Assert.Equal(append.ReadContext, actual.ReadContext);
                Assert.Equal(append.Entries[0].Kind, actual.Entries[0].Kind);
                Assert.Equal(append.Entries[0].Command.ToArray(), actual.Entries[0].Command.ToArray());
            }
            else if (message is SnapshotChunkRequest chunk)
            {
                var actual = Assert.IsType<SnapshotChunkRequest>(decoded.Message);
                Assert.Equal(chunk.Data.ToArray(), actual.Data.ToArray());
                Assert.Equal(100, actual.Metadata!.LastIncludedIndex);
                Assert.Equal(chunk.Metadata!.Voters.ToArray(), actual.Metadata.Voters.ToArray());
            }
            else Assert.Equal(message, decoded.Message);
        }
    }

    [Fact]
    public void Corrupt_snapshot_chunks_and_missing_v2_correlations_are_rejected()
    {
        var bytes = "snapshot-image"u8.ToArray();
        var request = new SnapshotChunkRequest(1, 1, "image", 0, bytes.Length, Crc32C.Compute(bytes), true, null, bytes);
        using var frame = RaftWireCodec.Encode("g", "n1", request, Options.MaxFrameBytes, 2, 1);
        var corrupt = frame.Memory.Span[4..].ToArray();
        corrupt[^1] ^= 1;
        Assert.Throws<InvalidDataException>(() => RaftWireCodec.Decode(corrupt, Options));
        Assert.Throws<InvalidDataException>(() => RaftWireCodec.Encode("g", "n1", request, Options.MaxFrameBytes, 2, 0));
    }
}
