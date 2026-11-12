using System.Buffers.Binary;
using Raft.Transport;

namespace Raft.Tests;

public sealed class WireCodecTests
{
    private static RaftOptions Options => new() { NodeId = "n1", Members = new[] { "n1", "n2" } };

    public static IEnumerable<object[]> Messages()
    {
        yield return new object[] { new VoteRequest(3, 10, 2) };
        yield return new object[] { new VoteResponse(3, true) };
        yield return new object[] { new VoteResponse(3, false) };
        yield return new object[] { new AppendRequest(3, 99, 0, 0, 1, new[] { new LogEntry(1, 1, "hello"u8), new LogEntry(2, 3, Array.Empty<byte>(), true) }) };
        yield return new object[] { new AppendResponse(3, 99, true, 2) };
        yield return new object[] { new AppendResponse(3, 99, false, 0, 1, 2) };
    }

    [Theory]
    [MemberData(nameof(Messages))]
    public void Round_trips_each_message_and_rejects_all_truncated_prefixes(RaftRpc message)
    {
        using var frame = RaftWireCodec.Encode("default", "n2", message, Options.MaxAppendBytes + 1024);
        var payload = frame.Memory.Span[4..].ToArray();
        Assert.Equal(payload.Length, BinaryPrimitives.ReadInt32LittleEndian(frame.Memory.Span));
        var envelope = RaftWireCodec.Decode(payload, Options);
        Assert.Equal("default", envelope.GroupId);
        Assert.Equal("n2", envelope.From);
        if (message is AppendRequest append)
        {
            var actual = Assert.IsType<AppendRequest>(envelope.Message);
            Assert.Equal(append.Term, actual.Term);
            Assert.Equal(append.RequestId, actual.RequestId);
            for (var i = 0; i < append.Entries.Count; i++) SimCluster.SameEntry(append.Entries[i], actual.Entries[i]);
        }
        else Assert.Equal(message, envelope.Message);
        for (var i = 0; i < payload.Length; i++)
        {
            var length = i;
            Assert.Throws<InvalidDataException>(() => RaftWireCodec.Decode(payload.AsSpan(0, length), Options));
        }
        Assert.Throws<InvalidDataException>(() => RaftWireCodec.Decode(payload.Concat(new byte[] { 0 }).ToArray(), Options));
    }

    [Fact]
    public void Entry_count_is_validated_before_allocating_the_entry_array()
    {
        using var frame = RaftWireCodec.Encode("default", "n2", new AppendRequest(1, 1, 0, 0, 0, Array.Empty<LogEntry>()), 1024);
        var payload = frame.Memory.Span[4..].ToArray();
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(payload.Length - 4), int.MaxValue);
        Assert.Throws<InvalidDataException>(() => RaftWireCodec.Decode(payload, Options));
    }

    [Fact]
    public void Invalid_boolean_and_wire_version_are_rejected()
    {
        using var frame = RaftWireCodec.Encode("default", "n2", new VoteResponse(1, true), 1024);
        var payload = frame.Memory.Span[4..].ToArray();
        payload[^1] = 2;
        Assert.Throws<InvalidDataException>(() => RaftWireCodec.Decode(payload, Options));
        payload[0] = 2;
        Assert.Throws<InvalidDataException>(() => RaftWireCodec.Decode(payload, Options));
    }

    [Fact]
    public void Random_corrupt_frames_have_bounded_parsing()
    {
        var random = new Random(20261112);
        for (var i = 0; i < 2000; i++)
        {
            var payload = new byte[random.Next(2, 512)];
            random.NextBytes(payload);
            payload[0] = 1;
            payload[1] = (byte)random.Next(1, 5);
            // Random 32-bit identifier lengths are almost always rejected before data allocation.
            Assert.ThrowsAny<Exception>(() => RaftWireCodec.Decode(payload, Options));
        }
    }
}
