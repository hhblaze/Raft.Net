using System.Collections.Immutable;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class CodecPropertyTests
{
    [Fact]
    public void Seeded_v2_messages_reencode_identically_after_the_receive_pool_is_reused()
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"] };
        for (var seed = 0; seed < 32; seed++)
        {
            var random = new Random(seed);
            for (var step = 0; step < 80; step++)
            {
                var term = random.NextInt64(2, 1000);
                var index = random.NextInt64(1, 10000);
                var requestId = random.NextInt64(1, long.MaxValue);
                var bytes = new byte[random.Next(0, 1024)];
                random.NextBytes(bytes);
                var config = Configuration(random);
                var snapshot = new SnapshotMetadata(index, term - 1, config.Voters, config.Learners,
                    "image" + step, Enumerable.Range(0, 32).Select(_ => (byte)random.Next(256)).ToImmutableArray());
                RaftRpc message = (step % 10) switch
                {
                    0 => new VoteRequest(term, index, term - 1),
                    1 => new VoteResponse(term, random.Next(2) == 0),
                    2 => new AppendRequest(term, requestId, index, term - 1, index,
                        [new LogEntry(index + 1, term, bytes), new LogEntry(index + 2, term, config.Encode(), LogEntryKind.Configuration),
                         new LogEntry(index + 3, term, [], true)]) { ReadContext = requestId },
                    3 => new AppendResponse(term, requestId, false, 0, index, term - 1) { ReadContext = requestId },
                    4 => new PreVoteRequest(term, requestId, index, term - 1),
                    5 => new PreVoteResponse(term - 1, requestId, term, random.Next(2) == 0),
                    6 => new TimeoutNowRequest(term, requestId),
                    7 => new TimeoutNowResponse(term, requestId, random.Next(2) == 0),
                    8 => new SnapshotChunkRequest(term, requestId, snapshot.StorageId, 0, bytes.Length,
                        Crc32C.Compute(bytes), true, snapshot, bytes),
                    _ => new SnapshotChunkResponse(term, requestId, snapshot.StorageId, bytes.Length, true, random.Next(2) == 0)
                };
                byte[] expected;
                RpcEnvelope decoded;
                using (var frame = RaftWireCodec.Encode("group", "n2", message, options.MaxFrameBytes, 2, requestId))
                {
                    expected = frame.Memory.ToArray();
                    decoded = RaftWireCodec.Decode(frame.Memory.Span[4..], options);
                }
                using (var overwrite = RaftWireCodec.Encode("group", "n3", new AppendRequest(term, 1, 0, 0, 0,
                    [new LogEntry(1, term, new byte[4096])]), options.MaxFrameBytes, 2, requestId)) { }
                using var actual = RaftWireCodec.Encode(decoded.GroupId, decoded.From, decoded.Message, options.MaxFrameBytes,
                    decoded.Version, decoded.ExchangeId);
                Assert.True(expected.AsSpan().SequenceEqual(actual.Memory.Span), $"seed={seed}, step={step}, kind={message.GetType().Name}");
            }
        }
    }

    [Fact]
    public void Random_configurations_preserve_membership_and_reject_truncation_duplicates_and_trailing_bytes()
    {
        var random = new Random(197);
        for (var iteration = 0; iteration < 512; iteration++)
        {
            var expected = Configuration(random);
            var bytes = expected.Encode();
            var decoded = ClusterConfiguration.Decode(bytes, iteration + 1, 9);
            Assert.Equal(expected.Voters.ToArray(), decoded.Voters.ToArray());
            Assert.Equal(expected.Learners.ToArray(), decoded.Learners.ToArray());
            Assert.Equal(iteration + 1, decoded.Index);
            Assert.Throws<InvalidDataException>(() => ClusterConfiguration.Decode(bytes.AsSpan(0, random.Next(bytes.Length))));
            Assert.Throws<InvalidDataException>(() => ClusterConfiguration.Decode([.. bytes, 0]));
            Assert.Throws<ArgumentException>(() => (expected with { Learners = [expected.Voters[0]] }).Encode());
        }
    }
    private static ClusterConfiguration Configuration(Random random)
    {
        var ids = Enumerable.Range(1, random.Next(2, 20)).Select(i => "n" + i).OrderBy(_ => random.Next()).ToArray();
        var voters = random.Next(1, ids.Length + 1);
        return new(ids[..voters].ToImmutableArray(), ids[voters..].ToImmutableArray());
    }
}
