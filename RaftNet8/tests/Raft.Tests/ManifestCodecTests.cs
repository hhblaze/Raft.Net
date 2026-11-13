using System.Buffers.Binary;
using Raft.Storage;

namespace Raft.Tests;

public sealed class ManifestCodecTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("n2")]
    [InlineData("maximum")]
    public void Packed_state_frontier_roundtrips_and_every_byte_is_checked(string? vote)
    {
        if (vote == "maximum") vote = new string('n', 128);
        var state = new HardState(7, vote, 123);
        var tail = new StorageCodecV2.Frontier(100, 150, 7, 10000, 3000);
        var record = StorageCodecV2.Manifest(state, tail);
        var actual = StorageCodecV2.Manifest(record);
        Assert.Equal(state, actual.State);
        Assert.Equal(tail, actual.Tail);
        Assert.Equal(72 + (vote?.Length ?? 0), record.Length);
        Assert.Throws<InvalidDataException>(() => StorageCodecV2.Hard(record));
        for (var offset = 0; offset < record.Length; offset++)
        {
            record[offset] ^= 1;
            Assert.Throws<InvalidDataException>(() => StorageCodecV2.Manifest(record));
            record[offset] ^= 1;
        }
    }

    [Fact]
    public void Valid_checksum_does_not_hide_bad_lengths_or_invalid_hard_state()
    {
        var valid = StorageCodecV2.Manifest(new(7, "n2", 123), new(100, 150, 7, 10000, 3000));
        var body = StorageCodecV2.Open(valid, StorageCodecV2.ManifestMagic).ToArray();
        foreach (var length in new[] { 0, 59, 189 })
            Assert.Throws<InvalidDataException>(() => StorageCodecV2.Manifest(StorageCodecV2.Wrap(new byte[length], StorageCodecV2.ManifestMagic)));
        foreach (var length in new[] { -1, 0, 3, 129, int.MaxValue })
        {
            BinaryPrimitives.WriteInt32LittleEndian(body.AsSpan(16), length);
            Assert.Throws<InvalidDataException>(() => StorageCodecV2.Manifest(StorageCodecV2.Wrap(body, StorageCodecV2.ManifestMagic)));
        }
        BinaryPrimitives.WriteInt32LittleEndian(body.AsSpan(16), 2);
        foreach (var term in new long[] { -1, 0 })
        {
            BinaryPrimitives.WriteInt64LittleEndian(body, term);
            Assert.Throws<InvalidDataException>(() => StorageCodecV2.Manifest(StorageCodecV2.Wrap(body, StorageCodecV2.ManifestMagic)));
        }
        BinaryPrimitives.WriteInt64LittleEndian(body, 7);
        BinaryPrimitives.WriteInt64LittleEndian(body.AsSpan(8), -1);
        Assert.Throws<InvalidDataException>(() => StorageCodecV2.Manifest(StorageCodecV2.Wrap(body, StorageCodecV2.ManifestMagic)));
    }
}
