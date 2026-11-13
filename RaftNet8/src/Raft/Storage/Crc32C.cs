using System.Buffers.Binary;
using System.Runtime.Intrinsics.X86;

namespace Raft.Storage;

/// <summary>Castagnoli CRC32C (not IEEE CRC32). Hardware and portable implementations share test vectors.</summary>
internal static class Crc32C
{
    private static readonly uint[] Table = BuildTable();
    public static uint Compute(ReadOnlySpan<byte> bytes) => ~Update(uint.MaxValue, bytes);
    internal static uint Update(uint crc, ReadOnlySpan<byte> bytes)
    {
        if (Sse42.X64.IsSupported)
            while (bytes.Length >= 8)
            {
                crc = (uint)Sse42.X64.Crc32(crc, BinaryPrimitives.ReadUInt64LittleEndian(bytes));
                bytes = bytes[8..];
            }
        if (Sse42.IsSupported)
        {
            while (bytes.Length >= 4)
            {
                crc = Sse42.Crc32(crc, BinaryPrimitives.ReadUInt32LittleEndian(bytes));
                bytes = bytes[4..];
            }
            foreach (var value in bytes) crc = Sse42.Crc32(crc, value);
            return crc;
        }
        return UpdatePortable(crc, bytes);
    }
    internal static uint UpdatePortable(uint crc, ReadOnlySpan<byte> bytes)
    {
        foreach (var value in bytes) crc = Table[(byte)(crc ^ value)] ^ (crc >> 8);
        return crc;
    }
    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < table.Length; i++)
        {
            var crc = i;
            for (var bit = 0; bit < 8; bit++) crc = (crc >> 1) ^ ((crc & 1) == 0 ? 0 : 0x82F63B78u);
            table[i] = crc;
        }
        return table;
    }
}
