using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Text;

namespace Raft;

/// <summary>One active configuration; learners never campaign or count toward its quorum.</summary>
public sealed record ClusterConfiguration(ImmutableArray<string> Voters, ImmutableArray<string> Learners,
    long Index = 0, long Term = 0)
{
    public int Quorum => Voters.Length / 2 + 1;
    public bool IsVoter(string id) => Voters.Contains(id, StringComparer.Ordinal);
    public bool Contains(string id) => IsVoter(id) || Learners.Contains(id, StringComparer.Ordinal);
    public IEnumerable<string> AllMembers => Voters.Concat(Learners);
    internal static bool SameMembers(ClusterConfiguration left, ClusterConfiguration right) =>
        left.Voters.ToHashSet(StringComparer.Ordinal).SetEquals(right.Voters) &&
        left.Learners.ToHashSet(StringComparer.Ordinal).SetEquals(right.Learners);

    internal ClusterConfiguration Validate()
    {
        if (Voters.IsDefaultOrEmpty || Learners.IsDefault || Voters.Length + Learners.Length > 1024 ||
            Index < 0 || Term < 0 || ((Index == 0) != (Term == 0)))
            throw new ArgumentException("Invalid configuration metadata.");
        var ids = new HashSet<string>(StringComparer.Ordinal);
        foreach (var id in AllMembers)
        {
            RaftOptions.ValidateId(id, nameof(Voters));
            if (!ids.Add(id)) throw new ArgumentException("Voters and learners must have unique IDs.");
        }
        return this;
    }

    internal byte[] Encode()
    {
        Validate();
        var buffer = new ArrayBufferWriter<byte>();
        PutInt(Voters.Length);
        foreach (var id in Voters) PutId(id);
        PutInt(Learners.Length);
        foreach (var id in Learners) PutId(id);
        return buffer.WrittenSpan.ToArray();

        void PutInt(int value) { BinaryPrimitives.WriteInt32LittleEndian(buffer.GetSpan(4), value); buffer.Advance(4); }
        void PutId(string id)
        {
            PutInt(id.Length);
            Encoding.ASCII.GetBytes(id, buffer.GetSpan(id.Length));
            buffer.Advance(id.Length);
        }
    }

    internal static ClusterConfiguration Decode(ReadOnlySpan<byte> bytes, long index = 0, long term = 0)
    {
        var reader = new Reader(bytes);
        var voters = reader.Ids();
        var learners = reader.Ids();
        if (!reader.IsEmpty) throw new InvalidDataException("Trailing configuration bytes.");
        try { return new ClusterConfiguration(voters, learners, index, term).Validate(); }
        catch (ArgumentException error) { throw new InvalidDataException("Invalid configuration.", error); }
    }

    private ref struct Reader(ReadOnlySpan<byte> bytes)
    {
        private ReadOnlySpan<byte> _remaining = bytes;
        public bool IsEmpty => _remaining.IsEmpty;
        private int Int()
        {
            if (_remaining.Length < 4) throw new InvalidDataException("Truncated configuration.");
            var value = BinaryPrimitives.ReadInt32LittleEndian(_remaining);
            _remaining = _remaining[4..];
            return value;
        }
        public ImmutableArray<string> Ids()
        {
            var count = Int();
            if (count is < 0 or > 1024) throw new InvalidDataException("Invalid member count.");
            var builder = ImmutableArray.CreateBuilder<string>(count);
            for (var i = 0; i < count; i++)
            {
                var length = Int();
                if (length is < 1 or > 128 || length > _remaining.Length) throw new InvalidDataException("Invalid member ID.");
                builder.Add(Encoding.ASCII.GetString(_remaining[..length]));
                _remaining = _remaining[length..];
            }
            return builder.MoveToImmutable();
        }
    }
}
