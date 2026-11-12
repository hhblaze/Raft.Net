using BenchmarkDotNet.Attributes;
using Raft.Transport;

namespace Raft.Benchmarks;

[MemoryDiagnoser]
public class WireBenchmarks
{
    private readonly RaftOptions _options = new() { NodeId = "n1", Members = new[] { "n1", "n2", "n3" } };
    private AppendRequest _append = null!;
    private readonly AppendRequest _heartbeat = new(1, 2, 32, 1, 32, Array.Empty<LogEntry>());
    private byte[] _encoded = null!;

    [GlobalSetup]
    public void Setup()
    {
        _append = new(1, 1, 0, 0, 0, Enumerable.Range(1, 32).Select(i => new LogEntry(i, 1, new byte[256], false)).ToArray());
        using var frame = RaftWireCodec.Encode("default", "n1", _append, _options.MaxAppendBytes + 1024);
        _encoded = frame.Memory.Span[4..].ToArray();
    }

    [Benchmark]
    public int EncodeHeartbeat()
    {
        using var frame = RaftWireCodec.Encode("default", "n1", _heartbeat, _options.MaxAppendBytes + 1024);
        return frame.Memory.Length;
    }

    [Benchmark]
    public int EncodeAppend()
    {
        using var frame = RaftWireCodec.Encode("default", "n1", _append, _options.MaxAppendBytes + 1024);
        return frame.Memory.Length;
    }

    [Benchmark] public RaftRpc DecodeAppend() => RaftWireCodec.Decode(_encoded, _options).Message;
}
