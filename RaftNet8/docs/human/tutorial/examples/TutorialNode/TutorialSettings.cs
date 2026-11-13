using System.Net;

namespace Raft.Tutorial;

internal sealed record TutorialSettings(RaftOptions Options, string Root, bool InMemory,
    IReadOnlyDictionary<string, IPEndPoint> Endpoints)
{
    public static TutorialSettings Parse(string[] args)
    {
        if (args.Length == 0) throw new ArgumentException(
            "Usage: n1|n2|n3 [--single] [--memory] [--data path] [--base-port 6300]");
        var nodeId = args[0];
        var single = false;
        var memory = false;
        var basePort = 6300;
        var dataRoot = ".data/tutorial";
        for (var i = 1; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--single": single = true; break;
                case "--memory": memory = true; break;
                case "--data" when i + 1 < args.Length: dataRoot = args[++i]; break;
                case "--base-port" when i + 1 < args.Length: basePort = int.Parse(args[++i]); break;
                default: throw new ArgumentException($"Unknown or incomplete option: {args[i]}");
            }
        }
        string[] members = single ? ["n1"] : ["n1", "n2", "n3"];
        if (!members.Contains(nodeId, StringComparer.Ordinal))
            throw new ArgumentException("The node ID must be one of the initial voters.");
        if (basePort is < 1024 or > 65532) throw new ArgumentOutOfRangeException(nameof(basePort));
        var root = Path.GetFullPath(Path.Combine(dataRoot, nodeId));
        // book:start options
        var options = new RaftOptions
        {
            NodeId = nodeId,
            GroupId = single ? "tutorial-single-v1" : "tutorial-three-v1",
            Members = members,
            WireVersion = 2,
            // Illustrative local-lab deadlines, deliberately above library defaults.
            TickInterval = TimeSpan.FromMilliseconds(25),
            HeartbeatTicks = 2,
            ElectionMinTicks = 40,
            ElectionMaxTicks = 80,
            RpcTimeout = TimeSpan.FromMilliseconds(500),
            SnapshotDirectory = memory ? null : Path.Combine(root, "snapshots"),
            SnapshotIntervalEntries = 0 // Manual snapshots in this exercise.
        };
        // book:end options
        var endpoints = members.Select((id, index) => (id, index)).ToDictionary(
            pair => pair.id, pair => new IPEndPoint(IPAddress.Loopback, basePort + pair.index));
        return new(options, root, memory, endpoints);
    }
}
