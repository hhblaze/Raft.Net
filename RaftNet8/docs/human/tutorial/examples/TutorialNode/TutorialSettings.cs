using System.Net;

namespace Raft.Tutorial;

internal sealed record TutorialSettings(RaftOptions Options, string Root, bool InMemory,
    IReadOnlyDictionary<string, IPEndPoint> Endpoints)
{
    public static TutorialSettings Parse(string[] args)
    {
        if (args.Length == 0) throw new ArgumentException(
            "Usage: n1..n5 [--join] [--single] [--memory] [--data path] [--base-port 6300]");
        var nodeId = args[0];
        var single = false;
        var memory = false;
        var join = false;
        var basePort = 6300;
        var dataRoot = ".data/tutorial-v4";
        for (var i = 1; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--single": single = true; break;
                case "--memory": memory = true; break;
                case "--join": join = true; break;
                case "--data" when i + 1 < args.Length: dataRoot = args[++i]; break;
                case "--base-port" when i + 1 < args.Length: basePort = int.Parse(args[++i]); break;
                default: throw new ArgumentException($"Unknown or incomplete option: {args[i]}");
            }
        }
        string[] members = single ? ["n1"] : ["n1", "n2", "n3"];
        if (!new[] { "n1", "n2", "n3", "n4", "n5" }.Contains(nodeId) ||
            (!members.Contains(nodeId, StringComparer.Ordinal) && !join))
            throw new ArgumentException("Use an initial voter ID, or --join for a new n1..n5 learner.");
        if (basePort is < 1024 or > 65531) throw new ArgumentOutOfRangeException(nameof(basePort));
        var root = Path.GetFullPath(Path.Combine(dataRoot, nodeId));
        // book:start options
        var initialPeers = members.Select(id => new PeerDescriptor(id, "127.0.0.1",
            basePort + int.Parse(id.AsSpan(1)) - 1)).ToArray();
        var options = new RaftOptions
        {
            NodeId = nodeId,
            GroupId = single ? "tutorial-single-v4" : "tutorial-three-v4",
            Members = members,
            JoiningNode = !members.Contains(nodeId),
            WireVersion = 3,
            InitialPeers = initialPeers,
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
        var endpoints = initialPeers.ToDictionary(peer => peer.NodeId, peer => new IPEndPoint(IPAddress.Loopback, peer.Port));
        endpoints[nodeId] = new(IPAddress.Loopback, basePort + int.Parse(nodeId.AsSpan(1)) - 1);
        return new(options, root, memory, endpoints);
    }
}
