using System.Net;
using System.Text.Json;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Demo;

internal sealed record WorkerCommand(long Id, string Action, string? Key = null, string? Value = null, string? Target = null)
{
    public Guid? OperationId { get; init; }
    public PeerDescriptor? Peer { get; init; }
    public int WaitMilliseconds { get; init; } = 15000;
}
internal sealed record WorkerStatus(string NodeId, RaftRole Role, long Term, string? LeaderId, long CommitIndex,
    long LastApplied, long LastLogIndex, long Keys, long SnapshotIndex, long? InstallingSnapshotIndex,
    string[] Voters, string[] Learners, IReadOnlyList<RaftPeerStatus> Peers, string? Fault)
{
    public MembershipState? Membership { get; init; }
    public int ProcessId { get; init; } = Environment.ProcessId;
}
internal sealed record WorkerResponse(long Id, bool Success, WorkerStatus? Status = null, ProposalResult? Proposal = null,
    string? Value = null, SnapshotMetadata? Snapshot = null, string? Error = null, string? LeaderId = null)
{
    public MembershipOperationStatus? Operation { get; init; }
}

/// <summary>One real disk/TCP node in one process. JSON lines on stdin/stdout are its local control channel.</summary>
internal static class WorkerHost
{
    internal static readonly JsonSerializerOptions Json = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    public static async Task<int> RunAsync(string[] args, CancellationToken stop)
    {
        string Required(string name)
        {
            var index = Array.IndexOf(args, name);
            if (index < 0 || index + 1 == args.Length) throw new ArgumentException($"Worker needs {name}.");
            return args[index + 1];
        }
        var id = Required("--id");
        var root = Path.GetFullPath(Required("--data"));
        var voters = Required("--members").Split(',');
        var endpoints = Required("--endpoints").Split(',').ToDictionary(item => item[..item.IndexOf('=')],
            item => IPEndPoint.Parse(item[(item.IndexOf('=') + 1)..]), StringComparer.Ordinal);
        var layout = (args.Contains("--log-layout") ? Required("--log-layout") : "v4") switch
        {
            "v2" => DBreezeLogLayout.SeparateTablesV2,
            "v3" => DBreezeLogLayout.UnifiedTableV3,
            "v4" => DBreezeLogLayout.UnifiedTableV4,
            _ => throw new ArgumentException("Worker log layout must be v2, v3 or v4.")
        };
        var options = new RaftOptions { NodeId = id, GroupId = "process_test", Members = voters,
            // A disk/process demonstration must not use a 300ms failure detector during recovery or native scheduling tails.
            ElectionMinTicks = 200, ElectionMaxTicks = 240, RpcTimeout = TimeSpan.FromSeconds(2),
            WireVersion = layout == DBreezeLogLayout.UnifiedTableV4 ? 3 : 2,
            JoiningNode = layout == DBreezeLogLayout.UnifiedTableV4 && !voters.Contains(id, StringComparer.Ordinal),
            Learners = layout == DBreezeLogLayout.UnifiedTableV4 || voters.Contains(id, StringComparer.Ordinal) ? [] : [id],
            KnownPeers = layout == DBreezeLogLayout.UnifiedTableV4 ? [] : endpoints.Keys.ToArray(),
            InitialPeers = layout == DBreezeLogLayout.UnifiedTableV4 ? voters.Select(peer => Descriptor(peer)).ToArray() : [],
            SnapshotDirectory = Path.Combine(root, "snapshots"), SnapshotRetainEntries = 8 };
        PeerDescriptor Descriptor(string peer) => new(peer, endpoints[peer].Address.ToString(), endpoints[peer].Port);
        using var machine = new SnapshotKeyValueMachine(Path.Combine(root, "application"), id);
        await using var log = new RaftStorageAdapter(new DBreezeLogStorage(Path.Combine(root, "log"), options, layout));
        await using var tcp = layout == DBreezeLogLayout.UnifiedTableV4
            ? new TcpRaftTransport(options, Descriptor(id)) : new TcpRaftTransport(options, endpoints);
        await using var node = await RaftNode.CreateAsync(options, log, tcp, machine, cancellationToken: stop);
        await node.StartAsync();
        tcp.Start(node.HandleAsync);
        await WriteAsync(new WorkerResponse(0, true, Status(node, machine)));
        while (!stop.IsCancellationRequested)
        {
            var input = Console.In.ReadLineAsync(stop).AsTask();
            var completed = await Task.WhenAny(input, node.Completion);
            if (completed == node.Completion) { await node.Completion; return 0; }
            var line = await input;
            if (line is null) return 0;
            var command = JsonSerializer.Deserialize<WorkerCommand>(line, Json) ?? throw new InvalidDataException("Invalid worker command.");
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(stop);
            if (command.WaitMilliseconds is < 1 or > 120000) throw new ArgumentException("Worker wait must be 1..120000ms.");
            deadline.CancelAfter(TimeSpan.FromMilliseconds(command.WaitMilliseconds));
            try
            {
                ProposalResult? proposal = null;
                SnapshotMetadata? snapshot = null;
                MembershipOperationStatus? operation = null;
                string? value = null;
                switch (command.Action)
                {
                    case "status": break;
                    case "set": proposal = await node.ProposeAsync(SnapshotKeyValueMachine.Encode(command.Key!, command.Value!), deadline.Token); break;
                    case "get": await node.ReadIndexAsync(deadline.Token); value = machine.Get(command.Key!); break;
                    case "stale": value = machine.Get(command.Key!); break;
                    case "transfer": await node.TransferLeadershipAsync(command.Target!, deadline.Token); break;
                    case "add": proposal = await node.AddLearnerAsync(command.Target!, deadline.Token); break;
                    case "promote": proposal = await node.PromoteToVoterAsync(command.Target!, deadline.Token); break;
                    case "remove": proposal = await node.RemoveNodeAsync(command.Target!, deadline.Token); break;
                    case "add-node": operation = await node.AddNodeAsync(command.Peer!, command.OperationId!.Value, deadline.Token); break;
                    case "remove-node": operation = await node.RemoveNodeAsync(command.Target!, command.OperationId!.Value, deadline.Token); break;
                    case "membership": operation = await node.GetMembershipOperationAsync(command.OperationId!.Value, deadline.Token); break;
                    case "cancel-membership": operation = await node.CancelMembershipOperationAsync(command.OperationId!.Value, deadline.Token); break;
                    case "snapshot": snapshot = await node.CreateSnapshotAsync(deadline.Token); break;
                    case "quit": await WriteAsync(new WorkerResponse(command.Id, true, Status(node, machine))); return 0;
                    default: throw new ArgumentException("Unknown worker action.");
                }
                await WriteAsync(new WorkerResponse(command.Id, true, Status(node, machine), proposal, value, snapshot) { Operation = operation });
            }
            catch (Exception error)
            {
                await WriteAsync(new WorkerResponse(command.Id, false, Error: error.GetType().Name + ": " + error.Message,
                    LeaderId: node.Status.LeaderId));
                if (node.Status.Fault is not null) { Console.Error.WriteLine(node.Status.Fault); return 1; }
            }
        }
        return 0;
    }

    private static WorkerStatus Status(RaftNode node, SnapshotKeyValueMachine machine)
    {
        var status = node.Status;
        return new(status.NodeId, status.Role, status.Term, status.LeaderId, status.CommitIndex, status.LastApplied,
            status.LastLogIndex, machine.Count, status.Snapshot?.LastIncludedIndex ?? 0, status.InstallingSnapshotIndex,
            status.Configuration!.Voters.ToArray(), status.Configuration.Learners.ToArray(), status.Peers, status.Fault?.ToString())
            { Membership = status.Membership };
    }
    private static async Task WriteAsync(WorkerResponse response)
    {
        await Console.Out.WriteLineAsync(JsonSerializer.Serialize(response, Json));
        await Console.Out.FlushAsync();
    }
}
