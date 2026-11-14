using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tutorial;

// Compiled integration fragments used in both HTML editions.
public static class IntegrationRecipes
{
    // book:start load
    public static Task SubmitBoundedAsync(RaftNode leader, IEnumerable<SetValue> commands,
        int maxCommandBytes, CancellationToken token) =>
        Parallel.ForEachAsync(commands, new ParallelOptions
        {
            MaxDegreeOfParallelism = 64,
            CancellationToken = token
        }, async (command, ct) =>
        {
            var bytes = BookData.Encode(command, maxCommandBytes);
            await leader.ProposeAsync(bytes, ct);
        });
    // book:end load

    // book:start membership
    public static Task<MembershipOperationStatus> AddAndPromoteAsync(RaftNode leader, PeerDescriptor newPeer,
        Guid operationId, CancellationToken token) =>
        // Start it with the SAME original configuration/directory and JoiningNode=true; the log assigns its role.
        // Keep operationId and descriptor for retry on a new leader. Writes may continue during catch-up.
        leader.AddNodeAsync(newPeer, operationId, token);

    public static Task<MembershipOperationStatus> RemoveAsync(RaftNode leader, string nodeId,
        Guid operationId, CancellationToken token) =>
        // Removing this leader persists intent, transfers, and continues on its successor.
        // A NotLeaderException means resolve the new leader and retry the SAME ID/content.
        leader.RemoveNodeAsync(nodeId, operationId, token);

    public static async Task<MembershipOperationStatus?> InspectOrCancelAsync(RaftNode leader,
        Guid operationId, bool explicitlyCancel, CancellationToken token)
    {
        if (explicitlyCancel)
            return await leader.CancelMembershipOperationAsync(operationId, token);
        return await leader.GetMembershipOperationAsync(operationId, token);
    }
    // book:end membership

    // book:start tls
    public static TcpRaftTransport CreateMutualTlsTransport(RaftOptions options,
        IReadOnlyDictionary<string, IPEndPoint> endpoints, X509Certificate2 localCertificate,
        IReadOnlyDictionary<string, string> allowedSha256)
    {
        // Keep platform chain/name/revocation checks AND bind each certificate to its Raft ID.
        // allowedSha256 is trusted deployment configuration, not received from the peer.
        var security = new TlsRaftTransportSecurity(localCertificate, (id, certificate) =>
            allowedSha256.TryGetValue(id, out var expected) &&
            string.Equals(expected, certificate.GetCertHashString(HashAlgorithmName.SHA256),
                StringComparison.OrdinalIgnoreCase));
        return new TcpRaftTransport(options, endpoints, security);
    }
    // book:end tls

    // book:start logging
    public static ValueTask<RaftNode> CreateWithLoggingAsync(RaftOptions options, ILogStore log,
        ITransport transport, IStateMachine application, ILoggerFactory loggerFactory,
        CancellationToken token) =>
        RaftNode.CreateAsync(options, log, transport, application,
            clock: null, random: null, cancellationToken: token, loggerFactory: loggerFactory);
    // book:end logging

    // book:start larger-command
    public static RaftOptions WithLargerCommands(RaftOptions bootstrap) => bootstrap with
    {
        MaxCommandBytes = 1024 * 1024,
        MaxAppendBytes = 2 * 1024 * 1024,
        MaxProposalBatchBytes = 4 * 1024 * 1024,
        MaxReplicationBytes = 4 * 1024 * 1024,
        MaxInFlightBytes = 16 * 1024 * 1024,
        MaxUncommittedBytes = 128 * 1024 * 1024
    };
    // book:end larger-command
}
