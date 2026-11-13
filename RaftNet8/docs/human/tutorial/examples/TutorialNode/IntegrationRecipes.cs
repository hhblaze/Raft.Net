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
    public static async Task AddAndPromoteAsync(RaftNode leader, string learnerId,
        CancellationToken token)
    {
        // First provision KnownPeers, routes, certificates and the new process.
        // Pause writes for this example so the catch-up target stops moving.
        await leader.ReadIndexAsync(token);
        await leader.AddLearnerAsync(learnerId, token);
        while (true)
        {
            var status = leader.Status;
            if (status.Role != RaftRole.Leader) throw new NotLeaderException(status.LeaderId);
            if (status.Peers.Any(peer => peer.NodeId == learnerId && peer.MatchIndex >= status.LastLogIndex))
                break;
            await Task.Delay(50, token);
        }
        await leader.PromoteToVoterAsync(learnerId, token);
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
