using System.Collections.Immutable;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class Wire3DirectoryTests
{
    [Fact]
    public void Every_bootstrap_role_needs_a_live_descriptor_before_the_node_can_start()
    {
        var options = MembershipStorageTests.Options() with { Learners = ["n4"] };
        Assert.Throws<ArgumentException>(() => DBreezeLogStorage.CreateInMemory(options, DBreezeLogLayout.UnifiedTableV4));
        var complete = options with { InitialPeers = options.InitialPeers.Append(new PeerDescriptor("n4", "127.0.0.1", 7304)).ToArray() };
        using var valid = DBreezeLogStorage.CreateInMemory(complete, DBreezeLogLayout.UnifiedTableV4);
        Assert.Equal(new[] { "n4" }, valid.Recover().Configuration!.Learners);
        Assert.Throws<ArgumentException>(() => DBreezeLogStorage.CreateInMemory(complete with
        {
            InitialPeers = complete.InitialPeers.Select(peer => peer.NodeId == "n1" ? peer with { IsRetired = true } : peer).ToArray()
        }, DBreezeLogLayout.UnifiedTableV4));
    }

    [Fact]
    public void A_joining_node_keeps_the_canonical_configuration_and_cannot_enable_the_option_on_old_wire()
    {
        var options = MembershipStorageTests.Options() with { NodeId = "n4", JoiningNode = true };
        var frozen = options.Freeze();
        Assert.Equal(options.Members, frozen.Members);
        Assert.Empty(frozen.Learners);
        Assert.Throws<ArgumentException>(() => (options with { JoiningNode = false }).Freeze());
        Assert.Throws<ArgumentException>(() => (options with { WireVersion = 2 }).Freeze());
        Assert.Throws<ArgumentException>(() => (options with { WireVersion = 1, EnablePreVote = false }).Freeze());
    }

    [Fact]
    public void Wire3_service_entries_and_directory_roundtrip_but_are_rejected_by_old_wire_and_storage()
    {
        var options = MembershipStorageTests.Options();
        var directory = new MembershipState(7, 7, [new("n1", "127.0.0.1", 7301), new("n2", "localhost", 7302)]);
        using var frame = RaftWireCodec.Encode("default", "n2", new PeerDirectoryRequest(directory), options.MaxFrameBytes, 3, 2);
        var envelope = RaftWireCodec.Decode(frame.Memory.Span[4..], options);
        Assert.Equal(directory.Peers.ToArray(), Assert.IsType<PeerDirectoryRequest>(envelope.Message).Directory.Peers.ToArray());
        Assert.Throws<InvalidDataException>(() => RaftWireCodec.Encode("default", "n2", new PeerDirectoryRequest(directory), options.MaxFrameBytes, 2, 1));
        var status = new MembershipOperationStatus(Guid.NewGuid(), MembershipOperationKind.AddNode, "n2", directory.Peers[1], MembershipOperationStage.Accepted, 1, 1);
        var entry = new LogEntry(1, 1, MembershipCodec.Encode(new(MembershipRecordKind.Start, status)), LogEntryKind.Membership);
        var request = new AppendRequest(1, 1, 0, 0, 0, [entry]);
        Assert.Throws<InvalidDataException>(() => RaftWireCodec.Encode("default", "n1", request, options.MaxFrameBytes, 2, 1));
        Assert.Throws<InvalidDataException>(() => StorageCodecV2.Entry(1, StorageCodecV2.Entry(entry), options.MaxCommandBytes));
        Assert.Equal(LogEntryKind.Membership, StorageCodecV2.Entry(1, StorageCodecV2.Entry(entry), options.MaxCommandBytes, true).Kind);
        Assert.Throws<InvalidDataException>(() => MembershipCodec.EncodeDirectory(directory with { Peers = Enumerable.Repeat(directory.Peers[0], 1025).ToImmutableArray() }));
        Assert.Throws<InvalidDataException>(() => MembershipCodec.Decode(new byte[MembershipCodec.MaxRecordBytes + 1]));
        Assert.Throws<InvalidDataException>(() => MembershipCodec.DecodeDirectory(new byte[MembershipCodec.MaxDirectoryBytes + 1]));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Unknown_ID_directory_repair_is_authorized_by_certificate_before_any_route_or_Raft_dispatch(bool authorized)
    {
        // Isolate native TLS session/credential caches from other fixtures using different certificates.
        var firstId = "n1-" + Guid.NewGuid().ToString("N");
        var joiningId = "n4-" + Guid.NewGuid().ToString("N");
        using var cert1 = MultiplexAndTlsTests.Certificate(firstId);
        using var cert4 = MultiplexAndTlsTests.Certificate(joiningId);
        var trusted = new HashSet<string>(StringComparer.Ordinal) { cert1.Thumbprint, cert4.Thumbprint };
        bool Chain(object _, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors) =>
            certificate is not null && trusted.Contains(certificate.GetCertHashString()) &&
            (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) == SslPolicyErrors.None;
        var serverSecurity = new TlsRaftTransportSecurity(cert1,
            (id, cert) => authorized && id == joiningId && cert.Thumbprint == cert4.Thumbprint, Chain, false);
        var peerSecurity = new TlsRaftTransportSecurity(cert4,
            (id, cert) => id == firstId && cert.Thumbprint == cert1.Thumbprint, Chain, false);
        var options = MembershipStorageTests.Options(firstId) with { Members = [firstId], InitialPeers = [new(firstId, "127.0.0.1", 7301)] };
        var endpoints = new Dictionary<string, IPEndPoint> { [firstId] = new(IPAddress.Loopback, 0) };
        await using var server = new TcpRaftTransport(options, endpoints, serverSecurity);
        var calls = 0;
        server.Start((_, request, _) =>
        {
            Interlocked.Increment(ref calls);
            return ValueTask.FromResult<RaftRpc>(new VoteResponse(request.Term, true));
        });
        var peerOptions = options with { NodeId = joiningId, JoiningNode = true };
        endpoints[firstId] = server.LocalEndPoint;
        endpoints[joiningId] = new(IPAddress.Loopback, 0);
        await using var peer = new TcpRaftTransport(peerOptions, endpoints, peerSecurity);
        var directory = new MembershipState(5, 5, [new(firstId, "127.0.0.1", server.LocalEndPoint.Port), new(joiningId, "127.0.0.1", 7304)]);
        await peer.UpdateDirectoryAsync(directory, default);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        if (authorized)
        {
            Assert.True(Assert.IsType<VoteResponse>(await peer.SendAsync(firstId, new VoteRequest(1, 0, 0), deadline.Token)).Granted);
            Assert.Equal(1, calls); // the directory exchange did not reach Raft
            Assert.Single(server.BootstrapPeers); // hints did not become authoritative cluster state
        }
        else
        {
            Assert.NotNull(await Record.ExceptionAsync(() => peer.SendAsync(firstId, new VoteRequest(1, 0, 0), deadline.Token).AsTask()));
            Assert.Equal(0, calls);
            using var raw = new TcpClient();
            // Authorization failure must also leave no usable route for the claimed new ID.
            Assert.Single(server.BootstrapPeers);
        }
    }
}
