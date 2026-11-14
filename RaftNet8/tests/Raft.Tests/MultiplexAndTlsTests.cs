using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Raft.Transport;

namespace Raft.Tests;

public sealed class MultiplexAndTlsTests
{
    private static RaftOptions Options(string id) => new()
    {
        NodeId = id, Members = ["n1", "n2"], TickInterval = TimeSpan.FromMilliseconds(100),
        RpcTimeout = TimeSpan.FromSeconds(1)
    };
    private static Dictionary<string, IPEndPoint> Endpoints() => new()
    { ["n1"] = new(IPAddress.Loopback, 0), ["n2"] = new(IPAddress.Loopback, 0) };

    [Fact]
    public async Task Responses_can_overtake_a_blocked_request_and_late_canceled_reply_does_not_poison_session()
    {
        var endpoints = Endpoints();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new TcpRaftTransport(Options("n1"), endpoints);
        server.Start(async (_, message, token) =>
        {
            var request = (AppendRequest)message;
            if (request.RequestId == 1) { entered.TrySetResult(); await release.Task.WaitAsync(token); }
            return new AppendResponse(request.Term, request.RequestId, true, 0);
        });
        endpoints["n1"] = server.LocalEndPoint;
        await using var peer = new TcpRaftTransport(Options("n2"), endpoints);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
        var first = peer.SendAsync("n1", new AppendRequest(1, 1, 0, 0, 0, []), canceled.Token).AsTask();
        await entered.Task.WaitAsync(deadline.Token);
        try
        {
            var second = await peer.SendAsync("n1", new AppendRequest(1, 2, 0, 0, 0, []), deadline.Token).AsTask()
                .WaitAsync(TimeSpan.FromSeconds(3));
            Assert.Equal(2, Assert.IsType<AppendResponse>(second).RequestId);
            Assert.False(first.IsCompleted);
            canceled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        }
        finally { release.TrySetResult(); }
        var tasks = Enumerable.Range(3, 100).Select(i => peer.SendAsync("n1", new AppendRequest(1, i, 0, 0, 0, []), deadline.Token).AsTask());
        var results = await Task.WhenAll(tasks);
        Assert.Equal(Enumerable.Range(3, 100).Select(i => (long)i), results.Cast<AppendResponse>().Select(r => r.RequestId));
        Assert.Equal(0, server.ConnectionErrors);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Mutual_TLS_verifies_certificate_to_Raft_ID_binding_before_dispatch(bool authorized)
    {
        using var cert1 = Certificate("n1");
        using var cert2 = Certificate("n2");
        var trusted = new HashSet<string>(StringComparer.Ordinal) { cert1.Thumbprint, cert2.Thumbprint };
        bool Chain(object sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors) =>
            certificate is not null && trusted.Contains(certificate.GetCertHashString()) &&
            (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) == SslPolicyErrors.None;
        var serverSecurity = new TlsRaftTransportSecurity(cert1,
            (id, cert) => authorized && id == "n2" && cert.Thumbprint == cert2.Thumbprint, Chain, checkRevocation: false);
        var peerSecurity = new TlsRaftTransportSecurity(cert2,
            (id, cert) => id == "n1" && cert.Thumbprint == cert1.Thumbprint, Chain, checkRevocation: false);
        var endpoints = Endpoints();
        await using var server = new TcpRaftTransport(Options("n1"), endpoints, serverSecurity);
        var calls = 0;
        server.Start((_, request, _) =>
        {
            Interlocked.Increment(ref calls);
            return ValueTask.FromResult<RaftRpc>(new VoteResponse(request.Term, true));
        });
        endpoints["n1"] = server.LocalEndPoint;
        await using var peer = new TcpRaftTransport(Options("n2"), endpoints, peerSecurity);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        if (authorized)
        {
            Assert.True(Assert.IsType<VoteResponse>(await peer.SendAsync("n1", new VoteRequest(1, 0, 0), deadline.Token)).Granted);
            Assert.Equal(1, calls);
        }
        else
        {
            Assert.NotNull(await Record.ExceptionAsync(() => peer.SendAsync("n1", new VoteRequest(1, 0, 0), deadline.Token).AsTask()));
            Assert.Equal(0, calls);
        }
    }

    [Fact]
    public async Task A_connection_cannot_switch_claimed_peer_ID_after_first_frame()
    {
        var endpoints = Endpoints();
        endpoints.Add("n3", new(IPAddress.Loopback, 0));
        var options = Options("n1") with { KnownPeers = ["n3"] };
        await using var server = new TcpRaftTransport(options, endpoints);
        var calls = 0;
        server.Start((_, request, _) =>
        {
            Interlocked.Increment(ref calls);
            return ValueTask.FromResult<RaftRpc>(new VoteResponse(request.Term, true));
        });
        using var client = new TcpClient();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await client.ConnectAsync(server.LocalEndPoint, deadline.Token);
        using (var first = RaftWireCodec.Encode("default", "n2", new VoteRequest(1, 0, 0), options.MaxFrameBytes, 2, 1))
            await client.GetStream().WriteAsync(first.Memory, deadline.Token);
        await RaftWireCodec.ReadAsync(client.GetStream(), new byte[4], options, deadline.Token);
        using (var other = RaftWireCodec.Encode("default", "n3", new VoteRequest(2, 0, 0), options.MaxFrameBytes, 2, 2))
            await client.GetStream().WriteAsync(other.Memory, deadline.Token);
        Assert.Equal(0, await client.GetStream().ReadAsync(new byte[1], deadline.Token));
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Updating_a_provisioned_route_closes_old_session_and_reconnects_to_new_endpoint()
    {
        var endpoints = Endpoints();
        await using var oldServer = new TcpRaftTransport(Options("n1"), endpoints);
        oldServer.Start((_, _, _) => ValueTask.FromResult<RaftRpc>(new VoteResponse(1, true)));
        endpoints["n1"] = oldServer.LocalEndPoint;
        await using var peer = new TcpRaftTransport(Options("n2"), endpoints);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        Assert.Equal(1, (await peer.SendAsync("n1", new VoteRequest(1, 0, 0), deadline.Token)).Term);
        var replacementEndpoints = Endpoints();
        await using var replacement = new TcpRaftTransport(Options("n1"), replacementEndpoints);
        replacement.Start((_, _, _) => ValueTask.FromResult<RaftRpc>(new VoteResponse(2, true)));
        await peer.UpdatePeerAsync("n1", replacement.LocalEndPoint);
        Assert.Equal(2, (await peer.SendAsync("n1", new VoteRequest(2, 0, 0), deadline.Token)).Term);
        await Assert.ThrowsAsync<ArgumentException>(() => peer.UpdatePeerAsync("unprovisioned", replacement.LocalEndPoint).AsTask());
    }

    internal static X509Certificate2 Certificate(string id)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={id}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(id);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            new OidCollection { new("1.3.6.1.5.5.7.3.1"), new("1.3.6.1.5.5.7.3.2") }, true));
        using var created = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1));
        // Windows SChannel needs an OS-backed key, even for a temporary test certificate.
        // DefaultKeySet deletes the imported temporary key when the certificate is disposed.
        return new X509Certificate2(created.Export(X509ContentType.Pfx));
    }
}
