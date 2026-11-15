using System.Net;
using System.Net.Http.Json;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Raft.Cluster;
using Raft.Cluster.Client;
using Raft.Cluster.Server;
using Raft.Transport;

namespace Raft.Tests;

[Collection("OS process clusters")]
public sealed class ClusterSecureProfileTests
{
    private static int Port()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
        try { return ((IPEndPoint)listener.LocalEndpoint).Port; } finally { listener.Stop(); }
    }
    private static HttpClient Client(X509Certificate2 server, X509Certificate2? certificate = null)
    {
        var handler = new SocketsHttpHandler { AllowAutoRedirect = false };
        handler.ConnectCallback = async (context, token) =>
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            try { await socket.ConnectAsync(IPAddress.Loopback, context.DnsEndPoint.Port, token); return new NetworkStream(socket, true); }
            catch { socket.Dispose(); throw; }
        };
        handler.SslOptions.RemoteCertificateValidationCallback = (_, remote, _, errors) => remote is not null &&
            remote.GetCertHashString() == server.Thumbprint && (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) == SslPolicyErrors.None;
        if (certificate is not null) handler.SslOptions.ClientCertificates = new X509CertificateCollection { certificate };
        return new(handler) { Timeout = TimeSpan.FromSeconds(5) };
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Diagnostic_certificate_reads_without_quorum_but_cannot_issue_tokens(bool authorized)
    {
        using var directory = new ClusterTestDirectory();
        var suffix = Guid.NewGuid().ToString("N");
        var id = "member-" + suffix;
        var webName = "web-" + suffix + ".test";
        using var peerCertificate = MultiplexAndTlsTests.Certificate(id);
        using var webCertificate = MultiplexAndTlsTests.Certificate(webName);
        using var diagnostic = MultiplexAndTlsTests.Certificate("diagnostic-" + suffix);
        using var other = MultiplexAndTlsTests.Certificate("other-" + suffix);
        var clientPort = Port(); var managementPort = Port();
        var peer = new PeerDescriptor(id, "127.0.0.1", Port());
        var ids = new[] { id, "offline-b-" + suffix, "offline-c-" + suffix };
        var peers = new[] { peer, new PeerDescriptor(ids[1], "127.0.0.1", Port()), new PeerDescriptor(ids[2], "127.0.0.1", Port()) };
        var management = new ManagedNodeEndpoint(id, new Uri($"https://{webName}:{managementPort}/"));
        await using var node = await RaftClusterNode.StartAsync(new()
        {
            DataDirectory = directory.Path, Peer = peer,
            PeerSecurity = new TlsRaftTransportSecurity(peerCertificate, (_, _) => false),
            Raft = new()
            {
                GroupId = "secure-" + suffix, NodeId = id, Members = ids, InitialPeers = peers, WireVersion = 3,
                TickInterval = TimeSpan.FromMilliseconds(25), ElectionMinTicks = 200, ElectionMaxTicks = 280
            },
            ClientServer = new()
            {
                Endpoint = new(id, new($"https://{webName}:{clientPort}/")),
                Security = new() { RequireClientTokens = true, SecureTransport = true },
                ConfigureHttpServer = builder =>
                {
                    builder.Configuration["AllowedHosts"] = webName;
                    builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, clientPort, listener => listener.UseHttps(webCertificate)));
                },
                Management = new()
                {
                    Endpoint = management.Address, DiagnosticCertificateThumbprints = [diagnostic.Thumbprint],
                    ConfigureHttpServer = builder =>
                    {
                        builder.Configuration["AllowedHosts"] = webName;
                        builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, managementPort,
                            listener => listener.UseHttps(webCertificate, https =>
                            {
                                https.ClientCertificateMode = ClientCertificateMode.AllowCertificate;
                                // Explicit fixture trust pins both certificates; only one has diagnostic authorization.
                                https.ClientCertificateValidation = (certificate, _, _) => certificate.Thumbprint == diagnostic.Thumbprint || certificate.Thumbprint == other.Thumbprint;
                            })));
                    }
                }
            }
        });
        using var http = Client(webCertificate, authorized ? diagnostic : other);
        using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(management.Address, Administration.ApiPath + "/diagnostics"));
        request.Headers.Add("X-Raft-GroupId", "secure-" + suffix);
        using var response = await http.SendAsync(request);
        Assert.Equal(authorized ? HttpStatusCode.OK : HttpStatusCode.Unauthorized, response.StatusCode);
        if (authorized)
        {
            var diagnostics = (await response.Content.ReadFromJsonAsync<NodeDiagnostics>())!;
            Assert.Equal(id, diagnostics.Status.NodeId);
            Assert.Equal(0, diagnostics.Status.CommitIndex);
            Assert.True(diagnostics.Resources.WorkingSetBytes > 0);
            Assert.True(diagnostics.Resources.ProcessId > 0);
            using var write = new HttpRequestMessage(HttpMethod.Post, new Uri(management.Address, Administration.ApiPath + "/tokens/issue"))
            { Content = JsonContent.Create(new TokenGrant("forbidden", ClusterPermission.All, ["*"], DateTimeOffset.UtcNow.AddDays(1))) };
            write.Headers.Add("X-Raft-GroupId", "secure-" + suffix);
            write.Headers.Add("X-Raft-RequestId", Guid.NewGuid().ToString("D"));
            using var denied = await http.SendAsync(write);
            Assert.False(denied.IsSuccessStatusCode);
            Assert.False(node.Ledger.SecurityInitialized);
        }
    }

    [Fact]
    public async Task Secure_profile_rejects_plaintext_before_creating_databases()
    {
        using var directory = new ClusterTestDirectory();
        var path = Path.Combine(directory.Path, "must-not-exist");
        using var certificate = MultiplexAndTlsTests.Certificate("unused-" + Guid.NewGuid().ToString("N"));
        await Assert.ThrowsAsync<ArgumentException>(() => RaftClusterNode.StartAsync(new()
        {
            DataDirectory = path, Peer = new("n1", "127.0.0.1", Port()),
            PeerSecurity = new TlsRaftTransportSecurity(certificate, (_, _) => false),
            Raft = new() { NodeId = "n1", Members = ["n1"], WireVersion = 3, InitialPeers = [new("n1", "127.0.0.1", Port())] },
            ClientServer = new() { Endpoint = new("n1", new("http://127.0.0.1:7101/")), Security = new() { SecureTransport = true } }
        }));
        Assert.False(Directory.Exists(path));
    }
}
