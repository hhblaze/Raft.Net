using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Raft.Cluster;
using Raft.Cluster.Client;

namespace Raft.Tests;

[Collection("OS process clusters")]
public sealed class ClusterFailureTests
{
    private sealed class ThrowOnRead : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => 0; set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => throw new IOException("Simulated truncated response body.");
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default) => ValueTask.FromException<int>(new IOException("Simulated truncated response body."));
        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
    private sealed class DropBody(string path) : DelegatingHandler(new SocketsHttpHandler { AllowAutoRedirect = false })
    {
        private int _dropped;
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            var response = await base.SendAsync(request, token);
            if (response.IsSuccessStatusCode && request.RequestUri!.AbsolutePath.EndsWith(path, StringComparison.Ordinal) && Interlocked.Exchange(ref _dropped, 1) == 0)
            { response.Content.Dispose(); response.Content = new StreamContent(new ThrowOnRead()); }
            return response;
        }
    }
    [Theory]
    [InlineData("/publish")]
    [InlineData("/messages")]
    public async Task A_broken_success_response_retries_without_losing_or_duplicating_messages(string path)
    {
        await using var cluster = new ClientTestCluster(1);
        await cluster.StartAsync(); await cluster.LeaderAsync();
        using var http = new HttpClient(new DropBody(path));
        await using var client = cluster.Client(http: http);
        Assert.Equal(1, await client.AddLogEntryAsync(new byte[] { 1 }, "body-loss"));
        var batch = await client.ReadBatchAsync("body-loss", 0);
        Assert.Equal(1, batch.Messages.Single().MessageId);
        Assert.Equal(1, (await client.GetEntityAsync("body-loss"))!.LastMessageId);
    }
    [Fact]
    public async Task Client_publications_continue_through_add_and_explicit_cancel_of_an_unreachable_learner()
    {
        await using var cluster = new ClientTestCluster(3);
        await cluster.StartAsync(); var leader = await cluster.LeaderAsync();
        await using var client = cluster.Client();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using var waiting = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
        var operation = Guid.NewGuid();
        var add = leader.Server.AddNodeAsync(cluster.Peer("n4"), cluster.Endpoint("n4"), operation, waiting.Token);
        await RuntimeCluster.EventuallyAsync(() => leader.Node.Status.Membership?.ActiveOperation?.OperationId == operation);
        for (var index = 1; index <= 32; index++) Assert.Equal(index, await client.AddLogEntryAsync(BitConverter.GetBytes(index), "during-add", cancellationToken: deadline.Token));
        waiting.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => add);
        Assert.Equal(operation, leader.Node.Status.Membership!.ActiveOperation!.OperationId);
        var cancelled = await leader.Node.CancelMembershipOperationAsync(operation, deadline.Token);
        Assert.Equal(MembershipOperationStage.Cancelled, cancelled.Stage);
        Assert.DoesNotContain("n4", leader.Node.Status.Configuration!.Learners);
        Assert.DoesNotContain("n4", (await client.RefreshTopologyAsync(deadline.Token)).Endpoints.Select(endpoint => endpoint.NodeId));
        Assert.Equal(33, await client.AddLogEntryAsync(new byte[] { 33 }, "during-add", cancellationToken: deadline.Token));
    }

    [Fact]
    public async Task Https_uses_host_authorization_and_normal_client_certificate_validation()
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var names = new SubjectAlternativeNameBuilder(); names.AddDnsName("localhost"); names.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(names.Build());
        using var created = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1));
        // Windows SChannel requires an OS-backed private key, matching the existing peer TLS fixture.
        using var certificate = new X509Certificate2(created.Export(X509ContentType.Pfx));
        await using var cluster = new ClientTestCluster(1, https: true) { Certificate = certificate, RequireToken = true };
        await cluster.StartAsync(); var secureLeader = await cluster.LeaderAsync();
        // Explicit private-PKI test pin; production default remains platform certificate validation.
        HttpClient Pinned() => new(new SocketsHttpHandler
        {
            SslOptions = new()
            {
                RemoteCertificateValidationCallback = (_, actual, _, errors) => actual is not null &&
                    (errors & SslPolicyErrors.RemoteCertificateNameMismatch) == 0 &&
                    CryptographicOperations.FixedTimeEquals(actual.GetCertHash(HashAlgorithmName.SHA256), certificate.GetCertHash(HashAlgorithmName.SHA256))
            }
        });
        using var deniedHttp = Pinned();
        await using (var denied = cluster.Client(http: deniedHttp))
        {
            var deniedError = await Assert.ThrowsAsync<ClusterRequestException>(() => denied.AddLogEntryAsync(new byte[] { 1 }));
            Assert.Equal("unauthorized", deniedError.Code);
        }
        using var allowedHttp = Pinned(); allowedHttp.DefaultRequestHeaders.Authorization = new("Bearer", "test-secret");
        await using (var allowed = cluster.Client(http: allowedHttp))
            Assert.Equal(1, await allowed.AddLogEntryAsync(new byte[] { 2 }, "secure"));
        await using var untrusted = new RaftClusterClient(new()
        { GroupId = cluster.GroupId, SeedEndpoints = [cluster.Endpoint("n1").Address], OperationTimeout = TimeSpan.FromSeconds(1) });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => untrusted.AddLogEntryAsync(new byte[] { 3 }, "secure"));
        Assert.Equal(1, secureLeader.Ledger.GetEntity("secure")!.LastMessageId);
    }

    [Fact]
    public void Large_message_limits_preserve_a_multiple_message_tcp_receive_window()
    {
        var configured = Raft.Cluster.Server.RaftClusterNode.ConfigureMessageLimits(new() { NodeId = "n1", Members = ["n1"] });
        Assert.True(configured.MaxInFlightBytes / configured.MaxFrameBytes >= configured.MaxInFlightMessages);
        Assert.InRange(configured.MaxCommandBytes, ClusterLimits.MaxCommandBytes, 16 * 1024 * 1024);
    }
    private sealed class MalformedBatch(bool denyPublication = false) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            if (request.RequestUri!.AbsolutePath.EndsWith("/topology", StringComparison.Ordinal))
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                { Content = new StringContent(JsonSerializer.Serialize(new ClusterTopology("default", "n1", 1, 0, 0, true, [new("n1", new("http://localhost:1234/"))]))) });
            if (denyPublication) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.Unauthorized) { Content = new ByteArrayContent([]) });
            var bytes = new byte[9];
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(bytes, ClusterLimits.BatchMagic);
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(4), int.MaxValue);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(bytes) });
        }
    }

    [Fact]
    public async Task Malformed_batch_count_is_rejected_before_payload_allocation()
    {
        using var http = new HttpClient(new MalformedBatch());
        await using var client = new RaftClusterClient(new() { SeedEndpoints = [new("http://localhost:1234/")], HttpClient = http });
        await Assert.ThrowsAsync<InvalidDataException>(() => client.ReadBatchAsync("events", 0));
    }
    [Fact]
    public async Task Authorization_expiry_after_discovery_has_a_clear_error_with_an_empty_response_body()
    {
        using var http = new HttpClient(new MalformedBatch(denyPublication: true));
        await using var client = new RaftClusterClient(new() { SeedEndpoints = [new("http://localhost:1234/")], HttpClient = http });
        var error = await Assert.ThrowsAsync<ClusterRequestException>(() => client.AddLogEntryAsync(new byte[] { 1 }));
        Assert.Equal("unauthorized", error.Code);
    }
}
