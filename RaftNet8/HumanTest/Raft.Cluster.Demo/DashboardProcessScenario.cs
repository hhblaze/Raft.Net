using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text.Json;
using Raft.Cluster.Client;

namespace Raft.Cluster.Demo;

/// <summary>Real independent processes, not a simulation. Owns only its generated experiment directory and children.</summary>
public static class DashboardProcessScenario
{
    public static async Task RunAsync(string parent, int voters, bool secure, CancellationToken token)
    {
        if (voters is not (3 or 5)) throw new ArgumentException("Use3 or5 voters.");
        var root = Path.Combine(parent, "dashboard-process-" + (secure ? "tls-" : "http-") + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var group = "admin-process-" + Guid.NewGuid().ToString("N")[..12];
        var secret = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
        await File.WriteAllTextAsync(Path.Combine(root, "cluster-bootstrap.secret"), secret, token);
        var pki = Path.Combine(root, "pki");
        if (secure) DemoSecurity.Generate(pki);
        using var security = secure ? new DemoSecurity(pki) : null;
        using var http = security?.HttpClient();
        var peerPort = Ports(); var clientPort = Ports(); while (Math.Abs(clientPort - peerPort) < 9) clientPort = Ports();
        var adminPort = Ports(); while (Math.Abs(adminPort - peerPort) < 9 || Math.Abs(adminPort - clientPort) < 9) adminPort = Ports();
        var scheme = secure ? "https" : "http";
        var processes = new Dictionary<string, (Process Process, Task<string> Out, Task<string> Error)>(StringComparer.Ordinal);
        string? credential = null;
        Uri Endpoint(int port, int index) => new($"{scheme}://127.0.0.1:{port + index - 1}/");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromMinutes(4)); token = deadline.Token;
        await using var admin = new RaftClusterAdminClient(new() { GroupId = group, SeedEndpoints = Enumerable.Range(1, voters).Select(index => Endpoint(adminPort, index)).ToArray(),
            AccessTokenProvider = _ => ValueTask.FromResult(credential), HttpClient = http, OperationTimeout = TimeSpan.FromSeconds(45) });
        try
        {
            for (var index = 1; index <= voters; index++) Start(index);
            var bootstrapId = Guid.NewGuid();
            while (credential is null)
            {
                token.ThrowIfCancellationRequested();
                try { credential = (await admin.BootstrapAsync(secret, bootstrapId, token)).Secret ?? throw new InvalidDataException("Lost bootstrap secret."); }
                catch (Exception error) when (error is HttpRequestException or OperationCanceledException && !token.IsCancellationRequested) { await Task.Delay(100, token); }
            }
            await admin.RefreshDirectoryAsync(token);
            var originalPids = processes.ToDictionary(pair => pair.Key, pair => pair.Value.Process.Id);
            await admin.CreateEntityAsync("events", EntityMode.Standard, Guid.NewGuid(), token);
            var grant = new TokenGrant("process publisher", ClusterPermission.Discover | ClusterPermission.Publish | ClusterPermission.ReadMetadata | ClusterPermission.ReadPayload,
                ["events"], DateTimeOffset.UtcNow.AddHours(1));
            var publisher = await admin.IssueClientTokenAsync(grant, Guid.NewGuid(), token);
            await using var client = new RaftClusterClient(new() { GroupId = group,
                SeedEndpoints = Enumerable.Range(1, voters).Select(index => Endpoint(clientPort, index)).ToArray(), HttpClient = http,
                AccessTokenProvider = _ => ValueTask.FromResult(publisher.Secret) });
            await client.RefreshTopologyAsync(token);
            long acknowledged = 0;
            using var publishing = CancellationTokenSource.CreateLinkedTokenSource(token);
            var load = Task.Run(async () =>
            {
                try { while (true) { await client.AddLogEntryAsync(new byte[128], "events", cancellationToken: publishing.Token); Interlocked.Increment(ref acknowledged); } }
                catch (OperationCanceledException) when (publishing.IsCancellationRequested) { }
            }, token);
            try
            {
                for (var index = voters + 1; index <= voters + 2; index++)
                {
                    Start(index); await Ready(index);
                    await Operation(new("add", Add: new(new("n" + index, "127.0.0.1", peerPort + index - 1),
                        new("n" + index, Endpoint(clientPort, index)), new("n" + index, Endpoint(adminPort, index)))));
                }
                foreach (var (id, pid) in originalPids) if (processes[id].Process.Id != pid || processes[id].Process.HasExited) throw new InvalidDataException("An original process restarted during Add.");
                var leader = await admin.GetStatusAsync(token);
                await Operation(new("remove", leader.NodeId));
                if ((await admin.GetStatusAsync(token)).NodeId == leader.NodeId) throw new InvalidDataException("Removed leader still leads.");
                await Operation(new("snapshot"));
                var joining = voters + 3; Start(joining); await Ready(joining);
                await Operation(new("add", Add: new(new("n" + joining, "127.0.0.1", peerPort + joining - 1),
                    new("n" + joining, Endpoint(clientPort, joining)), new("n" + joining, Endpoint(adminPort, joining)))));
                while (Interlocked.Read(ref acknowledged) < 32) await Task.Delay(25, token);
            }
            finally { publishing.Cancel(); await load; }
            // An explicitly issued publisher cannot use management. Revocation then blocks its next publication.
            await admin.RevokeClientTokenAsync(publisher.Token.TokenId, Guid.NewGuid(), token);
            try { await client.AddLogEntryAsync(new byte[] { 1 }, "events", cancellationToken: token); throw new InvalidDataException("Revoked publisher was accepted."); }
            catch (ClusterRequestException error) when (error.Code is "unauthorized" or "forbidden") { }
            var result = new { Passed = true, Voters = voters, Secure = secure, Root = root, Acknowledged = acknowledged,
                OriginalPids = originalPids, FinalPids = processes.ToDictionary(pair => pair.Key, pair => pair.Value.Process.Id),
                FinalStatus = await admin.GetStatusAsync(token), Entities = await admin.GetEntitiesAsync(token) };
            await File.WriteAllBytesAsync(Path.Combine(root, "result.json"), JsonSerializer.SerializeToUtf8Bytes(result, new JsonSerializerOptions { WriteIndented = true }), token);
            Console.WriteLine($"PROCESS SCENARIO PASS voters={voters} secure={secure} acknowledged={acknowledged}; {root}");
        }
        finally
        {
            foreach (var (id, child) in processes)
            {
                if (!child.Process.HasExited)
                {
                    try
                    {
                        await child.Process.StandardInput.WriteLineAsync("quit");
                        await child.Process.StandardInput.FlushAsync();
                        await child.Process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
                    }
                    catch (Exception error) when (error is TimeoutException or IOException or InvalidOperationException)
                    { if (!child.Process.HasExited) child.Process.Kill(entireProcessTree: true); }
                }
                await child.Process.WaitForExitAsync();
                await File.WriteAllTextAsync(Path.Combine(root, id + ".log"), await child.Out);
                await File.WriteAllTextAsync(Path.Combine(root, id + ".err"), await child.Error);
                child.Process.Dispose();
            }
        }
        async Task Operation(AdminOperationRequest request)
        {
            var id = Guid.NewGuid();
            await admin.StartOperationAsync(request, id, token);
            while (true)
            {
                var status = await admin.GetOperationAsync(id, token);
                if (status.Operation is { State: "Completed" }) return;
                if (status.Operation is { State: "Failed" or "Cancelled" } failed) throw new InvalidDataException(failed.Error ?? failed.State);
                await Task.Delay(50, token);
            }
        }
        async Task Ready(int index)
        {
            using var readiness = security?.HttpClient() ?? new HttpClient(new SocketsHttpHandler());
            readiness.Timeout = TimeSpan.FromSeconds(2);
            while (true)
            {
                token.ThrowIfCancellationRequested();
                try
                {
                    using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(Endpoint(adminPort, index), Administration.ApiPath + "/capabilities"));
                    request.Headers.Add("X-Raft-GroupId", group);
                    using var response = await readiness.SendAsync(request, token);
                    if (response.IsSuccessStatusCode) return;
                }
                catch (Exception error) when (error is HttpRequestException or OperationCanceledException && !token.IsCancellationRequested) { }
                await Task.Delay(100, token);
            }
        }
        void Start(int index)
        {
            var id = "n" + index;
            var start = new ProcessStartInfo("dotnet") { UseShellExecute = false, CreateNoWindow = true, WindowStyle = ProcessWindowStyle.Hidden,
                RedirectStandardOutput = true, RedirectStandardError = true, RedirectStandardInput = true };
            foreach (var argument in new[] { typeof(DemoCluster).Assembly.Location, "node", id, "--daemon", "--control-stdin", "--management", "true", "--tokens", "true", "--voters", voters.ToString(),
                "--root", root, "--group", group, "--peer-base-port", peerPort.ToString(), "--client-base-port", clientPort.ToString(), "--management-base-port", adminPort.ToString() }) start.ArgumentList.Add(argument);
            if (secure) { start.ArgumentList.Add("--pki"); start.ArgumentList.Add(pki); }
            var process = Process.Start(start) ?? throw new IOException("Node process did not start.");
            processes.Add(id, (process, process.StandardOutput.ReadToEndAsync(), process.StandardError.ReadToEndAsync()));
        }
    }
    private static int Ports()
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            var port = RandomNumberGenerator.GetInt32(10000, 60000);
            var listeners = Enumerable.Range(port, 9).Select(value => new TcpListener(IPAddress.Loopback, value)).ToArray();
            try { foreach (var listener in listeners) listener.Start(); return port; }
            catch (SocketException) { }
            finally { foreach (var listener in listeners) listener.Stop(); }
        }
        throw new IOException("Could not reserve9 consecutive loopback ports.");
    }
}
