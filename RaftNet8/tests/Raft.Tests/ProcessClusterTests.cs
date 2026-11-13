using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Raft.Demo;
using Raft.Storage;

namespace Raft.Tests;

[CollectionDefinition("OS process clusters", DisableParallelization = true)]
public sealed class ProcessClusterCollection;

[Collection("OS process clusters")]
public sealed class ProcessClusterTests
{
    [Theory]
    [InlineData(3, DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(5, DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(3, DBreezeLogLayout.UnifiedTableV3)]
    [InlineData(5, DBreezeLogLayout.UnifiedTableV3)]
    public async Task Real_processes_survive_killed_leader_snapshot_rolling_restart_and_membership(int voters, DBreezeLogLayout layout)
    {
        using var directory = new TestDirectory();
        await using var cluster = new ProcessCluster(directory.Path, voters, layout);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var token = deadline.Token;
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync(token);
        var first = await leader.SuccessAsync("set", "counter", "before-kill", token: token);
        await cluster.WaitAsync(status => status.LastApplied >= first.Proposal!.Value.Index, cluster.Voters, token);

        // This bypasses all finally/Dispose paths in the Raft process, including DBreeze shutdown.
        var killed = leader.Id;
        await cluster.StopAsync(killed, kill: true);
        leader = await cluster.LeaderAsync(token);
        for (var i = 0; i < 32; i++)
            await leader.SuccessAsync("set", "counter", $"after-kill-{i}-" + new string('x', 2048), token: token);
        var snapshot = (await leader.SuccessAsync("snapshot", token: token)).Snapshot!;
        Assert.True(snapshot.LastIncludedIndex > first.Proposal!.Value.Index + 8);
        await cluster.StartAsync(killed);
        await cluster.WaitAsync(s => s.SnapshotIndex >= snapshot.LastIncludedIndex, [killed], token);
        var expected = "after-kill-31-" + new string('x', 2048);
        await cluster.AssertValueAsync(expected, cluster.Voters, token);

        // Every voter is restarted from its own files; a leader hands over before its rolling stop.
        foreach (var id in cluster.Voters)
        {
            leader = await cluster.LeaderAsync(token);
            if (leader.Id == id)
            {
                var target = cluster.Voters.First(peer => peer != id);
                await cluster.WaitCaughtUpAsync(leader, target, token);
                await leader.SuccessAsync("transfer", target: target, token: token);
            }
            await cluster.StopAsync(id, kill: false);
            leader = await cluster.LeaderAsync(token);
            await leader.SuccessAsync("set", "counter", "rolling-" + id, token: token);
            await cluster.StartAsync(id);
            await cluster.AssertValueAsync("rolling-" + id, cluster.Voters, token);
        }

        var spare = "n" + (voters + 1);
        await cluster.StartAsync(spare);
        leader = await cluster.LeaderAsync(token);
        await leader.SuccessAsync("add", target: spare, token: token);
        await cluster.WaitCaughtUpAsync(leader, spare, token);
        await leader.SuccessAsync("promote", target: spare, token: token);
        await cluster.WaitCaughtUpAsync(leader, spare, token);
        await leader.SuccessAsync("transfer", target: spare, token: token);
        leader = await cluster.LeaderAsync(token, spare);
        await leader.SuccessAsync("remove", target: "n1", token: token);
        await cluster.WaitAsync(s => s.Role == RaftRole.Removed, ["n1"], token);
        await cluster.StopAsync("n1", kill: true);
        await cluster.StartAsync("n1");
        await cluster.WaitAsync(s => s.Role == RaftRole.Removed && !s.Voters.Contains("n1"), ["n1"], token);

        leader = await cluster.LeaderAsync(token, spare);
        var final = await leader.SuccessAsync("set", "counter", "final", token: token);
        var active = cluster.Voters.Where(id => id != "n1").Append(spare).ToArray();
        await cluster.WaitAsync(s => s.LastApplied >= final.Proposal!.Value.Index && s.Voters.Length == voters &&
            s.Voters.Contains(spare) && !s.Voters.Contains("n1") && s.Learners.Length == 0, active, token);
        Assert.Equal("final", (await leader.SuccessAsync("get", "counter", token: token)).Value);
        await cluster.AssertValueAsync("final", active, token);
    }

    private sealed class ProcessCluster : IAsyncDisposable
    {
        private readonly string _root;
        private readonly string _endpoints;
        private readonly DBreezeLogLayout _layout;
        private readonly Dictionary<string, Worker> _workers = new(StringComparer.Ordinal);
        public string[] Voters { get; }

        public ProcessCluster(string root, int voters, DBreezeLogLayout layout)
        {
            _root = root;
            _layout = layout;
            Voters = Enumerable.Range(1, voters).Select(i => "n" + i).ToArray();
            var listeners = Enumerable.Range(0, voters + 1).Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
            try
            {
                foreach (var listener in listeners) listener.Start();
                _endpoints = string.Join(',', listeners.Select((listener, i) => $"n{i + 1}={listener.LocalEndpoint}"));
            }
            finally { foreach (var listener in listeners) listener.Stop(); }
        }

        public async Task StartAsync()
        {
            foreach (var id in Voters) await StartAsync(id);
        }
        public async Task StartAsync(string id)
        {
            var worker = new Worker(id, Path.Combine(_root, id), string.Join(',', Voters), _endpoints, _layout);
            _workers.Add(id, worker);
            var ready = await worker.ReadAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(20));
            Assert.True(ready.Success && ready.Id == 0, ready.Error);
        }
        public async Task StopAsync(string id, bool kill)
        {
            var worker = _workers[id];
            if (!kill) await worker.SuccessAsync("quit");
            await worker.StopAsync(kill);
            _workers.Remove(id);
        }
        public async Task<Worker> LeaderAsync(CancellationToken token, string? expected = null)
        {
            while (true)
            {
                token.ThrowIfCancellationRequested();
                foreach (var worker in _workers.Values)
                {
                    var status = (await worker.SuccessAsync("status", token: token)).Status!;
                    Assert.Null(status.Fault);
                    if (status.Role != RaftRole.Leader || (expected is not null && worker.Id != expected)) continue;
                    // A role transition alone precedes committing the election no-op. Require a fresh fence.
                    var fence = await worker.SendAsync("get", "counter", token: token);
                    if (fence.Success) return worker;
                }
                await Task.Delay(40, token);
            }
        }
        public async Task WaitAsync(Func<WorkerStatus, bool> predicate, IEnumerable<string> ids, CancellationToken token)
        {
            while (true)
            {
                token.ThrowIfCancellationRequested();
                var statuses = await Task.WhenAll(ids.Select(id => _workers[id].SuccessAsync("status", token: token)));
                foreach (var status in statuses) Assert.Null(status.Status!.Fault);
                if (statuses.All(status => predicate(status.Status!))) return;
                await Task.Delay(40, token);
            }
        }
        public Task WaitCaughtUpAsync(Worker leader, string peer, CancellationToken token) =>
            WaitAsync(s => s.Peers.Any(p => p.NodeId == peer && p.MatchIndex >= s.LastLogIndex), [leader.Id], token);
        public async Task AssertValueAsync(string expected, IEnumerable<string> ids, CancellationToken token)
        {
            while (true)
            {
                token.ThrowIfCancellationRequested();
                var values = await Task.WhenAll(ids.Select(id => _workers[id].SuccessAsync("stale", "counter", token: token)));
                if (values.All(result => result.Value == expected)) return;
                await Task.Delay(40, token);
            }
        }
        public async ValueTask DisposeAsync()
        {
            var errors = new List<Exception>();
            foreach (var worker in _workers.Values)
            {
                try { await worker.StopAsync(kill: true); }
                catch (Exception error) { errors.Add(error); }
            }
            _workers.Clear();
            if (errors.Count > 0) throw new AggregateException(errors);
        }
    }

    private sealed class Worker
    {
        private readonly Process _process;
        private readonly Task<string> _stderr;
        private long _request;
        private bool _stopped;
        public string Id { get; }
        public Worker(string id, string data, string members, string endpoints, DBreezeLogLayout layout)
        {
            Id = id;
            var configuration = new DirectoryInfo(AppContext.BaseDirectory).Parent!.Name;
            var solution = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../../"));
            var assembly = Path.Combine(solution, "HumanTest", "Raft.Demo", "bin", configuration, "net8.0", "Raft.Demo.dll");
            var start = new ProcessStartInfo("dotnet") { UseShellExecute = false, CreateNoWindow = true,
                RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true };
            ChildProcessSettings.IsolateFaultHarness(start);
            foreach (var argument in new[] { assembly, "--worker", "--id", id, "--data", data,
                "--members", members, "--endpoints", endpoints, "--log-layout", layout == DBreezeLogLayout.UnifiedTableV3 ? "v3" : "v2" }) start.ArgumentList.Add(argument);
            _process = Process.Start(start) ?? throw new InvalidOperationException("Cannot launch Raft worker.");
            _stderr = _process.StandardError.ReadToEndAsync();
        }
        public async Task<WorkerResponse> SendAsync(string action, string? key = null, string? value = null,
            string? target = null, CancellationToken token = default)
        {
            var command = new WorkerCommand(++_request, action, key, value, target);
            await _process.StandardInput.WriteLineAsync(JsonSerializer.Serialize(command, WorkerHost.Json).AsMemory(), token);
            await _process.StandardInput.FlushAsync(token);
            var response = await ReadAsync(token);
            Assert.Equal(command.Id, response.Id);
            return response;
        }
        public async Task<WorkerResponse> SuccessAsync(string action, string? key = null, string? value = null,
            string? target = null, CancellationToken token = default)
        {
            var response = await SendAsync(action, key, value, target, token);
            Assert.True(response.Success, $"{Id}/{action}: {response.Error}");
            return response;
        }
        public async Task<WorkerResponse> ReadAsync(CancellationToken token)
        {
            var line = await _process.StandardOutput.ReadLineAsync(token);
            if (line is null) throw new InvalidOperationException($"{Id} exited ({_process.ExitCode}): {await _stderr}");
            return JsonSerializer.Deserialize<WorkerResponse>(line, WorkerHost.Json) ?? throw new InvalidDataException(line);
        }
        public async Task StopAsync(bool kill)
        {
            if (_stopped) return;
            try
            {
                if (kill && !_process.HasExited) _process.Kill(entireProcessTree: true);
                await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
                if (!kill) Assert.Equal(0, _process.ExitCode);
                var errors = await _stderr;
                Assert.True(string.IsNullOrEmpty(errors), $"{Id}: {errors}");
            }
            finally
            {
                if (!_process.HasExited)
                {
                    _process.Kill(entireProcessTree: true);
                    await _process.WaitForExitAsync();
                }
                _process.Dispose();
                _stopped = true;
            }
        }
    }
}
