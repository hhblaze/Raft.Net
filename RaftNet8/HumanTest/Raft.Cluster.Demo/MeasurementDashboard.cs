using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;

namespace Raft.Cluster.Demo;

internal sealed class MeasurementDashboard : IAsyncDisposable
{
    private readonly Process _process;
    private readonly Task<string> _stdout, _stderr;
    private readonly string _directory;
    private MeasurementDashboard(Process process, string directory)
    { _process = process; _directory = directory; _stdout = process.StandardOutput.ReadToEndAsync(); _stderr = process.StandardError.ReadToEndAsync(); }
    public static async Task<MeasurementDashboard> StartAsync(string assembly, string directory, string group, string[] endpoints, string secret, CancellationToken token)
    {
        var connection = Path.Combine(directory, "dashboard-connection.json");
        await File.WriteAllBytesAsync(connection, JsonSerializer.SerializeToUtf8Bytes(new { Name = "measurement", GroupId = group, ManagementEndpoints = endpoints, Credential = secret }), token);
        var state = Path.Combine(directory, "dashboard-state");
        using (var import = Start(assembly, ["import-cluster", "--connection-file", connection, "--Dashboard:DataDirectory", state]))
        {
            var output = import.StandardOutput.ReadToEndAsync(token);
            var errors = import.StandardError.ReadToEndAsync(token);
            await import.WaitForExitAsync(token);
            await File.WriteAllTextAsync(Path.Combine(directory, "dashboard-import.log"), await output + await errors, token);
            if (import.ExitCode != 0) throw new IOException("Dashboard profile import failed; inspect the retained import log.");
        }
        var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port; listener.Stop();
        var url = "http://127.0.0.1:" + port;
        var process = Start(assembly, ["--urls", url, "--Dashboard:DataDirectory", state, "--control-stdin", "true"]);
        var host = new MeasurementDashboard(process, directory);
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromMilliseconds(500) };
            using var ready = CancellationTokenSource.CreateLinkedTokenSource(token);
            ready.CancelAfter(TimeSpan.FromSeconds(30));
            while (true)
            {
                ready.Token.ThrowIfCancellationRequested();
                if (process.HasExited) throw new IOException("Dashboard exited before collecting live node observations.");
                try
                {
                    using var response = await http.GetAsync(url + "/health/ready", ready.Token);
                    if (response.IsSuccessStatusCode && response.Content.Headers.ContentType?.MediaType == "application/json")
                    {
                        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ready.Token));
                        if (body.RootElement.TryGetProperty("ready", out var observed) && observed.GetBoolean()) return host;
                    }
                }
                catch (HttpRequestException) { }
                catch (OperationCanceledException) when (!ready.IsCancellationRequested) { }
                await Task.Delay(50, ready.Token);
            }
        }
        catch { await host.DisposeAsync(); throw; }
    }
    private static Process Start(string assembly, IEnumerable<string> arguments)
    {
        if (!File.Exists(assembly)) throw new FileNotFoundException("Publish the dashboard before measuring it.", assembly);
        var start = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false, CreateNoWindow = true, WindowStyle = ProcessWindowStyle.Hidden,
            WorkingDirectory = Path.GetDirectoryName(Path.GetFullPath(assembly))!,
            RedirectStandardOutput = true, RedirectStandardError = true, RedirectStandardInput = true
        };
        start.ArgumentList.Add(assembly);
        foreach (var argument in arguments) start.ArgumentList.Add(argument);
        return Process.Start(start) ?? throw new IOException("Cannot start the owned dashboard measurement process.");
    }
    public async ValueTask DisposeAsync()
    {
        if (!_process.HasExited)
        {
            try
            {
                await _process.StandardInput.WriteLineAsync("quit");
                await _process.StandardInput.FlushAsync();
                await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(7));
            }
            catch (Exception error) when (error is IOException or InvalidOperationException or TimeoutException)
            { if (!_process.HasExited) _process.Kill(entireProcessTree: true); }
        }
        await _process.WaitForExitAsync();
        await File.WriteAllTextAsync(Path.Combine(_directory, "dashboard.log"), await _stdout);
        await File.WriteAllTextAsync(Path.Combine(_directory, "dashboard.err"), await _stderr);
        _process.Dispose();
    }
}
