using System.Net;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.DependencyInjection;
using RaftClusterDashboard;

namespace Raft.Tests;

[Collection("OS process clusters")]
public sealed class DashboardIdentityTests
{
    [Theory]
    [InlineData("onlylowercasepassword")]
    [InlineData("")]
    public async Task Bootstrap_password_rejection_has_a_distinct_exit_code_and_allows_retry_in_the_same_directory(string weakPassword)
    {
        using var directory = new ClusterTestDirectory();
        var previousPassword = Environment.GetEnvironmentVariable("RAFT_DASHBOARD_BOOTSTRAP_PASSWORD");
        var previousError = Console.Error;
        var previousInput = Console.In;
        using var error = new StringWriter();
        var arguments = new[] { "bootstrap-admin", "--urls", "http://127.0.0.1:0", "--Dashboard:DataDirectory", directory.Path,
            "--Logging:LogLevel:Default", "Error" };
        try
        {
            Environment.SetEnvironmentVariable("RAFT_DASHBOARD_BOOTSTRAP_PASSWORD", weakPassword);
            Console.SetIn(new StringReader(""));
            Console.SetError(error);
            Assert.Equal(2, await RaftClusterDashboard.Program.Main(arguments));
            Assert.Contains("uppercase", error.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("digit", error.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("non alphanumeric", error.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("onlylowercasepassword", error.ToString(), StringComparison.Ordinal);
            await using (var empty = DashboardHost.Create(arguments[1..]))
                Assert.Empty(empty.Services.GetRequiredService<DashboardStore>().List<DashboardUser>(1, 1));

            Environment.SetEnvironmentVariable("RAFT_DASHBOARD_BOOTSTRAP_PASSWORD", Password);
            Assert.Equal(0, await RaftClusterDashboard.Program.Main(arguments));
            await using var created = DashboardHost.Create(arguments[1..]);
            using var scope = created.Services.CreateScope();
            var users = scope.ServiceProvider.GetRequiredService<UserManager<DashboardUser>>();
            var admin = await users.FindByNameAsync("admin");
            Assert.NotNull(admin);
            Assert.True(await users.CheckPasswordAsync(admin, Password));
        }
        finally
        {
            Console.SetError(previousError);
            Console.SetIn(previousInput);
            Environment.SetEnvironmentVariable("RAFT_DASHBOARD_BOOTSTRAP_PASSWORD", previousPassword);
        }
    }

    [Fact]
    public async Task One_collector_isolates_two_clusters_with_the_same_node_and_entity_names_and_filters_viewer_access()
    {
        await using var first = new ClientTestCluster(1) { ManagementEnabled = true, NativeTokens = true };
        await first.StartAsync();
        await first.LeaderAsync();
        await using var second = new ClientTestCluster(1) { ManagementEnabled = true, NativeTokens = true };
        await second.StartAsync();
        await second.LeaderAsync();
        await using var firstBootstrap = first.Admin();
        await using var secondBootstrap = second.Admin();
        var firstToken = await firstBootstrap.BootstrapAsync(first.BootstrapSecret, Guid.NewGuid());
        var secondToken = await secondBootstrap.BootstrapAsync(second.BootstrapSecret, Guid.NewGuid());
        await using var firstClient = first.AuthenticatedClient(firstToken.Secret!);
        await using var secondClient = second.AuthenticatedClient(secondToken.Secret!);
        await firstClient.AddLogEntryAsync(new byte[] { 1 }, "events");
        await secondClient.AddLogEntryAsync(new byte[] { 2 }, "events");
        await secondClient.AddLogEntryAsync(new byte[] { 3 }, "events");

        using var directory = new ClusterTestDirectory();
        var (app, http) = await Start(directory);
        await using var application = app;
        using var client = http;
        var store = app.Services.GetRequiredService<DashboardStore>();
        var firstId = Guid.NewGuid().ToString("N");
        var secondId = Guid.NewGuid().ToString("N");
        store.SaveProfile(new(firstId, "first", first.GroupId, [first.ManagementEndpoint("n1").Address.AbsoluteUri], store.Protect(firstToken.Secret!)));
        store.SaveProfile(new(secondId, "second", second.GroupId, [second.ManagementEndpoint("n1").Address.AbsoluteUri], store.Protect(secondToken.Secret!)));
        var monitor = app.Services.GetRequiredService<ClusterMonitor>();
        await RuntimeCluster.EventuallyAsync(() => monitor.Get(firstId) is { Error: null, Status: not null } one &&
            monitor.Get(secondId) is { Error: null, Status: not null } two &&
            one.Entities.SingleOrDefault()?.Entity.LastMessageId == 1 && two.Entities.SingleOrDefault()?.Entity.LastMessageId == 2, 20_000);
        Assert.Equal(first.GroupId, monitor.Get(firstId)!.Status!.GroupId);
        Assert.Equal(second.GroupId, monitor.Get(secondId)!.Status!.GroupId);
        Assert.Equal("n1", monitor.Get(firstId)!.Status!.NodeId);
        Assert.Equal("n1", monitor.Get(secondId)!.Status!.NodeId);

        await Login(http, "admin");
        var observations = await http.GetStringAsync("/api/admin/observations");
        using (var all = JsonDocument.Parse(observations)) Assert.Equal(2, all.RootElement.GetArrayLength());
        Assert.False(observations.Contains(firstToken.Secret!, StringComparison.Ordinal));
        Assert.False(observations.Contains(secondToken.Secret!, StringComparison.Ordinal));
        string viewerId;
        using (var scope = app.Services.CreateScope())
            viewerId = (await scope.ServiceProvider.GetRequiredService<UserManager<DashboardUser>>().FindByNameAsync("viewer"))!.Id;
        using var assigned = await Post(http, "/api/admin/users", new
            { Id = viewerId, Username = "viewer", Role = "Viewer", ClusterIds = new[] { firstId }, CanReadPayload = false }, await Csrf(http));
        Assert.Equal(HttpStatusCode.OK, assigned.StatusCode);
        using var logout = await Post(http, "/api/account/logout", new { }, await Csrf(http));
        Assert.Equal(HttpStatusCode.OK, logout.StatusCode);
        await Login(http, "viewer");
        using var visible = JsonDocument.Parse(await http.GetStringAsync("/api/admin/observations"));
        Assert.Equal(firstId, Assert.Single(visible.RootElement.EnumerateArray()).GetProperty("id").GetString());
        using var forbidden = await http.GetAsync("/api/admin/clusters/" + secondId + "/configuration");
        Assert.Equal(HttpStatusCode.Forbidden, forbidden.StatusCode);
    }

    [Fact]
    public async Task Default_history_budget_covers_a_week_for_every_default_cluster()
    {
        var defaults = new DashboardSettings();
        var minuteRows = defaults.MaxClusters * defaults.HistoryDays * 24 * 60;
        Assert.True(defaults.MaxHistoryRows >= minuteRows);
        using var directory = new ClusterTestDirectory();
        var shippedRoot = Path.GetDirectoryName(typeof(DashboardHost).Assembly.Location)!;
        Assert.True(File.Exists(Path.Combine(shippedRoot, "appsettings.json")));
        await using var app = DashboardHost.Create(["--contentRoot", shippedRoot, "--urls", "http://127.0.0.1:0", "--Dashboard:DataDirectory", directory.Path]);
        var configured = app.Services.GetRequiredService<DashboardSettings>();
        Assert.True(configured.MaxHistoryRows >= configured.MaxClusters * configured.HistoryDays * 24 * 60);
    }

    [Fact]
    public async Task Bounded_history_page_keeps_the_latest_points_when_retention_exceeds_the_display_window()
    {
        using var directory = new ClusterTestDirectory();
        await using var app = DashboardHost.Create(["--urls", "http://127.0.0.1:0", "--Dashboard:DataDirectory", directory.Path]);
        var store = app.Services.GetRequiredService<DashboardStore>();
        var start = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        for (var day = 0; day < 20; day++)
            store.SaveHistory("cluster", new(start.AddDays(day), day, null, null, null, null), 30, 100);
        var recent = store.History("cluster", start, maximum: 7);
        Assert.Equal(Enumerable.Range(13, 7).Select(day => start.AddDays(day)), recent.Select(point => point.At));
        Assert.Equal(new[] { start.AddDays(18), start.AddDays(19) }, store.History("cluster", start.AddDays(18), maximum: 7).Select(point => point.At));
    }

    [Fact]
    public void Browser_message_IDs_and_quotas_preserve_Int64_precision()
    {
        const long exact = 9_007_199_254_740_993;
        var json = JsonSerializer.Serialize(new Raft.Cluster.EntityInfo("events", Raft.Cluster.EntityMode.Standard, 1, exact), BrowserJson.Options);
        using var document = JsonDocument.Parse(json);
        Assert.Equal(exact.ToString(), document.RootElement.GetProperty("lastMessageId").GetString());
        Assert.Equal(exact, JsonSerializer.Deserialize<Raft.Cluster.EntitySettings>("{\"maxRetainedBytes\":\"9007199254740993\"}", BrowserJson.Options)!.MaxRetainedBytes);
    }
    private const string Password = "Test-Only-Strong-Password-835!";
    [Fact]
    public async Task Secure_deployment_rejects_stored_plaintext_profiles_before_connecting()
    {
        using var directory = new ClusterTestDirectory();
        await using (var local = DashboardHost.Create(["--urls", "http://127.0.0.1:0", "--Dashboard:DataDirectory", directory.Path]))
        {
            var store = local.Services.GetRequiredService<DashboardStore>();
            store.SaveProfile(new(Guid.NewGuid().ToString("N"), "old", "group", ["http://127.0.0.1:7201/"], store.Protect("test-only")));
        }
        Assert.Throws<ArgumentException>(() => DashboardHost.Create(["--urls", "https://127.0.0.1:0", "--Dashboard:SecureProfile", "true", "--Dashboard:DataDirectory", directory.Path]));
    }
    [Fact]
    public async Task Account_grants_and_session_revocation_share_one_durable_update()
    {
        using var directory = new ClusterTestDirectory();
        var (app, http) = await Start(directory);
        await using var application = app;
        using var client = http;
        await Login(http, "viewer");
        using var scope = app.Services.CreateScope();
        var users = scope.ServiceProvider.GetRequiredService<UserManager<DashboardUser>>();
        var user = (await users.FindByNameAsync("viewer"))!;
        var oldStamp = user.SecurityStamp;
        user.CanReadPayload = true;
        Assert.True((await users.UpdateAsync(user)).Succeeded);
        Assert.NotEqual(oldStamp, user.SecurityStamp);
        using var invalidated = await http.GetAsync("/api/admin/clusters");
        Assert.Equal(HttpStatusCode.Unauthorized, invalidated.StatusCode);
    }
    private static async Task<(WebApplication App, HttpClient Http)> Start(ClusterTestDirectory directory, bool mfa = false)
    {
        var app = DashboardHost.Create(["--urls", "http://127.0.0.1:0", "--Dashboard:DataDirectory", directory.Path,
            "--Dashboard:RequirePrivilegedMfa", mfa.ToString(), "--Logging:LogLevel:Default", "Error"]);
        using (var scope = app.Services.CreateScope())
        {
            var manager = scope.ServiceProvider.GetRequiredService<UserManager<DashboardUser>>();
            Assert.True((await manager.CreateAsync(new() { UserName = "admin", Role = "Admin", LockoutEnabled = true }, Password)).Succeeded);
            Assert.True((await manager.CreateAsync(new() { UserName = "viewer", Role = "Viewer", LockoutEnabled = true }, Password)).Succeeded);
        }
        await app.StartAsync();
        var http = new HttpClient(new HttpClientHandler { CookieContainer = new(), AllowAutoRedirect = false }) { BaseAddress = new(app.Urls.Single()) };
        return (app, http);
    }
    private static async Task<string> Csrf(HttpClient http)
    {
        using var document = JsonDocument.Parse(await http.GetStringAsync("/api/session"));
        return document.RootElement.GetProperty("csrf").GetString()!;
    }
    private static async Task<HttpResponseMessage> Post(HttpClient http, string path, object body, string? csrf = null)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, path) { Content = JsonContent.Create(body) };
        if (csrf is not null) request.Headers.Add("X-Raft-CSRF", csrf);
        return await http.SendAsync(request);
    }
    private static async Task Login(HttpClient http, string username)
    {
        using var result = await Post(http, "/api/login", new { Username = username, Password }, await Csrf(http));
        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
    }

    [Fact]
    public async Task Cookies_require_csrf_and_authenticated_roles_are_checked_on_backend()
    {
        using var directory = new ClusterTestDirectory();
        var (app, http) = await Start(directory);
        await using var application = app;
        using var client = http;
        using var noCsrf = await Post(http, "/api/login", new { Username = "admin", Password });
        Assert.Equal(HttpStatusCode.BadRequest, noCsrf.StatusCode);
        await Login(http, "viewer");
        using var forbidden = await http.GetAsync("/api/admin/users");
        Assert.Equal(HttpStatusCode.Forbidden, forbidden.StatusCode);
        using var logout = await Post(http, "/api/account/logout", new { }, await Csrf(http));
        Assert.Equal(HttpStatusCode.OK, logout.StatusCode);
        await Login(http, "admin");
        using var payloadDenied = await http.GetAsync("/api/admin/clusters/unknown/preview?entity=events&messageId=1");
        Assert.Equal(HttpStatusCode.Forbidden, payloadDenied.StatusCode); // Admin still needs the explicit payload grant.
        using var allowed = await http.GetAsync("/api/admin/users");
        Assert.Equal(HttpStatusCode.OK, allowed.StatusCode);
        var text = await allowed.Content.ReadAsStringAsync();
        Assert.DoesNotContain("passwordHash", text, StringComparison.OrdinalIgnoreCase);
        using var missingCsrf = await Post(http, "/api/admin/users", new { Username = "new", Role = "Admin", Password, ClusterIds = Array.Empty<string>() });
        Assert.Equal(HttpStatusCode.BadRequest, missingCsrf.StatusCode);
    }

    [Fact]
    public async Task Privileged_mfa_enrollment_and_cookie_refresh_preserve_verified_identity()
    {
        using var directory = new ClusterTestDirectory();
        var (app, http) = await Start(directory, true);
        await using var application = app;
        using var client = http;
        await Login(http, "admin");
        using var gated = await http.GetAsync("/api/admin/users");
        Assert.Equal(HttpStatusCode.Forbidden, gated.StatusCode);
        using var started = await Post(http, "/api/account/mfa/start", new { Password }, await Csrf(http));
        Assert.Equal(HttpStatusCode.OK, started.StatusCode);
        using var setup = JsonDocument.Parse(await started.Content.ReadAsStringAsync());
        var key = setup.RootElement.GetProperty("key").GetString()!;
        using var confirmed = await Post(http, "/api/account/mfa/confirm", new { Code = Totp(key) }, await Csrf(http));
        Assert.Equal(HttpStatusCode.OK, confirmed.StatusCode);
        using var codesDocument = JsonDocument.Parse(await confirmed.Content.ReadAsStringAsync());
        var codes = codesDocument.RootElement.GetProperty("recoveryCodes").EnumerateArray().Select(value => value.GetString()!).ToArray();
        Assert.Equal(10, codes.Length);
        for (var attempt = 0; attempt < 3; attempt++)
        {
            using var allowed = await http.GetAsync("/api/admin/users");
            Assert.Equal(HttpStatusCode.OK, allowed.StatusCode); // SecurityStampValidator refreshes on each request.
        }
        using var scope = app.Services.CreateScope();
        var users = scope.ServiceProvider.GetRequiredService<UserManager<DashboardUser>>();
        var user = (await users.FindByNameAsync("admin"))!;
        Assert.NotEqual(key, user.ProtectedAuthenticatorKey);
        Assert.DoesNotContain(codes[0], user.RecoveryCodeHashes);
        Assert.True((await users.RedeemTwoFactorRecoveryCodeAsync(user, codes[0])).Succeeded);
        Assert.False((await users.RedeemTwoFactorRecoveryCodeAsync(user, codes[0])).Succeeded);
    }

    [Fact]
    public async Task Last_administrator_cannot_be_demoted_and_failed_logins_lock_the_account()
    {
        using var directory = new ClusterTestDirectory();
        var (app, http) = await Start(directory);
        await using var application = app;
        using var client = http;
        using var scope = app.Services.CreateScope();
        var users = scope.ServiceProvider.GetRequiredService<UserManager<DashboardUser>>();
        var root = (await users.FindByNameAsync("admin"))!;
        root.Role = "Viewer";
        Assert.False((await users.UpdateAsync(root)).Succeeded);
        var signIn = scope.ServiceProvider.GetRequiredService<SignInManager<DashboardUser>>();
        var viewer = (await users.FindByNameAsync("viewer"))!;
        for (var index = 0; index < 5; index++) Assert.False((await signIn.CheckPasswordSignInAsync(viewer, "wrong", true)).Succeeded);
        Assert.True(await users.IsLockedOutAsync(viewer));
    }
    private static string Totp(string key)
    {
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        var buffer = new List<byte>(); int bits = 0, value = 0;
        foreach (var character in key.TrimEnd('='))
        {
            value = (value << 5) | alphabet.IndexOf(character); bits += 5;
            if (bits >= 8) { bits -= 8; buffer.Add((byte)(value >> bits)); }
        }
        Span<byte> counter = stackalloc byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(counter, DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 30);
        var hash = HMACSHA1.HashData(buffer.ToArray(), counter);
        var offset = hash[^1] & 15;
        var binary = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(hash.AsSpan(offset)) & int.MaxValue;
        return (binary % 1_000_000).ToString("D6", System.Globalization.CultureInfo.InvariantCulture);
    }
}
