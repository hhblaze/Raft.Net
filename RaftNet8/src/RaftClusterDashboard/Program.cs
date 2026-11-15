using System.Security.Claims;
using System.Threading.RateLimiting;
using DBreeze.Utils;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.RateLimiting;
using Raft.Cluster;

namespace RaftClusterDashboard;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var command = args.FirstOrDefault();
        var bootstrap = command == "bootstrap-admin";
        var import = command == "import-cluster";
        var app = DashboardHost.Create(bootstrap || import ? args[1..] : args);
        if (import)
        {
            var path = app.Configuration["connection-file"] ?? throw new ArgumentException("Supply --connection-file with a protected experiment connection file.");
            if (new FileInfo(path).Length > 64 * 1024) throw new ArgumentException("Connection file exceeds64KiB.");
            var connection = System.Text.Json.JsonSerializer.Deserialize<ImportedConnection>(await File.ReadAllBytesAsync(path))
                ?? throw new InvalidDataException("Missing connection.");
            ClusterLimits.ValidateName(connection.Name); ClusterLimits.ValidateName(connection.GroupId);
            if (connection.ManagementEndpoints.Length is < 1 or > ClusterLimits.MaxEndpoints || string.IsNullOrWhiteSpace(connection.Credential))
                throw new ArgumentException("Invalid imported connection.");
            var settings = app.Services.GetRequiredService<DashboardSettings>();
            foreach (var endpoint in connection.ManagementEndpoints)
            {
                var uri = new ClientEndpoint("seed", new Uri(endpoint)).Validate().Address;
                if (settings.SecureProfile && uri.Scheme != "https") throw new ArgumentException("Secure profiles require HTTPS cluster routes.");
            }
            if (connection.CertificateAlias is { } alias && !settings.DiagnosticCertificates.ContainsKey(alias)) throw new ArgumentException("Unknown diagnostic certificate alias.");
            var store = app.Services.GetRequiredService<DashboardStore>();
            store.SaveProfile(new(Guid.NewGuid().ToString("N"), connection.Name, connection.GroupId, connection.ManagementEndpoints, store.Protect(connection.Credential), connection.CertificateAlias));
            Console.WriteLine("Cluster profile imported; its credential is protected in the dashboard database.");
            await app.DisposeAsync();
            return;
        }
        if (bootstrap)
        {
            using var scope = app.Services.CreateScope();
            var store = scope.ServiceProvider.GetRequiredService<DashboardStore>();
            if (store.List<DashboardUser>(1, 1).Count != 0) throw new InvalidOperationException("An account already exists; bootstrap is one-time.");
            // "USERNAME" is a built-in Windows environment variable, not a bootstrap request.
            var usernameArgument = Array.IndexOf(args, "--username");
            var username = usernameArgument >= 0 && usernameArgument + 1 < args.Length
                ? args[usernameArgument + 1] : app.Configuration["Bootstrap:Username"] ?? "admin";
            var password = Environment.GetEnvironmentVariable("RAFT_DASHBOARD_BOOTSTRAP_PASSWORD");
            if (string.IsNullOrEmpty(password))
            {
                Console.Write("Administrator password (stdin; not a command-line argument): ");
                password = Console.ReadLine();
            }
            var users = scope.ServiceProvider.GetRequiredService<UserManager<DashboardUser>>();
            var result = await users.CreateAsync(new() { UserName = username, Role = "Admin", LockoutEnabled = true }, password ?? "");
            if (!result.Succeeded) throw new InvalidOperationException(string.Join("; ", result.Errors.Select(error => error.Description)));
            Console.WriteLine("Administrator created. Start the dashboard and complete MFA enrollment when required.");
            await app.DisposeAsync();
            return;
        }
        if (app.Configuration.GetValue<bool>("control-stdin"))
        {
            await app.StartAsync();
            while (await Console.In.ReadLineAsync() is { } control && control != "quit") { }
            using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await app.StopAsync(shutdown.Token);
            await app.DisposeAsync();
        }
        else await app.RunAsync();
    }
}
internal sealed record ImportedConnection(string Name, string GroupId, string[] ManagementEndpoints, string Credential, string? CertificateAlias = null);

public static class DashboardHost
{
    public static WebApplication Create(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        if (builder.Configuration["urls"] is null) builder.WebHost.UseUrls("http://127.0.0.1:7400");
        builder.WebHost.ConfigureKestrel(server =>
        {
            server.Limits.MaxRequestBodySize = 64 * 1024;
            server.Limits.MaxRequestHeadersTotalSize = 16 * 1024;
            server.Limits.MaxConcurrentConnections = 512;
            server.Limits.RequestHeadersTimeout = TimeSpan.FromSeconds(15);
        });
        var settings = builder.Configuration.GetSection("Dashboard").Get<DashboardSettings>() ?? new();
        settings = settings with { DataDirectory = Path.GetFullPath(settings.DataDirectory) };
        if (settings.SecureProfile && (builder.Configuration["urls"] ?? "").Split(';').Any(url => !url.StartsWith("https://", StringComparison.OrdinalIgnoreCase)))
            throw new ArgumentException("The secure dashboard profile requires HTTPS URLs and a configured host certificate.");
        if (settings.MaxClusters is < 1 or > 32 || settings.MaxSseConnections is < 1 or > 256 || settings.MaxObservedNodes is < 1 or > 1024 ||
            settings.PollIntervalSeconds is < 1 or > 60 || settings.DirectoryRefreshSeconds is < 1 or > 300 ||
            settings.DetailedHistorySeconds is < 60 or > 3600 || settings.HistoryDays is < 1 or > 30 || settings.MaxHistoryRows is < 1000 or > 1_000_000)
            throw new ArgumentException("Invalid dashboard resource limits.");
        Directory.CreateDirectory(settings.DataDirectory);
        builder.Services.AddSingleton(settings);
        builder.Services.ConfigureHttpJsonOptions(options => options.SerializerOptions.Converters.Add(new BrowserJson.Int64Converter()));
        var protection = builder.Services.AddDataProtection().SetApplicationName("RaftClusterDashboard.v1")
            .PersistKeysToFileSystem(new DirectoryInfo(Path.Combine(settings.DataDirectory, "keys")));
        if (OperatingSystem.IsWindows()) protection.ProtectKeysWithDpapi();
        builder.Services.AddSingleton<DashboardStore>();
        builder.Services.AddIdentityCore<DashboardUser>(options =>
        {
            options.Password.RequiredLength = 12;
            options.Lockout.MaxFailedAccessAttempts = 5;
            options.Lockout.DefaultLockoutTimeSpan = TimeSpan.FromMinutes(15);
        }).AddUserStore<DashboardIdentityStore>().AddClaimsPrincipalFactory<DashboardClaimsFactory>()
            .AddSignInManager().AddDefaultTokenProviders();
        builder.Services.AddAuthentication(IdentityConstants.ApplicationScheme).AddIdentityCookies();
        builder.Services.ConfigureApplicationCookie(options =>
        {
            options.Cookie.Name = "RaftDashboard";
            options.Cookie.HttpOnly = true;
            options.Cookie.SameSite = SameSiteMode.Strict;
            options.Cookie.SecurePolicy = settings.SecureProfile ? CookieSecurePolicy.Always : CookieSecurePolicy.SameAsRequest;
            options.ExpireTimeSpan = TimeSpan.FromHours(8);
            options.SlidingExpiration = true;
            options.Events.OnRedirectToLogin = context => { context.Response.StatusCode = 401; return Task.CompletedTask; };
            options.Events.OnRedirectToAccessDenied = context => { context.Response.StatusCode = 403; return Task.CompletedTask; };
        });
        builder.Services.Configure<SecurityStampValidatorOptions>(options =>
        {
            options.ValidationInterval = TimeSpan.Zero;
            options.OnRefreshingPrincipal = context =>
            {
                if (context.NewPrincipal?.Identity is ClaimsIdentity identity)
                    foreach (var claim in context.CurrentPrincipal?.Claims.Where(claim => claim.Type is "amr" or "reauth") ?? []) identity.AddClaim(claim);
                return Task.CompletedTask;
            };
        });
        builder.Services.AddAuthorization();
        builder.Services.AddAntiforgery(options => { options.HeaderName = "X-Raft-CSRF"; options.Cookie.SameSite = SameSiteMode.Strict; });
        builder.Services.AddRateLimiter(options =>
        {
            options.AddPolicy("login", context => RateLimitPartition.GetFixedWindowLimiter(context.Connection.RemoteIpAddress?.ToString() ?? "unknown",
                _ => new() { PermitLimit = 10, Window = TimeSpan.FromMinutes(1), QueueLimit = 0 }));
            options.RejectionStatusCode = 429;
        });
        builder.Services.AddSingleton<ClusterMonitor>();
        builder.Services.AddHostedService(provider => provider.GetRequiredService<ClusterMonitor>());
        var app = builder.Build();
        if (settings.SecureProfile)
        {
            try
            {
                if (app.Services.GetRequiredService<DashboardStore>().Profiles().Any(profile => profile.ManagementEndpoints.Any(endpoint => new Uri(endpoint).Scheme != "https")))
                    throw new ArgumentException("Secure profile cannot use a stored plaintext cluster connection; update its routes before changing the deployment profile.");
            }
            catch { app.DisposeAsync().AsTask().GetAwaiter().GetResult(); throw; }
        }
        app.UseAuthentication();
        app.UseAuthorization();
        app.Use(async (context, next) =>
        {
            context.Response.Headers.XContentTypeOptions = "nosniff";
            context.Response.Headers["Referrer-Policy"] = "same-origin";
            context.Response.Headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'";
            if (context.Request.Path.StartsWithSegments("/api")) context.Response.Headers.CacheControl = "no-store";
            if (context.Request.Path.StartsWithSegments("/api") && context.Request.Method is not ("GET" or "HEAD" or "OPTIONS"))
            {
                try { await context.RequestServices.GetRequiredService<IAntiforgery>().ValidateRequestAsync(context); }
                catch (AntiforgeryValidationException) { context.Response.StatusCode = 400; return; }
            }
            try { await next(); }
            catch (ClusterRequestException error) { context.Response.StatusCode = error.Code == "forbidden" ? 403 : 409; await context.Response.WriteAsJsonAsync(new { error.Code, error.Message }); }
            catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested) { }
            catch (OperationCanceledException) { context.Response.StatusCode = 503; await context.Response.WriteAsJsonAsync(new { code = "unavailable", message = "Cluster request timed out; use the same operation ID to observe or retry." }); }
        });
        app.UseRateLimiter();
        DashboardApi.Map(app, settings);
        app.MapGet("/health/ready", (ClusterMonitor monitor) =>
        {
            var ready = monitor.Observations.Any(observation => observation.ObservedAt is { } at && DateTimeOffset.UtcNow - at < TimeSpan.FromSeconds(observation.StaleAfterSeconds) && observation.Error is null);
            return Results.Json(new { Ready = ready }, statusCode: ready ? 200 : 503);
        });
        app.UseDefaultFiles();
        app.UseStaticFiles();
        app.MapFallbackToFile("index.html");
        return app;
    }
}

internal sealed record LoginRequest(string Username, string Password, string? Code = null, string? RecoveryCode = null);
internal sealed record MfaRequest(string? Password, string? Code);
internal sealed record SaveClusterRequest(string? Id, string Name, string GroupId, string[] ManagementEndpoints, string? Credential, string? CertificateAlias);
internal sealed record SaveUserRequest(string? Id, string Username, string Role, string[] ClusterIds, string? Password, bool CanReadPayload = false);
internal sealed record ConfigureEntityRequest(string Entity, EntitySettings Settings, Guid OperationId);
internal sealed record StartOperationRequest(AdminOperationRequest Operation, Guid OperationId);
internal sealed record IssueTokenRequest(TokenGrant Grant, Guid OperationId);
internal sealed record CreateEntityRequest(string Name, EntityMode Mode, Guid OperationId);

internal static class DashboardApi
{
    private static bool NeedsMfa(DashboardUser user, DashboardSettings settings) => user.Role is "Admin" or "Operator" && (settings.SecureProfile || settings.RequirePrivilegedMfa);
    private static async Task SignIn(SignInManager<DashboardUser> signIn, DashboardUser user, string method) =>
        await signIn.SignInWithClaimsAsync(user, false, [new("amr", method), new("reauth", DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString(System.Globalization.CultureInfo.InvariantCulture))]);
    private static bool Recent(HttpContext context) => long.TryParse(context.User.FindFirstValue("reauth"), out var at) && DateTimeOffset.UtcNow.ToUnixTimeSeconds() - at < 300;
    private static bool Allowed(HttpContext context, string cluster, bool write = false) =>
        (context.User.IsInRole("Admin") || context.User.HasClaim("raft.cluster", cluster)) && (!write || context.User.IsInRole("Admin") || context.User.IsInRole("Operator"));
    public static void Map(WebApplication app, DashboardSettings settings)
    {
        app.MapGet("/api/session", async (HttpContext context, IAntiforgery antiforgery, UserManager<DashboardUser> users) =>
        {
            var user = await users.GetUserAsync(context.User);
            return Results.Json(new { csrf = antiforgery.GetAndStoreTokens(context).RequestToken, username = user?.UserName, role = user?.Role,
                mfaEnabled = user?.TwoFactorEnabled ?? false, mfaRequired = user is not null && NeedsMfa(user, settings), secure = settings.SecureProfile,
                canReadPayload = user?.CanReadPayload ?? false });
        });
        app.MapPost("/api/login", async (LoginRequest request, HttpContext context, UserManager<DashboardUser> users,
            SignInManager<DashboardUser> signIn, DashboardStore store) =>
        {
            var user = await users.FindByNameAsync(request.Username);
            if (user is null || !(await signIn.CheckPasswordSignInAsync(user, request.Password, true)).Succeeded) return Results.Unauthorized();
            if (user.TwoFactorEnabled)
            {
                var valid = request.RecoveryCode is { Length: > 0 } recovery
                    ? (await users.RedeemTwoFactorRecoveryCodeAsync(user, recovery)).Succeeded
                    : await users.VerifyTwoFactorTokenAsync(user, TokenOptions.DefaultAuthenticatorProvider, request.Code?.Replace(" ", "", StringComparison.Ordinal) ?? "");
                if (!valid) { await users.AccessFailedAsync(user); return Results.Unauthorized(); }
            }
            await users.ResetAccessFailedCountAsync(user);
            await SignIn(signIn, user, user.TwoFactorEnabled ? "mfa" : "password");
            store.Audit(user.Id, "login", "dashboard", true);
            return Results.Ok(new { mfaEnrollmentRequired = NeedsMfa(user, settings) && !user.TwoFactorEnabled });
        }).RequireRateLimiting("login");
        var account = app.MapGroup("/api/account").RequireAuthorization();
        account.MapPost("/logout", async (SignInManager<DashboardUser> signIn) => { await signIn.SignOutAsync(); return Results.Ok(); });
        account.MapPost("/reauth", async (LoginRequest request, HttpContext context, UserManager<DashboardUser> users, SignInManager<DashboardUser> signIn) =>
        {
            var user = await users.GetUserAsync(context.User);
            if (user is null || !(await signIn.CheckPasswordSignInAsync(user, request.Password, true)).Succeeded ||
                user.TwoFactorEnabled && !await users.VerifyTwoFactorTokenAsync(user, TokenOptions.DefaultAuthenticatorProvider, request.Code ?? "")) return Results.Unauthorized();
            await SignIn(signIn, user, user.TwoFactorEnabled ? "mfa" : "password");
            return Results.Ok();
        }).RequireRateLimiting("login");
        account.MapPost("/mfa/start", async (MfaRequest request, HttpContext context, UserManager<DashboardUser> users, SignInManager<DashboardUser> signIn) =>
        {
            var user = (await users.GetUserAsync(context.User))!;
            if (request.Password is null || !await users.CheckPasswordAsync(user, request.Password) ||
                user.TwoFactorEnabled && !await users.VerifyTwoFactorTokenAsync(user, TokenOptions.DefaultAuthenticatorProvider, request.Code ?? "")) return Results.Unauthorized();
            await users.SetTwoFactorEnabledAsync(user, false);
            await users.ResetAuthenticatorKeyAsync(user);
            await SignIn(signIn, user, "password");
            return Results.Json(new { key = await users.GetAuthenticatorKeyAsync(user), username = user.UserName });
        }).RequireRateLimiting("login");
        account.MapPost("/mfa/confirm", async (MfaRequest request, HttpContext context, UserManager<DashboardUser> users, SignInManager<DashboardUser> signIn) =>
        {
            var user = (await users.GetUserAsync(context.User))!;
            if (!await users.VerifyTwoFactorTokenAsync(user, TokenOptions.DefaultAuthenticatorProvider, request.Code ?? "")) return Results.BadRequest();
            await users.SetTwoFactorEnabledAsync(user, true);
            var codes = await users.GenerateNewTwoFactorRecoveryCodesAsync(user, 10);
            await SignIn(signIn, user, "mfa");
            return Results.Json(new { recoveryCodes = codes });
        }).RequireRateLimiting("login");
        var api = app.MapGroup("/api/admin").RequireAuthorization().AddEndpointFilter(async (invocation, next) =>
        {
            var context = invocation.HttpContext;
            var user = await context.RequestServices.GetRequiredService<UserManager<DashboardUser>>().GetUserAsync(context.User);
            if (user is null) return Results.Unauthorized();
            if (NeedsMfa(user, settings) && (!user.TwoFactorEnabled || context.User.FindFirstValue("amr") != "mfa")) return Results.Json(new { code = "mfa_required" }, statusCode: 403);
            var result = await next(invocation);
            if (context.Request.Method == "POST")
            {
                var operation = invocation.Arguments.Select(argument => argument switch
                { StartOperationRequest value => value.OperationId, ConfigureEntityRequest value => value.OperationId,
                  IssueTokenRequest value => value.OperationId, CreateEntityRequest value => value.OperationId, Guid id => id, _ => Guid.Empty }).FirstOrDefault(id => id != Guid.Empty);
                context.RequestServices.GetRequiredService<DashboardStore>().Audit(user.Id, context.Request.Path,
                    operation == Guid.Empty ? "dashboard" : operation.ToString("D"), result is not IStatusCodeHttpResult status || status.StatusCode is null or < 400);
            }
            return result;
        });
        api.MapGet("/clusters", (HttpContext context, DashboardStore store) => Results.Json(store.Profiles().Where(profile => Allowed(context, profile.Id))
            .Select(profile => new { profile.Id, profile.Name, profile.GroupId, profile.ManagementEndpoints, CertificateAlias = profile.DiagnosticCertificatePath })));
        api.MapPost("/clusters", (SaveClusterRequest request, HttpContext context, DashboardStore store) =>
        {
            if (!context.User.IsInRole("Admin") || !Recent(context)) return Results.Forbid();
            ClusterLimits.ValidateName(request.Name); ClusterLimits.ValidateName(request.GroupId);
            if (request.ManagementEndpoints.Length is < 1 or > ClusterLimits.MaxEndpoints) return Results.BadRequest();
            foreach (var endpoint in request.ManagementEndpoints)
            {
                var uri = new ClientEndpoint("seed", new Uri(endpoint)).Validate().Address;
                if (settings.SecureProfile && uri.Scheme != "https") return Results.BadRequest(new { code = "https_required" });
            }
            if (request.CertificateAlias is { Length: > 0 } alias && !settings.DiagnosticCertificates.ContainsKey(alias)) return Results.BadRequest();
            var id = request.Id ?? Guid.NewGuid().ToString("N");
            if (!Guid.TryParseExact(id, "N", out _)) return Results.BadRequest();
            var previous = store.Profile(id);
            if (previous is null && store.Profiles().Count >= settings.MaxClusters) return Results.Conflict();
            var credential = request.Credential is { Length: > 0 } secret ? store.Protect(secret) : previous?.ProtectedCredential;
            if (credential is null) return Results.BadRequest(new { code = "credential_required" });
            store.SaveProfile(new(id, request.Name, request.GroupId, request.ManagementEndpoints, credential,
                string.IsNullOrEmpty(request.CertificateAlias) ? null : request.CertificateAlias));
            store.Audit(context.User.FindFirstValue(ClaimTypes.NameIdentifier)!, "save_cluster", id, true);
            return Results.Ok(new { id });
        });
        api.MapGet("/observations", (HttpContext context, ClusterMonitor monitor) => Results.Json(monitor.Observations.Where(item => Allowed(context, item.Id))));
        api.MapGet("/clusters/{id}/history", (string id, HttpContext context, DashboardStore store) =>
            Allowed(context, id) ? Results.Json(store.History(id, DateTimeOffset.UtcNow.AddDays(-settings.HistoryDays))) : Results.Forbid());
        api.MapGet("/stream", async (HttpContext context, ClusterMonitor monitor) =>
        {
            var subscription = monitor.Subscribe();
            context.Response.ContentType = "text/event-stream";
            context.Response.Headers["X-Accel-Buffering"] = "no";
            try
            {
                await foreach (var _ in subscription.Reader.ReadAllAsync(context.RequestAborted))
                {
                    var users = context.RequestServices.GetRequiredService<UserManager<DashboardUser>>();
                    var current = await users.GetUserAsync(context.User);
                    var stampClaim = context.RequestServices.GetRequiredService<Microsoft.Extensions.Options.IOptions<IdentityOptions>>().Value.ClaimsIdentity.SecurityStampClaimType;
                    if (current is null || current.SecurityStamp != context.User.FindFirstValue(stampClaim)) break;
                    var data = System.Text.Json.JsonSerializer.Serialize(monitor.Observations.Where(item => Allowed(context, item.Id)),
                        BrowserJson.Options);
                    await context.Response.WriteAsync("event: observation\ndata: " + data + "\n\n", context.RequestAborted);
                    await context.Response.Body.FlushAsync(context.RequestAborted);
                }
            }
            finally { monitor.Unsubscribe(subscription.Id); }
        });
        api.MapPost("/clusters/{id}/operations", async (string id, StartOperationRequest request, HttpContext context, ClusterMonitor monitor, DashboardStore store) =>
        {
            if (!Allowed(context, id, true)) return Results.Forbid();
            await monitor.WithClientAsync(id, async client => { await client.StartOperationAsync(request.Operation, request.OperationId, context.RequestAborted); return true; });
            store.Audit(context.User.FindFirstValue(ClaimTypes.NameIdentifier)!, "start_" + request.Operation.Kind, id + "/" + request.OperationId, true);
            return Results.Accepted(value: new { request.OperationId });
        });
        api.MapGet("/clusters/{id}/operation/{operationId:guid}", async (string id, Guid operationId, HttpContext context, ClusterMonitor monitor) =>
            Allowed(context, id) ? Results.Json(await monitor.WithClientAsync(id, client => client.GetOperationAsync(operationId, context.RequestAborted))) : Results.Forbid());
        api.MapPost("/clusters/{id}/cancel/{operationId:guid}", async (string id, Guid operationId, HttpContext context, ClusterMonitor monitor) =>
        {
            if (!Allowed(context, id, true)) return Results.Forbid();
            await monitor.WithClientAsync(id, async client => { await client.CancelMembershipOperationAsync(operationId, context.RequestAborted); return true; });
            return Results.Ok();
        });
        api.MapPost("/clusters/{id}/entity-settings", async (string id, ConfigureEntityRequest request, HttpContext context, ClusterMonitor monitor) =>
        {
            if (!Allowed(context, id, true)) return Results.Forbid();
            await monitor.WithClientAsync(id, async client => { await client.ConfigureEntityAsync(request.Entity, request.Settings, request.OperationId, context.RequestAborted); return true; });
            return Results.Ok();
        });
        api.MapPost("/clusters/{id}/entities", async (string id, CreateEntityRequest request, HttpContext context, ClusterMonitor monitor) =>
        {
            if (!Allowed(context, id, true)) return Results.Forbid();
            await monitor.WithClientAsync(id, async client => { await client.CreateEntityAsync(request.Name, request.Mode, request.OperationId, context.RequestAborted); return true; });
            return Results.Ok();
        });
        api.MapGet("/clusters/{id}/tokens", async (string id, HttpContext context, ClusterMonitor monitor) =>
            context.User.IsInRole("Admin") ? Results.Json(await monitor.WithClientAsync(id, client => client.GetTokensAsync(token: context.RequestAborted))) : Results.Forbid());
        api.MapPost("/clusters/{id}/tokens", async (string id, IssueTokenRequest request, HttpContext context, ClusterMonitor monitor) =>
            context.User.IsInRole("Admin") && Recent(context) ? Results.Json(await monitor.WithClientAsync(id,
                client => client.IssueClientTokenAsync(request.Grant, request.OperationId, context.RequestAborted))) : Results.Forbid());
        api.MapPost("/clusters/{id}/tokens/{tokenId:guid}/revoke/{operationId:guid}", async (string id, Guid tokenId, Guid operationId, HttpContext context, ClusterMonitor monitor) =>
        {
            if (!context.User.IsInRole("Admin") || !Recent(context)) return Results.Forbid();
            await monitor.WithClientAsync(id, async client => { await client.RevokeClientTokenAsync(tokenId, operationId, context.RequestAborted); return true; });
            return Results.Ok();
        });
        api.MapGet("/clusters/{id}/audit", async (string id, HttpContext context, ClusterMonitor monitor) =>
            Allowed(context, id) ? Results.Json(await monitor.WithClientAsync(id, client => client.GetAuditAsync(token: context.RequestAborted))) : Results.Forbid());
        api.MapGet("/clusters/{id}/configuration", async (string id, HttpContext context, ClusterMonitor monitor) =>
            Allowed(context, id) ? Results.Json(await monitor.WithClientAsync(id, client => client.GetConfigurationAsync(context.RequestAborted))) : Results.Forbid());
        api.MapGet("/clusters/{id}/preview", async (string id, string entity, long messageId, HttpContext context, ClusterMonitor monitor) =>
            Allowed(context, id) && context.User.HasClaim("raft.payload", "allowed") ? Results.Json(await monitor.WithClientAsync(id,
                client => client.PreviewMessageAsync(entity, messageId, context.RequestAborted))) : Results.Forbid());
        api.MapPost("/clusters/{id}/tokens/update/{operationId:guid}", async (string id, Guid operationId, ClientTokenInfo update, HttpContext context, ClusterMonitor monitor) =>
        {
            if (!context.User.IsInRole("Admin") || !Recent(context)) return Results.Forbid();
            await monitor.WithClientAsync(id, async client => { await client.UpdateClientTokenAsync(update, operationId, context.RequestAborted); return true; });
            return Results.Ok();
        });
        api.MapGet("/users", (HttpContext context, DashboardStore store) => context.User.IsInRole("Admin")
            ? Results.Json(store.List<DashboardUser>(1, 1024).Select(user => new { user.Id, user.UserName, user.Role, user.ClusterIds, user.TwoFactorEnabled, user.CanReadPayload })) : Results.Forbid());
        api.MapGet("/dashboard-audit", (HttpContext context, DashboardStore store) => context.User.IsInRole("Admin")
            ? Results.Json(store.List<DashboardAudit>(5, 10000).TakeLast(100).Reverse()) : Results.Forbid());
        api.MapPost("/users", async (SaveUserRequest request, HttpContext context, UserManager<DashboardUser> users, DashboardStore store) =>
        {
            if (!context.User.IsInRole("Admin") || !Recent(context)) return Results.Forbid();
            if (request.Role is not ("Admin" or "Operator" or "Viewer") || request.ClusterIds.Any(id => store.Profile(id) is null)) return Results.BadRequest();
            var user = request.Id is null ? new DashboardUser() : await users.FindByIdAsync(request.Id);
            if (user is null) return Results.NotFound();
            user.UserName = request.Username; user.Role = request.Role; user.ClusterIds = request.ClusterIds; user.LockoutEnabled = true; user.CanReadPayload = request.CanReadPayload;
            if (request.Password is { Length: > 0 } proposedPassword)
                foreach (var validator in users.PasswordValidators)
                {
                    var validation = await validator.ValidateAsync(users, user, proposedPassword);
                    if (!validation.Succeeded) return Results.BadRequest(new { errors = validation.Errors.Select(error => error.Description) });
                }
            var result = request.Id is null ? await users.CreateAsync(user, request.Password ?? "") : await users.UpdateAsync(user);
            if (!result.Succeeded) return Results.BadRequest(new { errors = result.Errors.Select(error => error.Description) });
            if (request.Id is not null && request.Password is { Length: > 0 })
            {
                result = await users.ResetPasswordAsync(user, await users.GeneratePasswordResetTokenAsync(user), request.Password);
                if (!result.Succeeded) return Results.BadRequest(new { errors = result.Errors.Select(error => error.Description) });
            }
            if (request.Id is not null) await users.UpdateSecurityStampAsync(user);
            store.Audit(context.User.FindFirstValue(ClaimTypes.NameIdentifier)!, "save_user", user.Id, true);
            return Results.Ok();
        });
        app.MapMethods("/api/{**path}", ["GET", "POST", "PUT", "DELETE", "PATCH"], () => Results.NotFound());
    }
}
