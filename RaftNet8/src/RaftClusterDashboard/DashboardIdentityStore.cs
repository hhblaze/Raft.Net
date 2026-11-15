using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using DBreeze.Utils;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Options;

namespace RaftClusterDashboard;

public sealed class DashboardUser : IdentityUser
{
    public string Role { get; set; } = "Viewer";
    public string[] ClusterIds { get; set; } = [];
    public string? ProtectedAuthenticatorKey { get; set; }
    public string[] RecoveryCodeHashes { get; set; } = [];
    public bool CanReadPayload { get; set; }
}

public sealed class DashboardClaimsFactory(UserManager<DashboardUser> manager, IOptions<IdentityOptions> options)
    : UserClaimsPrincipalFactory<DashboardUser>(manager, options)
{
    protected override async Task<ClaimsIdentity> GenerateClaimsAsync(DashboardUser user)
    {
        var identity = await base.GenerateClaimsAsync(user);
        identity.AddClaim(new(ClaimTypes.Role, user.Role));
        if (user.CanReadPayload) identity.AddClaim(new("raft.payload", "allowed"));
        foreach (var cluster in user.ClusterIds) identity.AddClaim(new("raft.cluster", cluster));
        return identity;
    }
}

/// <summary>ASP.NET Core Identity persistence; the DBreeze engine is owned by DashboardStore.</summary>
public sealed class DashboardIdentityStore(DashboardStore database) :
    IUserPasswordStore<DashboardUser>, IUserSecurityStampStore<DashboardUser>, IUserLockoutStore<DashboardUser>,
    IUserTwoFactorStore<DashboardUser>, IUserAuthenticatorKeyStore<DashboardUser>, IUserTwoFactorRecoveryCodeStore<DashboardUser>
{
    private static readonly IdentityErrorDescriber Errors = new();
    public Task<IdentityResult> CreateAsync(DashboardUser user, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        return Task.FromResult(database.Write(tx =>
        {
            if (DashboardStore.Range<DashboardUser>(tx, 1, 1024).Count >= 1024)
                return IdentityResult.Failed(new IdentityError { Code = "user_limit", Description = "Dashboard account limit reached." });
            if (DashboardStore.Get<string>(tx, 2.ToIndex(user.NormalizedUserName!)) is not null)
                return IdentityResult.Failed(Errors.DuplicateUserName(user.UserName!));
            DashboardStore.Put(tx, 1.ToIndex(user.Id), user);
            DashboardStore.Put(tx, 2.ToIndex(user.NormalizedUserName!), user.Id);
            return IdentityResult.Success;
        }));
    }
    public Task<IdentityResult> UpdateAsync(DashboardUser user, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        return Task.FromResult(database.Write(tx =>
        {
            var previous = DashboardStore.Get<DashboardUser>(tx, 1.ToIndex(user.Id));
            if (previous is null || previous.ConcurrencyStamp != user.ConcurrencyStamp) return IdentityResult.Failed(Errors.ConcurrencyFailure());
            var owner = DashboardStore.Get<string>(tx, 2.ToIndex(user.NormalizedUserName!));
            if (owner is not null && owner != user.Id) return IdentityResult.Failed(Errors.DuplicateUserName(user.UserName!));
            if (previous.Role == "Admin" && user.Role != "Admin" && DashboardStore.Range<DashboardUser>(tx, 1, 1024).Count(item => item.Role == "Admin") <= 1)
                return IdentityResult.Failed(new IdentityError { Code = "last_admin", Description = "The last administrator cannot be removed." });
            if (previous.NormalizedUserName != user.NormalizedUserName) DashboardStore.Remove(tx, 2.ToIndex(previous.NormalizedUserName!));
            if (previous.Role != user.Role || previous.CanReadPayload != user.CanReadPayload ||
                !previous.ClusterIds.SequenceEqual(user.ClusterIds, StringComparer.Ordinal))
                user.SecurityStamp = Guid.NewGuid().ToString("N"); // Grants and stream/session invalidation commit atomically.
            user.ConcurrencyStamp = Guid.NewGuid().ToString("N");
            DashboardStore.Put(tx, 1.ToIndex(user.Id), user);
            DashboardStore.Put(tx, 2.ToIndex(user.NormalizedUserName!), user.Id);
            return IdentityResult.Success;
        }));
    }
    public Task<IdentityResult> DeleteAsync(DashboardUser user, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        return Task.FromResult(database.Write(tx =>
        {
            var previous = DashboardStore.Get<DashboardUser>(tx, 1.ToIndex(user.Id));
            if (previous is null || previous.ConcurrencyStamp != user.ConcurrencyStamp) return IdentityResult.Failed(Errors.ConcurrencyFailure());
            if (previous.Role == "Admin" && DashboardStore.Range<DashboardUser>(tx, 1, 1024).Count(item => item.Role == "Admin") <= 1)
                return IdentityResult.Failed(new IdentityError { Code = "last_admin", Description = "The last administrator cannot be removed." });
            DashboardStore.Remove(tx, 1.ToIndex(user.Id));
            DashboardStore.Remove(tx, 2.ToIndex(user.NormalizedUserName!));
            return IdentityResult.Success;
        }));
    }
    public Task<DashboardUser?> FindByIdAsync(string id, CancellationToken token)
    { token.ThrowIfCancellationRequested(); return Task.FromResult(database.Get<DashboardUser>(1.ToIndex(id))); }
    public Task<DashboardUser?> FindByNameAsync(string name, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        return Task.FromResult(database.Read(tx =>
        {
            var id = DashboardStore.Get<string>(tx, 2.ToIndex(name));
            return id is null ? null : DashboardStore.Get<DashboardUser>(tx, 1.ToIndex(id));
        }));
    }
    public Task<string> GetUserIdAsync(DashboardUser user, CancellationToken token) => Task.FromResult(user.Id);
    public Task<string?> GetUserNameAsync(DashboardUser user, CancellationToken token) => Task.FromResult(user.UserName);
    public Task SetUserNameAsync(DashboardUser user, string? name, CancellationToken token) { user.UserName = name; return Task.CompletedTask; }
    public Task<string?> GetNormalizedUserNameAsync(DashboardUser user, CancellationToken token) => Task.FromResult(user.NormalizedUserName);
    public Task SetNormalizedUserNameAsync(DashboardUser user, string? name, CancellationToken token) { user.NormalizedUserName = name; return Task.CompletedTask; }
    public Task SetPasswordHashAsync(DashboardUser user, string? hash, CancellationToken token) { user.PasswordHash = hash; return Task.CompletedTask; }
    public Task<string?> GetPasswordHashAsync(DashboardUser user, CancellationToken token) => Task.FromResult(user.PasswordHash);
    public Task<bool> HasPasswordAsync(DashboardUser user, CancellationToken token) => Task.FromResult(user.PasswordHash is not null);
    public Task SetSecurityStampAsync(DashboardUser user, string stamp, CancellationToken token) { user.SecurityStamp = stamp; return Task.CompletedTask; }
    public Task<string?> GetSecurityStampAsync(DashboardUser user, CancellationToken token) => Task.FromResult(user.SecurityStamp);
    public Task<DateTimeOffset?> GetLockoutEndDateAsync(DashboardUser user, CancellationToken token) => Task.FromResult(user.LockoutEnd);
    public Task SetLockoutEndDateAsync(DashboardUser user, DateTimeOffset? end, CancellationToken token) { user.LockoutEnd = end; return Task.CompletedTask; }
    public Task<int> IncrementAccessFailedCountAsync(DashboardUser user, CancellationToken token) => Task.FromResult(++user.AccessFailedCount);
    public Task ResetAccessFailedCountAsync(DashboardUser user, CancellationToken token) { user.AccessFailedCount = 0; return Task.CompletedTask; }
    public Task<int> GetAccessFailedCountAsync(DashboardUser user, CancellationToken token) => Task.FromResult(user.AccessFailedCount);
    public Task<bool> GetLockoutEnabledAsync(DashboardUser user, CancellationToken token) => Task.FromResult(user.LockoutEnabled);
    public Task SetLockoutEnabledAsync(DashboardUser user, bool enabled, CancellationToken token) { user.LockoutEnabled = enabled; return Task.CompletedTask; }
    public Task SetTwoFactorEnabledAsync(DashboardUser user, bool enabled, CancellationToken token) { user.TwoFactorEnabled = enabled; return Task.CompletedTask; }
    public Task<bool> GetTwoFactorEnabledAsync(DashboardUser user, CancellationToken token) => Task.FromResult(user.TwoFactorEnabled);
    public Task SetAuthenticatorKeyAsync(DashboardUser user, string key, CancellationToken token)
    { user.ProtectedAuthenticatorKey = database.Protect(key); return Task.CompletedTask; }
    public Task<string?> GetAuthenticatorKeyAsync(DashboardUser user, CancellationToken token) =>
        Task.FromResult(user.ProtectedAuthenticatorKey is null ? null : database.Unprotect(user.ProtectedAuthenticatorKey));
    private static string CodeHash(string code) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(code)));
    public Task ReplaceCodesAsync(DashboardUser user, IEnumerable<string> codes, CancellationToken token)
    { user.RecoveryCodeHashes = codes.Select(CodeHash).ToArray(); return Task.CompletedTask; }
    public Task<int> CountCodesAsync(DashboardUser user, CancellationToken token) => Task.FromResult(user.RecoveryCodeHashes.Length);
    public Task<bool> RedeemCodeAsync(DashboardUser user, string code, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        return Task.FromResult(database.Write(tx =>
        {
            var stored = DashboardStore.Get<DashboardUser>(tx, 1.ToIndex(user.Id));
            var hash = CodeHash(code);
            if (stored is null || stored.ConcurrencyStamp != user.ConcurrencyStamp || !stored.RecoveryCodeHashes.Contains(hash)) return false;
            user.RecoveryCodeHashes = stored.RecoveryCodeHashes.Where(item => item != hash).ToArray();
            DashboardStore.Put(tx, 1.ToIndex(user.Id), user);
            return true;
        }));
    }
    public void Dispose() { }
}
