using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text.Json;
using Raft.Cluster;
using Raft.Cluster.Server;
using Raft.Storage;

namespace Raft.Tests;

public sealed class ClusterAdministrationTests
{
    [Fact]
    public async Task Preview_reads_only_a_bounded_payload_prefix()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(directory.Path, version: ClusterLedgerVersion.V2);
        var payload = Enumerable.Range(0, 1024 * 1024).Select(value => (byte)value).ToArray();
        await ledger.ApplyAsync(Entry(1, new(MessageOperation.Append, Guid.NewGuid(), "large", EntityMode.Standard, payload)), default);
        var preview = ledger.PreviewMessage("large", 1)!;
        Assert.True(preview.Truncated);
        Assert.Equal(payload.Length, preview.PayloadLength);
        Assert.Equal(payload[..4096], Convert.FromBase64String(preview.Base64));
        Assert.Null(ledger.PreviewMessage("large", 2));
    }
    private static readonly DateTimeOffset Now = DateTimeOffset.FromUnixTimeMilliseconds(100_000);
    private static (StoredClientToken Stored, string Secret) Credential(ClusterPermission permissions = ClusterPermission.All, params string[] entities)
    {
        var bytes = RandomNumberGenerator.GetBytes(32);
        var id = Guid.NewGuid();
        return (new(new(id, new("test", permissions, entities.Length == 0 ? ["*"] : entities, Now.AddHours(1)), false, 0), SHA256.HashData(bytes)),
            $"rc2.{id:N}.{Convert.ToBase64String(bytes)}");
    }
    private static MessageCommand Command(MessageOperation operation, string name, object value, CommandAuthorization? auth = null, Guid? id = null) =>
        new(operation, id ?? Guid.NewGuid(), name, EntityMode.Standard, JsonSerializer.SerializeToUtf8Bytes(value), auth);
    private static LogEntry Entry(long index, MessageCommand command) => new(index, 1, command.Encode());
    private static async Task<CommandAuthorization> Initialize(MessageLedger ledger, StoredClientToken root, bool required = true)
    {
        await ledger.ApplyAsync(Entry(1, Command(MessageOperation.BootstrapSecurity, "security", new BootstrapSecurity(root, required))), default);
        return new(root.Info.TokenId, Now.ToUnixTimeMilliseconds());
    }

    [Fact]
    public async Task Bootstrap_is_one_time_and_security_survives_restart()
    {
        using var directory = new ClusterTestDirectory();
        var root = Credential();
        using (var ledger = new MessageLedger(directory.Path, version: ClusterLedgerVersion.V2))
        {
            await Initialize(ledger, root.Stored);
            var second = Command(MessageOperation.BootstrapSecurity, "security", new BootstrapSecurity(Credential().Stored, false));
            await ledger.ApplyAsync(Entry(2, second), default);
            Assert.Equal("already_initialized", ledger.Outcome(second.RequestId)!.Error);
            Assert.True(ledger.RequiresClientTokens);
        }
        using var recovered = new MessageLedger(directory.Path, version: ClusterLedgerVersion.V2);
        Assert.True(recovered.SecurityInitialized);
        Assert.True(recovered.RequiresClientTokens);
        Assert.Equal(root.Stored.Info.TokenId, recovered.Authenticate(root.Secret, ClusterPermission.Observe, "", Now).TokenId);
        Assert.Single(recovered.GetTokens());
    }

    [Fact]
    public async Task Revocation_is_checked_at_apply_after_admission()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(directory.Path, version: ClusterLedgerVersion.V2);
        var root = Credential();
        var admin = await Initialize(ledger, root.Stored);
        var publisher = Credential(ClusterPermission.Publish | ClusterPermission.CreateEntity, "events");
        await ledger.ApplyAsync(Entry(2, Command(MessageOperation.IssueToken, "publisher", publisher.Stored, admin)), default);
        var admitted = ledger.Authenticate(publisher.Secret, ClusterPermission.Publish, "events", Now);
        var revoke = new MessageCommand(MessageOperation.RevokeToken, Guid.NewGuid(), publisher.Stored.Info.TokenId.ToString("N"), EntityMode.Standard, ReadOnlyMemory<byte>.Empty, admin);
        await ledger.ApplyAsync(Entry(3, revoke), default);
        var late = new MessageCommand(MessageOperation.Append, Guid.NewGuid(), "events", EntityMode.Standard, new byte[] { 1 }, admitted);
        await ledger.ApplyAsync(Entry(4, late), default);
        Assert.Equal("forbidden", ledger.Outcome(late.RequestId)!.Error);
        Assert.Null(ledger.GetEntity("events"));
        Assert.Equal(4, ledger.LastApplied);
        Assert.Throws<ClusterRequestException>(() => ledger.Authenticate(publisher.Secret, ClusterPermission.Publish, "events", Now));
    }

    [Fact]
    public async Task Permissions_entity_scope_and_expiry_are_enforced()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(directory.Path, version: ClusterLedgerVersion.V2);
        var root = Credential();
        var admin = await Initialize(ledger, root.Stored);
        var reader = Credential(ClusterPermission.ReadMetadata, "a");
        await ledger.ApplyAsync(Entry(2, Command(MessageOperation.IssueToken, "reader", reader.Stored, admin)), default);
        Assert.NotNull(ledger.Authenticate(reader.Secret, ClusterPermission.ReadMetadata, "a", Now));
        Assert.Throws<ClusterRequestException>(() => ledger.Authenticate(reader.Secret, ClusterPermission.ReadPayload, "a", Now));
        Assert.Throws<ClusterRequestException>(() => ledger.Authenticate(reader.Secret, ClusterPermission.ReadMetadata, "b", Now));
        Assert.Throws<ClusterRequestException>(() => ledger.Authenticate(reader.Secret, ClusterPermission.ReadMetadata, "a", Now.AddHours(2)));
        Assert.Throws<ClusterRequestException>(() => ledger.Authenticate(reader.Secret + "bad", ClusterPermission.ReadMetadata, "a", Now));
        var unsigned = new MessageCommand(MessageOperation.Append, Guid.NewGuid(), "a", EntityMode.Standard, new byte[] { 1 });
        await ledger.ApplyAsync(Entry(3, unsigned), default);
        Assert.Equal("unauthorized", ledger.Outcome(unsigned.RequestId)!.Error);
    }

    [Fact]
    public async Task Quota_and_pause_are_ordered_with_publications_without_losing_history()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(directory.Path, version: ClusterLedgerVersion.V2);
        var admin = await Initialize(ledger, Credential().Stored, required: false);
        await ledger.ApplyAsync(ClusterLedgerTests.Create(2, "events", EntityMode.Standard), default);
        await ledger.ApplyAsync(Entry(3, Command(MessageOperation.ConfigureEntity, "events", new EntitySettings(false, 4, 16), admin)), default);
        var commands = Enumerable.Range(4, 32).Select(index =>
            Entry(index, new(MessageOperation.Append, Guid.NewGuid(), "events", EntityMode.Standard, new byte[] { 1, 2, 3, 4 }))).ToArray();
        await ledger.ApplyBatchAsync(commands, default);
        Assert.Equal(4, ledger.GetEntity("events")!.LastMessageId);
        Assert.Equal(16, ledger.GetEntity("events")!.RetainedBytes);
        Assert.Equal(28, commands.Count(entry => ledger.Outcome(MessageCommand.Decode(entry.Command).RequestId)!.Error == "entity_quota"));
        await ledger.ApplyAsync(Entry(36, Command(MessageOperation.ConfigureEntity, "events", new EntitySettings(true), admin)), default);
        var paused = new MessageCommand(MessageOperation.Append, Guid.NewGuid(), "events", EntityMode.Standard, new byte[] { 1 });
        await ledger.ApplyAsync(Entry(37, paused), default);
        Assert.Equal("entity_paused", ledger.Outcome(paused.RequestId)!.Error);
        Assert.Equal(4, ledger.ReadBatch("events", 0).Messages.Count);
    }

    [Fact]
    public async Task Cache_quota_counts_only_its_latest_durable_value()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(directory.Path, version: ClusterLedgerVersion.V2);
        var admin = await Initialize(ledger, Credential().Stored, required: false);
        await ledger.ApplyAsync(ClusterLedgerTests.Create(2, "cache", EntityMode.HighSpeedCache), default);
        await ledger.ApplyAsync(Entry(3, Command(MessageOperation.ConfigureEntity, "cache", new EntitySettings(false, 8, 8), admin)), default);
        await ledger.ApplyBatchAsync([ClusterLedgerTests.Append(4, "cache", new byte[8]), ClusterLedgerTests.Append(5, "cache", new byte[8])], default);
        Assert.Equal(2, ledger.GetEntity("cache")!.LastMessageId);
        Assert.Equal(8, ledger.GetEntity("cache")!.RetainedBytes);
        Assert.Equal(1, ledger.GetEntity("cache")!.RetainedMessages);
    }

    [Fact]
    public void Retry_hash_ignores_admission_clock_but_binds_credential_and_content()
    {
        var command = new MessageCommand(MessageOperation.Append, Guid.NewGuid(), "a", EntityMode.Standard, new byte[] { 1 },
            new(Guid.NewGuid(), Now.ToUnixTimeMilliseconds()));
        Assert.Equal(MessageCommand.Hash(command.Encode()), MessageCommand.Hash((command with { Authorization = command.Authorization! with { AuthorizedAtUnixMilliseconds = 123456 } }).Encode()));
        Assert.NotEqual(MessageCommand.Hash(command.Encode()), MessageCommand.Hash((command with { Payload = new byte[] { 2 } }).Encode()));
        Assert.NotEqual(MessageCommand.Hash(command.Encode()), MessageCommand.Hash((command with { Authorization = new(Guid.NewGuid(), 123456) }).Encode()));
    }

    [Fact]
    public async Task V1_reader_rejects_v2_data_and_commands()
    {
        using var directory = new ClusterTestDirectory();
        using (var v2 = new MessageLedger(directory.Path, version: ClusterLedgerVersion.V2))
            await Initialize(v2, Credential().Stored);
        Assert.Throws<InvalidDataException>(() => new MessageLedger(directory.Path));
        using var second = new ClusterTestDirectory();
        using var v1 = new MessageLedger(second.Path);
        await Assert.ThrowsAsync<InvalidDataException>(() => v1.ApplyAsync(Entry(1,
            Command(MessageOperation.BootstrapSecurity, "security", new BootstrapSecurity(Credential().Stored, true))), default).AsTask());
    }

    [Fact]
    public async Task Only_one_durable_admin_intent_can_be_active_and_result_releases_it()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(directory.Path, version: ClusterLedgerVersion.V2);
        var root = Credential();
        var admin = await Initialize(ledger, root.Stored);
        var first = Command(MessageOperation.AdminIntent, "n2", new AdminOperationRequest("remove", "n2"), admin);
        var second = Command(MessageOperation.AdminIntent, "n3", new AdminOperationRequest("remove", "n3"), admin);
        await ledger.ApplyBatchAsync([Entry(2, first), Entry(3, second)], default);
        Assert.Equal(first.RequestId, ledger.ActiveAdminOperation!.OperationId);
        Assert.Equal("membership_busy", ledger.Outcome(second.RequestId)!.Error);
        await ledger.ApplyAsync(Entry(4, Command(MessageOperation.AdminResult, "result", new AdminResult(first.RequestId, "done", null))), default);
        Assert.Null(ledger.ActiveAdminOperation);
        Assert.Equal("Completed", ledger.GetAdminOperation(first.RequestId)!.State);
        Assert.Single(ledger.GetAdminOperations());
        Assert.Equal(4, ledger.GetAudit().Count);
    }

    [Fact]
    public async Task Snapshot_carries_tokens_policies_and_pending_operations_without_token_secrets()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(Path.Combine(directory.Path, "original"), version: ClusterLedgerVersion.V2);
        var root = Credential();
        var auth = new CommandAuthorization(root.Stored.Info.TokenId, Now.ToUnixTimeMilliseconds());
        LogEntry[] entries =
        [
            Entry(1, Command(MessageOperation.BootstrapSecurity, "security", new BootstrapSecurity(root.Stored, true))),
            Entry(2, new(MessageOperation.Append, Guid.NewGuid(), "events", EntityMode.Standard, new byte[] { 1 }, auth)),
            Entry(3, Command(MessageOperation.ConfigureEntity, "events", new EntitySettings(true, 100, 1000), auth)),
            Entry(4, Command(MessageOperation.AdminIntent, "n2", new AdminOperationRequest("remove", "n2"), auth))
        ];
        await ledger.ApplyBatchAsync(entries, default);
        Assert.Equal(-1, entries.SelectMany(entry => entry.Command.ToArray()).ToArray().AsSpan().IndexOf(System.Text.Encoding.UTF8.GetBytes(root.Secret)));
        var context = new SnapshotBuildContext(ledger.Checkpoint, new(["n1"], [], 0), null, new() { NodeId = "n1", Members = ["n1"] },
            (start, count, _, _) => ValueTask.FromResult(new LogReadResult(Entries: entries.Where(entry => entry.Index >= start).Take(count).ToImmutableArray())));
        var path = Path.Combine(directory.Path, "snapshot.zip");
        await ledger.CreateSnapshotAsync(context, path, default);
        var metadata = new SnapshotMetadata(4, 1, ["n1"], [], "admin-image", SHA256.HashData(File.ReadAllBytes(path)).ToImmutableArray());
        var image = new SnapshotImage(metadata, path, new FileInfo(path).Length);
        using var recovered = new MessageLedger(Path.Combine(directory.Path, "restored"), version: ClusterLedgerVersion.V2);
        await recovered.PrepareSnapshotAsync(image, 64 * 1024 * 1024, default);
        await recovered.InstallSnapshotAsync(image, default);
        Assert.True(recovered.GetEntitySettings("events").WritesPaused);
        Assert.NotNull(recovered.ActiveAdminOperation);
        Assert.NotNull(recovered.Authenticate(root.Secret, ClusterPermission.Observe, "", Now));
        Assert.Equal(1, recovered.GetMessage("events", 1)!.PayloadLength);
    }
}
