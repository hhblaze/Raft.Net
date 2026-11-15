using System.Collections.Immutable;
using System.Security.Cryptography;
using Raft.Cluster;
using Raft.Cluster.Server;
using Raft.Storage;

namespace Raft.Tests;

internal sealed class ClusterTestDirectory : IDisposable
{
    private readonly string _parent;
    public string Path { get; }
    public ClusterTestDirectory()
    {
        _parent = System.IO.Path.GetFullPath(Environment.GetEnvironmentVariable("RAFT_EXPERIMENT_ROOT") ?? System.IO.Path.Combine(System.IO.Path.GetTempPath(), "RaftNet8"));
        Path = System.IO.Path.Combine(_parent, "client-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path);
    }
    public void Dispose()
    {
        var full = System.IO.Path.GetFullPath(Path);
        if (!full.StartsWith(_parent.TrimEnd(System.IO.Path.DirectorySeparatorChar) + System.IO.Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Test cleanup escaped the experiment root.");
        if (Directory.Exists(full)) Directory.Delete(full, recursive: true);
    }
}

public sealed class ClusterLedgerTests
{
    [Fact]
    public async Task Failed_application_batch_does_not_publish_an_uncommitted_cached_result()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(directory.Path);
        var id = Guid.NewGuid();
        await Assert.ThrowsAsync<InvalidDataException>(() => ledger.ApplyBatchAsync(
            [Append(1, "events", [1], id), new LogEntry(2, 1, [0])], default).AsTask());
        Assert.Null(ledger.CachedOutcome(id));
    }

    [Fact]
    public async Task Historical_snapshot_replay_does_not_publish_results_into_the_live_request_cache()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(directory.Path);
        var entries = Enumerable.Range(1, 1100).Select(index => Append(index, "events", [1], Guid.NewGuid())).ToArray();
        for (var start = 0; start < entries.Length; start += 100)
            await ledger.ApplyBatchAsync(entries.Skip(start).Take(100).ToArray(), default);
        var oldId = MessageCommand.Decode(entries[0].Command).RequestId;
        var lastId = MessageCommand.Decode(entries[^1].Command).RequestId;
        Assert.Null(ledger.CachedOutcome(oldId));
        Assert.Equal(1100, ledger.Outcome(lastId)!.MessageId); // Warm only the live cache before historical replay.
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        var context = new SnapshotBuildContext(new(500, 1), new(["n1"], [], 0), null, options,
            (start, count, _, _) => ValueTask.FromResult(new LogReadResult(Entries: entries.Where(entry => entry.Index >= start).Take(count).ToImmutableArray())));
        await ledger.CreateSnapshotAsync(context, System.IO.Path.Combine(directory.Path, "old-prefix.zip"), default);
        Assert.Null(ledger.CachedOutcome(oldId));
        Assert.Equal(1100, ledger.CachedOutcome(lastId)!.MessageId);
        Assert.Equal(1100, ledger.Checkpoint.Index);
    }

    [Fact]
    public async Task Evicting_a_bounded_request_cache_does_not_lose_the_durable_result()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(directory.Path);
        var requests = Enumerable.Range(1, 1100).Select(_ => Guid.NewGuid()).ToArray();
        for (var start = 0; start < requests.Length; start += 100)
            await ledger.ApplyBatchAsync(Enumerable.Range(start, 100).Select(index => Append(index + 1, "events", [1], requests[index])).ToArray(), default);
        foreach (var request in requests) Assert.NotNull(ledger.Outcome(request));
        Assert.Null(ledger.CachedOutcome(requests[0]));
        Assert.NotNull(ledger.CachedOutcome(requests[^1]));
        Assert.Equal(1, ledger.Outcome(requests[0])!.MessageId);
        await ledger.ApplyAsync(Append(1101, "events", [1], requests[0]), default);
        Assert.Equal(1100, ledger.GetEntity("events")!.LastMessageId);
    }
    [Fact]
    public async Task Restart_after_only_election_noops_accepts_an_empty_ledger()
    {
        using var directory = new ClusterTestDirectory();
        using (var ledger = new MessageLedger(directory.Path)) await ledger.ApplyAsync(new(1, 1, [], true), default);
        using var recovered = new MessageLedger(directory.Path);
        Assert.Equal(1, recovered.LastApplied);
        Assert.Empty(recovered.GetEntities());
        await recovered.ApplyAsync(Append(2, "first", [1]), default);
        Assert.Equal(1, recovered.GetEntity("first")!.LastMessageId);
    }
    internal static LogEntry Append(long index, string entity, byte[] data, Guid? request = null) =>
        new(index, 1, new MessageCommand(MessageOperation.Append, request ?? Guid.NewGuid(), entity, EntityMode.Standard, data).Encode());
    internal static LogEntry Create(long index, string entity, EntityMode mode) =>
        new(index, 1, new MessageCommand(MessageOperation.Create, Guid.NewGuid(), entity, mode, ReadOnlyMemory<byte>.Empty).Encode());

    [Fact]
    public async Task Entity_counters_ignore_consensus_indices_and_duplicate_requests()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(directory.Path);
        var request = Guid.NewGuid();
        await ledger.ApplyBatchAsync([
            new(1, 1, [], true), Append(2, "alpha", [1], request), Append(3, "beta", [2]),
            Append(4, "alpha", [3]), Append(5, "alpha", [1], request),
            Append(6, "beta", [99], request), // conflict must not mutate another entity or fault the node
            new(7, 1, [], true)
        ], default);
        Assert.Equal(7, ledger.LastApplied);
        Assert.Equal(2, ledger.GetEntity("alpha")!.LastMessageId);
        Assert.Equal(1, ledger.GetEntity("beta")!.LastMessageId);
        Assert.Equal(new long[] { 1, 2 }, ledger.ReadBatch("alpha", 0).Messages.Select(message => message.MessageId));
        Assert.Equal(new byte[] { 1 }, ledger.GetMessage("alpha", 1)!.Data.ToArray());
        Assert.Equal(new byte[] { 2 }, ledger.GetMessage("beta", 1)!.Data.ToArray());
    }

    [Fact]
    public async Task Cache_keeps_latest_payload_and_durable_counter_across_restart()
    {
        using var directory = new ClusterTestDirectory();
        using (var ledger = new MessageLedger(directory.Path, 8))
        {
            await ledger.ApplyBatchAsync([Create(1, "cache", EntityMode.HighSpeedCache), Append(2, "cache", [1, 2]),
                Append(3, "cache", [3, 4]), Append(4, "other", [9])], default);
            Assert.Null(ledger.GetMessage("cache", 1));
            Assert.Equal(2, ledger.ReadBatch("cache", 0).Messages.Single().MessageId);
            Assert.Equal(new byte[] { 3, 4 }, ledger.GetMessage("cache", 2)!.Data.ToArray());
        }
        using (var ledger = new MessageLedger(directory.Path, 8))
        {
            Assert.InRange(ledger.CachedPayloadBytes, 2, 8);
            var reads = ledger.PayloadReads;
            Assert.Equal(2, ledger.GetMessage("cache", 2)!.MessageId);
            Assert.Equal(reads, ledger.PayloadReads);
            await ledger.ApplyAsync(Append(5, "cache", [5]), default);
            Assert.Equal(3, ledger.GetEntity("cache")!.LastMessageId);
            Assert.Null(ledger.GetMessage("cache", 2));
            Assert.Equal(3, ledger.ReadBatch("cache", 1).Messages.Single().MessageId);
        }
    }

    [Fact]
    public async Task Id_only_range_does_not_load_payloads_and_respects_entity_boundaries()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(directory.Path);
        var data = new byte[64 * 1024];
        await ledger.ApplyBatchAsync(Enumerable.Range(1, 10).Select(index => Append(index, index <= 8 ? "a" : "ab", data)).ToArray(), default);
        var before = ledger.PayloadReads;
        var batch = ledger.ReadBatch("a", 5, idsOnly: true, maxCount: 2);
        Assert.Equal(new long[] { 6, 7 }, batch.Messages.Select(message => message.MessageId));
        Assert.All(batch.Messages, message => { Assert.True(message.Data.IsEmpty); Assert.Equal(data.Length, message.PayloadLength); });
        Assert.Equal(before, ledger.PayloadReads);
        Assert.Empty(ledger.ReadBatch("a", 8, idsOnly: true).Messages);
        Assert.Empty(ledger.ReadBatch("a", long.MaxValue).Messages);
    }

    [Fact]
    public async Task Resume_after_10000_retrieves_the_next_10000_in_bounded_batches()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(directory.Path, 0);
        for (var start = 1; start <= 20000; start += 100)
            await ledger.ApplyBatchAsync(Enumerable.Range(start, 100).Select(index => Append(index, "events", BitConverter.GetBytes(index))).ToArray(), default);
        long cursor = 10000;
        var count = 0;
        while (true)
        {
            var batch = ledger.ReadBatch("events", cursor);
            if (batch.Messages.Count == 0) break;
            Assert.InRange(batch.Messages.Count, 1, ClusterLimits.MaxBatchCount);
            foreach (var message in batch.Messages)
            {
                Assert.Equal(++cursor, message.MessageId);
                Assert.Equal(cursor, BitConverter.ToInt32(message.Data.Span));
                count++;
            }
        }
        Assert.Equal(10000, count);
        Assert.Equal(20000, cursor);
    }

    [Fact]
    public async Task Snapshot_restores_full_standard_history_cache_and_request_results()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(System.IO.Path.Combine(directory.Path, "original"));
        var request = Guid.NewGuid();
        LogEntry[] entries = [new(1, 1, [], true), Append(2, "history", [1, 2, 3], request),
            Create(3, "cache", EntityMode.HighSpeedCache), Append(4, "cache", [4]), Append(5, "cache", [5]), Append(6, "history", [6])];
        await ledger.ApplyBatchAsync(entries, default);
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        var config = new ClusterConfiguration(["n1"], [], 0);
        var context = new SnapshotBuildContext(ledger.Checkpoint, config, null, options,
            (start, count, _, _) => ValueTask.FromResult(new LogReadResult(Entries: entries.Where(entry => entry.Index >= start).Take(count).ToImmutableArray())));
        var imagePath = System.IO.Path.Combine(directory.Path, "image.zip");
        await ledger.CreateSnapshotAsync(context, imagePath, default);
        var metadata = new SnapshotMetadata(6, 1, ["n1"], [], "cluster-image",
            SHA256.HashData(File.ReadAllBytes(imagePath)).ToImmutableArray());
        using var replacement = new MessageLedger(System.IO.Path.Combine(directory.Path, "replacement"));
        var image = new SnapshotImage(metadata, imagePath, new FileInfo(imagePath).Length);
        await replacement.PrepareSnapshotAsync(image, 64 * 1024 * 1024, default);
        await replacement.InstallSnapshotAsync(image, default);
        Assert.Equal(ledger.Checkpoint, replacement.Checkpoint);
        Assert.Equal(new long[] { 1, 2 }, replacement.ReadBatch("history", 0).Messages.Select(message => message.MessageId));
        Assert.Equal(new byte[] { 1, 2, 3 }, replacement.GetMessage("history", 1)!.Data.ToArray());
        Assert.Null(replacement.GetMessage("cache", 1));
        Assert.Equal(new byte[] { 5 }, replacement.GetMessage("cache", 2)!.Data.ToArray());
        Assert.Equal(1, replacement.Outcome(request)!.MessageId);
    }

    [Fact]
    public async Task Recreating_entity_cannot_change_mode_or_reset_counter()
    {
        using var directory = new ClusterTestDirectory();
        using var ledger = new MessageLedger(directory.Path);
        await ledger.ApplyBatchAsync([Append(1, "a", []), Create(2, "a", EntityMode.HighSpeedCache), Create(3, "a", EntityMode.Standard)], default);
        Assert.Equal(EntityMode.Standard, ledger.GetEntity("a")!.Mode);
        Assert.Equal(1, ledger.GetEntity("a")!.LastMessageId);
        Assert.Empty(ledger.GetMessage("a", 1)!.Data.ToArray());
    }
}
