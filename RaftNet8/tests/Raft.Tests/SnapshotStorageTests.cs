using System.Collections.Immutable;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text.Json;
using Raft.Storage;

namespace Raft.Tests;

public sealed class SnapshotStorageTests
{
    [Fact]
    public async Task Image_retention_keeps_latest_images_and_pins_an_older_active_transfer()
    {
        using var directory = new TestDirectory();
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"], SnapshotDirectory = Path.Combine(directory.Path, "snapshots") };
        using var log = DBreezeLogStorage.CreateInMemory(options);
        log.SaveHardState(new(1));
        var entries = Enumerable.Range(1, 5).Select(i => new LogEntry(i, 1, BitConverter.GetBytes(1))).ToArray();
        log.Append(entries);
        log.SaveHardState(new(1, CommitIndex: 5));
        using var machine = Machine(Path.Combine(directory.Path, "application"));
        await machine.ApplyBatchAsync(entries, default);
        await using var repository = new SnapshotRepository(options);
        var images = new List<SnapshotImage>();
        for (var index = 1; index <= 5; index++)
        {
            var context = new SnapshotBuildContext(new(index, 1), log.Recover().Configuration!, null, options,
                (from, count, bytes, _) => ValueTask.FromResult(log.Query(new LogQuery.Entries(from, count, bytes))));
            images.Add(await repository.CreateAsync(machine, context, default));
        }
        repository.Collect([images[0].Metadata.StorageId]);
        Assert.Equal(3, Directory.GetFiles(Path.Combine(options.SnapshotDirectory, "completed"), "*.bin").Length);
        foreach (var image in new[] { images[0], images[3], images[4] }) await repository.OpenAsync(image.Metadata);
        await Assert.ThrowsAsync<FileNotFoundException>(() => repository.OpenAsync(images[1].Metadata).AsTask());
    }

    internal static void ApplyCounter(DBreeze.Transactions.Transaction tx, LogEntry entry)
    {
        var old = tx.Select<string, long>("counter", "value");
        tx.Insert<string, long>("counter", "value", (old.Exists ? old.Value : 0) + BitConverter.ToInt32(entry.Command.Span));
    }
    internal static DBreezeGenerationStateMachine Machine(string path) => new(path, "counter-v1", "checkpoint", ["counter"], ApplyCounter);
    internal static long Value(DBreezeGenerationStateMachine machine) => machine.Read(tx => tx.Select<string, long>("counter", "value").Value);
    private static SnapshotMetadata Metadata(string id, long index = 5, long term = 2, byte[]? digest = null) =>
        new(index, term, ["n1"], [], id, ImmutableArray.CreateRange(digest ?? new byte[32]));

    [Fact]
    public async Task Replay_image_is_consistent_at_its_fence_while_live_application_continues_and_next_image_reuses_it()
    {
        using var directory = new TestDirectory();
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"], SnapshotDirectory = Path.Combine(directory.Path, "snapshots") };
        using var store = DBreezeLogStorage.CreateInMemory(options);
        store.SaveHardState(new(1));
        var entries = Enumerable.Range(1, 20).Select(i => new LogEntry(i, 1, BitConverter.GetBytes(1))).ToArray();
        store.Append(entries);
        store.SaveHardState(new(1, CommitIndex: 20));
        using var source = Machine(Path.Combine(directory.Path, "source"));
        await source.ApplyBatchAsync(entries.Take(10).ToArray(), default);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async ValueTask<LogReadResult> Read(long from, int count, int bytes, CancellationToken token)
        {
            entered.TrySetResult();
            await release.Task.WaitAsync(token);
            return store.Query(new LogQuery.Entries(from, count, bytes));
        }
        var context = new SnapshotBuildContext(new(10, 1), store.Recover().Configuration!, null, options, Read);
        await using var repository = new SnapshotRepository(options);
        var building = repository.CreateAsync(source, context, default).AsTask();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            await source.ApplyBatchAsync(entries.Skip(10).ToArray(), default);
            Assert.Equal(20, source.LastApplied);
            Assert.Equal(20, Value(source));
        }
        finally { release.TrySetResult(); }
        var image = await building;
        using var target = Machine(Path.Combine(directory.Path, "target"));
        await target.PrepareSnapshotAsync(image, options.MaxSnapshotUnpackedBytes, default);
        await target.InstallSnapshotAsync(image, default);
        Assert.Equal(new(10, 1), target.Checkpoint);
        Assert.Equal(10, Value(target));
        store.Write(new LogWrite.PublishSnapshot(image.Metadata) { RetainEntries = 3 });
        Assert.Equal(8, store.Recover().FirstIndex);
        var nextContext = new SnapshotBuildContext(new(20, 1), store.Recover().Configuration!, image, options,
            (from, count, bytes, _) => ValueTask.FromResult(store.Query(new LogQuery.Entries(from, count, bytes))));
        var next = await repository.CreateAsync(source, nextContext, default);
        await target.PrepareSnapshotAsync(next, options.MaxSnapshotUnpackedBytes, default);
        await target.InstallSnapshotAsync(next, default);
        Assert.Equal(new(20, 1), target.Checkpoint);
        Assert.Equal(20, Value(target));
        Assert.Equal(20, Value(source));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Durable_install_intent_survives_reopen_and_snapshot_publication_atomically_reconciles_suffix(bool retain)
    {
        using var directory = new TestDirectory();
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        var intent = new SnapshotInstallIntent(Metadata("image"), retain);
        using (var store = new DBreezeLogStorage(directory.Path, options))
        {
            store.SaveHardState(new(3));
            store.Append(Enumerable.Range(1, 8).Select(i => new LogEntry(i, retain && i >= 5 ? 2 : 1, BitConverter.GetBytes(i))).ToArray());
            store.SaveHardState(new(3, CommitIndex: 2));
            store.Write(new LogWrite.BeginSnapshotInstall(intent));
            Assert.Equal(2, store.HardState.CommitIndex);
            Assert.NotNull(store.Recover().PendingSnapshotInstall);
        }
        using (var store = new DBreezeLogStorage(directory.Path, options))
        {
            Assert.Equal("image", store.Recover().PendingSnapshotInstall!.Metadata.StorageId);
            store.Write(new LogWrite.CompleteSnapshotInstall(intent));
            Assert.Null(store.Recover().PendingSnapshotInstall);
            Assert.Equal(5, store.HardState.CommitIndex);
            Assert.Equal(retain ? 8 : 5, store.LastIndex);
            Assert.Equal(6, store.Recover().FirstIndex);
            Assert.Equal(2, store.GetTerm(5));
        }
        using var reopened = new DBreezeLogStorage(directory.Path, options);
        Assert.Equal(5, reopened.HardState.CommitIndex);
        Assert.Equal(retain ? 8 : 5, reopened.LastIndex);
        Assert.Equal(retain ? 75 : 0, reopened.Recover().UncommittedBytes);
    }

    [Fact]
    public async Task Chunk_progress_is_durable_resumable_and_whole_image_checksum_is_verified()
    {
        using var directory = new TestDirectory();
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"], SnapshotDirectory = directory.Path, SnapshotChunkBytes = 4096 };
        var bytes = new byte[12000];
        new Random(7).NextBytes(bytes);
        var metadata = Metadata("resumable", digest: SHA256.HashData(bytes));
        SnapshotChunkRequest Chunk(int offset, int count) => new(2, offset + 1, metadata.StorageId, offset, bytes.Length,
            Crc32C.Compute(bytes.AsSpan(offset, count)), offset + count == bytes.Length, metadata, bytes.AsMemory(offset, count));
        await using (var original = new SnapshotRepository(options))
            Assert.Equal(4096, (await original.ReceiveAsync(Chunk(0, 4096), default)).NextOffset);
        await using var resumed = new SnapshotRepository(options);
        Assert.Equal(4096, (await resumed.ReceiveAsync(Chunk(0, 4096), default)).NextOffset);
        Assert.False((await resumed.ReceiveAsync(Chunk(8192, 3808), default)).Accepted);
        Assert.Equal(8192, (await resumed.ReceiveAsync(Chunk(4096, 4096), default)).NextOffset);
        var received = await resumed.ReceiveAsync(Chunk(8192, 3808), default);
        var image = await received.Verification!;
        Assert.NotNull(image);
        var opened = await resumed.OpenAsync(metadata);
        Assert.Equal(bytes, await File.ReadAllBytesAsync(opened.Path));
        Assert.Equal(bytes.Length, (await resumed.ReceiveAsync(Chunk(0, 4096), default)).NextOffset);
    }

    [Fact]
    public async Task A_wrong_whole_image_checksum_rejects_staging_without_publishing_an_image()
    {
        using var directory = new TestDirectory();
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"], SnapshotDirectory = directory.Path };
        await using var repository = new SnapshotRepository(options);
        var metadata = Metadata("bad-digest");
        var chunk = new SnapshotChunkRequest(2, 1, metadata.StorageId, 0, 10, Crc32C.Compute(new byte[10]), true, metadata, new byte[10]);
        var staged = await repository.ReceiveAsync(chunk, default);
        Assert.Null(await staged.Verification!);
        Assert.Empty(Directory.GetFiles(Path.Combine(directory.Path, "completed")));
    }

    [Theory]
    [InlineData("database/../../escape")]
    [InlineData("database/../image.json")]
    [InlineData("database/file:alternate")]
    public async Task Unsafe_archive_paths_never_replace_the_active_application(string name)
    {
        using var directory = new TestDirectory();
        using var machine = Machine(Path.Combine(directory.Path, "application"));
        var path = Path.Combine(directory.Path, "bad.zip");
        using (var file = File.Create(path))
        using (var archive = new ZipArchive(file, ZipArchiveMode.Create))
        {
            using (var manifest = archive.CreateEntry("image.json").Open())
                manifest.Write(StorageCodecV2.Wrap(JsonSerializer.SerializeToUtf8Bytes(new { ApplicationId = "counter-v1", Index = 5L, Term = 2L })));
            using var unsafeEntry = archive.CreateEntry(name).Open();
            unsafeEntry.Write("unsafe"u8);
        }
        var image = new SnapshotImage(Metadata("unsafe-image"), path, new FileInfo(path).Length);
        await Assert.ThrowsAsync<SnapshotValidationException>(() => machine.PrepareSnapshotAsync(image, 1_000_000, default).AsTask());
        Assert.Equal(new(0, 0), machine.Checkpoint);
        Assert.Equal(0, Value(machine));
        Assert.False(File.Exists(Path.Combine(directory.Path, "escape")));
    }
}
