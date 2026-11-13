using System.IO.Compression;
using System.Text.Json;
using DBreeze;
using DBreeze.Transactions;

namespace Raft.Storage;

/// <summary>
/// Snapshot-capable DBreeze application with separate closed replay/install generations.
/// Keep this directory separate from the Raft log. All business mutations must come through Apply.
/// The deterministic handler writes only its supplied transaction; replay invokes it again.
/// </summary>
public sealed class DBreezeGenerationStateMachine : IRaftBatchStateMachine, ISnapshotStateMachine, IDisposable
{
    private sealed record Manifest(string ApplicationId, long Index, long Term);
    private sealed record Generation(string Id, DBreezeEngine Engine, DBreezeStateMachine Machine);
    private readonly ReaderWriterLockSlim _gate = new();
    private readonly SemaphoreSlim _snapshotOperations = new(1, 1);
    private readonly FileStream _owner;
    private readonly string _root, _generations, _applicationId, _checkpointTable;
    private readonly string[] _tables;
    private readonly Action<Transaction, LogEntry> _apply;
    private readonly Action<SnapshotPersistenceBoundary>? _observe;
    private Generation _active;
    private Exception? _failure;
    private bool _disposed;
    private string? _preparedId;

    public DBreezeGenerationStateMachine(string directory, string applicationId, string checkpointTable,
        IEnumerable<string> applicationWriteTables, Action<Transaction, LogEntry> applyCommand,
        Action<SnapshotPersistenceBoundary>? observe = null)
    {
        if (!Path.IsPathFullyQualified(directory)) throw new ArgumentException("Use an absolute application directory.", nameof(directory));
        RaftOptions.ValidateId(applicationId, nameof(applicationId));
        _root = Path.GetFullPath(directory);
        _generations = Path.Combine(_root, "generations");
        _applicationId = applicationId;
        _checkpointTable = checkpointTable;
        _tables = applicationWriteTables.ToArray();
        _apply = applyCommand;
        _observe = observe;
        Directory.CreateDirectory(_generations);
        _owner = new FileStream(Path.Combine(_root, "owner.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        try
        {
            var pointer = Path.Combine(_root, "current");
            var id = File.Exists(pointer) ? SnapshotFiles.ReadPointer(pointer) : Guid.NewGuid().ToString("N");
            RaftOptions.ValidateId(id, nameof(directory));
            _active = Open(id);
            if (!File.Exists(pointer)) SnapshotFiles.WritePointer(pointer, id);
            foreach (var abandoned in Directory.EnumerateDirectories(_generations))
            {
                var name = Path.GetFileName(abandoned);
                if (name.StartsWith("gen-", StringComparison.Ordinal) && name != "gen-" + id)
                    SnapshotFiles.DeleteOwnedGeneration(_generations, name[4..]);
            }
        }
        catch { _active?.Engine.Dispose(); _owner.Dispose(); throw; }
    }

    private Generation Open(string id)
    {
        var path = SnapshotFiles.Child(_generations, "gen-" + id);
        var engine = new DBreezeEngine(Path.Combine(path, "database"));
        try { return new(id, engine, new DBreezeStateMachine(engine, _checkpointTable, _tables, _apply)); }
        catch { engine.Dispose(); throw; }
    }

    public AppliedCheckpoint Checkpoint => Volatile.Read(ref _active).Machine.Checkpoint;
    public long LastApplied => Checkpoint.Index;

    /// <summary>Read a materialized result inside one short synchronous transaction; never return lazy DBreeze rows.</summary>
    public T Read<T>(Func<Transaction, T> read)
    {
        _gate.EnterReadLock();
        try
        {
            Check();
            using var transaction = _active.Engine.GetTransaction();
            return read(transaction);
        }
        finally { _gate.ExitReadLock(); }
    }

    public ValueTask ApplyAsync(LogEntry entry, CancellationToken cancellationToken) => ApplyBatchAsync([entry], cancellationToken);
    public ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken cancellationToken)
    {
        _gate.EnterReadLock();
        try { Check(); return _active.Machine.ApplyBatchAsync(entries, cancellationToken); }
        catch (Exception error) { Volatile.Write(ref _failure, error); throw; }
        finally { _gate.ExitReadLock(); }
    }

    public async ValueTask CreateSnapshotAsync(SnapshotBuildContext context, string outputPath, CancellationToken token)
    {
        await _snapshotOperations.WaitAsync(token).ConfigureAwait(false);
        var id = "build-" + Guid.NewGuid().ToString("N");
        var work = SnapshotFiles.Child(_generations, "gen-" + id);
        try
        {
            if (context.Previous is { } previous)
                await ExtractAsync(previous, work, context.MaxUnpackedBytes, token).ConfigureAwait(false);
            using (var engine = new DBreezeEngine(Path.Combine(work, "database")))
            {
                var replay = new DBreezeStateMachine(engine, _checkpointTable, _tables, _apply);
                var initial = context.Previous is { } seed ? new AppliedCheckpoint(seed.Metadata.LastIncludedIndex, seed.Metadata.LastIncludedTerm) : new(0, 0);
                if (replay.Checkpoint != initial) throw new SnapshotValidationException("Replay image checkpoint differs from metadata.");
                await foreach (var batch in context.ReadBatchesAsync(token).ConfigureAwait(false))
                    await replay.ApplyBatchAsync(batch, token).ConfigureAwait(false);
                if (replay.Checkpoint != context.Checkpoint) throw new InvalidDataException("Replay did not reach the snapshot fence.");
            }
            var manifest = new Manifest(_applicationId, context.Checkpoint.Index, context.Checkpoint.Term);
            SnapshotFiles.AtomicWrite(Path.Combine(work, "image.json"), StorageCodecV2.Wrap(JsonSerializer.SerializeToUtf8Bytes(manifest)));
            await using var output = new FileStream(outputPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 64 * 1024, FileOptions.Asynchronous);
            using (var archive = new ZipArchive(output, ZipArchiveMode.Create, leaveOpen: true))
            {
                long total = 0;
                var count = 0;
                foreach (var file in Files(work))
                {
                    token.ThrowIfCancellationRequested();
                    var length = new FileInfo(file).Length;
                    if (++count > 8192 || length > context.MaxUnpackedBytes - total)
                        throw new InvalidDataException("Replay generation exceeds snapshot file/byte limits.");
                    total += length;
                    var name = Path.GetRelativePath(work, file).Replace('\\', '/');
                    var entry = archive.CreateEntry(name, CompressionLevel.NoCompression);
                    await using var target = entry.Open();
                    await using var source = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
                    await source.CopyToAsync(target, 64 * 1024, token).ConfigureAwait(false);
                }
            }
            output.Flush(flushToDisk: true);
        }
        finally
        {
            try { SnapshotFiles.DeleteOwnedGeneration(_generations, id); }
            finally { _snapshotOperations.Release(); }
        }
    }

    public async ValueTask PrepareSnapshotAsync(SnapshotImage image, long maxUnpackedBytes, CancellationToken token)
    {
        await _snapshotOperations.WaitAsync(token).ConfigureAwait(false);
        try
        {
        Check();
        RaftOptions.ValidateId(image.Metadata.StorageId, nameof(image));
        var id = image.Metadata.StorageId;
        var work = SnapshotFiles.Child(_generations, "gen-" + id);
        if (Volatile.Read(ref _active).Id == id) return;
        if (_preparedId is { } old && old != id && old != _active.Id) SnapshotFiles.DeleteOwnedGeneration(_generations, old);
        _preparedId = id;
        var ready = Path.Combine(work, "ready");
        if (File.Exists(ready) && File.ReadAllBytes(ready).AsSpan().SequenceEqual(StorageCodecV2.Snapshot(image.Metadata))) return;
        SnapshotFiles.DeleteOwnedGeneration(_generations, id);
        await ExtractAsync(image, work, maxUnpackedBytes, token).ConfigureAwait(false);
        using (var engine = new DBreezeEngine(Path.Combine(work, "database")))
        {
            var machine = new DBreezeStateMachine(engine, _checkpointTable, _tables, _apply);
            if (machine.Checkpoint != new AppliedCheckpoint(image.Metadata.LastIncludedIndex, image.Metadata.LastIncludedTerm))
                throw new SnapshotValidationException("Installed DBreeze checkpoint differs from snapshot metadata.");
        }
        SnapshotFiles.AtomicWrite(ready, StorageCodecV2.Snapshot(image.Metadata));
        }
        finally { _snapshotOperations.Release(); }
    }

    public async ValueTask InstallSnapshotAsync(SnapshotImage image, CancellationToken token)
    {
        await _snapshotOperations.WaitAsync(token).ConfigureAwait(false);
        _gate.EnterWriteLock();
        Generation? candidate = null;
        try
        {
            Check();
            var metadata = image.Metadata;
            if (_active.Id == metadata.StorageId)
            {
                if (Checkpoint.Index < metadata.LastIncludedIndex) throw new InvalidDataException("Activated generation is behind its image.");
                return;
            }
            if (metadata.LastIncludedIndex < Checkpoint.Index) throw new InvalidOperationException("Snapshot would roll back applied application state.");
            var ready = SnapshotFiles.Child(_generations, "gen-" + metadata.StorageId + "/ready");
            if (!File.Exists(ready) || !File.ReadAllBytes(ready).AsSpan().SequenceEqual(StorageCodecV2.Snapshot(metadata)))
                throw new InvalidOperationException("Prepare and verify the snapshot generation before activation.");
            candidate = Open(metadata.StorageId);
            if (candidate.Machine.Checkpoint != new AppliedCheckpoint(metadata.LastIncludedIndex, metadata.LastIncludedTerm))
                throw new InvalidDataException("Prepared generation checkpoint changed before activation.");
            _observe?.Invoke(SnapshotPersistenceBoundary.BeforeGenerationSwitch);
            SnapshotFiles.WritePointer(Path.Combine(_root, "current"), candidate.Id);
            _observe?.Invoke(SnapshotPersistenceBoundary.AfterGenerationPointer);
            var previous = _active;
            Volatile.Write(ref _active, candidate);
            candidate = null;
            previous.Engine.Dispose();
            _observe?.Invoke(SnapshotPersistenceBoundary.AfterGenerationPublication);
            SnapshotFiles.DeleteOwnedGeneration(_generations, previous.Id);
            return;
        }
        catch (Exception error) { _failure = error; candidate?.Engine.Dispose(); throw; }
        finally { _gate.ExitWriteLock(); _snapshotOperations.Release(); }
    }

    private async ValueTask ExtractAsync(SnapshotImage image, string destination, long maxBytes, CancellationToken token)
    {
        Directory.CreateDirectory(destination);
        await using var input = new FileStream(image.Path, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
        using var archive = new ZipArchive(input, ZipArchiveMode.Read);
        if (archive.Entries.Count is < 1 or > 8192) throw new SnapshotValidationException("Invalid snapshot file count.");
        var manifestEntry = archive.GetEntry("image.json");
        if (manifestEntry is null || manifestEntry.Length > 4096) throw new SnapshotValidationException("Missing/oversized image manifest.");
        using (var manifestStream = manifestEntry.Open())
        {
            var bytes = new byte[(int)manifestEntry.Length];
            manifestStream.ReadExactly(bytes);
            if (manifestStream.ReadByte() != -1) throw new SnapshotValidationException("Oversized image manifest.");
            var manifest = JsonSerializer.Deserialize<Manifest>(StorageCodecV2.Open(bytes));
            if (manifest is null || manifest.ApplicationId != _applicationId || manifest.Index != image.Metadata.LastIncludedIndex || manifest.Term != image.Metadata.LastIncludedTerm)
                throw new SnapshotValidationException("Snapshot application format/checkpoint mismatch.");
        }
        long total = 0;
        var names = new HashSet<string>(OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
        foreach (var entry in archive.Entries)
        {
            token.ThrowIfCancellationRequested();
            if ((entry.FullName != "image.json" && !entry.FullName.StartsWith("database/", StringComparison.Ordinal)) ||
                !names.Add(entry.FullName) || entry.Length < 0 || entry.Length > maxBytes - total)
                throw new SnapshotValidationException("Unsafe snapshot entry or unpacked byte limit exceeded.");
            total += entry.Length;
            var path = SnapshotFiles.Child(destination, entry.FullName);
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            await using var target = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 64 * 1024, FileOptions.Asynchronous);
            await using var source = entry.Open();
            await CopyBoundedAsync(source, target, entry.Length, token).ConfigureAwait(false);
            if (target.Length != entry.Length) throw new SnapshotValidationException("Snapshot file length mismatch.");
            target.Flush(flushToDisk: true);
        }
    }

    private static async ValueTask CopyBoundedAsync(Stream source, Stream target, long expected, CancellationToken token)
    {
        var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            long written = 0;
            while (true)
            {
                var count = await source.ReadAsync(buffer.AsMemory(0, (int)Math.Min(buffer.Length, expected - written + 1)), token).ConfigureAwait(false);
                if (count == 0) break;
                if (count > expected - written) throw new SnapshotValidationException("Snapshot expands beyond its declared file size.");
                await target.WriteAsync(buffer.AsMemory(0, count), token).ConfigureAwait(false);
                written += count;
            }
            if (written != expected) throw new SnapshotValidationException("Truncated snapshot file.");
        }
        finally { System.Buffers.ArrayPool<byte>.Shared.Return(buffer); }
    }

    private static IEnumerable<string> Files(string root)
    {
        var pending = new Stack<string>();
        pending.Push(root);
        while (pending.TryPop(out var directory))
        {
            foreach (var entry in Directory.EnumerateFileSystemEntries(directory))
            {
                var attributes = File.GetAttributes(entry);
                if ((attributes & FileAttributes.ReparsePoint) != 0) throw new IOException("Snapshot generation contains a reparse point.");
                if ((attributes & FileAttributes.Directory) != 0) pending.Push(entry);
                else if (Path.GetFileName(entry) != "ready") yield return entry;
            }
        }
    }
    private void Check()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (Volatile.Read(ref _failure) is { } failure) throw new InvalidOperationException("Application generation failed; recover before use.", failure);
    }
    public void Dispose()
    {
        _snapshotOperations.Wait();
        _gate.EnterWriteLock();
        try { if (_disposed) return; _disposed = true; try { _active.Engine.Dispose(); } finally { _owner.Dispose(); } }
        finally { _gate.ExitWriteLock(); _snapshotOperations.Release(); }
    }
}
