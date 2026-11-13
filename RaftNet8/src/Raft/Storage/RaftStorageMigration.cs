using System.Security.Cryptography;
using System.Text.Json;
using DBreeze;

namespace Raft.Storage;

/// <summary>Offline, copy-based migration. Stop every application using the source before invoking it.</summary>
public static class RaftStorageMigration
{
    public sealed record Result(string Source, string Destination, long LastIndex, long CommitIndex, int CopiedFiles);
    private sealed record CopiedFile(string Name, long Length, string Sha256);

    public static Result CopyV1ToV2(string sourcePath, string destinationPath, RaftOptions options,
        Action<long>? progress = null, CancellationToken cancellationToken = default) =>
        Copy(sourcePath, destinationPath, options, false, progress, cancellationToken);

    /// <summary>Copies a stopped v2 database into v3, preserving files/tables within that database folder.
    /// Application roots and snapshot repositories outside it must be copied separately while stopped.</summary>
    public static Result CopyV2ToV3(string sourcePath, string destinationPath, RaftOptions options,
        Action<long>? progress = null, CancellationToken cancellationToken = default) =>
        Copy(sourcePath, destinationPath, options, true, progress, cancellationToken);

    private static Result Copy(string sourcePath, string destinationPath, RaftOptions options, bool toV3,
        Action<long>? progress, CancellationToken cancellationToken)
    {
        options = options.Freeze();
        var source = Path.GetFullPath(sourcePath).TrimEnd(Path.DirectorySeparatorChar);
        var destination = Path.GetFullPath(destinationPath).TrimEnd(Path.DirectorySeparatorChar);
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        if (destination.Equals(source, comparison) || destination.StartsWith(source + Path.DirectorySeparatorChar, comparison))
            throw new ArgumentException("Migration destination must be outside the source directory.");
        if (!Directory.Exists(source) || Directory.Exists(destination) || File.Exists(destination))
            throw new ArgumentException("Source must exist and destination must not exist.");
        // Validate identity and exclusive availability, then close the engine before physical copying.
        using (var engine = new DBreezeEngine(source))
        {
            // The ordinary v1 constructor initializes a missing identity. Migration must never
            // turn an unrelated/historical database into an empty v1 log, or write Raft metadata
            // into that source as a side effect of probing its format.
            using (var probe = engine.GetTransaction())
            {
                var table = toV3 ? DBreezeLogStorage.MetaTable : "raft8/meta";
                var identity = probe.Select<string, byte[]>(table, "identity");
                if (!identity.Exists || !probe.Select<string, byte[]>(table, "state").Exists ||
                    (!toV3 && !identity.Value.AsSpan().StartsWith("raft8-v1\n"u8)))
                    throw new InvalidDataException($"Migration requires an initialized raft8-v{(toV3 ? 2 : 1)} database; historical/unrecognized formats need an application-specific migration.");
                if (toV3)
                {
                    var stored = StorageCodecV2.Identity(identity.Value);
                    if (stored.GroupId != options.GroupId || stored.NodeId != options.NodeId ||
                        !probe.Select<string, byte[]>(table, "frontier").Exists ||
                        (engine.Scheme.IfUserTableExists(DBreezeLogStorage.UnifiedTable) &&
                         probe.SelectForward<byte[], byte[]>(DBreezeLogStorage.UnifiedTable).Any()))
                        throw new InvalidDataException("Source identity/format is not the requested v2 database.");
                }
            }
            // V2 record/frontier validation is done on the copy: its recovery may
            // discard unpromised tail rows and must never change the source log.
            using var original = toV3 ? null : new DBreezeRaftStorage(engine, options);
            foreach (var table in engine.Scheme.GetUserTableNamesStartingWith(string.Empty))
            {
                var path = engine.Scheme.GetTablePathFromTableName(table);
                if (path == "MEMORY" || !Path.GetFullPath(path).StartsWith(source + Path.DirectorySeparatorChar, comparison))
                    throw new NotSupportedException("Offline copy migration requires all durable tables inside the source folder.");
            }
        }
        var files = EnumerateFiles(source, allowCompletedV2Migration: toV3).Order(StringComparer.Ordinal).ToArray();
        if (files.Length > 8192) throw new IOException("Too many files for the offline read-lock migration; use a filesystem snapshot.");
        var handles = new List<(string Name, FileStream Stream)>();
        var copied = new List<CopiedFile>();
        try
        {
            // Deny writable opens during the copy. The operator must still keep all source applications stopped.
            foreach (var file in files)
            {
                cancellationToken.ThrowIfCancellationRequested();
                handles.Add((Path.GetRelativePath(source, file), new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read)));
            }
            Directory.CreateDirectory(destination);
            DurableFile(Path.Combine(destination, ".raft-migration-incomplete"), toV3 ? "v2-to-v3\n"u8 : "v1-to-v2\n"u8);
            foreach (var file in handles)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var target = Path.GetFullPath(Path.Combine(destination, file.Name));
                if (!target.StartsWith(destination + Path.DirectorySeparatorChar, comparison)) throw new IOException("Invalid migration path.");
                Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                using var output = new FileStream(target, FileMode.CreateNew, FileAccess.Write, FileShare.None);
                using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
                var buffer = new byte[64 * 1024];
                int count;
                while ((count = file.Stream.Read(buffer)) != 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    output.Write(buffer.AsSpan(0, count));
                    hash.AppendData(buffer.AsSpan(0, count));
                }
                output.Flush(flushToDisk: true);
                copied.Add(new(file.Name, output.Length, Convert.ToHexString(hash.GetHashAndReset())));
            }
            if (!files.SequenceEqual(EnumerateFiles(source, allowCompletedV2Migration: toV3).Order(StringComparer.Ordinal)))
                throw new IOException("Source files changed during migration; keep the source offline.");
            HardState hard;
            long last;
            if (toV3)
                (hard, last) = ConvertV2Copy(destination, options, progress, cancellationToken);
            else
            {
            using (var engine = new DBreezeEngine(destination))
            {
                using var legacy = new DBreezeRaftStorage(engine, options);
                using var modern = DBreezeLogStorage.OpenMigrationTarget(engine, options);
                hard = legacy.HardState;
                last = legacy.LastIndex;
                modern.SaveHardState(new(hard.Term, hard.VotedFor));
                long next = 1;
                while (next <= last)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var batch = legacy.Read(next, options.MaxAppendEntries, options.MaxAppendBytes);
                    modern.Append(batch);
                    next = batch[^1].Index + 1;
                    progress?.Invoke(next - 1);
                }
                modern.SaveHardState(hard);
                if (engine.Scheme.IfUserTableExists("demo/checkpoint"))
                    CompleteIndexOnlyCheckpoint(engine, modern, "demo/checkpoint", "lastApplied", "lastAppliedTerm", writeProtectedCheckpoint: false);
            }
            cancellationToken.ThrowIfCancellationRequested();
            // Reopen and validate every migrated record before atomically marking the target ready.
            using (var engine = new DBreezeEngine(destination))
            {
                using (var verified = DBreezeLogStorage.OpenMigrationTarget(engine, options))
                    if (verified.HardState != hard || verified.LastIndex != last)
                        throw new InvalidDataException("Migrated state differs from source.");
                using var tx = engine.GetTransaction();
                tx.SynchronizeTables(DBreezeLogStorage.MetaTable, "raft8/meta");
                tx.Insert<string, byte[]>(DBreezeLogStorage.MetaTable, "migration", StorageCodecV2.Wrap("complete"u8));
                // Old binaries must not accidentally start the copied database after v2 activation.
                tx.Insert<string, byte[]>("raft8/meta", "identity", "raft8-v2-migrated-use-v2"u8.ToArray());
                tx.Commit();
            }
            }
            var manifest = JsonSerializer.SerializeToUtf8Bytes(new { Format = toV3 ? "raft8-v3" : "raft8-v2", Source = source,
                LastIndex = last, HardState = hard, Files = copied });
            DurableFile(Path.Combine(destination, toV3 ? "raft-v3-migration.json" : "raft-migration.json"), manifest);
            File.Delete(Path.Combine(destination, ".raft-migration-incomplete"));
            return new(source, destination, last, hard.CommitIndex, copied.Count);
        }
        finally { foreach (var file in handles) file.Stream.Dispose(); }
    }

    private static (HardState Hard, long Last) ConvertV2Copy(string destination, RaftOptions options,
        Action<long>? progress, CancellationToken cancellationToken)
    {
        using var engine = new DBreezeEngine(destination);
        using var original = new DBreezeLogStorage(engine, options);
        var before = original.Recover(); // Validates every retained record and configuration index on the copy.
        using (var tx = engine.GetTransaction())
        {
            tx.SynchronizeTables(DBreezeLogStorage.UnifiedTable);
            foreach (var row in tx.SelectForward<string, byte[]>(DBreezeLogStorage.MetaTable))
                if (row.Key is not "migration" and not "state" and not "frontier" and not "manifest") tx.Insert<byte[], byte[]>(DBreezeLogStorage.UnifiedTable,
                    DBreezeLogTransaction.Key(1, row.Key), row.Value);
            var tail = StorageCodecV2.Tail(tx.Select<string, byte[]>(DBreezeLogStorage.MetaTable, "frontier").Value);
            tx.Insert<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, DBreezeLogTransaction.Key(1, "manifest"),
                StorageCodecV2.Manifest(before.HardState, tail));
            tx.Insert<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, DBreezeLogTransaction.Key(1, "state"),
                StorageCodecV2.Wrap("packed-v3"u8));
            tx.Insert<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, DBreezeLogTransaction.Key(1, "migration"),
                StorageCodecV2.Wrap("incomplete"u8));
            tx.Commit();
        }
        CopyRows(DBreezeLogStorage.LogTable, 2, progress);
        CopyRows(DBreezeLogStorage.ConfigurationTable, 3, null);
        cancellationToken.ThrowIfCancellationRequested();
        using (var tx = engine.GetTransaction())
        {
            // Publish the layout switch only on the destination. Every old reader
            // must refuse it, including v1 binaries left by an earlier migration.
            tx.SynchronizeTables(DBreezeLogStorage.UnifiedTable, DBreezeLogStorage.MetaTable,
                DBreezeLogStorage.LogTable, DBreezeLogStorage.ConfigurationTable, "raft8/meta");
            tx.RemoveAllKeys(DBreezeLogStorage.LogTable, false);
            tx.RemoveAllKeys(DBreezeLogStorage.ConfigurationTable, false);
            tx.RemoveAllKeys(DBreezeLogStorage.MetaTable, false);
            tx.Insert<string, byte[]>(DBreezeLogStorage.MetaTable, "state", StorageCodecV2.Wrap("raft8-v3"u8));
            tx.RemoveKey<string>("raft8/meta", "identity");
            tx.Insert<string, byte[]>("raft8/meta", "state", "raft8-v3"u8.ToArray());
            tx.Commit();
        }
        using (var verified = DBreezeLogStorage.OpenMigrationTarget(engine, options, DBreezeLogLayout.UnifiedTableV3))
        {
            var after = verified.Recover();
            if (after.HardState != before.HardState || after.LastIndex != before.LastIndex ||
                after.LastTerm != before.LastTerm || after.UncommittedBytes != before.UncommittedBytes ||
                !after.Configuration!.Encode().AsSpan().SequenceEqual(before.Configuration!.Encode()))
                throw new InvalidDataException("Migrated v3 state differs from the verified v2 copy.");
        }
        cancellationToken.ThrowIfCancellationRequested();
        using (var tx = new DBreezeLogTransaction(engine.GetTransaction(), DBreezeLogLayout.UnifiedTableV3, writable: true))
        {
            tx.SynchronizeTables(DBreezeLogStorage.UnifiedTable);
            tx.Insert<string, byte[]>(DBreezeLogStorage.MetaTable, "migration", StorageCodecV2.Wrap("complete"u8));
            tx.Commit();
        }
        return (before.HardState, before.LastIndex);

        void CopyRows(string table, byte slot, Action<long>? onProgress)
        {
            long from = 1;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var count = 0;
                long bytes = 0, last = 0;
                using (var tx = engine.GetTransaction())
                {
                    tx.SynchronizeTables(DBreezeLogStorage.UnifiedTable);
                        foreach (var row in tx.SelectForwardStartFrom<long, byte[]>(table, from, true))
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            var value = row.Value;
                            if (count > 0 && bytes + value.Length > options.MaxAppendBytes) break;
                            tx.Insert<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, DBreezeLogTransaction.Key(slot, row.Key), value);
                            bytes += value.Length;
                            last = row.Key;
                            if (++count == options.MaxAppendEntries) break;
                        }
                    if (count == 0) return;
                    tx.Commit();
                }
                onProgress?.Invoke(last);
                if (last == long.MaxValue) return;
                from = last + 1;
            }
        }
    }

    /// <summary>Offline application adapter migration; effects already correspond to the copied legacy index.</summary>
    public static AppliedCheckpoint CompleteIndexOnlyCheckpoint(DBreezeEngine engine, IRaftStorage log,
        string checkpointTable, string indexKey, string? termKey = null, bool writeProtectedCheckpoint = true)
    {
        long index;
        using (var read = engine.GetTransaction())
        {
            var row = read.Select<string, long>(checkpointTable, indexKey);
            index = row.Exists ? row.Value : 0;
        }
        if (index < 0 || index > log.HardState.CommitIndex) throw new InvalidDataException("Legacy application checkpoint is not committed.");
        var checkpoint = new AppliedCheckpoint(index, index == 0 ? 0 : log.GetTerm(index));
        using var tx = engine.GetTransaction();
        tx.SynchronizeTables(checkpointTable);
        var current = tx.Select<string, long>(checkpointTable, indexKey);
        if ((current.Exists ? current.Value : 0) != index) throw new InvalidDataException("Checkpoint changed during offline migration.");
        if (termKey is not null)
        {
            var term = tx.Select<string, long>(checkpointTable, termKey);
            if (term.Exists && term.Value != checkpoint.Term) throw new InvalidDataException("Legacy checkpoint term disagrees with the log.");
            tx.Insert<string, long>(checkpointTable, termKey, checkpoint.Term);
        }
        if (writeProtectedCheckpoint)
            tx.Insert<string, byte[]>(checkpointTable, "applied", DBreezeStateMachine.EncodeCheckpoint(checkpoint));
        tx.Commit();
        return checkpoint;
    }

    private static IEnumerable<string> EnumerateFiles(string root, bool allowCompletedV2Migration = false)
    {
        var pending = new Stack<string>();
        pending.Push(root);
        while (pending.TryPop(out var directory))
            foreach (var item in Directory.EnumerateFileSystemEntries(directory))
            {
                var attributes = File.GetAttributes(item);
                if ((attributes & FileAttributes.ReparsePoint) != 0) throw new IOException("Migration does not follow reparse points.");
                if ((attributes & FileAttributes.Directory) != 0) pending.Push(item);
                else
                {
                    if (Path.GetFileName(item) is ".raft-migration-incomplete" or "raft-v3-migration.json" ||
                        (!allowCompletedV2Migration && Path.GetFileName(item) == "raft-migration.json"))
                        throw new IOException("Source has migration artifacts; inspect it before migrating.");
                    yield return item;
                }
            }
    }
    private static void DurableFile(string path, ReadOnlySpan<byte> bytes)
    {
        using var file = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        file.Write(bytes);
        file.Flush(flushToDisk: true);
    }
}
