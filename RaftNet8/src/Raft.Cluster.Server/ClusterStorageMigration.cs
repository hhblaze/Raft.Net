using System.Buffers.Binary;
using System.Collections.Immutable;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using DBreeze;
using DBreeze.Utils;
using Raft.Storage;

namespace Raft.Cluster.Server;

/// <summary>Offline, copy-only application upgrade. Source files are held read-only against concurrent writers.</summary>
public static class ClusterStorageMigration
{
    public sealed record Result(string Source, string Destination, AppliedCheckpoint Checkpoint, int Files, int Images, int Entities);
    private sealed record ImageManifest(string ApplicationId, long Index, long Term);
    private sealed record Descriptor(SnapshotMetadata Metadata, long Length);
    private const string Table = "cluster-v1"; // Physical table name remains stable; its explicit format marker changes.

    public static Result CopyNodeV1ToV2(string sourcePath, string destinationPath, RaftOptions options, CancellationToken token = default)
    {
        var source = Path.GetFullPath(sourcePath).TrimEnd(Path.DirectorySeparatorChar);
        var destination = Path.GetFullPath(destinationPath).TrimEnd(Path.DirectorySeparatorChar);
        if (!Directory.Exists(source) || Directory.Exists(destination) || File.Exists(destination) ||
            source.Equals(destination, StringComparison.OrdinalIgnoreCase) || destination.StartsWith(source + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase) ||
            source.StartsWith(destination + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("Use an existing stopped source and a new independent destination.");
        if (!Directory.Exists(Path.Combine(source, "messages")) || !Directory.Exists(Path.Combine(source, "log")))
            throw new InvalidDataException("Expected a RaftClusterNode data root containing log and messages.");
        if (options.SnapshotDirectory is { } images && Path.GetFullPath(images).TrimEnd(Path.DirectorySeparatorChar) != Path.Combine(source, "images"))
            throw new ArgumentException("This converter requires the node's snapshots under its data-root/images directory.");
        var paths = Files(source).Take(32769).ToArray();
        if (paths.Length > 32768) throw new IOException("File count exceeds offline migration bound.");
        var locks = new List<FileStream>();
        try
        {
            // Opening any live DBreeze or node owner file fails here before a destination is created.
            foreach (var path in paths) locks.Add(new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024, FileOptions.SequentialScan));
            var total = locks.Sum(file => file.Length);
            if (new DriveInfo(Path.GetPathRoot(destination)!).AvailableFreeSpace < checked(total * 3 + 64 * 1024 * 1024))
                throw new IOException("Migration needs at least3x the source size plus64MiB on the destination volume.");
            Directory.CreateDirectory(destination);
            for (var index = 0; index < paths.Length; index++)
            {
                token.ThrowIfCancellationRequested();
                var target = Child(destination, Path.GetRelativePath(source, paths[index]));
                Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                using var output = new FileStream(target, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 64 * 1024);
                locks[index].CopyTo(output);
                output.Flush(true);
                locks[index].Position = 0; output.Position = 0;
                if (!SHA256.HashData(locks[index]).AsSpan().SequenceEqual(SHA256.HashData(output)))
                    throw new IOException("Copied source file digest differs from its destination.");
            }
            var copiedOptions = RaftClusterNode.ConfigureMessageLimits(options) with { SnapshotDirectory = Path.Combine(destination, "images") };
            using var log = new DBreezeLogStorage(Path.Combine(destination, "log"), copiedOptions, DBreezeLogLayout.UnifiedTableV4);
            var recovery = log.Recover();
            if (recovery.PendingSnapshotInstall is not null || File.Exists(Path.Combine(destination, "images", "incoming", "progress")))
                throw new InvalidDataException("Finish snapshot installation with the old application before migrating.");
            var appRoot = Path.Combine(destination, "messages");
            var id = Encoding.UTF8.GetString(OpenEnvelope(ReadMetadata(Path.Combine(appRoot, "current"))));
            if (!Guid.TryParseExact(id, "N", out _)) throw new InvalidDataException("Invalid application generation pointer.");
            var active = Child(Path.Combine(appRoot, "generations"), "gen-" + id);
            var converted = ConvertDatabase(Path.Combine(active, "database"), token);
            if (converted.Checkpoint.Index > recovery.HardState.CommitIndex ||
                converted.Checkpoint.Index < (recovery.Snapshot?.LastIncludedIndex ?? 0))
                throw new InvalidDataException("Application checkpoint is outside the durable committed/snapshot boundary.");
            var manifest = Path.Combine(active, "image.json");
            if (File.Exists(manifest)) RewriteManifest(manifest);
            var completed = Path.Combine(destination, "images", "completed");
            var imageCount = 0;
            if (Directory.Exists(completed))
                foreach (var descriptorPath in Directory.EnumerateFiles(completed, "*.json"))
                {
                    token.ThrowIfCancellationRequested();
                    var descriptor = JsonSerializer.Deserialize<Descriptor>(OpenEnvelope(ReadMetadata(descriptorPath)))
                        ?? throw new InvalidDataException("Missing snapshot descriptor.");
                    if (Path.GetFileNameWithoutExtension(descriptorPath) != descriptor.Metadata.StorageId)
                        throw new InvalidDataException("Snapshot descriptor identity mismatch.");
                    var imagePath = Child(completed, descriptor.Metadata.StorageId + ".bin");
                    using (var input = File.OpenRead(imagePath))
                        if (input.Length != descriptor.Length || !SHA256.HashData(input).AsSpan().SequenceEqual(descriptor.Metadata.Digest.AsSpan()))
                            throw new InvalidDataException("Snapshot digest/length mismatch.");
                    var replacement = ConvertImage(imagePath, descriptor.Metadata, copiedOptions, destination, token);
                    var updated = descriptor with { Metadata = descriptor.Metadata with { Digest = replacement.Digest }, Length = replacement.Length };
                    File.WriteAllBytes(descriptorPath, WrapEnvelope(JsonSerializer.SerializeToUtf8Bytes(updated)));
                    if (recovery.Snapshot?.StorageId == descriptor.Metadata.StorageId)
                    {
                        var retain = (int)Math.Clamp(descriptor.Metadata.LastIncludedIndex - recovery.FirstIndex + 1, 0, int.MaxValue);
                        log.Write(new LogWrite.PublishSnapshot(updated.Metadata) { RetainEntries = retain });
                    }
                    imageCount++;
                }
            using (var verified = new MessageLedger(appRoot, version: ClusterLedgerVersion.V2))
                if (verified.Checkpoint != converted.Checkpoint || verified.GetEntities().Count != converted.Entities)
                    throw new InvalidDataException("Converted application verification failed.");
            var result = new Result(source, destination, converted.Checkpoint, paths.Length, imageCount, converted.Entities);
            File.WriteAllBytes(Path.Combine(destination, "cluster-v2-migration.json"), JsonSerializer.SerializeToUtf8Bytes(result, new JsonSerializerOptions { WriteIndented = true }));
            return result;
        }
        finally { foreach (var file in locks) file.Dispose(); }
    }
    private static (AppliedCheckpoint Checkpoint, int Entities) ConvertDatabase(string path, CancellationToken token)
    {
        using var engine = new DBreezeEngine(path);
        using var tx = engine.GetTransaction();
        tx.SynchronizeTables(Table);
        var format = tx.Select<byte[], string>(Table, 6.ToIndex("format"));
        if (format.Exists && format.Value != "raft-cluster-v1") throw new InvalidDataException("The source is not a cluster-v1 application.");
        var checkpoint = tx.Select<string, byte[]>(Table, "applied");
        var fence = checkpoint.Exists ? DBreezeStateMachine.DecodeCheckpoint(checkpoint.Value) : new(0, 0);
        var prefix = 1.ToIndex();
        var entities = new List<EntityInfo>();
        foreach (var row in tx.SelectForwardStartFrom<byte[], byte[]>(Table, prefix, true))
        {
            if (!row.Key.AsSpan().StartsWith(prefix)) break;
            if (entities.Count >= ClusterLimits.MaxEntities) throw new InvalidDataException("Entity catalog exceeds its bound.");
            entities.Add(JsonSerializer.Deserialize<EntityInfo>(row.Value) ?? throw new InvalidDataException("Invalid entity."));
        }
        if (!format.Exists && entities.Count > 0) throw new InvalidDataException("Missing source application format.");
        foreach (var entity in entities)
        {
            token.ThrowIfCancellationRequested();
            long count = 0, bytes = 0, latest = 0;
            var latestLength = 0;
            var headers = 2.ToIndex(entity.EntityId);
            foreach (var row in tx.SelectForwardStartFrom<byte[], int>(Table, headers, true))
            {
                token.ThrowIfCancellationRequested();
                if (!row.Key.AsSpan().StartsWith(headers)) break;
                if (row.Key.Length != headers.Length + 8) throw new InvalidDataException("Invalid message key.");
                var messageId = BinaryPrimitives.ReadInt64BigEndian(row.Key.AsSpan(headers.Length)) ^ long.MinValue;
                ClusterLimits.ValidatePayloadLength(row.Value);
                if (entity.Mode == EntityMode.Standard && messageId != count + 1) throw new InvalidDataException("Standard history has a gap.");
                if (messageId <= latest || messageId > entity.LastMessageId) throw new InvalidDataException("Invalid retained message ID.");
                var payload = tx.Select<byte[], byte[]>(Table, 3.ToIndex(entity.EntityId, messageId));
                if (!payload.Exists || payload.Value.Length != row.Value) throw new InvalidDataException("Retained payload length mismatch.");
                count++; bytes = checked(bytes + row.Value); latest = messageId; latestLength = row.Value;
            }
            if (latest != entity.LastMessageId || entity.Mode == EntityMode.HighSpeedCache && count > 1)
                throw new InvalidDataException("Entity counter differs from retained messages.");
            var migrated = entity with { RetainedMessages = count, RetainedBytes = bytes, LatestPayloadBytes = latestLength, PublishedBytes = bytes };
            tx.Insert<byte[], byte[]>(Table, 1.ToIndex(entity.Name), JsonSerializer.SerializeToUtf8Bytes(migrated));
        }
        tx.Insert<byte[], string>(Table, 6.ToIndex("format"), "raft-cluster-v2");
        tx.Commit();
        return (fence, entities.Count);
    }
    private static (ImmutableArray<byte> Digest, long Length) ConvertImage(string imagePath, SnapshotMetadata metadata, RaftOptions options, string destination, CancellationToken token)
    {
        var work = Child(destination, ".cluster-migration-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(work);
        try
        {
            var appZip = Path.Combine(work, "application.zip");
            var generation = Path.Combine(work, "generation");
            long tailOffset;
            byte[] state;
            using (var input = File.OpenRead(imagePath))
            {
                var header = new byte[20]; input.ReadExactly(header);
                var stateLength = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8));
                var appLength = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(12));
                if (!header.AsSpan(0, 8).SequenceEqual("RAFT8V4S"u8) || stateLength is < 1 or > 1024 * 1024 ||
                    appLength < 1 || appLength > input.Length - 20 - stateLength || metadata.Membership is null)
                    throw new InvalidDataException("Invalid wire3 snapshot container.");
                state = new byte[stateLength]; input.ReadExactly(state);
                if (!JsonSerializer.SerializeToUtf8Bytes(metadata.Membership).AsSpan().SequenceEqual(state))
                    throw new InvalidDataException("Snapshot membership differs from descriptor.");
                using var output = File.Create(appZip); CopyBytes(input, output, appLength, token);
                tailOffset = input.Position;
            }
            Directory.CreateDirectory(generation);
            using (var zip = ZipFile.OpenRead(appZip))
            {
                if (zip.Entries.Count is < 1 or > 8192) throw new InvalidDataException("Invalid application image file count.");
                long total = 0;
                foreach (var entry in zip.Entries)
                {
                    if (entry.FullName.EndsWith('/')) continue;
                    total = checked(total + entry.Length);
                    if (total > options.MaxSnapshotUnpackedBytes) throw new InvalidDataException("Unpacked application image exceeds its limit.");
                    var target = Child(generation, entry.FullName);
                    Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                    using var input = entry.Open(); using var output = new FileStream(target, FileMode.CreateNew, FileAccess.Write);
                    CopyBytes(input, output, entry.Length, token);
                    if (input.ReadByte() != -1) throw new InvalidDataException("Expanded image entry exceeds its declared length.");
                }
            }
            var converted = ConvertDatabase(Path.Combine(generation, "database"), token);
            if (converted.Checkpoint != new AppliedCheckpoint(metadata.LastIncludedIndex, metadata.LastIncludedTerm))
                throw new InvalidDataException("Application image checkpoint differs from its descriptor.");
            var manifestPath = Path.Combine(generation, "image.json");
            var manifest = RewriteManifest(manifestPath);
            if (manifest.Index != converted.Checkpoint.Index || manifest.Term != converted.Checkpoint.Term)
                throw new InvalidDataException("Image manifest differs from its database checkpoint.");
            var updatedZip = Path.Combine(work, "converted.zip");
            using (var zip = ZipFile.Open(updatedZip, ZipArchiveMode.Create))
                foreach (var path in Files(generation)) zip.CreateEntryFromFile(path, Path.GetRelativePath(generation, path).Replace('\\', '/'), CompressionLevel.NoCompression);
            var replacement = Path.Combine(work, "converted.bin");
            using (var output = File.Create(replacement))
            {
                var header = new byte[20]; "RAFT8V4S"u8.CopyTo(header);
                BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(8), state.Length);
                BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(12), new FileInfo(updatedZip).Length);
                output.Write(header); output.Write(state);
                using (var zip = File.OpenRead(updatedZip)) zip.CopyTo(output);
                using var original = File.OpenRead(imagePath); original.Position = tailOffset; original.CopyTo(output);
                output.Flush(true);
            }
            var length = new FileInfo(replacement).Length;
            if (length > options.MaxSnapshotBytes) throw new InvalidDataException("Converted image exceeds configured byte limit.");
            ImmutableArray<byte> digest;
            using (var input = File.OpenRead(replacement)) digest = SHA256.HashData(input).ToImmutableArray();
            File.Move(replacement, imagePath, true);
            return (digest, length);
        }
        finally
        {
            // The generated work directory is checked against the newly created destination before recursive cleanup.
            _ = Child(destination, Path.GetRelativePath(destination, work));
            if (Directory.Exists(work)) { _ = Files(work).Count(); Directory.Delete(work, true); }
        }
    }
    private static ImageManifest RewriteManifest(string path)
    {
        if (new FileInfo(path).Length > 4096) throw new InvalidDataException("Oversized application image manifest.");
        var manifest = JsonSerializer.Deserialize<ImageManifest>(OpenEnvelope(ReadMetadata(path))) ?? throw new InvalidDataException("Missing manifest.");
        if (manifest.ApplicationId != "raft-cluster-v1") throw new InvalidDataException("Wrong application image version.");
        var updated = manifest with { ApplicationId = "raft-cluster-v2" };
        File.WriteAllBytes(path, WrapEnvelope(JsonSerializer.SerializeToUtf8Bytes(updated)));
        return updated;
    }
    private static byte[] ReadMetadata(string path)
    {
        using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        if (file.Length > 1024 * 1024) throw new InvalidDataException("Migration metadata exceeds1MiB.");
        var bytes = new byte[checked((int)file.Length)];
        file.ReadExactly(bytes);
        return bytes;
    }
    private static IEnumerable<string> Files(string root)
    {
        if ((File.GetAttributes(root) & FileAttributes.ReparsePoint) != 0) throw new IOException("Reparse points are prohibited in migration paths.");
        foreach (var path in Directory.EnumerateFileSystemEntries(root))
        {
            var attributes = File.GetAttributes(path);
            if ((attributes & FileAttributes.ReparsePoint) != 0) throw new IOException("Reparse points are prohibited in migration paths.");
            if ((attributes & FileAttributes.Directory) != 0) { foreach (var file in Files(path)) yield return file; }
            else yield return path;
        }
    }
    private static string Child(string root, string relative)
    {
        var fullRoot = Path.GetFullPath(root).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
        var full = Path.GetFullPath(Path.Combine(fullRoot, relative));
        if (Path.IsPathRooted(relative) || relative.IndexOfAny([':', '\0']) >= 0 ||
            relative.Split(['/', '\\']).Any(part => part is ".." or "." || part.EndsWith(' ') || part.EndsWith('.')) ||
            !full.StartsWith(fullRoot, StringComparison.OrdinalIgnoreCase)) throw new InvalidDataException("Migration path escapes its root.");
        return full;
    }
    private static void CopyBytes(Stream input, Stream output, long remaining, CancellationToken token)
    {
        var buffer = new byte[64 * 1024];
        while (remaining > 0)
        {
            token.ThrowIfCancellationRequested();
            var count = input.Read(buffer, 0, (int)Math.Min(buffer.Length, remaining));
            if (count == 0) throw new EndOfStreamException();
            output.Write(buffer, 0, count); remaining -= count;
        }
    }
    // Cold tooling codec for the existing checked metadata envelope; intentionally independent of internal runtime types.
    private static byte[] OpenEnvelope(byte[] record)
    {
        if (record.Length < 12 || BinaryPrimitives.ReadUInt32LittleEndian(record) != 0x4d325246 ||
            BinaryPrimitives.ReadInt32LittleEndian(record.AsSpan(4)) != record.Length ||
            BinaryPrimitives.ReadUInt32LittleEndian(record.AsSpan(8)) != EnvelopeCrc(record)) throw new InvalidDataException("Invalid metadata envelope/CRC32C.");
        return record[12..];
    }
    private static byte[] WrapEnvelope(byte[] body)
    {
        var record = new byte[checked(body.Length + 12)];
        BinaryPrimitives.WriteUInt32LittleEndian(record, 0x4d325246);
        BinaryPrimitives.WriteInt32LittleEndian(record.AsSpan(4), record.Length);
        body.CopyTo(record, 12);
        BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(8), EnvelopeCrc(record));
        return record;
    }
    private static uint EnvelopeCrc(byte[] record)
    {
        var crc = uint.MaxValue;
        for (var index = 0; index < record.Length; index++)
        {
            if (index is >= 8 and < 12) continue;
            crc ^= record[index];
            for (var bit = 0; bit < 8; bit++) crc = (crc >> 1) ^ ((crc & 1) != 0 ? 0x82f63b78u : 0);
        }
        return ~crc;
    }
}
