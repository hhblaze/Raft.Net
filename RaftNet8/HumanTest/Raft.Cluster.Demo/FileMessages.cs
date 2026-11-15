using System.Security.Cryptography;
using System.Text.Json;
using Raft.Cluster.Client;

namespace Raft.Cluster.Demo;

public sealed record FileManifest(string Name, string ChunkEntity, long[] MessageIds, long Length, string Sha256);

/// <summary>Application-level example, not a Raft file protocol. Compression/encryption remain application concerns.</summary>
public static class FileMessages
{
    // book:start client-upload
    public static async Task<long> UploadAsync(RaftClusterClient client, string path, string chunkEntity,
        string metadataEntity, int chunkBytes = ClusterLimits.MaxPayloadBytes, CancellationToken token = default)
    {
        ClusterLimits.ValidatePayloadLength(chunkBytes);
        if (chunkBytes == 0) throw new ArgumentOutOfRangeException(nameof(chunkBytes));
        await using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        if (file.Length / chunkBytes > 500_000) throw new ArgumentException("Example manifest is limited to 500,000 chunks.");
        var ids = new List<long>();
        var buffer = GC.AllocateUninitializedArray<byte>(chunkBytes);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        long remaining = file.Length;
        while (remaining > 0)
        {
            var count = (int)Math.Min(buffer.Length, remaining);
            await file.ReadExactlyAsync(buffer.AsMemory(0, count), token);
            hash.AppendData(buffer, 0, count);
            // Persist this request ID with an outbox if the uploader must survive its own crash.
            var requestId = Guid.NewGuid();
            var id = await client.AddLogEntryAsync(buffer.AsMemory(0, count), chunkEntity, requestId, token);
            ids.Add(id); remaining -= count;
            Console.WriteLine($"CHUNK committed {chunkEntity}/{id} bytes={count} remaining={remaining}");
        }
        var manifest = new FileManifest(Path.GetFileName(path), chunkEntity, ids.ToArray(), file.Length,
            Convert.ToHexString(hash.GetHashAndReset()));
        // Only publish metadata after every complete chunk has a quorum-backed successful result.
        return await client.AddLogEntryAsync(JsonSerializer.SerializeToUtf8Bytes(manifest), metadataEntity, cancellationToken: token);
    }
    // book:end client-upload

    // book:start client-download
    public static async Task DownloadAsync(RaftClusterClient client, string metadataEntity, long manifestId,
        string destination, CancellationToken token = default)
    {
        using var metadata = new MemoryStream();
        if (!await client.CopyMessageToAsync(metadataEntity, manifestId, metadata, token)) throw new FileNotFoundException("Manifest not found.");
        var manifest = JsonSerializer.Deserialize<FileManifest>(metadata.GetBuffer().AsSpan(0, (int)metadata.Length))
            ?? throw new InvalidDataException("Missing manifest.");
        if (manifest.Length < 0 || manifest.MessageIds.Length > 500_000 || manifest.MessageIds.Any(id => id <= 0))
            throw new InvalidDataException("Invalid manifest.");
        await using (var output = new FileStream(destination, FileMode.CreateNew, FileAccess.Write, FileShare.None, 64 * 1024, FileOptions.Asynchronous))
        {
            foreach (var id in manifest.MessageIds)
                if (!await client.CopyMessageToAsync(manifest.ChunkEntity, id, output, token)) throw new FileNotFoundException($"Chunk {id} not found.");
            await output.FlushAsync(token);
            if (output.Length != manifest.Length) throw new InvalidDataException("Reassembled length differs from manifest.");
        }
        await using var check = File.OpenRead(destination);
        if (!Convert.ToHexString(await SHA256.HashDataAsync(check, token)).Equals(manifest.Sha256, StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("Reassembled SHA-256 differs from manifest.");
    }
    // book:end client-download
}
