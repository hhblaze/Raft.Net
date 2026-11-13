using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text.Json;

namespace Raft.Storage;

internal sealed record SnapshotChunkData(long TotalLength, ReadOnlyMemory<byte> Data);
internal sealed record ReceivedChunk(long NextOffset, bool Accepted, SnapshotImage? Image = null, Task<SnapshotImage?>? Verification = null);

/// <summary>Immutable checked images and one resumable, durably acknowledged incoming transfer.</summary>
public sealed class SnapshotRepository : IAsyncDisposable
{
    private sealed record Descriptor(SnapshotMetadata Metadata, long Length);
    private sealed record Incoming(SnapshotMetadata Metadata, long Length, long Offset);
    private readonly string _completed, _incoming, _building;
    private readonly RaftOptions _options;
    private readonly SemaphoreSlim _receive = new(1, 1);
    private readonly CancellationTokenSource _stop = new();
    private readonly Action<SnapshotPersistenceBoundary>? _observe;
    private readonly FileStream _owner;
    private Incoming? _progress;
    private Task<SnapshotImage?>? _verification;

    public SnapshotRepository(RaftOptions options, Action<SnapshotPersistenceBoundary>? observe = null)
    {
        // The recovered configuration may have retired this node. Snapshot files still need
        // validation/recovery on restart; owning their directory does not confer voting rights.
        _options = options.Freeze(allowMissingSelf: true);
        var directory = _options.SnapshotDirectory ?? throw new ArgumentException("Configure SnapshotDirectory.", nameof(options));
        _completed = Path.Combine(directory, "completed");
        _incoming = Path.Combine(directory, "incoming");
        _building = Path.Combine(directory, "building");
        _observe = observe;
        Directory.CreateDirectory(_completed);
        Directory.CreateDirectory(_incoming);
        Directory.CreateDirectory(_building);
        _owner = new FileStream(Path.Combine(directory, "owner.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        try
        {
        foreach (var abandoned in Directory.EnumerateFiles(_building, "*.part"))
        {
            var full = SnapshotFiles.Child(_building, Path.GetFileName(abandoned));
            if ((File.GetAttributes(full) & FileAttributes.ReparsePoint) != 0) throw new IOException("Reparse point in snapshot staging.");
            File.Delete(full); // only unpublished local-build files, under an exclusively owned absolute directory
        }
        var progress = Path.Combine(_incoming, "progress");
        if (File.Exists(progress))
        {
            _progress = JsonSerializer.Deserialize<Incoming>(StorageCodecV2.Open(ReadRecord(progress)))
                ?? throw new InvalidDataException("Invalid incoming snapshot progress.");
            Validate(_progress.Metadata, _progress.Length);
            if (_progress.Offset < 0 || _progress.Offset > _progress.Length) throw new InvalidDataException("Invalid incoming snapshot offset.");
        }
        }
        catch { _owner.Dispose(); throw; }
    }

    public async ValueTask<SnapshotImage> CreateAsync(ISnapshotStateMachine machine, SnapshotBuildContext context, CancellationToken token)
    {
        var id = Guid.NewGuid().ToString("N");
        var path = SnapshotFiles.Child(_building, id + ".part");
        try
        {
            await machine.CreateSnapshotAsync(context, path, token).ConfigureAwait(false);
            var length = new FileInfo(path).Length;
            if (length is < 1 || length > _options.MaxSnapshotBytes) throw new InvalidDataException("Created snapshot exceeds its byte limit.");
            var digest = await DigestAsync(path, token).ConfigureAwait(false);
            var metadata = SnapshotFiles.Metadata(context.Checkpoint, context.Configuration, id, digest);
            return Publish(metadata, path, length);
        }
        finally { if (File.Exists(path)) File.Delete(path); }
    }

    public async ValueTask<SnapshotImage> OpenAsync(SnapshotMetadata metadata, CancellationToken token = default)
    {
        var image = OpenDescriptor(metadata);
        var digest = await DigestAsync(image.Path, token).ConfigureAwait(false);
        if (!CryptographicOperations.FixedTimeEquals(digest.AsSpan(), metadata.Digest.AsSpan()))
            throw new InvalidDataException("Durable snapshot checksum mismatch.");
        return image;
    }

    internal async ValueTask<SnapshotChunkData> ReadChunkAsync(SnapshotMetadata metadata, long offset, int count, CancellationToken token)
    {
        var image = OpenDescriptor(metadata);
        if (offset < 0 || offset > image.Length || count < 1 || count > _options.SnapshotChunkBytes)
            throw new InvalidOperationException("Invalid snapshot read range.");
        var bytes = new byte[(int)Math.Min(count, image.Length - offset)];
        await using var file = new FileStream(image.Path, FileMode.Open, FileAccess.Read, FileShare.Read, _options.SnapshotChunkBytes, FileOptions.Asynchronous | FileOptions.RandomAccess);
        file.Position = offset;
        await file.ReadExactlyAsync(bytes, token).ConfigureAwait(false);
        return new(image.Length, bytes);
    }

    internal async ValueTask<ReceivedChunk> ReceiveAsync(SnapshotChunkRequest request, CancellationToken token)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(token, _stop.Token);
        token = linked.Token;
        await _receive.WaitAsync(token).ConfigureAwait(false);
        try
        {
            var metadata = request.Metadata ?? throw new InvalidOperationException("Validated chunks require metadata.");
            Validate(metadata, request.TotalLength);
            var descriptor = DescriptorPath(metadata.StorageId);
            if (File.Exists(descriptor)) return new(request.TotalLength, true, OpenDescriptor(metadata));
            if (_verification is { IsCompleted: false } && _progress?.Metadata.StorageId != metadata.StorageId)
                return new(0, false);
            if (_progress is null || _progress.Metadata.StorageId != metadata.StorageId)
            {
                if (_progress is not null && _progress.Metadata.LastIncludedIndex > metadata.LastIncludedIndex) return new(0, false);
                if (_progress is not null)
                {
                    var abandoned = PartPath(_progress.Metadata.StorageId);
                    if (File.Exists(abandoned)) File.Delete(abandoned);
                }
                _progress = new(metadata, request.TotalLength, 0);
                _verification = null;
                PersistProgress();
            }
            if (_progress.Length != request.TotalLength || !Same(_progress.Metadata, metadata)) return new(_progress.Offset, false);
            if (request.Offset > _progress.Offset) return new(_progress.Offset, false);
            if (request.Offset == _progress.Offset && request.Data.Length > 0)
            {
                await using var file = new FileStream(PartPath(metadata.StorageId), FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read,
                    _options.SnapshotChunkBytes, FileOptions.Asynchronous | FileOptions.RandomAccess);
                if (file.Length < _progress.Offset) throw new InvalidDataException("Acknowledged snapshot prefix is missing.");
                file.SetLength(_progress.Offset); // discard only bytes beyond the durable acknowledged frontier
                file.Position = _progress.Offset;
                await file.WriteAsync(request.Data, token).ConfigureAwait(false);
                file.Flush(flushToDisk: true);
                _progress = _progress with { Offset = _progress.Offset + request.Data.Length };
                PersistProgress(); // progress must never precede data fsync
            }
            if (_progress.Offset != _progress.Length) return new(_progress.Offset, true);
            if (_verification is null)
            {
                var complete = _progress;
                _verification = Task.Run(() => VerifyReceivedAsync(complete, _stop.Token));
            }
            if (_verification.IsCompletedSuccessfully && _verification.Result is null) return new(0, false);
            return new(_progress.Offset, true, Verification: _verification);
        }
        finally { _receive.Release(); }
    }

    private async Task<SnapshotImage?> VerifyReceivedAsync(Incoming complete, CancellationToken token)
    {
        var part = PartPath(complete.Metadata.StorageId);
        // A crash can follow the image rename but precede descriptor publication.
        var path = File.Exists(part) ? part : ImagePath(complete.Metadata.StorageId);
        var actual = await DigestAsync(path, token).ConfigureAwait(false);
        await _receive.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (new FileInfo(path).Length != complete.Length ||
                !CryptographicOperations.FixedTimeEquals(actual.AsSpan(), complete.Metadata.Digest.AsSpan()))
            {
                _progress = null;
                File.Delete(path);
                File.Delete(Path.Combine(_incoming, "progress"));
                return null;
            }
            var image = Publish(complete.Metadata, path, complete.Length);
            _progress = null;
            File.Delete(Path.Combine(_incoming, "progress"));
            return image;
        }
        finally { _receive.Release(); }
    }

    private SnapshotImage Publish(SnapshotMetadata metadata, string source, long length)
    {
        Validate(metadata, length);
        _observe?.Invoke(SnapshotPersistenceBoundary.BeforeImagePublication);
        var target = ImagePath(metadata.StorageId);
        if (!string.Equals(source, target, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal))
            File.Move(source, target, overwrite: true);
        SnapshotFiles.AtomicWrite(DescriptorPath(metadata.StorageId), StorageCodecV2.Wrap(JsonSerializer.SerializeToUtf8Bytes(new Descriptor(metadata, length))));
        _observe?.Invoke(SnapshotPersistenceBoundary.AfterImagePublication);
        return new(metadata, target, length);
    }

    private SnapshotImage OpenDescriptor(SnapshotMetadata metadata)
    {
        RaftOptions.ValidateId(metadata.StorageId, nameof(metadata));
        var descriptor = JsonSerializer.Deserialize<Descriptor>(StorageCodecV2.Open(ReadRecord(DescriptorPath(metadata.StorageId))))
            ?? throw new InvalidDataException("Missing snapshot descriptor.");
        Validate(descriptor.Metadata, descriptor.Length);
        var path = ImagePath(metadata.StorageId);
        if (!Same(metadata, descriptor.Metadata) || new FileInfo(path).Length != descriptor.Length)
            throw new InvalidDataException("Snapshot metadata/length mismatch.");
        return new(metadata, path, descriptor.Length);
    }
    private void Validate(SnapshotMetadata metadata, long length)
    {
        _ = StorageCodecV2.Snapshot(StorageCodecV2.Snapshot(metadata));
        if (length is < 1 || length > _options.MaxSnapshotBytes) throw new InvalidDataException("Snapshot byte limit exceeded.");
    }
    private static bool Same(SnapshotMetadata left, SnapshotMetadata right) =>
        StorageCodecV2.Snapshot(left).AsSpan().SequenceEqual(StorageCodecV2.Snapshot(right));
    private static byte[] ReadRecord(string path)
    {
        if (new FileInfo(path).Length > 1024 * 1024) throw new InvalidDataException("Snapshot descriptor exceeds its bound.");
        return File.ReadAllBytes(path);
    }
    private void PersistProgress() => SnapshotFiles.AtomicWrite(Path.Combine(_incoming, "progress"), StorageCodecV2.Wrap(JsonSerializer.SerializeToUtf8Bytes(_progress)));
    // Called with runtime snapshot creation/reception admission paused. Existing transfer IDs are pinned.
    internal void Collect(IReadOnlyCollection<string> protectedIds)
    {
        _receive.Wait();
        try
        {
            var keep = protectedIds.ToHashSet(StringComparer.Ordinal);
            if (_progress is not null) keep.Add(_progress.Metadata.StorageId);
            var newest = new List<(long Index, string Id)>();
            foreach (var path in Directory.EnumerateFiles(_completed, "*.json"))
            {
                var descriptor = JsonSerializer.Deserialize<Descriptor>(StorageCodecV2.Open(ReadRecord(path)))
                    ?? throw new InvalidDataException("Invalid snapshot descriptor during retention.");
                Validate(descriptor.Metadata, descriptor.Length);
                if (Path.GetFileNameWithoutExtension(path) != descriptor.Metadata.StorageId)
                    throw new InvalidDataException("Snapshot descriptor identity differs from its filename.");
                newest.Add((descriptor.Metadata.LastIncludedIndex, descriptor.Metadata.StorageId));
                newest.Sort((a, b) => a.Index != b.Index ? a.Index.CompareTo(b.Index) : StringComparer.Ordinal.Compare(a.Id, b.Id));
                if (newest.Count > _options.SnapshotRetainedImages) newest.RemoveAt(0);
            }
            foreach (var item in newest) keep.Add(item.Id);
            foreach (var path in Directory.EnumerateFiles(_completed, "*.json"))
            {
                var id = Path.GetFileNameWithoutExtension(path);
                RaftOptions.ValidateId(id, nameof(protectedIds));
                if (keep.Contains(id)) continue;
                File.Delete(DescriptorPath(id)); // delete descriptor first: a crash can leave only an unreferenced image
                File.Delete(ImagePath(id));
            }
            foreach (var path in Directory.EnumerateFiles(_completed, "*.bin"))
            {
                var id = Path.GetFileNameWithoutExtension(path);
                RaftOptions.ValidateId(id, nameof(protectedIds));
                if (!keep.Contains(id) && !File.Exists(DescriptorPath(id))) File.Delete(ImagePath(id));
            }
        }
        finally { _receive.Release(); }
    }
    private string PartPath(string id) => SnapshotFiles.Child(_incoming, id + ".part");
    private string DescriptorPath(string id) => SnapshotFiles.Child(_completed, id + ".json");
    private string ImagePath(string id) => SnapshotFiles.Child(_completed, id + ".bin");
    private static async ValueTask<ImmutableArray<byte>> DigestAsync(string path, CancellationToken token)
    {
        await using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
        return ImmutableArray.CreateRange(await SHA256.HashDataAsync(file, token).ConfigureAwait(false));
    }

    internal void Stop() => _stop.Cancel();
    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        try
        {
            await _receive.WaitAsync().ConfigureAwait(false);
            _receive.Release();
            if (_verification is not null)
            {
                try { await _verification.ConfigureAwait(false); }
                catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
            }
        }
        finally { _owner.Dispose(); }
    }
}
