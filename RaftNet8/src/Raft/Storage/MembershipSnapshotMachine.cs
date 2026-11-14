using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text.Json;

namespace Raft.Storage;

/// <summary>Wire3 images bind the bounded directory, streamed indexed results and application to one fence.</summary>
internal sealed class MembershipSnapshotMachine(ISnapshotStateMachine application, ILogStore log, RaftOptions options)
    : ISnapshotStateMachine
{
    private static ReadOnlySpan<byte> Magic => "RAFT8V4S"u8;
    public AppliedCheckpoint Checkpoint => application.Checkpoint;
    public long LastApplied => application.LastApplied;
    public ValueTask ApplyAsync(LogEntry entry, CancellationToken token) => application.ApplyAsync(entry, token);

    public async ValueTask CreateSnapshotAsync(SnapshotBuildContext context, string outputPath, CancellationToken token)
    {
        var membership = context.Membership ?? throw new InvalidDataException("V4 snapshot needs its membership fence.");
        using var previous = context.Previous is { } image ? await UnpackAsync(image, stage: false, token).ConfigureAwait(false) : null;
        var appPath = TemporaryPath();
        try
        {
            await application.CreateSnapshotAsync(context.WithPrevious(previous?.Image), appPath, token).ConfigureAwait(false);
            var appLength = new FileInfo(appPath).Length;
            var state = JsonSerializer.SerializeToUtf8Bytes(membership.Validate());
            if (state.Length > MembershipCodec.MaxDirectoryBytes || appLength < 1 ||
                appLength > options.MaxSnapshotBytes - state.Length - 20)
                throw new InvalidDataException("V4 snapshot section exceeds its byte limit.");
            await using var output = File(outputPath, FileMode.CreateNew, FileAccess.Write);
            var header = new byte[20];
            Magic.CopyTo(header);
            BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(8), state.Length);
            BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(12), appLength);
            await output.WriteAsync(header, token).ConfigureAwait(false);
            await output.WriteAsync(state, token).ConfigureAwait(false);
            await using (var app = File(appPath, FileMode.Open, FileAccess.Read))
                await app.CopyToAsync(output, 64 * 1024, token).ConfigureAwait(false);
            string? after = null;
            do
            {
                var page = await log.ReadAsync(new LogQuery.MembershipHistory(after, 128, context.Checkpoint.Index), token).ConfigureAwait(false);
                foreach (var result in page.MembershipHistory)
                {
                    var bytes = JsonSerializer.SerializeToUtf8Bytes(result);
                    if (bytes.Length > MembershipCodec.MaxRecordBytes || output.Position > options.MaxSnapshotBytes - bytes.Length - 4)
                        throw new InvalidDataException("V4 snapshot history exceeds its byte limit.");
                    BinaryPrimitives.WriteInt32LittleEndian(header, bytes.Length);
                    await output.WriteAsync(header.AsMemory(0, 4), token).ConfigureAwait(false);
                    await output.WriteAsync(bytes, token).ConfigureAwait(false);
                }
                after = page.ContinuationKey;
            } while (after is not null);
            output.Flush(flushToDisk: true);
        }
        finally { if (System.IO.File.Exists(appPath)) System.IO.File.Delete(appPath); }
    }

    public async ValueTask PrepareSnapshotAsync(SnapshotImage image, long maxUnpackedBytes, CancellationToken token)
    {
        using var unpacked = await UnpackAsync(image, stage: true, token).ConfigureAwait(false);
        await application.PrepareSnapshotAsync(unpacked.Image, maxUnpackedBytes, token).ConfigureAwait(false);
        // Only a completely validated service stream and application may become an install candidate.
        await log.WriteAsync(new LogWrite.StageMembershipHistory(image.Metadata.StorageId,
            ImmutableArray<MembershipOperationStatus>.Empty, Complete: true, image.Metadata.LastIncludedIndex), token).ConfigureAwait(false);
    }

    public async ValueTask InstallSnapshotAsync(SnapshotImage image, CancellationToken token)
    {
        using var unpacked = await UnpackAsync(image, stage: false, token).ConfigureAwait(false);
        await application.InstallSnapshotAsync(unpacked.Image, token).ConfigureAwait(false);
    }

    private sealed record Unpacked(SnapshotImage Image) : IDisposable
    {
        public void Dispose() { if (System.IO.File.Exists(Image.Path)) System.IO.File.Delete(Image.Path); }
    }

    private async ValueTask<Unpacked> UnpackAsync(SnapshotImage image, bool stage, CancellationToken token)
    {
        var path = TemporaryPath();
        try
        {
            var membership = image.Metadata.Membership?.Validate()
                ?? throw new SnapshotValidationException("V4 snapshot has no service state.");
            await using var input = File(image.Path, FileMode.Open, FileAccess.Read);
            var header = new byte[20];
            await ReadExactlyAsync(input, header, token).ConfigureAwait(false);
            var stateLength = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8));
            var appLength = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(12));
            if (!header.AsSpan(0, 8).SequenceEqual(Magic) || stateLength is < 1 or > MembershipCodec.MaxDirectoryBytes ||
                membership.Index > image.Metadata.LastIncludedIndex || appLength < 1 ||
                appLength > input.Length - 20 - stateLength || input.Length != image.Length || input.Length > options.MaxSnapshotBytes)
                throw new SnapshotValidationException("Invalid V4 snapshot sections.");
            var stateBytes = new byte[stateLength];
            await ReadExactlyAsync(input, stateBytes, token).ConfigureAwait(false);
            var state = JsonSerializer.Deserialize<MembershipState>(stateBytes)?.Validate();
            if (state is null || !JsonSerializer.SerializeToUtf8Bytes(state).AsSpan().SequenceEqual(JsonSerializer.SerializeToUtf8Bytes(membership)))
                throw new SnapshotValidationException("V4 snapshot directory differs from its checked image.");
            ImmutableArray<byte> applicationDigest;
            using (var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
            await using (var output = File(path, FileMode.CreateNew, FileAccess.Write))
            {
                var buffer = new byte[64 * 1024];
                var remaining = appLength;
                while (remaining > 0)
                {
                    var count = (int)Math.Min(buffer.Length, remaining);
                    await ReadExactlyAsync(input, buffer.AsMemory(0, count), token).ConfigureAwait(false);
                    hash.AppendData(buffer, 0, count);
                    await output.WriteAsync(buffer.AsMemory(0, count), token).ConfigureAwait(false);
                    remaining -= count;
                }
                output.Flush(flushToDisk: true);
                applicationDigest = ImmutableArray.CreateRange(hash.GetHashAndReset());
            }
            var batch = ImmutableArray.CreateBuilder<MembershipOperationStatus>(128);
            string? previous = null;
            while (input.Position < input.Length)
            {
                await ReadExactlyAsync(input, header.AsMemory(0, 4), token).ConfigureAwait(false);
                var length = BinaryPrimitives.ReadInt32LittleEndian(header);
                if (length is < 1 or > MembershipCodec.MaxRecordBytes || length > input.Length - input.Position)
                    throw new SnapshotValidationException("Invalid V4 history record size.");
                var bytes = new byte[length];
                await ReadExactlyAsync(input, bytes, token).ConfigureAwait(false);
                var result = JsonSerializer.Deserialize<MembershipOperationStatus>(bytes)
                    ?? throw new SnapshotValidationException("Missing V4 history result.");
                MembershipCodec.ValidateStatus(result);
                var key = result.OperationId.ToString("N");
                if (!result.IsTerminal || result.Index is < 1 || result.Term is < 1 ||
                    result.Index > membership.Index || result.Term > image.Metadata.LastIncludedTerm ||
                    result.OperationId == membership.ActiveOperation?.OperationId ||
                    (previous is not null && StringComparer.Ordinal.Compare(previous, key) >= 0))
                    throw new SnapshotValidationException("Invalid V4 history order, result or fence.");
                previous = key;
                if (!stage) continue;
                batch.Add(result);
                if (batch.Count == 128)
                {
                    await StageAsync(batch, image, token).ConfigureAwait(false);
                    batch.Clear();
                }
            }
            if (stage && batch.Count > 0) await StageAsync(batch, image, token).ConfigureAwait(false);
            // The outer repository authenticates the complete container. The application
            // still receives a SnapshotImage whose digest describes its own actual file.
            return new(new(image.Metadata with { Digest = applicationDigest }, path, appLength));
        }
        catch
        {
            if (System.IO.File.Exists(path)) System.IO.File.Delete(path);
            throw;
        }
    }

    private ValueTask StageAsync(ImmutableArray<MembershipOperationStatus>.Builder entries, SnapshotImage image, CancellationToken token) =>
        log.WriteAsync(new LogWrite.StageMembershipHistory(image.Metadata.StorageId, entries.ToImmutable(),
            ThroughIndex: image.Metadata.LastIncludedIndex), token);

    private string TemporaryPath() => SnapshotFiles.Child(Path.Combine(options.SnapshotDirectory!, "building"),
        "service-" + Guid.NewGuid().ToString("N") + ".part");

    private static FileStream File(string path, FileMode mode, FileAccess access) =>
        new(path, mode, access, FileShare.Read, 64 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);

    private static async ValueTask ReadExactlyAsync(Stream source, Memory<byte> bytes, CancellationToken token)
    {
        try { await source.ReadExactlyAsync(bytes, token).ConfigureAwait(false); }
        catch (EndOfStreamException) { throw new SnapshotValidationException("Truncated V4 snapshot."); }
    }
}
