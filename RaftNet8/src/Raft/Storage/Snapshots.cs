using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Text;

namespace Raft.Storage;

public sealed record SnapshotImage(SnapshotMetadata Metadata, string Path, long Length);
public sealed record SnapshotInstallIntent(SnapshotMetadata Metadata, bool RetainSuffix);
public enum SnapshotPersistenceBoundary
{
    BeforeImagePublication, AfterImagePublication, BeforeGenerationSwitch, AfterGenerationPointer, AfterGenerationPublication
}

/// <summary>Owned replay view through a committed applied fence; the runtime pins its source until publication.</summary>
public sealed class SnapshotBuildContext
{
    private readonly Func<long, int, int, CancellationToken, ValueTask<LogReadResult>> _read;
    public AppliedCheckpoint Checkpoint { get; }
    public ClusterConfiguration Configuration { get; }
    public SnapshotImage? Previous { get; }
    internal MembershipState? Membership { get; init; }
    public long MaxUnpackedBytes { get; }
    private readonly int _batchEntries, _batchBytes;

    public SnapshotBuildContext(AppliedCheckpoint checkpoint, ClusterConfiguration configuration, SnapshotImage? previous,
        RaftOptions options, Func<long, int, int, CancellationToken, ValueTask<LogReadResult>> read)
    {
        options = options.Freeze(allowMissingSelf: true);
        configuration.Validate();
        if (checkpoint.Index < 1 || checkpoint.Term < 1 || configuration.Index > checkpoint.Index ||
            (previous is not null && previous.Metadata.LastIncludedIndex > checkpoint.Index))
            throw new ArgumentException("Invalid committed replay fence.", nameof(checkpoint));
        Checkpoint = checkpoint;
        Configuration = configuration;
        Previous = previous;
        MaxUnpackedBytes = options.MaxSnapshotUnpackedBytes;
        _read = read ?? throw new ArgumentNullException(nameof(read));
        _batchEntries = options.MaxAppendEntries;
        _batchBytes = options.MaxAppendBytes;
    }

    private SnapshotBuildContext(SnapshotBuildContext source, SnapshotImage? previous)
    {
        Checkpoint = source.Checkpoint;
        Configuration = source.Configuration;
        Previous = previous;
        MaxUnpackedBytes = source.MaxUnpackedBytes;
        _read = source._read;
        _batchEntries = source._batchEntries;
        _batchBytes = source._batchBytes;
        Membership = source.Membership;
    }

    internal SnapshotBuildContext WithPrevious(SnapshotImage? previous) => new(this, previous);

    public async IAsyncEnumerable<IReadOnlyList<LogEntry>> ReadBatchesAsync([EnumeratorCancellation] CancellationToken token = default)
    {
        var next = (Previous?.Metadata.LastIncludedIndex ?? 0) + 1;
        while (next <= Checkpoint.Index)
        {
            var count = (int)Math.Min(_batchEntries, Checkpoint.Index - next + 1);
            var result = await _read(next, count, _batchBytes, token).ConfigureAwait(false);
            if (result.Entries.IsDefaultOrEmpty || result.Entries[0].Index != next || result.Entries[^1].Index > Checkpoint.Index)
                throw new InvalidDataException("Snapshot replay prefix is missing or outside its fence.");
            next = result.Entries[^1].Index + 1;
            yield return result.Entries;
        }
    }
}

/// <summary>Creation/validation runs outside apply; activation atomically switches a durable application generation.</summary>
public interface ISnapshotStateMachine : ICheckpointStateMachine
{
    ValueTask CreateSnapshotAsync(SnapshotBuildContext context, string outputPath, CancellationToken token);
    ValueTask PrepareSnapshotAsync(SnapshotImage image, long maxUnpackedBytes, CancellationToken token);
    ValueTask InstallSnapshotAsync(SnapshotImage image, CancellationToken token);
}

internal sealed class SnapshotValidationException(string message) : Exception(message);

internal static class SnapshotFiles
{
    public static void AtomicWrite(string path, ReadOnlySpan<byte> bytes)
    {
        var temporary = path + ".tmp-" + Guid.NewGuid().ToString("N");
        try
        {
            using (var file = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                file.Write(bytes);
                file.Flush(flushToDisk: true);
            }
            File.Move(temporary, path, overwrite: true);
        }
        finally { if (File.Exists(temporary)) File.Delete(temporary); }
    }

    public static string Child(string root, string relative)
    {
        var fullRoot = Path.GetFullPath(root).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
        var path = Path.GetFullPath(Path.Combine(fullRoot, relative));
        if (Path.IsPathRooted(relative) || relative.IndexOfAny([':', '\0']) >= 0 ||
            relative.Split(['/', '\\']).Any(part => part is ".." or "." || part.EndsWith(' ') || part.EndsWith('.')) ||
            !path.StartsWith(fullRoot, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal))
            throw new SnapshotValidationException("Snapshot path escapes its owned directory.");
        return path;
    }

    public static void DeleteOwnedGeneration(string root, string id)
    {
        RaftOptions.ValidateId(id, nameof(id));
        var path = Child(root, "gen-" + id);
        if (!Directory.Exists(path)) return;
        var directories = new Stack<string>();
        directories.Push(path);
        while (directories.TryPop(out var directory))
        {
            if ((File.GetAttributes(directory) & FileAttributes.ReparsePoint) != 0)
                throw new IOException("Reparse points are prohibited in owned snapshot generations.");
            foreach (var child in Directory.EnumerateFileSystemEntries(directory))
            {
                var attributes = File.GetAttributes(child);
                if ((attributes & FileAttributes.ReparsePoint) != 0)
                    throw new IOException("Reparse points are prohibited in owned snapshot generations.");
                if ((attributes & FileAttributes.Directory) != 0) directories.Push(child);
            }
        }
        Directory.Delete(path, recursive: true); // absolute path verified beneath the generation root above
    }

    public static string ReadPointer(string path) => Encoding.UTF8.GetString(StorageCodecV2.Open(File.ReadAllBytes(path)));
    public static void WritePointer(string path, string id) => AtomicWrite(path, StorageCodecV2.Wrap(Encoding.UTF8.GetBytes(id)));
    public static SnapshotMetadata Metadata(AppliedCheckpoint checkpoint, ClusterConfiguration configuration, string id, ImmutableArray<byte> digest) =>
        new(checkpoint.Index, checkpoint.Term, configuration.Voters, configuration.Learners, id, digest);
}
