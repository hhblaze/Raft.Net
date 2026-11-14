using System.Text.Json;
using Raft;
using Raft.Storage;

internal static class SnapshotCrashScenario
{
    public static async Task RunAsync(string root, string mode, DBreezeLogLayout layout)
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"], SnapshotDirectory = Path.Combine(root, "snapshots") };
        if (layout == DBreezeLogLayout.UnifiedTableV4) options = options with { WireVersion = 3,
            InitialPeers = options.Members.Select((id, index) => new PeerDescriptor(id, "127.0.0.1", 7301 + index)).ToArray() };
        var intent = JsonSerializer.Deserialize<SnapshotInstallIntent>(File.ReadAllBytes(Path.Combine(root, "intent.json")))!;
        if (mode.StartsWith("snapshot-image-", StringComparison.Ordinal))
        {
            var target = Enum.Parse<SnapshotPersistenceBoundary>(mode[15..]);
            await using var repository = new SnapshotRepository(options, point => { if (point == target) Pause(); });
            using var sourceLog = DBreezeLogStorage.CreateInMemory(options, layout);
            sourceLog.SaveHardState(new(2));
            var entries = Enumerable.Range(1, 10).Select(i => new LogEntry(i, 2, BitConverter.GetBytes(1))).ToArray();
            sourceLog.Append(entries);
            sourceLog.SaveHardState(new(2, CommitIndex: 10));
            using var source = Machine(Path.Combine(root, "source-app"));
            var context = new SnapshotBuildContext(new(10, 2), sourceLog.Recover().Configuration!, null, options,
                (from, count, bytes, _) => ValueTask.FromResult(sourceLog.Query(new LogQuery.Entries(from, count, bytes))))
                { Membership = sourceLog.Recover().Membership?.Committed };
            await using var sourceAdapter = new RaftStorageAdapter(sourceLog);
            ISnapshotStateMachine sourceMachine = layout == DBreezeLogLayout.UnifiedTableV4
                ? new MembershipSnapshotMachine(source, sourceAdapter, options) : source;
            await repository.CreateAsync(sourceMachine, context, default);
            return;
        }
        var generation = mode.StartsWith("snapshot-generation-", StringComparison.Ordinal);
        var publish = mode.StartsWith("snapshot-publish-", StringComparison.Ordinal);
        var storagePoint = generation ? default : Enum.Parse<PersistenceBoundary>(mode[(publish ? 17 : 16)..]);
        var generationPoint = generation ? Enum.Parse<SnapshotPersistenceBoundary>(mode[20..]) : default;
        var armed = !publish && !generation;
        using var store = new DBreezeLogStorage(Path.Combine(root, "log"), options, layout, point =>
        {
            if (armed && point == storagePoint) Pause();
        });
        store.Write(new LogWrite.BeginSnapshotInstall(intent));
        if (!generation && !publish) throw new InvalidOperationException("Intent boundary was not reached.");
        using var machine = Machine(Path.Combine(root, "application"), point => { if (generation && point == generationPoint) Pause(); });
        await using var images = new SnapshotRepository(options);
        var image = await images.OpenAsync(intent.Metadata);
        await using var adapter = new RaftStorageAdapter(store);
        ISnapshotStateMachine installed = layout == DBreezeLogLayout.UnifiedTableV4
            ? new MembershipSnapshotMachine(machine, adapter, options) : machine;
        await installed.PrepareSnapshotAsync(image, options.MaxSnapshotUnpackedBytes, default);
        await installed.InstallSnapshotAsync(image, default);
        armed = publish;
        store.Write(new LogWrite.CompleteSnapshotInstall(intent));
    }

    private static DBreezeGenerationStateMachine Machine(string path, Action<SnapshotPersistenceBoundary>? observe = null) =>
        new(path, "counter-v1", "checkpoint", ["counter"], (tx, entry) =>
        {
            var old = tx.Select<string, long>("counter", "value");
            tx.Insert<string, long>("counter", "value", (old.Exists ? old.Value : 0) + BitConverter.ToInt32(entry.Command.Span));
        }, observe);

    private static void Pause()
    {
        Console.WriteLine("READY");
        Console.Out.Flush();
        Thread.Sleep(Timeout.Infinite); // only the isolated child fault harness; the parent kills this process
    }
}
