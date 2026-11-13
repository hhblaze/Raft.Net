using DBreeze;
using Raft;
using Raft.Storage;

if (args.Length != 2) return 2;
var options = new RaftOptions { NodeId = "n1", Members = new[] { "n1", "n2", "n3" } };
if (args[1] == "committed")
{
    var storage = new DBreezeRaftStorage(args[0], options);
    storage.SaveHardState(new HardState(7, "n2"));
    storage.Append(new[] { new LogEntry(1, 7, "crash-durable"u8) });
    storage.SaveHardState(new HardState(7, "n2", 1));
    // Deliberately skip Dispose and all finally blocks, modelling abrupt process exit after acknowledgment.
    Environment.Exit(17);
}
else if (args[1].StartsWith("snapshot-", StringComparison.Ordinal) || args[1].StartsWith("v3-snapshot-", StringComparison.Ordinal))
{
    var unified = args[1].StartsWith("v3-", StringComparison.Ordinal);
    await SnapshotCrashScenario.RunAsync(args[0], unified ? args[1][3..] : args[1],
        unified ? DBreezeLogLayout.UnifiedTableV3 : DBreezeLogLayout.SeparateTablesV2);
}
else if (args[1].StartsWith("apply-v2-", StringComparison.Ordinal))
{
    var boundary = Enum.Parse<ApplicationPersistenceBoundary>(args[1][9..]);
    using var engine = new DBreezeEngine(args[0]);
    var machine = new DBreezeStateMachine(engine, "business/checkpoint", ["business/values"], (tx, entry) =>
    {
        var current = tx.Select<string, long>("business/values", "counter");
        tx.Insert<string, long>("business/values", "counter", (current.Exists ? current.Value : 0) + 1);
    }, observe: point =>
    {
        if (point != boundary) return;
        Console.WriteLine("READY");
        Console.Out.Flush();
        Thread.Sleep(Timeout.Infinite);
    });
    machine.ApplyAsync(new LogEntry(2, 8, "increment"u8), default).GetAwaiter().GetResult();
}
else if (args[1].StartsWith("v2-", StringComparison.Ordinal) || args[1].StartsWith("v3-", StringComparison.Ordinal))
{
    var boundary = Enum.Parse<PersistenceBoundary>(args[1][3..]);
    var layout = args[1].StartsWith("v3-", StringComparison.Ordinal) ? DBreezeLogLayout.UnifiedTableV3 : DBreezeLogLayout.SeparateTablesV2;
    using var storage = new DBreezeLogStorage(args[0], options, layout, point =>
    {
        if (point != boundary) return;
        Console.WriteLine("READY");
        Console.Out.Flush();
        Thread.Sleep(Timeout.Infinite);
    });
    storage.Write(new LogWrite.Batch([
        new LogWrite.HardStateUpdate(new(8, "n2", 1)),
        new LogWrite.Append([new LogEntry(2, 8, "v2-durable"u8)]),
        new LogWrite.HardStateUpdate(new(8, "n2", 2))
    ]));
}
else if (args[1] == "uncommitted")
{
    var engine = new DBreezeEngine(args[0]);
    var transaction = engine.GetTransaction();
    transaction.SynchronizeTables("raft8/meta", "raft8/log");
    transaction.Insert<string, byte[]>("raft8/meta", "state", new byte[] { 99 });
    transaction.RemoveKey<long>("raft8/log", 1);
    transaction.Insert<long, byte[]>("raft8/log", 2, new byte[] { 99 });
    Console.WriteLine("READY");
    Console.Out.Flush();
    // The test kills this process while a multi-table transaction is deliberately uncommitted.
    Thread.Sleep(Timeout.Infinite);
}
return 3;
