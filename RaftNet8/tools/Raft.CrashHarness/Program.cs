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
