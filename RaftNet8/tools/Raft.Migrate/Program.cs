using Raft;
using Raft.Storage;

var toV3 = args.Length == 6 && args[0] == "--v2-to-v3";
if (args.Length != 5 && !toV3)
{
    Console.Error.WriteLine("Offline use only: Raft.Migrate [--v2-to-v3] <source> <new-destination> <group> <node> <voter1,voter2,...>");
    return 2;
}
try
{
    var values = toV3 ? args[1..] : args;
    var options = new RaftOptions { GroupId = values[2], NodeId = values[3], Members = values[4].Split(',') };
    Action<long> progress = index => Console.WriteLine($"Verified/copied through log index {index}");
    var result = toV3
        ? RaftStorageMigration.CopyV2ToV3(values[0], values[1], options, progress)
        : RaftStorageMigration.CopyV1ToV2(values[0], values[1], options, progress);
    Console.WriteLine($"MIGRATION COMPLETE: {result.LastIndex} entries, commit {result.CommitIndex}, {result.CopiedFiles} physical files. Source retained.");
    return 0;
}
catch (Exception error) { Console.Error.WriteLine(error); return 1; }
