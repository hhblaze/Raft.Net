using DBreeze;
using DotNext.Net.Cluster.Consensus.Raft;
using Raft.Storage;

namespace Raft.Benchmarks;

/// <summary>Checks benchmark validity, especially the client acknowledgment boundary.</summary>
internal static class AdapterChecks
{
    public static async Task<int> RunAsync()
    {
        var parent = Path.GetFullPath(".artifacts/dotnext-comparison/adapter-checks");
        var root = Path.Combine(parent, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expectedIndex = 33L; // one genuine empty native entry + 32 real commands
        var expectedBatch = 15; // floor(256 KiB / (16 KiB + 21))
        var success = false;
        try
        {
            using (var engine = new DBreezeEngine(root))
            using (var log = new DotNextAuditTrail(Path.Combine(root, "native-wal"), new(engine), 128, 16384)
            {
                BeforeBusinessApply = async () => { entered.TrySetResult(); await release.Task; }
            })
            {
                await log.InitializeAsync();
                await log.UpdateTermAsync(1, true);
                var entries = Enumerable.Range(0, 33).Select(i => new BinaryLogEntry
                    { Term = 1, Content = i == 0 ? ReadOnlyMemory<byte>.Empty : new byte[16384] }).ToArray();
                var index = await log.AppendBatchAsync(entries, CancellationToken.None);
                if (index != expectedIndex) throw new InvalidDataException("Wrong native append indices.");
                var waiting = log.WaitForApplyAsync(index).AsTask();
                var committing = log.CommitAsync(index).AsTask();
                try
                {
                    await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
                    if (waiting.IsCompleted || log.LastCommittedEntryIndex != 0 || log.Machine.Commands != 0)
                        throw new InvalidDataException("Client acknowledged before the DBreeze transaction!");
                }
                finally { release.TrySetResult(); }
                await Task.WhenAll(waiting, committing).WaitAsync(TimeSpan.FromSeconds(10));
                log.Machine.Validate(32);
                if (log.MaxObservedApplyBatch != expectedBatch || log.ApplyBatches != 3)
                    throw new InvalidDataException("Application batch limit is not enforced.");
                await log.CommitAsync(index); // repeated notification must not apply twice
                if (log.Machine.Commands != 32 || log.ApplyBatches != 3)
                    throw new InvalidDataException("A repeated commit duplicated business effects.");
                await log.UpdateTermAsync(7, true);
                using var canceled = new CancellationTokenSource();
                canceled.Cancel();
                try
                {
                    await log.WaitForApplyAsync(index + 1, canceled.Token);
                    throw new InvalidDataException("Canceled pending waiter completed successfully.");
                }
                catch (OperationCanceledException) { }
            }
            using (var engine = new DBreezeEngine(root))
            {
                using (var tx = engine.GetTransaction())
                {
                    var checkpoint = DBreezeStateMachine.DecodeCheckpoint(tx.Select<string, byte[]>("bench/checkpoint", "applied").Value);
                    if (checkpoint != new AppliedCheckpoint(expectedIndex, 1) ||
                        tx.Select<string, long>("bench/checkpoint", "commands").Value != 32)
                        throw new InvalidDataException("Reopened application differs.");
                }
                using var log = new DotNextAuditTrail(Path.Combine(root, "native-wal"), new(engine), 128, 16384);
                await log.InitializeAsync();
                if (log.Term != 7 || log.LastEntryIndex != expectedIndex) throw new InvalidDataException("Native WAL term/tail did not survive reopen.");
                await log.CommitAsync(expectedIndex); // native replay feeds the same atomic application
                log.Machine.Validate(32);
                if (log.LastCommittedEntryIndex != expectedIndex) throw new InvalidDataException("Replayed application frontier differs.");
            }
            Console.WriteLine("Adapter checks passed: no early ACK, actual empty-entry metadata, 15-entry application bound, repeated commit, canceled waiter, native WAL term/tail and DBreeze reopen/replay.");
            success = true;
            return 0;
        }
        finally
        {
            // Root is a literal task parent plus one freshly created GUID, never user data.
            var directory = new DirectoryInfo(root);
            if (success && directory.Parent?.FullName == parent && Guid.TryParseExact(directory.Name, "N", out _) && directory.LinkTarget is null)
                directory.Delete(recursive: true);
        }
    }
}
