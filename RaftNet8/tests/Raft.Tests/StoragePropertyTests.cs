using System.Collections.Immutable;
using DBreeze;
using Raft.Simulation;
using Raft.Storage;

namespace Raft.Tests;

public sealed class StoragePropertyTests
{
    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3)]
    public void Random_atomic_log_operations_and_reopens_match_an_independent_list_oracle(DBreezeLogLayout layout)
    {
        for (var seed = 0; seed < 32; seed++)
        {
            var random = new Random(seed);
            var options = new RaftOptions { NodeId = "n1", Members = ["n1"], KnownPeers = ["n2"], MaxCommandBytes = 128 };
            using var engine = new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
            var store = new DBreezeLogStorage(engine, options, layout);
            var model = new ModelDisk(options);
            var oracle = new List<LogEntry>();
            var hard = new HardState(1);
            try
            {
                Write(new LogWrite.HardStateUpdate(hard));
                for (var step = 0; step < 160; step++)
                {
                    switch (random.Next(4))
                    {
                        case 0:
                            hard = hard with { Term = hard.Term + 1, VotedFor = null };
                            Write(new LogWrite.HardStateUpdate(hard));
                            break;
                        case 1:
                            hard = hard with { CommitIndex = random.NextInt64(hard.CommitIndex, oracle.Count + 1) };
                            Write(new LogWrite.HardStateUpdate(hard));
                            break;
                        default:
                            var from = random.Next(2) == 0 ? oracle.Count + 1 : (int)random.NextInt64(hard.CommitIndex + 1, oracle.Count + 2);
                            var entries = ImmutableArray.CreateBuilder<LogEntry>();
                            var count = random.Next(6);
                            for (var i = 0; i < count; i++)
                            {
                                var kind = random.Next(8);
                                if (kind == 0) entries.Add(new(from + i, hard.Term, [], true));
                                else if (kind == 1)
                                {
                                    var config = new ClusterConfiguration(["n1"], random.Next(2) == 0 ? [] : ["n2"]);
                                    entries.Add(new(from + i, hard.Term, config.Encode(), LogEntryKind.Configuration));
                                }
                                else
                                {
                                    var bytes = new byte[random.Next(1, 80)];
                                    random.NextBytes(bytes);
                                    entries.Add(new(from + i, hard.Term, bytes));
                                }
                            }
                            var suffix = entries.ToImmutable();
                            Write(new LogWrite.ReplaceSuffix(from, suffix));
                            oracle.RemoveRange(from - 1, oracle.Count - from + 1);
                            oracle.AddRange(suffix);
                            break;
                    }
                    if (step % 17 == 0) { store.Dispose(); store = new DBreezeLogStorage(engine, options, layout); }
                    var actual = store.Recover();
                    var simulated = model.Recover();
                    Assert.True(actual.HardState == hard && simulated.HardState == hard, $"seed={seed}, step={step}");
                    Assert.Equal(oracle.Count, actual.LastIndex);
                    Assert.Equal(actual.LastIndex, simulated.LastIndex);
                    Assert.Equal(oracle.Where(e => e.Index > hard.CommitIndex).Sum(e => (long)ModelDisk.Size(e)), actual.UncommittedBytes);
                    Assert.Equal(actual.UncommittedBytes, simulated.UncommittedBytes);
                    Assert.Equal(actual.UncommittedBytes, store.UncommittedBytes);
                    var actualEntries = store.Read(1, 10000, 16 * 1024 * 1024);
                    Assert.Equal(oracle.Count, actualEntries.Count);
                    for (var i = 0; i < oracle.Count; i++) Assert.True(ModelDisk.Same(oracle[i], actualEntries[i]), $"seed={seed}, step={step}, index={i + 1}");
                    var expectedConfig = oracle.LastOrDefault(e => e.Kind == LogEntryKind.Configuration);
                    Assert.Equal(expectedConfig?.Index ?? 0, actual.Configuration!.Index);
                    Assert.Equal(actual.Configuration.Index, simulated.Configuration!.Index);
                }
            }
            finally { store.Dispose(); }
            continue;

            void Write(LogWrite write) { store.Write(write); model.WriteAtomic(write); }
        }
    }

    [Fact]
    public void Model_recovery_discards_only_unpromised_torn_tail_and_detects_promised_corruption()
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        var disk = new ModelDisk(options);
        disk.WriteAtomic(new LogWrite.HardStateUpdate(new(1)));
        disk.WriteAtomic(new LogWrite.Append([new LogEntry(1, 1, "promised"u8)]));
        disk.TearUnpromisedTail();
        Assert.Equal(1, disk.Recover().LastIndex);
        Assert.Equal("promised"u8.ToArray(), disk.Query(new LogQuery.Entries(1, 1, 1024)).Entries[0].Command.ToArray());
        disk.CorruptPromisedRecord(1);
        Assert.Throws<InvalidDataException>(() => disk.Recover());
    }
}
