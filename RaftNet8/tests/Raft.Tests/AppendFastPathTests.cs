using System.Collections.Immutable;
using Raft.Runtime;
using Raft.Storage;

namespace Raft.Tests;

public sealed class AppendFastPathTests
{
    private sealed class ObservedLog(DBreezeLogStorage storage) : ILogStore, ILogStoreProgress
    {
        internal readonly List<LogWrite> Writes = [];
        internal readonly List<LogQuery> Queries = [];
        public long UncommittedBytes => storage.Recover().UncommittedBytes;
        public ValueTask<RecoveredLogState> RecoverAsync(CancellationToken token) => ValueTask.FromResult(storage.Recover());
        public ValueTask<LogReadResult> ReadAsync(LogQuery query, CancellationToken token)
        { Queries.Add(query); return ValueTask.FromResult(storage.Query(query)); }
        public ValueTask WriteAsync(LogWrite write, CancellationToken token)
        { Writes.Add(write); storage.Write(write); return ValueTask.CompletedTask; }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Theory]
    [InlineData(0)]
    [InlineData(32)]
    public async Task Extending_the_durable_tail_does_not_replace_a_suffix_or_reload_configuration(int existing)
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"] };
        using var storage = DBreezeLogStorage.CreateInMemory(options);
        storage.SaveHardState(new(1));
        if (existing > 0) storage.Append(Enumerable.Range(1, existing).Select(i => new LogEntry(i, 1, "old"u8)).ToArray());
        var observed = new ObservedLog(storage);
        var driver = new RaftDriver(options, observed, storage.Recover(), new SystemRaftRandom());
        var incoming = Enumerable.Range(existing + 1, 15).Select(i => new LogEntry(i, 1, "new"u8)).ToArray();
        var response = await driver.HandleRequestAsync("n2", new AppendRequest(1, 42, existing, existing == 0 ? 0 : 1, existing, incoming));
        Assert.Equal(new AppendResponse(1, 42, true, existing + 15), response);
        Assert.DoesNotContain(observed.Queries, query => query is LogQuery.ConfigurationAt);
        Assert.DoesNotContain(observed.Writes.SelectMany(Flatten), write => write is LogWrite.ReplaceSuffix);
        Assert.Contains(observed.Writes.SelectMany(Flatten), write => write is LogWrite.Append);
        Assert.Equal(existing + 15, storage.LastIndex);
        Assert.Equal(existing, storage.HardState.CommitIndex);
        Assert.Equal(new[] { "n1", "n2", "n3" }, driver.State.Configuration.Voters);
    }

    [Fact]
    public async Task A_real_suffix_conflict_still_rolls_back_the_active_configuration()
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"] };
        using var storage = DBreezeLogStorage.CreateInMemory(options);
        storage.SaveHardState(new(2));
        var configuration = new ClusterConfiguration(["n1", "n2"], [], 2, 1);
        storage.Append([new LogEntry(1, 1, "old"u8), new LogEntry(2, 1, configuration.Encode(), LogEntryKind.Configuration)]);
        var observed = new ObservedLog(storage);
        var driver = new RaftDriver(options, observed, storage.Recover(), new SystemRaftRandom());
        var response = await driver.HandleRequestAsync("n2", new AppendRequest(2, 43, 1, 1, 1, [new LogEntry(2, 2, "replacement"u8)]));
        Assert.Equal(new AppendResponse(2, 43, true, 2), response);
        Assert.Contains(observed.Queries, query => query is LogQuery.ConfigurationAt);
        Assert.Contains(observed.Writes.SelectMany(Flatten), write => write is LogWrite.ReplaceSuffix);
        Assert.Equal(new[] { "n1", "n2", "n3" }, driver.State.Configuration.Voters);
        Assert.Equal(2, storage.GetTerm(2));
    }

    private static IEnumerable<LogWrite> Flatten(LogWrite write) =>
        write is LogWrite.Batch batch ? batch.Writes : new[] { write };
}
