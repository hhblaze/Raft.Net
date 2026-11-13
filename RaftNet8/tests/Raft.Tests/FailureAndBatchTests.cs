using Raft.Storage;

namespace Raft.Tests;

public sealed class FailureAndBatchTests
{
    private sealed class NoPeers : IRaftTransport
    {
        public ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken) =>
            throw new IOException("Unreachable.");
    }

    private sealed class FailingStorage(IRaftStorage inner) : IRaftStorage
    {
        public bool FailSave, FailAppend;
        public Exception? SaveFailure;
        public bool FailAfterSave;
        public HardState HardState => inner.HardState;
        public long LastIndex => inner.LastIndex;
        public long LastTerm => inner.LastTerm;
        public void SaveHardState(HardState state)
        {
            if (SaveFailure is { } error)
            {
                SaveFailure = null;
                if (FailAfterSave) inner.SaveHardState(state);
                throw error;
            }
            if (FailSave) throw new IOException("save failed");
            inner.SaveHardState(state);
        }
        public void Append(IReadOnlyList<LogEntry> entries) { if (FailAppend) throw new IOException("append failed"); inner.Append(entries); }
        public void ReplaceSuffix(long fromIndex, IReadOnlyList<LogEntry> entries) => inner.ReplaceSuffix(fromIndex, entries);
        public long GetTerm(long index) => inner.GetTerm(index);
        public IReadOnlyList<LogEntry> Read(long fromIndex, int maxCount, int maxBytes) => inner.Read(fromIndex, maxCount, maxBytes);
        public long FirstIndexOfTerm(long index) => inner.FirstIndexOfTerm(index);
        public long LastIndexOfTerm(long term) => inner.LastIndexOfTerm(term);
        public void Dispose() => inner.Dispose();
    }

    private sealed class BatchMachine : IRaftBatchStateMachine
    {
        private long _applied;
        public long LastApplied => Interlocked.Read(ref _applied);
        public int LargestBatch;
        public ValueTask ApplyAsync(LogEntry entry, CancellationToken cancellationToken) => throw new InvalidOperationException("Batch path required.");
        public ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken cancellationToken)
        {
            Assert.Equal(LastApplied + 1, entries[0].Index);
            LargestBatch = Math.Max(LargestBatch, entries.Count);
            Interlocked.Exchange(ref _applied, entries[^1].Index);
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task Failed_term_persistence_cannot_produce_a_vote_acknowledgment()
    {
        var options = new RaftOptions { NodeId = "n1", Members = new[] { "n1", "n2", "n3" } };
        var storage = new FailingStorage(DBreezeRaftStorage.CreateInMemory(options)) { FailSave = true };
        await using var node = new RaftNode(options, storage, new NoPeers(), new TestStateMachine());
        await node.StartAsync();
        var error = await Assert.ThrowsAsync<IOException>(() => node.HandleAsync("n2", new VoteRequest(10, 0, 0)).AsTask());
        Assert.Equal("save failed", error.Message);
        await Assert.ThrowsAsync<IOException>(() => node.Completion);
        Assert.Equal(0, storage.HardState.Term);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Any_persistence_failure_including_argument_errors_and_uncertain_writes_must_fail_stop(bool argument, bool afterWrite)
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"] };
        Exception failure = argument ? new ArgumentException("storage failure") : new IOException("storage failure");
        var storage = new FailingStorage(DBreezeRaftStorage.CreateInMemory(options))
            { SaveFailure = failure, FailAfterSave = afterWrite };
        await using var node = new RaftNode(options, storage, new NoPeers(), new TestStateMachine());
        await node.StartAsync();
        Assert.Same(failure, await Assert.ThrowsAnyAsync<Exception>(() => node.HandleAsync("n2", new VoteRequest(10, 0, 0)).AsTask()));
        Assert.Same(failure, await Assert.ThrowsAnyAsync<Exception>(() => node.Completion.WaitAsync(TimeSpan.FromSeconds(2))));
        Assert.Same(failure, node.Status.Fault);
        Assert.Equal(afterWrite ? 10 : 0, storage.HardState.Term);
        var stopped = await Assert.ThrowsAsync<InvalidOperationException>(() => node.HandleAsync("n3", new VoteRequest(11, 0, 0)).AsTask());
        Assert.Same(failure, stopped.InnerException);
    }

    [Fact]
    public async Task Uncertain_vote_write_stops_the_node_and_recovery_retains_the_durable_vote()
    {
        using var directory = new TestDirectory();
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"] };
        var raw = new DBreezeRaftStorage(directory.Path, options);
        raw.SaveHardState(new HardState(10));
        var failure = new ArgumentException("failure after vote commit");
        var storage = new FailingStorage(raw) { SaveFailure = failure, FailAfterSave = true };
        await using (var node = new RaftNode(options, storage, new NoPeers(), new TestStateMachine()))
        {
            await node.StartAsync();
            Assert.Same(failure, await Assert.ThrowsAsync<ArgumentException>(() => node.HandleAsync("n2", new VoteRequest(10, 0, 0)).AsTask()));
            Assert.Same(failure, await Assert.ThrowsAsync<ArgumentException>(() => node.Completion));
            Assert.Equal("n2", storage.HardState.VotedFor);
        }
        using var reopened = new DBreezeRaftStorage(directory.Path, options);
        var recovered = new RaftCore(options, reopened);
        Assert.False(((VoteResponse)recovered.HandleRequest("n3", new VoteRequest(10, 0, 0))).Granted);
        Assert.True(((VoteResponse)recovered.HandleRequest("n2", new VoteRequest(10, 0, 0))).Granted);
    }

    [Fact]
    public async Task Failed_log_persistence_faults_pending_proposal_and_closes_admission()
    {
        var options = new RaftOptions { NodeId = "n1", Members = new[] { "n1" } };
        var storage = new FailingStorage(DBreezeRaftStorage.CreateInMemory(options));
        await using var node = new RaftNode(options, storage, new NoPeers(), new TestStateMachine());
        await node.StartAsync();
        await RuntimeCluster.EventuallyAsync(() => node.Status.Role == RaftRole.Leader && node.Status.CommitIndex == 1);
        storage.FailAppend = true;
        Assert.Equal("append failed", (await Assert.ThrowsAsync<IOException>(() => node.ProposeAsync("data"u8.ToArray()))).Message);
        await Assert.ThrowsAsync<IOException>(() => node.Completion);
        Assert.Equal(1, storage.LastIndex);
        Assert.Equal(0, node.Metrics.PendingProposals);
    }

    [Fact]
    public async Task Optional_batch_application_preserves_order_and_acknowledges_every_proposal()
    {
        var options = new RaftOptions { NodeId = "n1", Members = new[] { "n1" } };
        var machine = new BatchMachine();
        await using var node = new RaftNode(options, DBreezeRaftStorage.CreateInMemory(options), new NoPeers(), machine);
        await node.StartAsync();
        await RuntimeCluster.EventuallyAsync(() => node.Status.Role == RaftRole.Leader && node.Status.CommitIndex == 1);
        var tasks = Enumerable.Range(0, 500).Select(i => node.ProposeAsync(BitConverter.GetBytes(i))).ToArray();
        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(500, results.Length);
        Assert.True(machine.LargestBatch > 1);
        Assert.Equal(501, machine.LastApplied);
        Assert.Equal(501, node.Status.LastApplied);
    }
}
