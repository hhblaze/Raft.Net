using Raft.Runtime;
using Raft.Storage;

namespace Raft.Tests;

public sealed class AsyncDriverTests
{
    private sealed class EndedClock : IClock, ITicker
    {
        public ITicker CreateTicker(TimeSpan interval) => this;
        public ValueTask<bool> WaitForNextTickAsync(CancellationToken cancellationToken) => new(false);
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task Unexpected_worker_exit_faults_completion_instead_of_leaving_a_half_running_node()
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        var adapter = new RaftStorageAdapter(DBreezeRaftStorage.CreateInMemory(options));
        await using var node = await RaftNode.CreateAsync(options, adapter, new NoPeers(), new TestStateMachine(), new EndedClock());
        await node.StartAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => node.Completion.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.NotNull(node.Status.Fault);
    }

    private sealed class FixedRandom : IRandom
    {
        public int Calls;
        public int Next(int minInclusive, int maxExclusive) { Calls++; return minInclusive; }
    }

    private sealed class GateStore(IRaftStorage storage) : ILogStore
    {
        private int _hold = 1;
        public TaskCompletionSource<LogWrite> Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ValueTask<RecoveredLogState> RecoverAsync(CancellationToken token) => new(RaftStorageAdapter.Recover(storage));
        public ValueTask<LogReadResult> ReadAsync(LogQuery query, CancellationToken token) => new(RaftStorageAdapter.Read(storage, query));
        public async ValueTask WriteAsync(LogWrite write, CancellationToken token)
        {
            if (Interlocked.Exchange(ref _hold, 0) == 1)
            {
                Entered.TrySetResult(write);
                await Release.Task.WaitAsync(token);
            }
            RaftStorageAdapter.Write(storage, write);
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task Driver_does_not_release_election_messages_while_durability_is_pending()
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"] };
        using var storage = DBreezeRaftStorage.CreateInMemory(options);
        var gate = new GateStore(storage);
        var random = new FixedRandom();
        var driver = new RaftDriver(options, gate, new(new(), 0, 0), random);
        var campaign = driver.CampaignAsync().AsTask();
        Assert.IsType<LogWrite.HardStateUpdate>(await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.False(campaign.IsCompleted);
        Assert.Equal(0, driver.Term);
        Assert.False(driver.TryDequeue(out _));
        gate.Release.SetResult();
        await campaign.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, storage.HardState.Term);
        Assert.Equal(1, driver.Term);
        Assert.Equal(2, random.Calls);
        for (var i = 0; i < 2; i++)
        {
            Assert.True(driver.TryDequeue(out var rpc));
            Assert.Equal(new VoteRequest(1, 0, 0), rpc.Request);
        }
        Assert.False(driver.TryDequeue(out _));
    }

    [Fact]
    public async Task Driver_waits_for_follower_log_and_commit_before_completing_the_RPC()
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"] };
        using var storage = DBreezeRaftStorage.CreateInMemory(options);
        storage.SaveHardState(new HardState(3));
        var gate = new GateStore(storage);
        var driver = new RaftDriver(options, gate, new(new(3), 0, 0), new FixedRandom());
        var request = driver.HandleRequestAsync("n2", new AppendRequest(3, 5, 0, 0, 1,
            [new LogEntry(1, 3, "data"u8)])).AsTask();
        Assert.IsType<LogWrite.Batch>(await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.False(request.IsCompleted);
        Assert.Equal(0, driver.LastIndex);
        Assert.Equal(0, driver.ApplyThrough);
        gate.Release.SetResult();
        Assert.Equal(new AppendResponse(3, 5, true, 1), await request.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(1, storage.HardState.CommitIndex);
        Assert.Equal(1, driver.ApplyThrough);
    }

    [Fact]
    public async Task Driver_stops_without_sending_when_async_storage_fails()
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"] };
        using var storage = DBreezeRaftStorage.CreateInMemory(options);
        var gate = new GateStore(storage);
        var driver = new RaftDriver(options, gate, new(new(), 0, 0), new FixedRandom());
        var campaign = driver.CampaignAsync().AsTask();
        await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var error = new IOException("flush failed");
        gate.Release.SetException(error);
        Assert.Same(error, await Assert.ThrowsAsync<IOException>(() => campaign));
        Assert.Equal(new Raft.Core.CoreFailure(typeof(IOException).FullName!, error.Message), driver.State.Fault);
        Assert.Equal(0, driver.Term);
        Assert.False(driver.TryDequeue(out _));
    }

    private sealed class PausedClock : IClock, ITicker
    {
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool Disposed;
        public ITicker CreateTicker(TimeSpan interval) => this;
        public async ValueTask<bool> WaitForNextTickAsync(CancellationToken cancellationToken)
        {
            Started.TrySetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return false;
        }
        public ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
    }
    private sealed class NoPeers : ITransport
    {
        public ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken) =>
            throw new IOException("unreachable");
    }

    [Fact]
    public async Task Async_factory_uses_and_disposes_the_injected_clock_and_preserves_the_legacy_machine_contract()
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"] };
        var adapter = new RaftStorageAdapter(DBreezeRaftStorage.CreateInMemory(options));
        var clock = new PausedClock();
        var random = new FixedRandom();
        await using var node = await RaftNode.CreateAsync(options, adapter, new NoPeers(), new TestStateMachine(), clock, random);
        await node.StartAsync();
        await clock.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, node.Status.Term);
        Assert.Equal(RaftRole.Follower, node.Status.Role);
        Assert.Equal(new VoteResponse(1, true), await node.HandleAsync("n2", new VoteRequest(1, 0, 0)));
        Assert.Equal(3, random.Calls); // initialization, higher-term adoption, vote grant
        await node.DisposeAsync();
        await node.Completion;
        Assert.True(clock.Disposed);
    }
}
