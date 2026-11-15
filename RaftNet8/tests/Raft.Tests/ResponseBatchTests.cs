using System.Collections.Concurrent;
using System.Threading.Channels;
using Raft.Storage;

namespace Raft.Tests;

public sealed class ResponseBatchTests
{
    private sealed class Clock : IClock, ITicker
    {
        private readonly Channel<bool> _ticks = Channel.CreateBounded<bool>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });
        public ITicker CreateTicker(TimeSpan interval) => this;
        public ValueTask<bool> WaitForNextTickAsync(CancellationToken token) => _ticks.Reader.ReadAsync(token);
        internal void Pulse() => _ticks.Writer.TryWrite(true);
        public ValueTask DisposeAsync() { _ticks.Writer.TryComplete(); return ValueTask.CompletedTask; }
    }
    private sealed record Call(string Peer, AppendRequest Request, AppendResponse Reply)
    {
        internal readonly TaskCompletionSource<RaftRpc> Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    private sealed class Network
    {
        internal readonly ConcurrentDictionary<string, RaftNode> Nodes = new();
        internal readonly ConcurrentQueue<Call> Calls = new();
        internal string? HeldLeader;
        internal void ReleaseAll()
        {
            Volatile.Write(ref HeldLeader, null);
            foreach (var call in Calls) call.Release.TrySetResult(call.Reply);
        }
        internal sealed class Transport(string from, Network network) : ITransport
        {
            public async ValueTask<RaftRpc> SendAsync(string peer, RaftRpc request, CancellationToken token)
            {
                var reply = await network.Nodes[peer].HandleAsync(from, request, token);
                if (Volatile.Read(ref network.HeldLeader) != from || request is not AppendRequest { Entries.Count: > 0 } append)
                    return reply;
                var call = new Call(peer, append, Assert.IsType<AppendResponse>(reply));
                network.Calls.Enqueue(call);
                if (Volatile.Read(ref network.HeldLeader) != from) call.Release.TrySetResult(reply);
                return await call.Release.Task.WaitAsync(token);
            }
        }
    }
    private sealed class Store(ILogStore inner) : ILogStore, ILogStoreProgress
    {
        internal readonly TaskCompletionSource AppendEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource ReleaseAppend = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource CommitEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource ReleaseCommit = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly ConcurrentQueue<long> Commits = new();
        internal int PauseAppend, PauseCommit;
        private long _commit;
        public long UncommittedBytes => ((ILogStoreProgress)inner).UncommittedBytes;
        public ValueTask<RecoveredLogState> RecoverAsync(CancellationToken token) => inner.RecoverAsync(token);
        public ValueTask<LogReadResult> ReadAsync(LogQuery query, CancellationToken token) => inner.ReadAsync(query, token);
        public async ValueTask WriteAsync(LogWrite write, CancellationToken token)
        {
            if (Commands(write) > 0 && Interlocked.Exchange(ref PauseAppend, 0) != 0)
            { AppendEntered.TrySetResult(); await ReleaseAppend.Task.WaitAsync(token); }
            var commit = Commit(write);
            if (commit > _commit && Volatile.Read(ref PauseCommit) != 0)
            {
                Commits.Enqueue(commit);
                if (Interlocked.CompareExchange(ref PauseCommit, 2, 1) == 1)
                { CommitEntered.TrySetResult(); await ReleaseCommit.Task.WaitAsync(token); }
            }
            await inner.WriteAsync(write, token);
            _commit = Math.Max(_commit, commit);
        }
        private static int Commands(LogWrite write) => write switch
        {
            LogWrite.Append a => a.Entries.Count(e => !e.IsNoOp),
            LogWrite.Batch b => b.Writes.Sum(Commands), _ => 0
        };
        private static long Commit(LogWrite write) => write switch
        {
            LogWrite.HardStateUpdate h => h.State.CommitIndex,
            LogWrite.Batch b => b.Writes.Max(Commit), _ => 0
        };
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Already_queued_cumulative_ACKs_skip_intermediate_flushes_but_cannot_cross_a_read_control(bool readBarrier)
    {
        var network = new Network();
        var stores = new Dictionary<string, Store>();
        var clocks = new Dictionary<string, Clock>();
        var machines = new Dictionary<string, TestStateMachine>();
        using var stopPulses = new CancellationTokenSource();
        Task? pulses = null;
        var ids = new[] { "n1", "n2", "n3" };
        try
        {
            foreach (var id in ids)
            {
                var options = new RaftOptions { NodeId = id, Members = ids, ElectionMinTicks = 200, ElectionMaxTicks = 240,
                    RpcTimeout = TimeSpan.FromSeconds(2), MaxAppendEntries = 64, MaxAppendBytes = 256, MaxReplicationBytes = 256,
                    MaxCommandBytes = 64, ProposalBatchDelay = TimeSpan.Zero };
                var store = new Store(new RaftStorageAdapter(DBreezeLogStorage.CreateInMemory(options, DBreezeLogLayout.UnifiedTableV3)));
                var machine = new TestStateMachine();
                var clock = new Clock();
                stores.Add(id, store); clocks.Add(id, clock); machines.Add(id, machine);
                network.Nodes[id] = await RaftNode.CreateAsync(options, store, new Network.Transport(id, network), machine, clock);
            }
            foreach (var node in network.Nodes.Values) await node.StartAsync();
            pulses = Task.Run(async () =>
            {
                while (!stopPulses.IsCancellationRequested)
                { foreach (var clock in clocks.Values) clock.Pulse(); await Task.Delay(1, stopPulses.Token); }
            });
            await RuntimeCluster.EventuallyAsync(() => network.Nodes.Values.Count(n => n.Status.Role == RaftRole.Leader && n.Status.CommitIndex == 1) == 1);
            var leader = network.Nodes.Values.Single(n => n.Status.Role == RaftRole.Leader);
            // The initial quorum does not imply that the third peer has left probing. This fixture needs both full windows.
            await RuntimeCluster.EventuallyAsync(() => leader.Status.Peers.All(peer => peer.MatchIndex == 1));
            stopPulses.Cancel();
            try { await pulses; } catch (OperationCanceledException) { }
            await leader.ReadIndexAsync().WaitAsync(TimeSpan.FromSeconds(5));
            // Drain startup/no-op/read flights before intercepting data replies; clocks stay stopped during the assertions.
            await RuntimeCluster.EventuallyAsync(() => leader.Status.Peers.All(peer => peer.InFlightMessages == 0));
            var storeOfLeader = stores[leader.Status.NodeId];
            Volatile.Write(ref network.HeldLeader, leader.Status.NodeId);
            Volatile.Write(ref storeOfLeader.PauseAppend, 1);
            Volatile.Write(ref storeOfLeader.PauseCommit, 1);
            var proposals = new List<Task<ProposalResult>> { leader.ProposeAsync(new byte[64]) };
            await storeOfLeader.AppendEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            for (var i = 1; i < 12; i++) proposals.Add(leader.ProposeAsync(new byte[64]));
            storeOfLeader.ReleaseAppend.TrySetResult();
            var peer = ids.First(id => id != leader.Status.NodeId);
            await RuntimeCluster.EventuallyAsync(() => network.Calls.Count(c => c.Peer == peer) >= 4);
            var calls = network.Calls.Where(c => c.Peer == peer).OrderBy(c => c.Reply.MatchIndex).ToArray();
            Assert.Equal(new long[] { 2, 5, 8, 11 }, calls.Select(c => c.Reply.MatchIndex));
            Assert.All(calls, c => Assert.True(c.Reply.Success));
            await leader.ReportRpcCompletionAsync(peer, calls[0].Request, calls[0].Reply);
            await storeOfLeader.CommitEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await leader.ReportRpcCompletionAsync(peer, calls[1].Request, calls[1].Reply);
            Task<ReadIndexResult>? read = readBarrier ? leader.ReadIndexAsync() : null;
            await leader.ReportRpcCompletionAsync(peer, calls[2].Request, calls[2].Reply);
            await leader.ReportRpcCompletionAsync(peer, calls[3].Request, calls[3].Reply);
            storeOfLeader.ReleaseCommit.TrySetResult();
            await RuntimeCluster.EventuallyAsync(() => network.Calls.Any(c => c.Peer == peer && c.Reply.MatchIndex == 13));
            var last = network.Calls.Single(c => c.Peer == peer && c.Reply.MatchIndex == 13);
            await leader.ReportRpcCompletionAsync(peer, last.Request, last.Reply);
            await Task.WhenAll(proposals).WaitAsync(TimeSpan.FromSeconds(5));
            if (read is not null) await read.WaitAsync(TimeSpan.FromSeconds(5));
            if (readBarrier) Assert.Equal(new long[] { 2, 5, 8, 11, 13 }, storeOfLeader.Commits);
            else Assert.InRange(storeOfLeader.Commits.Count, 2, 3);
            Assert.Equal(13, leader.Status.CommitIndex);
            Assert.Equal(0, leader.Metrics.PendingProposals);
            network.ReleaseAll(); // Deliver the real RPCs too: all are now duplicates/stale and must be harmless.
            await RuntimeCluster.EventuallyAsync(() => leader.Status.Peers.All(p => p.InFlightMessages == 0));
            clocks[leader.Status.NodeId].Pulse(); // publish the coalesced frontier on the scheduled heartbeat
            await RuntimeCluster.EventuallyAsync(() => machines.Values.All(m => m.LastApplied == 13));
            Assert.All(network.Nodes.Values, n => Assert.Null(n.Status.Fault));
        }
        finally
        {
            stopPulses.Cancel();
            if (pulses is not null) try { await pulses; } catch (OperationCanceledException) { }
            foreach (var store in stores.Values) { store.ReleaseAppend.TrySetResult(); store.ReleaseCommit.TrySetResult(); }
            network.ReleaseAll();
            foreach (var node in network.Nodes.Values) await node.DisposeAsync();
        }
    }
}
