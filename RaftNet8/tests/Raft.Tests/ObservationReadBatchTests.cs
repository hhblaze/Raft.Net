using System.Threading.Channels;
using Raft.Cluster.Server;

namespace Raft.Tests;

public sealed class ObservationReadBatchTests
{
    [Fact]
    public async Task Readers_arriving_after_a_barrier_starts_wait_for_the_next_fresh_barrier()
    {
        using var lifetime = new CancellationTokenSource();
        var windows = Channel.CreateUnbounded<TaskCompletionSource>();
        var reads = Channel.CreateUnbounded<TaskCompletionSource<ReadIndexResult>>();
        Task Collect(CancellationToken token)
        {
            var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            windows.Writer.TryWrite(completion);
            return completion.Task.WaitAsync(token);
        }
        Task<ReadIndexResult> Read(CancellationToken token)
        {
            var completion = new TaskCompletionSource<ReadIndexResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            reads.Writer.TryWrite(completion);
            return completion.Task.WaitAsync(token);
        }
        await using var batch = new ObservationReadBatch(Read, lifetime.Token, TimeSpan.FromSeconds(5), Collect);
        try
        {
            var initial = Enumerable.Range(0, 32).Select(_ => batch.ReadAsync(default)).ToArray();
            var firstWindow = await windows.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(reads.Reader.TryRead(out _));
            firstWindow.SetResult();
            var firstRead = await reads.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            var later = batch.ReadAsync(default);
            firstRead.SetResult(new(10, 1));
            Assert.All(await Task.WhenAll(initial), result => Assert.Equal(10, result.Index));
            Assert.False(later.IsCompleted); // It cannot reuse the read that started before it was admitted.
            var secondWindow = await windows.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            secondWindow.SetResult();
            var secondRead = await reads.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            secondRead.SetResult(new(12, 1));
            Assert.Equal(12, (await later).Index);
            Assert.False(reads.Reader.TryRead(out _));
        }
        finally { lifetime.Cancel(); }
    }

    [Fact]
    public async Task Cancelling_one_waiter_does_not_cancel_others_and_the_original_failure_is_preserved()
    {
        using var lifetime = new CancellationTokenSource();
        using var waiter = new CancellationTokenSource();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failure = new IOException("uncertain read execution");
        var calls = 0;
        Task<ReadIndexResult> Read(CancellationToken token) => Interlocked.Increment(ref calls) == 1
            ? Task.FromException<ReadIndexResult>(failure) : Task.FromResult(new ReadIndexResult(20, 2));
        await using var batch = new ObservationReadBatch(Read, lifetime.Token, TimeSpan.FromSeconds(5), token => release.Task.WaitAsync(token));
        try
        {
            var cancelled = batch.ReadAsync(waiter.Token);
            var surviving = batch.ReadAsync(default);
            waiter.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelled);
            release.SetResult();
            Assert.Same(failure, await Assert.ThrowsAsync<IOException>(() => surviving));
            Assert.Equal(20, (await batch.ReadAsync(default)).Index);
            Assert.Equal(2, calls);
        }
        finally { lifetime.Cancel(); }
    }

    [Fact]
    public async Task Shutdown_releases_both_the_running_and_the_waiting_batch()
    {
        using var lifetime = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task<ReadIndexResult> Read(CancellationToken token)
        {
            entered.SetResult();
            await Task.Delay(Timeout.Infinite, token);
            return new(0, 0);
        }
        await using var batch = new ObservationReadBatch(Read, lifetime.Token, TimeSpan.FromSeconds(5), _ => Task.CompletedTask);
        var first = batch.ReadAsync(default);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var waiting = batch.ReadAsync(default);
        lifetime.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
    }
}
