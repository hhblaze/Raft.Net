namespace Raft.Cluster.Server;

/// <summary>Groups admitted observations before starting a fresh quorum read. Never reuses an earlier fence.</summary>
internal sealed class ObservationReadBatch(
    Func<CancellationToken, Task<ReadIndexResult>> read,
    CancellationToken lifetime,
    TimeSpan timeout,
    Func<CancellationToken, Task>? collect = null) : IAsyncDisposable
{
    private readonly object _gate = new();
    private TaskCompletionSource<ReadIndexResult>? _waiting;
    private bool _running;
    private Task _loop = Task.CompletedTask;

    public Task<ReadIndexResult> ReadAsync(CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        lifetime.ThrowIfCancellationRequested();
        lock (_gate)
        {
            _waiting ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
            var result = _waiting.Task;
            if (!_running)
            {
                _running = true;
                _loop = Task.Run(RunAsync); // No network callback or timer runs under the gate.
            }
            return result.WaitAsync(token);
        }
    }

    private async Task RunAsync()
    {
        try
        {
            while (true)
            {
                lifetime.ThrowIfCancellationRequested();
                await (collect?.Invoke(lifetime) ?? Task.Delay(2, lifetime)).ConfigureAwait(false);
                lifetime.ThrowIfCancellationRequested();
                TaskCompletionSource<ReadIndexResult> batch;
                lock (_gate)
                {
                    batch = _waiting!;
                    _waiting = null; // Later arrivals must wait for the following fresh barrier.
                }
                try
                {
                    using var deadline = CancellationTokenSource.CreateLinkedTokenSource(lifetime);
                    deadline.CancelAfter(timeout);
                    batch.TrySetResult(await read(deadline.Token).ConfigureAwait(false));
                }
                catch (Exception error) { batch.TrySetException(error); }
                lock (_gate)
                {
                    if (_waiting is not null) continue;
                    _running = false;
                    return;
                }
            }
        }
        catch (Exception error)
        {
            lock (_gate)
            {
                _waiting?.TrySetException(error);
                _waiting = null;
                _running = false;
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        // The owning server cancels its lifetime before disposal. At most one read and one waiting batch exist.
        lock (_gate) return new(_loop);
    }
}
