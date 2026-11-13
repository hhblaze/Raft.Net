using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Raft.Core;
using Raft.Storage;

namespace Raft;

public sealed partial class RaftNode
{
    private sealed record SnapshotCreatedWork(long ClientId, SnapshotMetadata Metadata) : Work;
    private sealed record SnapshotReadyWork(string From, long Term, SnapshotMetadata Metadata) : Work;
    private sealed record SnapshotInstalledWork(long Id, SnapshotMetadata Metadata) : Work;
    private sealed class SnapshotSupersededException() : InvalidOperationException("Snapshot was superseded by installation.");
    private readonly SnapshotRepository? _snapshots;
    private readonly SemaphoreSlim _applyGate = new(1, 1), _snapshotBandwidth = new(1, 1);
    private readonly object _snapshotGate = new();
    private readonly ConcurrentDictionary<long, TaskCompletionSource<ControlResult.Snapshot>> _snapshotCompletions = new();
    private CancellationTokenSource? _buildStop;
    private Task<SnapshotMetadata>? _snapshotBuilder;
    private Task? _snapshotInstallation;
    private Task<SnapshotImage?>? _snapshotPreparation;
    private string? _preparingSnapshotId;
    private long _snapshotNextSend;
    private int _snapshotActivationPending;
    private bool _snapshotStopping;
    private bool _snapshotMaintenanceBusy;
    private Task? _snapshotMaintenance;
    private int _snapshotReceivers, _snapshotCollectionElapsed;

    /// <summary>Creates a consistent image outside apply and publishes/compacts after it is durable. Cancellation stops waiting.</summary>
    public async Task<SnapshotMetadata> CreateSnapshotAsync(CancellationToken cancellationToken = default)
    {
        EnsureRunning();
        if (_snapshots is null || _stateMachine is not ISnapshotStateMachine)
            throw new NotSupportedException("Configure SnapshotDirectory and a snapshot-capable state machine.");
        Task<SnapshotMetadata> builder;
        lock (_snapshotGate)
        {
            if (_snapshotStopping) throw new OperationCanceledException(_lifetime.Token);
            if (_snapshotMaintenanceBusy) throw new InvalidOperationException("Snapshot retention is running; retry shortly.");
            if (_snapshotBuilder is { IsCompleted: false }) throw new InvalidOperationException("A snapshot is already being created.");
            if (_snapshotActivationPending != 0 || _snapshotPreparation is { IsCompleted: false })
                throw new InvalidOperationException("Snapshot reception/installation is in progress.");
            if (_stateMachine.LastApplied == 0) throw new InvalidOperationException("Apply an entry before creating a snapshot.");
            builder = StartSnapshotBuilder();
        }
        return await builder.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private Task<SnapshotMetadata> StartSnapshotBuilder()
    {
        _buildStop?.Dispose();
        _buildStop = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        _snapshotBuilder = Task.Run(() => BuildSnapshotAsync(_buildStop.Token));
        _ = _snapshotBuilder.ContinueWith(faulted => _ = faulted.Exception, CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return _snapshotBuilder;
    }

    private async Task<SnapshotMetadata> BuildSnapshotAsync(CancellationToken token)
    {
        try
        {
            var machine = (ISnapshotStateMachine)_stateMachine;
            var checkpoint = machine.Checkpoint;
            var previousMetadata = Volatile.Read(ref _status).Snapshot;
            if (previousMetadata is not null && checkpoint.Index <= previousMetadata.LastIncludedIndex) return previousMetadata;
            SnapshotImage? previous = null;
            if (previousMetadata is not null) previous = await _snapshots!.OpenAsync(previousMetadata, token).ConfigureAwait(false);
            var configuration = await _logStore.ReadAsync(new LogQuery.ConfigurationAt(checkpoint.Index), token).ConfigureAwait(false);
            var context = new SnapshotBuildContext(checkpoint, configuration.Configuration ?? throw new InvalidDataException("Missing snapshot configuration."),
                previous, _options, (from, count, bytes, ct) => _logStore.ReadAsync(new LogQuery.Entries(from, count, bytes), ct));
            var image = await _snapshots!.CreateAsync(machine, context, token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            var id = Interlocked.Increment(ref _controlId);
            var completion = new TaskCompletionSource<ControlResult.Snapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
            _snapshotCompletions.TryAdd(id, completion);
            try
            {
                await _work.Writer.WriteAsync(new SnapshotCreatedWork(id, image.Metadata), token).ConfigureAwait(false);
                // Keep the source pinned through the owner's publication, even if an incoming install cancels the builder.
                var result = await completion.Task.ConfigureAwait(false);
                if (result.Error is not null) throw new SnapshotSupersededException();
                return result.Metadata;
            }
            finally { _snapshotCompletions.TryRemove(id, out _); }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { throw; }
        catch (ChannelClosedException) when (_lifetime.IsCancellationRequested) { throw new OperationCanceledException(_lifetime.Token); }
        catch (SnapshotSupersededException) { throw; }
        catch (Exception error) { Fail(error); throw; }
    }

    private void MaybeStartAutomaticSnapshot(long applied)
    {
        if (_snapshots is null || _options.SnapshotIntervalEntries == 0 ||
            applied - (Volatile.Read(ref _status).Snapshot?.LastIncludedIndex ?? 0) < _options.SnapshotIntervalEntries) return;
        lock (_snapshotGate)
            if (!_snapshotStopping && !_snapshotMaintenanceBusy && _snapshotActivationPending == 0 && _snapshotBuilder is not { IsCompleted: false } &&
                _snapshotPreparation is not { IsCompleted: false }) _ = StartSnapshotBuilder();
    }

    private async ValueTask<RaftRpc> ReceiveSnapshotChunkAsync(string from, SnapshotChunkRequest request, long term, CancellationToken token)
    {
        lock (_snapshotGate)
        {
            if (_snapshotStopping || _snapshotMaintenanceBusy)
                return new SnapshotChunkResponse(term, request.RequestId, request.StorageId, 0, false, false);
            _snapshotReceivers++;
        }
        try
        {
            lock (_snapshotGate)
                if (_snapshotPreparation is { IsCompleted: false } && _preparingSnapshotId != request.StorageId)
                    return new SnapshotChunkResponse(term, request.RequestId, request.StorageId, 0, false, false);
            var staged = await _snapshots!.ReceiveAsync(request, token).ConfigureAwait(false);
            RaftInstrumentation.SnapshotBytes(_options.GroupId, _options.NodeId, from, request.Data.Length, send: false);
            Interlocked.Exchange(ref _receivedSnapshotOffset, staged.NextOffset);
            Task<SnapshotImage?>? prepared = null;
            if (staged.Image is not null || staged.Verification is not null)
            {
                lock (_snapshotGate)
                {
                    if (_snapshotStopping) throw new OperationCanceledException(_lifetime.Token);
                    if (_preparingSnapshotId != request.StorageId || _snapshotPreparation is null)
                    {
                        _preparingSnapshotId = request.StorageId;
                        _snapshotPreparation = Task.Run(() => PrepareIncomingSnapshotAsync(staged, from, term));
                    }
                    prepared = _snapshotPreparation;
                }
                if (prepared.IsCompletedSuccessfully)
                {
                    if (prepared.Result is null) return new SnapshotChunkResponse(term, request.RequestId, request.StorageId, 0, false, false);
                    _work.Writer.TryWrite(new SnapshotReadyWork(from, term, prepared.Result.Metadata));
                }
            }
            return new SnapshotChunkResponse(term, request.RequestId, request.StorageId, staged.NextOffset, staged.Accepted, false);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested || _lifetime.IsCancellationRequested) { throw; }
        catch (Exception error) { Fail(error); throw; }
        finally { lock (_snapshotGate) _snapshotReceivers--; }
    }

    private void MaybeCollectSnapshots()
    {
        if (_snapshots is null || ++_snapshotCollectionElapsed < 128) return;
        lock (_snapshotGate)
        {
            if (_snapshotStopping || _snapshotMaintenanceBusy || _snapshotReceivers != 0 || _snapshotActivationPending != 0 ||
                _snapshotBuilder is { IsCompleted: false } || _snapshotPreparation is { IsCompleted: false }) return;
            _snapshotCollectionElapsed = 0;
            var ids = _core.State.Peers.Select(peer => peer.SendingSnapshot?.StorageId)
                .Append(_core.State.Snapshot?.StorageId).Append(_preparingSnapshotId).OfType<string>().ToArray();
            _snapshotMaintenanceBusy = true;
            _snapshotMaintenance = Task.Run(() =>
            {
                try { _snapshots.Collect(ids); }
                catch (Exception error) { Fail(error); }
                finally { lock (_snapshotGate) _snapshotMaintenanceBusy = false; }
            });
        }
    }

    private async Task<SnapshotImage?> PrepareIncomingSnapshotAsync(ReceivedChunk staged, string from, long term)
    {
        try
        {
            var image = staged.Image ?? await staged.Verification!.ConfigureAwait(false);
            if (image is null) return null;
            Task<SnapshotMetadata>? builder;
            lock (_snapshotGate) { _buildStop?.Cancel(); builder = _snapshotBuilder; }
            if (builder is not null)
            {
                try { await builder.ConfigureAwait(false); }
                catch (OperationCanceledException) { }
                catch (SnapshotSupersededException) { }
            }
            await ((ISnapshotStateMachine)_stateMachine).PrepareSnapshotAsync(image, _options.MaxSnapshotUnpackedBytes, _lifetime.Token).ConfigureAwait(false);
            await _work.Writer.WriteAsync(new SnapshotReadyWork(from, term, image.Metadata), _lifetime.Token).ConfigureAwait(false);
            return image;
        }
        catch (SnapshotValidationException) { return null; } // invalid unactivated images leave the current generation intact
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { return null; }
        catch (ChannelClosedException) when (_lifetime.IsCancellationRequested) { return null; }
        catch (Exception error) { Fail(error); return null; }
    }

    private void StartSnapshotInstallation(long id, SnapshotMetadata metadata)
    {
        Interlocked.Exchange(ref _snapshotActivationPending, 1);
        lock (_snapshotGate)
        {
            _buildStop?.Cancel();
            var builder = _snapshotBuilder;
            _snapshotInstallation = Task.Run(async () =>
            {
                try
                {
                    if (builder is not null)
                    {
                        try { await builder.ConfigureAwait(false); }
                        catch (OperationCanceledException) { }
                        catch (SnapshotSupersededException) { }
                    }
                    var image = await _snapshots!.OpenAsync(metadata, _lifetime.Token).ConfigureAwait(false);
                    await _applyGate.WaitAsync(_lifetime.Token).ConfigureAwait(false);
                    try
                    {
                        await ((ISnapshotStateMachine)_stateMachine).InstallSnapshotAsync(image, _lifetime.Token).ConfigureAwait(false);
                        if (((ISnapshotStateMachine)_stateMachine).Checkpoint != new AppliedCheckpoint(metadata.LastIncludedIndex, metadata.LastIncludedTerm))
                            throw new InvalidDataException("Activated application checkpoint differs from its durable install intent.");
                        Interlocked.Exchange(ref _lastApplied, metadata.LastIncludedIndex);
                        foreach (var pair in _pending)
                            if (pair.Key <= metadata.LastIncludedIndex && _pending.TryRemove(pair.Key, out var proposal))
                                Finish(proposal, new(pair.Key, proposal.Term), null);
                        CompleteReadyReads();
                    }
                    finally { _applyGate.Release(); }
                    await _work.Writer.WriteAsync(new SnapshotInstalledWork(id, metadata), _lifetime.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
                catch (ChannelClosedException) when (_lifetime.IsCancellationRequested) { }
                catch (Exception error) { Fail(error); }
            });
        }
    }

    private async ValueTask ThrottleSnapshotAsync(int bytes, CancellationToken token)
    {
        if (bytes == 0) return;
        await _snapshotBandwidth.WaitAsync(token).ConfigureAwait(false);
        try
        {
            var now = Stopwatch.GetTimestamp();
            if (_snapshotNextSend > now)
                await Task.Delay(TimeSpan.FromSeconds((double)(_snapshotNextSend - now) / Stopwatch.Frequency), token).ConfigureAwait(false);
            _snapshotNextSend = Stopwatch.GetTimestamp() + (long)Math.Ceiling((double)bytes * Stopwatch.Frequency / _options.SnapshotBytesPerSecond);
        }
        finally { _snapshotBandwidth.Release(); }
    }

    private async Task StopSnapshotWorkersAsync()
    {
        _work.Writer.TryComplete(_fault);
        Task?[] tasks;
        lock (_snapshotGate)
        {
            _snapshotStopping = true;
            _buildStop?.Cancel();
            tasks = [_snapshotBuilder, _snapshotPreparation, _snapshotInstallation, _snapshotMaintenance];
        }
        foreach (var pair in _snapshotCompletions)
            if (_snapshotCompletions.TryRemove(pair.Key, out var completion))
            {
                if (_fault is { } error) completion.TrySetException(error);
                else completion.TrySetCanceled(_lifetime.Token);
            }
        _snapshots?.Stop();
        foreach (var task in tasks)
        {
            if (task is null) continue;
            try { await task.ConfigureAwait(false); }
            catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
            catch (SnapshotSupersededException) { }
            catch (Exception error) { Fail(error); }
        }
        try { if (_snapshots is not null) await _snapshots.DisposeAsync().ConfigureAwait(false); }
        catch (Exception error) { Fail(error); }
        lock (_snapshotGate) { _buildStop?.Dispose(); _buildStop = null; }
    }

    private static async ValueTask<RecoveredLogState> RecoverSnapshotAsync(RaftOptions options, ILogStore log,
        IStateMachine machine, RecoveredLogState recovered, CancellationToken token)
    {
        if (recovered.PendingSnapshotInstall is null && recovered.Snapshot is null) return recovered;
        if (options.SnapshotDirectory is null || machine is not ISnapshotStateMachine snapshots)
            throw new InvalidOperationException("Compacted/installing data requires SnapshotDirectory and a snapshot-capable application.");
        await using var repository = new SnapshotRepository(options);
        if (recovered.PendingSnapshotInstall is { } intent)
        {
            var image = await repository.OpenAsync(intent.Metadata, token).ConfigureAwait(false);
            await snapshots.PrepareSnapshotAsync(image, options.MaxSnapshotUnpackedBytes, token).ConfigureAwait(false);
            await snapshots.InstallSnapshotAsync(image, token).ConfigureAwait(false);
            if (snapshots.Checkpoint != new AppliedCheckpoint(intent.Metadata.LastIncludedIndex, intent.Metadata.LastIncludedTerm))
                throw new InvalidDataException("Recovered application checkpoint differs from the pending install.");
            await log.WriteAsync(new LogWrite.CompleteSnapshotInstall(intent), token).ConfigureAwait(false);
            recovered = await log.RecoverAsync(token).ConfigureAwait(false);
        }
        if (recovered.Snapshot is { } metadata)
        {
            var image = await repository.OpenAsync(metadata, token).ConfigureAwait(false);
            if (snapshots.Checkpoint.Index < metadata.LastIncludedIndex)
            {
                await snapshots.PrepareSnapshotAsync(image, options.MaxSnapshotUnpackedBytes, token).ConfigureAwait(false);
                await snapshots.InstallSnapshotAsync(image, token).ConfigureAwait(false);
            }
        }
        return recovered;
    }
}
