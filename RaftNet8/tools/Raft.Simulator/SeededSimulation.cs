using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Raft.Core;
using Raft.Storage;
using Reducer = Raft.Core.RaftCore;

namespace Raft.Simulation;

public enum SimulationScenario { Elections, Partitions, CrashRecovery, Reads, Membership, Snapshots, Mixed }
public sealed record SimulationResult(int Seed, SimulationScenario Scenario, int Nodes, long Events,
    int SuccessfulWrites, int SuccessfulReads, int Crashes, int Restarts, int TornTails, int StorageFailures,
    int DroppedMessages, int Duplicates, int StaleCallbacks, int SnapshotInstalls, long Flushes, string Digest);

public static class SimulationRunner
{
    public static SimulationResult Run(int seed, SimulationScenario scenario, int steps = 3000, int voters = 3)
    {
        if (steps < 100 || voters is not 3 and not 5) throw new ArgumentOutOfRangeException(nameof(steps));
        return new SeededSimulation(seed, scenario, steps, voters).Run();
    }
}

internal sealed partial class SeededSimulation
{
    private sealed record Scheduled(string Label, Action Run);
    private sealed record RpcContext(string From, int Epoch, long Id, RaftRpc Request);
    private sealed class Input(CoreEvent value, long? client = null, RpcContext? rpc = null)
    {
        public readonly CoreEvent Event = value;
        public readonly long? Client = client;
        public readonly RpcContext? Rpc = rpc;
        public RaftRpc? Reply;
        public long Proposed;
    }
    private sealed class Node(string id, RaftOptions options, int tickPeriod)
    {
        public readonly string Id = id;
        public readonly RaftOptions Options = options;
        public readonly int TickPeriod = tickPeriod;
        public readonly ModelDisk Disk = new(options);
        public readonly ModelApplication Application = new();
        public RaftState State = null!;
        public int Epoch;
        public bool Online = true, Faulted, TickQueued, Applying, ApplyPaused, Building;
        public long ApplyTarget, BuildId;
        public readonly Queue<Input> Inputs = new();
        public Input? Active;
        public readonly List<OutboundRpc> Outbound = new();
        public readonly Dictionary<long, (long Index, long Term)> Writes = new();
        public readonly Dictionary<long, long> Reads = new();
        public ModelIncoming? Incoming;
    }

    private readonly int _seed, _steps, _voters;
    private readonly SimulationScenario _scenario;
    private readonly Random _random;
    private readonly Dictionary<string, Node> _nodes = new(StringComparer.Ordinal);
    private readonly PriorityQueue<Scheduled, (long Time, long Order)> _events = new();
    private readonly HashSet<(string From, string To)> _blocked = new();
    private readonly Dictionary<(string From, int Epoch, long Id), RpcContext> _requests = new();
    private readonly Dictionary<long, HistoryOperation> _history = new();
    private readonly Queue<string> _trace = new();
    private long _now, _order, _eventCount, _clientId, _historyTime, _rpcId;
    private bool _healthy;
    private int _crashes, _restarts, _torn, _storageFailures, _dropped, _duplicates, _stale, _installs;

    public SeededSimulation(int seed, SimulationScenario scenario, int steps, int voters)
    {
        _seed = seed; _scenario = scenario; _steps = steps; _voters = voters; _random = new(seed);
        var members = Enumerable.Range(1, voters).Select(i => $"n{i}").ToArray();
        var ids = members.Append($"n{voters + 1}").ToArray();
        foreach (var id in ids)
        {
            var options = new RaftOptions { NodeId = id, Members = members, Learners = members.Contains(id) ? [] : [id], KnownPeers = ids,
                TickInterval = TimeSpan.FromMilliseconds(20), ElectionMinTicks = 12, ElectionMaxTicks = 24,
                RpcTimeout = TimeSpan.FromMilliseconds(150), MaxAppendEntries = 8, MaxAppendBytes = 4096, MaxCommandBytes = 512,
                MaxInFlightBytes = 16384, MaxUncommittedBytes = 65536, MinimumVoters = 2, SnapshotChunkBytes = 1024,
                SnapshotRetainEntries = 3, MaxSnapshotBytes = 1024 * 1024, MaxSnapshotUnpackedBytes = 2 * 1024 * 1024,
                SnapshotDirectory = scenario is SimulationScenario.Snapshots or SimulationScenario.Mixed ? Path.GetFullPath("simulated-snapshots/" + id) : null }.Freeze();
            var node = new Node(id, options, _random.Next(16, 31));
            _nodes.Add(id, node);
            Start(node, initial: true);
            Schedule(_random.Next(20), $"tick:{id}", () => Tick(node));
        }
        Schedule(10, "workload", Workload);
    }

    public SimulationResult Run()
    {
        try
        {
            for (var i = 0; i < _steps; i++) Step();
            _healthy = true;
            _blocked.Clear();
            foreach (var node in _nodes.Values) if (!node.Online || node.Faulted) Start(node);
            long write = 0, read = 0;
            for (var budget = 0; budget < 20000; budget++)
            {
                var leader = Leader();
                if (leader is not null && leader.State.HasCommittedCurrentTerm)
                {
                    if (write == 0) write = Invoke(leader, read: false, key: 0, value: 1_000_000 + Math.Abs(_seed % 100000));
                    else if (read == 0 && _history[write].Outcome == HistoryOutcome.Success && _history[write].CompletedAt is not null)
                        read = Invoke(leader, read: true, key: 0);
                }
                if (read != 0 && _history[read] is { Outcome: HistoryOutcome.Success, CompletedAt: not null }) break;
                // A leader can lose an old queued client while delayed pre-heal packets drain; retry with a new operation.
                if (write != 0 && _history[write].CompletedAt is not null && _history[write].Outcome != HistoryOutcome.Success) { write = 0; read = 0; }
                if (read != 0 && _history[read].CompletedAt is not null && _history[read].Outcome != HistoryOutcome.Success) read = 0;
                Step();
            }
            Require(read != 0 && _history[read] is { Outcome: HistoryOutcome.Success, CompletedAt: not null }, "No progress after healing.");
            var result = LinearizabilityChecker.Check(_history.Values.ToArray());
            Require(result.IsLinearizable && !result.SearchExhausted, $"History is not linearizable (states={result.States}, exhausted={result.SearchExhausted}).");
            var digest = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(string.Join('|',
                _nodes.Values.Select(n => $"{n.Id}:{n.State.HardState}:{n.Application.Signature}:{n.Application.Checkpoint}")) +
                JsonSerializer.Serialize(_history.Values))));
            return new(_seed, _scenario, _nodes.Count, _eventCount,
                _history.Values.Count(h => !h.IsRead && h.Outcome == HistoryOutcome.Success && h.CompletedAt is not null),
                _history.Values.Count(h => h.IsRead && h.Outcome == HistoryOutcome.Success && h.CompletedAt is not null),
                _crashes, _restarts, _torn, _storageFailures, _dropped, _duplicates, _stale, _installs, _nodes.Values.Sum(n => n.Disk.Flushes), digest);
        }
        catch (Exception error)
        {
            throw new InvalidOperationException($"Replay: --seed {_seed} --scenario {_scenario} --steps {_steps} --voters {_voters}\n" +
                $"event={_eventCount}, time={_now}\n" + string.Join('\n', _trace) + "\n" +
                string.Join('\n', _nodes.Values.Select(n => $"{n.Id} epoch={n.Epoch} online={n.Online} fault={n.Faulted} {n.State.Role} term={n.State.HardState.Term} commit={n.State.HardState.CommitIndex} last={n.State.LastIndex} applied={n.Application.Checkpoint} pending={n.State.Pending?.Effect}")) +
                "\nhistory=" + JsonSerializer.Serialize(_history.Values), error);
        }
    }

    private void Schedule(long delay, string label, Action action) => _events.Enqueue(new(label, action), (_now + Math.Max(0, delay), ++_order));
    private void Step()
    {
        Require(_events.TryDequeue(out var item, out var priority), "Scheduler became empty.");
        _now = priority.Time;
        _eventCount++;
        _trace.Enqueue($"{_eventCount}@{_now}:{item!.Label}");
        if (_trace.Count > 64) _trace.Dequeue();
        item.Run();
        CheckInvariants();
    }
    private bool Disrupted => !_healthy && _eventCount > _steps / 5;
    private int IoDelay() => _random.Next(1, Disrupted ? 31 : 5);
    private Node? Leader() => _nodes.Values.FirstOrDefault(n => n.Online && !n.Faulted && n.State.Role == RaftRole.Leader);
    private static void Require(bool condition, string message) { if (!condition) throw new InvalidOperationException(message); }

    private void Tick(Node node)
    {
        if (node.Online && !node.Faulted && !node.TickQueued)
        {
            node.TickQueued = true;
            Enqueue(node, new(new CoreEvent.Tick()));
        }
        Schedule(node.TickPeriod + _random.Next(4), $"tick:{node.Id}", () => Tick(node));
    }
    private void Enqueue(Node node, Input input)
    {
        if (!node.Online || node.Faulted) return;
        Require(node.Inputs.Count < 2048, "Simulated owner queue exceeded its bound.");
        node.Inputs.Enqueue(input);
        Drive(node);
    }
    private void Drive(Node node)
    {
        while (node.Online && !node.Faulted && node.State.IsIdle && node.Active is null && node.Inputs.TryDequeue(out var input))
        {
            if (input.Event is CoreEvent.Tick) node.TickQueued = false;
            if (input.Event is CoreEvent.Propose proposal && (node.State.Role != RaftRole.Leader || node.State.Transfer is not null ||
                node.State.UncommittedBytes + proposal.Commands.Sum(c => c.Bytes.Length + LogEntry.EncodedOverhead) > node.Options.MaxUncommittedBytes))
            {
                if (input.Client is { } client) Finish(client, HistoryOutcome.Rejected);
                continue;
            }
            node.Active = input;
            Reduce(node, input.Event);
        }
    }

    private void Reduce(Node node, CoreEvent input)
    {
        if (!node.Online || node.Faulted) return;
        var transition = Reducer.Step(node.State, input);
        node.State = transition.State;
        foreach (var effect in transition.Effects)
        {
            switch (effect)
            {
                case CoreEffect.ResetTimer sample:
                    Reduce(node, new CoreEvent.TimeoutSampled(sample.Id, _random.Next(node.Options.ElectionMinTicks, node.Options.ElectionMaxTicks)));
                    break;
                case CoreEffect.ReadLog read:
                    AtInstance(node, IoDelay(), "read", () => Reduce(node, new CoreEvent.StorageCompleted(read.Id, node.Disk.Query(read.Query))));
                    break;
                case CoreEffect.Persist persist: Persist(node, persist); break;
                case CoreEffect.Send send: CheckSend(node, send.Rpc); node.Outbound.Add(send.Rpc); break;
                case CoreEffect.ClearOutbound: node.Outbound.Clear(); break;
                case CoreEffect.Reply reply:
                    Require(node.Active?.Rpc is not null, "Reply has no admitted request.");
                    CheckAcknowledgment(node, node.Active!.Rpc!, reply.Message);
                    node.Active!.Reply = reply.Message;
                    break;
                case CoreEffect.Proposed proposed:
                    Require(node.Active?.Client is not null, "Proposal has no client.");
                    node.Active!.Proposed = proposed.FirstIndex;
                    break;
                case CoreEffect.Apply apply: node.ApplyTarget = Math.Max(node.ApplyTarget, apply.ThroughIndex); break;
                case CoreEffect.Control control: Control(node, control.Result); break;
                case CoreEffect.ReadSnapshotChunk read: ReadSnapshot(node, read); break;
                case CoreEffect.ActivateSnapshot activate: ActivateSnapshot(node, activate); break;
                case CoreEffect.Halt: Halt(node); break;
            }
        }
        if (!node.Faulted && node.State.IsIdle && node.Active is { } active)
        {
            node.Active = null;
            if (active.Client is { } client && active.Proposed > 0) node.Writes[client] = (active.Proposed, node.State.HardState.Term);
            foreach (var rpc in node.Outbound) Send(node, rpc);
            node.Outbound.Clear();
            if (active.Reply is not null && active.Rpc is { } request) Reply(node, request, active.Reply);
            Publish(node);
            Drive(node);
        }
    }

    private void Persist(Node node, CoreEffect.Persist effect)
    {
        CheckPersistence(node, effect.Write);
        AtInstance(node, IoDelay(), "flush", () =>
        {
            var fail = Disrupted && _scenario is SimulationScenario.CrashRecovery or SimulationScenario.Mixed && _random.Next(100) < 3;
            if (!fail || _random.Next(2) == 0) node.Disk.WriteAtomic(effect.Write);
            if (fail)
            {
                _storageFailures++;
                Reduce(node, new CoreEvent.StorageFailed(effect.Id, new("SimulatedDiskFailure", "Uncertain modeled flush failure.")));
            }
            else
            {
                AtInstance(node, _random.Next(5), "flushed-completion", () =>
                    Reduce(node, new CoreEvent.StorageCompleted(effect.Id, new(Number: node.Disk.UncommittedBytes))));
                if (Disrupted && _random.Next(50) == 0)
                    AtInstance(node, 40, "duplicate-storage-completion", () => Reduce(node, new CoreEvent.StorageCompleted(effect.Id)));
            }
        });
    }
    private void AtInstance(Node node, long delay, string label, Action action)
    {
        var epoch = node.Epoch;
        Schedule(delay, $"{label}:{node.Id}/{epoch}", () =>
        {
            if (!node.Online || node.Faulted || node.Epoch != epoch) { _stale++; return; }
            action();
        });
    }

    private void Send(Node node, OutboundRpc rpc)
    {
        var context = new RpcContext(node.Id, node.Epoch, ++_rpcId, rpc.Request);
        _requests[(node.Id, node.Epoch, context.Id)] = context;
        Schedule((long)node.Options.RpcTimeout.TotalMilliseconds, $"rpc-deadline:{node.Id}/{context.Id}", () =>
        {
            if (_requests.Remove((node.Id, context.Epoch, context.Id)) && node.Online && !node.Faulted && node.Epoch == context.Epoch)
                Enqueue(node, new(new CoreEvent.Unreachable(rpc.PeerId, rpc.Request)));
        });
        Network(node.Id, rpc.PeerId, () =>
        {
            var peer = _nodes[rpc.PeerId];
            if (peer.Online && !peer.Faulted) Enqueue(peer, new(new CoreEvent.Request(node.Id, rpc.Request), rpc: context));
        });
    }
    private void Reply(Node node, RpcContext request, RaftRpc reply)
    {
        CheckAcknowledgment(node, request, reply);
        if (request.Request is SnapshotChunkRequest chunk && reply is SnapshotChunkResponse { Success: true, Complete: false })
        {
            StageSnapshot(node, request, chunk);
            return;
        }
        SendReply(node, request, reply);
    }
    private void SendReply(Node node, RpcContext request, RaftRpc reply) => Network(node.Id, request.From, () =>
    {
        var sender = _nodes[request.From];
        if (!sender.Online || sender.Faulted || sender.Epoch != request.Epoch) { _stale++; return; }
        if (_requests.Remove((sender.Id, request.Epoch, request.Id)) || (Disrupted && _random.Next(5) == 0))
            Enqueue(sender, new(new CoreEvent.Response(node.Id, reply)));
    });
    private void Network(string from, string to, Action deliver)
    {
        var chance = _scenario == SimulationScenario.Elections ? 5 : 15;
        if (Disrupted && (_blocked.Contains((from, to)) || _random.Next(100) < chance)) { _dropped++; return; }
        Schedule(_random.Next(1, Disrupted ? 81 : 6), $"net:{from}>{to}", () =>
        {
            if (Disrupted && _blocked.Contains((from, to))) { _dropped++; return; }
            deliver();
        });
        if (Disrupted && _random.Next(8) == 0)
        {
            _duplicates++;
            Schedule(_random.Next(1, 100), $"duplicate:{from}>{to}", deliver);
        }
    }

    private void Publish(Node node)
    {
        if (node.State.Role != RaftRole.Leader)
            foreach (var pair in node.Writes.ToArray())
                if (pair.Value.Index > node.State.HardState.CommitIndex) { Finish(pair.Key, HistoryOutcome.Unknown); node.Writes.Remove(pair.Key); }
        if (node.State.InstallingSnapshot is null) node.ApplyPaused = false;
        StartApply(node);
    }
    private void StartApply(Node node)
    {
        if (node.Applying || node.ApplyPaused || !node.Online || node.Faulted || node.Application.Checkpoint.Index >= node.ApplyTarget) return;
        var entries = node.Disk.Query(new LogQuery.Entries(node.Application.Checkpoint.Index + 1,
            (int)Math.Min(node.Options.MaxAppendEntries, node.ApplyTarget - node.Application.Checkpoint.Index), node.Options.MaxAppendBytes)).Entries;
        node.Applying = true;
        AtInstance(node, IoDelay() + _random.Next(10), "application-flush", () =>
        {
            node.Application.Apply(entries);
            node.Applying = false;
            CompleteClients(node);
            StartApply(node);
        });
    }
    private void CompleteClients(Node node)
    {
        foreach (var pair in node.Writes.ToArray())
            if (pair.Value.Index <= node.Application.Checkpoint.Index)
            {
                var entry = node.Disk.History[pair.Value.Index];
                var matches = entry.Kind == LogEntryKind.Command && Decode(entry).Id == pair.Key && entry.Term == pair.Value.Term;
                Finish(pair.Key, matches ? HistoryOutcome.Success : HistoryOutcome.Unknown);
                node.Writes.Remove(pair.Key);
            }
        foreach (var pair in node.Reads.ToArray())
            if (pair.Value <= node.Application.Checkpoint.Index)
            {
                Finish(pair.Key, HistoryOutcome.Success, node.Application.Values.GetValueOrDefault(_history[pair.Key].Key));
                node.Reads.Remove(pair.Key);
            }
    }
    private void Control(Node node, ControlResult result)
    {
        if (result is ControlResult.Read read)
        {
            if (read.Error is not null) Finish(read.ClientId, HistoryOutcome.Rejected);
            else { node.Reads[read.ClientId] = read.Index; CompleteClients(node); }
        }
        if (result is ControlResult.Snapshot) node.Building = false;
    }
    private long Invoke(Node node, bool read, int? key = null, int? value = null)
    {
        var id = ++_clientId;
        var selectedKey = key ?? _random.Next(4);
        var selectedValue = value ?? (int)id;
        _history[id] = new(id, ++_historyTime, null, read, selectedKey, selectedValue, null, HistoryOutcome.Unknown);
        Enqueue(node, read ? new(new CoreEvent.ReadIndex([id]), id) : new(new CoreEvent.Propose([new(Encode(id, selectedKey, selectedValue))]), id));
        Schedule(1200, $"client-timeout:{id}", () => Finish(id, read ? HistoryOutcome.Rejected : HistoryOutcome.Unknown));
        return id;
    }
    private void Finish(long id, HistoryOutcome outcome, int? observed = null)
    {
        if (_history.TryGetValue(id, out var operation) && operation.CompletedAt is null)
            _history[id] = operation with { CompletedAt = ++_historyTime, Outcome = outcome, Observed = observed };
    }
    private static byte[] Encode(long id, int key, int value)
    {
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, id);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(8), key);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(12), value);
        return bytes;
    }
    private static (long Id, int Key, int Value) Decode(LogEntry entry) =>
        (BinaryPrimitives.ReadInt64LittleEndian(entry.Command.Span), BinaryPrimitives.ReadInt32LittleEndian(entry.Command.Span[8..]),
            BinaryPrimitives.ReadInt32LittleEndian(entry.Command.Span[12..]));
}
