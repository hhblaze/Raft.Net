using System.Collections.Immutable;
using Raft.Core;
using Raft.Storage;
using Reducer = Raft.Core.RaftCore;

namespace Raft.Simulation;

internal sealed partial class SeededSimulation
{
    private sealed class ModelApplication
    {
        public AppliedCheckpoint Checkpoint { get; private set; }
        public Dictionary<int, int> Values { get; private set; } = new();
        public HashSet<long> CommandIds { get; private set; } = new();
        public SortedDictionary<long, string> Signatures { get; private set; } = new();
        public string Signature => string.Join(',', Values.OrderBy(pair => pair.Key).Select(pair => $"{pair.Key}={pair.Value}"));
        public void Apply(IReadOnlyList<LogEntry> entries)
        {
            foreach (var entry in entries)
            {
                Require(entry.Index == Checkpoint.Index + 1 && entry.Term >= Checkpoint.Term, "Application order/checkpoint regressed.");
                if (entry.Kind == LogEntryKind.Command)
                {
                    var command = Decode(entry);
                    if (CommandIds.Add(command.Id)) Values[command.Key] = command.Value;
                }
                Checkpoint = new(entry.Index, entry.Term);
                Signatures[entry.Index] = Signature;
            }
        }
        public void Install(ModelImage image)
        {
            Require(image.Metadata.LastIncludedIndex >= Checkpoint.Index, "Image would roll back applied state.");
            var oracle = new ModelApplication();
            oracle.Apply(image.Prefix);
            Require(oracle.Checkpoint == new AppliedCheckpoint(image.Metadata.LastIncludedIndex, image.Metadata.LastIncludedTerm) &&
                oracle.Values.Count == image.Values.Count && oracle.Values.All(pair => image.Values.TryGetValue(pair.Key, out var value) && value == pair.Value) &&
                oracle.CommandIds.SetEquals(image.CommandIds), "Snapshot business state differs from its committed replay prefix.");
            Checkpoint = oracle.Checkpoint;
            Values = oracle.Values;
            CommandIds = oracle.CommandIds;
            Signatures = oracle.Signatures;
        }
    }

    private void Start(Node node, bool initial = false)
    {
        node.Epoch++;
        node.Online = true;
        node.Faulted = false;
        node.TickQueued = false;
        node.Applying = false;
        node.ApplyPaused = false;
        node.Building = false;
        node.BuildId++;
        node.Active = null;
        node.Inputs.Clear();
        node.Outbound.Clear();
        node.Writes.Clear();
        node.Reads.Clear();
        if (node.Incoming is not null) node.Incoming.Verifying = false;
        var recovered = node.Disk.Recover();
        if (recovered.PendingSnapshotInstall is { } intent)
        {
            node.Application.Install(node.Disk.Images[intent.Metadata.StorageId]);
            node.Disk.WriteAtomic(new LogWrite.CompleteSnapshotInstall(intent));
            recovered = node.Disk.Recover();
        }
        if (recovered.Snapshot is { } snapshot && node.Application.Checkpoint.Index < snapshot.LastIncludedIndex)
            node.Application.Install(node.Disk.Images[snapshot.StorageId]);
        Require(node.Application.Checkpoint.Index <= recovered.HardState.CommitIndex, "Restarted application is ahead of the durable commit.");
        node.State = Reducer.Create(node.Options, recovered, _random.Next(node.Options.ElectionMinTicks, node.Options.ElectionMaxTicks));
        node.ApplyTarget = recovered.HardState.CommitIndex;
        if (!initial) _restarts++;
        StartApply(node);
    }

    private void Crash(Node node)
    {
        if (!node.Online || node.Faulted) return;
        AbandonClients(node);
        node.Online = false;
        node.Epoch++; // queued I/O completions can never enter a restarted instance
        node.Outbound.Clear();
        node.Inputs.Clear();
        node.Active = null;
        node.Disk.TearUnpromisedTail();
        _torn++;
        _crashes++;
        foreach (var key in _requests.Keys.Where(key => key.From == node.Id).ToArray()) _requests.Remove(key);
        Schedule(_random.Next(100, 501), $"restart:{node.Id}", () => { if (!node.Online || node.Faulted) Start(node); });
    }
    private void Halt(Node node)
    {
        AbandonClients(node);
        node.Faulted = true;
        node.Outbound.Clear();
        node.Inputs.Clear();
        node.Active = null;
        Schedule(_random.Next(100, 501), $"recover-failed:{node.Id}", () => { if (node.Faulted || !node.Online) Start(node); });
    }
    private void AbandonClients(Node node)
    {
        foreach (var id in node.Writes.Keys) Finish(id, HistoryOutcome.Unknown);
        foreach (var id in node.Reads.Keys) Finish(id, HistoryOutcome.Rejected);
        foreach (var input in node.Inputs.Append(node.Active).OfType<Input>())
            if (input.Client is { } id) Finish(id, _history[id].IsRead ? HistoryOutcome.Rejected : HistoryOutcome.Unknown);
    }

    private void Workload()
    {
        var leader = Leader();
        var action = _random.Next(100);
        if (Disrupted && action < 9 && _scenario is SimulationScenario.Partitions or SimulationScenario.Membership or SimulationScenario.Mixed or SimulationScenario.Reads)
        {
            _blocked.Clear();
            foreach (var from in _nodes.Keys)
                foreach (var to in _nodes.Keys)
                    if (from != to && _random.Next(100) < 28) _blocked.Add((from, to)); // intentionally asymmetric
        }
        else if (Disrupted && action < 17 && _scenario is SimulationScenario.CrashRecovery or SimulationScenario.Membership or SimulationScenario.Snapshots or SimulationScenario.Mixed &&
            _nodes.Values.Count(n => !n.Online || n.Faulted) < _voters / 2)
        {
            var live = _nodes.Values.Where(n => n.Online && !n.Faulted).ToArray();
            if (live.Length > 0) Crash(live[_random.Next(live.Length)]);
        }
        else if (!_healthy && action < 22 && leader is not null && _scenario is SimulationScenario.Membership or SimulationScenario.Mixed)
        {
            var targets = leader.State.Configuration.Voters.Where(id => id != leader.Id).ToArray();
            if (targets.Length > 0) Enqueue(leader, new(new CoreEvent.TransferLeadership(targets[_random.Next(targets.Length)]) { ClientId = ++_clientId }));
        }
        else if (!_healthy && action < 31 && leader is not null && _scenario is SimulationScenario.Membership or SimulationScenario.Mixed)
            ChangeMembership(leader);
        else if (!_healthy && action < 42 && _scenario is SimulationScenario.Snapshots or SimulationScenario.Mixed)
        {
            var live = _nodes.Values.Where(n => n.Online && !n.Faulted && !n.Building && n.Application.Checkpoint.Index > (n.State.Snapshot?.LastIncludedIndex ?? 0) + 2).ToArray();
            if (live.Length > 0) BuildSnapshot(live[_random.Next(live.Length)]);
        }
        else if (leader is not null && _history.Count < (Disrupted
            ? Math.Min(24, 6 + (int)(18L * (_eventCount - _steps / 5) / Math.Max(1, _steps - _steps / 5))) : 6))
            Invoke(leader, read: _random.Next(100) < 40);
        Schedule(_random.Next(10, 36), "workload", Workload);
    }

    private void ChangeMembership(Node leader)
    {
        var config = leader.State.Configuration;
        var spare = $"n{_voters + 1}";
        var voters = config.Voters;
        var learners = config.Learners;
        if (!config.Contains(spare)) learners = learners.Add(spare);
        else if (learners.Contains(spare)) { learners = learners.Remove(spare); voters = voters.Add(spare); }
        else if (voters.Length > _voters)
        {
            var candidates = voters.Where(id => id != spare).ToArray();
            voters = voters.Remove(candidates[_random.Next(candidates.Length)]); // includes leader self-removal
        }
        else
        {
            var missing = _nodes.Keys.FirstOrDefault(id => !config.Contains(id));
            if (missing is not null) learners = learners.Add(missing);
            else if (!learners.IsEmpty)
            {
                var promote = learners[0];
                learners = learners.Remove(promote);
                voters = voters.Add(promote);
            }
            else return;
        }
        Enqueue(leader, new(new CoreEvent.ConfigChange(voters, learners) { ClientId = ++_clientId }));
    }
}
