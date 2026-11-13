using System.Security.Cryptography;
using Raft.Core;
using Raft.Storage;

namespace Raft.Simulation;

internal sealed partial class SeededSimulation
{
    private readonly Dictionary<long, string> _leaders = new();
    private readonly SortedDictionary<long, LogEntry> _committed = new();
    private readonly Dictionary<string, (long Term, long Commit, long DiskCommit, long Applied)> _previous = new(StringComparer.Ordinal);
    private readonly Dictionary<(long Index, long Term), string> _matching = new();
    private readonly Dictionary<(string Node, long Term), Dictionary<long, LogEntry>> _leaderLogs = new();
    private readonly Dictionary<string, long> _checkedFlush = new(StringComparer.Ordinal);
    private readonly Dictionary<long, string> _applied = new();

    private void CheckInvariants()
    {
        foreach (var node in _nodes.Values)
        {
            var before = _previous.GetValueOrDefault(node.Id);
            var hard = node.Disk.Hard;
            Require(node.State.HardState.Term >= before.Term && node.State.HardState.CommitIndex >= before.Commit && hard.CommitIndex >= before.DiskCommit,
                $"Term/commit regressed on {node.Id}.");
            Require(node.State.HardState.Term <= hard.Term && node.State.HardState.CommitIndex <= node.State.LastIndex,
                $"Core metadata exceeds its durable/log bounds on {node.Id}.");
            Require(node.Application.Checkpoint.Index >= before.Applied && node.Application.Checkpoint.Index <=
                Math.Max(hard.CommitIndex, node.Disk.RecoverIntentIndex()), $"Atomic application checkpoint is outside its durable fence on {node.Id}.");
            for (var index = before.DiskCommit + 1; index <= hard.CommitIndex; index++)
            {
                Require(node.Disk.History.TryGetValue(index, out var entry), $"Committed index {index} is missing on {node.Id}.");
                if (_committed.TryGetValue(index, out var old)) Require(ModelDisk.Same(old, entry!), $"Committed entry {index} was replaced.");
                else _committed[index] = entry!;
            }
            for (var index = before.Applied + 1; index <= node.Application.Checkpoint.Index; index++)
            {
                var signature = node.Application.Signatures[index];
                if (_applied.TryGetValue(index, out var previous)) Require(signature == previous, $"State-machine safety failed at {index}.");
                else _applied[index] = signature;
            }
            if (node.Application.Checkpoint.Index > 0)
                Require(_applied[node.Application.Checkpoint.Index] == node.Application.Signature, "Published business state differs at an unchanged checkpoint.");
            _previous[node.Id] = (node.State.HardState.Term, node.State.HardState.CommitIndex, hard.CommitIndex, node.Application.Checkpoint.Index);
        }
        foreach (var node in _nodes.Values)
        {
            if (node.State.Role == RaftRole.Leader)
            {
                var term = node.State.HardState.Term;
                if (_leaders.TryGetValue(term, out var leader)) Require(leader == node.Id, $"Two leaders in term {term}: {leader}, {node.Id}.");
                else
                {
                    _leaders[term] = node.Id;
                    foreach (var pair in _committed)
                        Require(node.Disk.History.TryGetValue(pair.Key, out var entry) && ModelDisk.Same(pair.Value, entry),
                            $"New leader {node.Id} lacks committed entry {pair.Key}.");
                }
                var key = (node.Id, term);
                if (!_leaderLogs.TryGetValue(key, out var prefix)) _leaderLogs[key] = prefix = new();
                foreach (var pair in prefix)
                    Require(node.Disk.History.TryGetValue(pair.Key, out var entry) && ModelDisk.Same(pair.Value, entry), "Leader append-only failed.");
                foreach (var pair in node.Disk.History) prefix[pair.Key] = pair.Value;
                foreach (var peer in node.State.Peers)
                {
                    Require(peer.NextIndex >= peer.MatchIndex + 1 && peer.MatchIndex <= node.State.LastIndex, "Replication progress escaped its bounds.");
                    Require(peer.Flights.Length <= (peer.Mode == ReplicationMode.Replicate ? node.Options.MaxInFlightMessages : 1) &&
                        peer.InFlightBytes <= node.Options.MaxInFlightBytes, "Replication window exceeded its memory limit.");
                }
            }
            Require(node.State.Peers.Select(peer => peer.Id).Distinct(StringComparer.Ordinal).Count() == node.State.Peers.Length, "Duplicate peer progress.");
            if (node.State.IsIdle && !node.Faulted && node.Online)
            {
                var config = node.Disk.Query(new LogQuery.ConfigurationAt(node.Disk.Last)).Configuration!;
                Require(config.Index == node.State.Configuration.Index && config.Term == node.State.Configuration.Term &&
                    ClusterConfiguration.SameMembers(config, node.State.Configuration), "Core is not using the latest durable configuration.");
                if (!config.IsVoter(node.Id))
                    Require(node.State.Role is not RaftRole.Candidate and not RaftRole.PreCandidate &&
                        (node.State.Role != RaftRole.Leader || config.Index > node.State.HardState.CommitIndex), "Non-voter campaigned or remained leader after committed removal.");
            }
            if (_checkedFlush.GetValueOrDefault(node.Id, -1) == node.Disk.Flushes) continue;
            _checkedFlush[node.Id] = node.Disk.Flushes;
            var chain = new byte[32];
            foreach (var pair in node.Disk.History)
            {
                var encoded = StorageCodecV2.Entry(pair.Value);
                var input = new byte[chain.Length + encoded.Length];
                chain.CopyTo(input, 0); encoded.CopyTo(input, chain.Length);
                chain = SHA256.HashData(input);
                var fingerprint = Convert.ToHexString(chain);
                var key = (pair.Key, pair.Value.Term);
                if (_matching.TryGetValue(key, out var prefix)) Require(prefix == fingerprint, $"Log matching failed at {pair.Key}/{pair.Value.Term}.");
                else _matching[key] = fingerprint;
            }
        }
    }

    private static void CheckPersistence(Node node, LogWrite write)
    {
        foreach (var item in Flatten(write))
        {
            if (node.State.Role == RaftRole.Leader) Require(item is not LogWrite.ReplaceSuffix, "A leader replaced a suffix.");
            if (node.State.Role != RaftRole.Leader || item is not LogWrite.Append append) continue;
            foreach (var entry in append.Entries.Where(e => e.Kind == LogEntryKind.Configuration))
            {
                var previous = node.State.Configuration;
                var config = ClusterConfiguration.Decode(entry.Command.Span);
                Require(node.State.HasCommittedCurrentTerm && previous.Index <= node.State.HardState.CommitIndex,
                    "Configuration was created before current-term/prior-config commitment.");
                var changed = previous.AllMembers.Concat(config.AllMembers).Distinct(StringComparer.Ordinal).Count(id =>
                    (previous.IsVoter(id) ? 2 : previous.Contains(id) ? 1 : 0) != (config.IsVoter(id) ? 2 : config.Contains(id) ? 1 : 0));
                Require(changed == 1 && config.Voters.Length >= node.Options.MinimumVoters, "Unsafe concurrent/multi-server configuration mutation.");
            }
        }
    }
    private static IEnumerable<LogWrite> Flatten(LogWrite write) => write is LogWrite.Batch batch ? batch.Writes.SelectMany(Flatten) : [write];

    private static void CheckSend(Node node, OutboundRpc rpc)
    {
        if (rpc.Request is VoteRequest vote)
            Require(node.Disk.Hard.Term >= vote.Term && node.Disk.Hard.VotedFor == node.Id, "Vote requests escaped before durable self-vote.");
        if (rpc.Request is AppendRequest append)
            foreach (var entry in append.Entries)
                Require(node.Disk.History.TryGetValue(entry.Index, out var saved) && ModelDisk.Same(entry, saved), "Replication escaped before durable local append.");
    }
    private static void CheckAcknowledgment(Node node, RpcContext request, RaftRpc reply)
    {
        Require(node.Disk.Hard.Term >= reply.Term, "Reply escaped before durable term adoption.");
        if (reply is VoteResponse { Granted: true }) Require(node.Disk.Hard.VotedFor == request.From, "Granted vote escaped before durable vote.");
        if (reply is AppendResponse { Success: true } append && request.Request is AppendRequest source)
        {
            Require(append.MatchIndex == source.PreviousIndex + source.Entries.Count && node.Disk.Last >= append.MatchIndex,
                "Append acknowledgment exceeds the durable frontier.");
            foreach (var entry in source.Entries)
                Require(node.Disk.History.TryGetValue(entry.Index, out var saved) && ModelDisk.Same(entry, saved), "Acknowledged data is not durable.");
        }
    }
}
