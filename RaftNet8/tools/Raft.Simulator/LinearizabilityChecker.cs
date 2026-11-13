namespace Raft.Simulation;

public enum HistoryOutcome { Success, Unknown, Rejected }
public sealed record HistoryOperation(long Id, long InvokedAt, long? CompletedAt, bool IsRead,
    int Key, int Value, int? Observed, HistoryOutcome Outcome);
public readonly record struct LinearizabilityResult(bool IsLinearizable, bool SearchExhausted, long States);

/// <summary>
/// Bounded backtracking register-history checker with real-time precedence. Unknown writes may
/// take effect after a timeout; rejected, unadmitted operations have no effect.
/// </summary>
public static class LinearizabilityChecker
{
    public static LinearizabilityResult Check(IReadOnlyList<HistoryOperation> history,
        IReadOnlyDictionary<int, int>? initial = null, long maxStates = 1_000_000)
    {
        if (maxStates < 1) throw new ArgumentOutOfRangeException(nameof(maxStates));
        var operations = history.Where(op => op.Outcome == HistoryOutcome.Success ||
                (!op.IsRead && op.Outcome == HistoryOutcome.Unknown))
            .OrderBy(op => op.Outcome == HistoryOutcome.Unknown ? long.MaxValue : op.CompletedAt ?? long.MaxValue)
            .ThenBy(op => op.InvokedAt).ThenBy(op => op.Id).ToArray();
        if (operations.Length > 63 || operations.Any(op => op.InvokedAt < 0 ||
            (op.CompletedAt is { } end && end < op.InvokedAt) ||
            (op.Outcome == HistoryOutcome.Success && (op.CompletedAt is null || (op.IsRead && op.Observed is null)))))
            throw new ArgumentException("History needs valid intervals and at most 63 relevant operations.", nameof(history));
        var predecessors = new ulong[operations.Length];
        for (var i = 0; i < operations.Length; i++)
            for (var j = 0; j < operations.Length; j++)
                if (i != j && operations[j].Outcome == HistoryOutcome.Success &&
                    operations[j].CompletedAt < operations[i].InvokedAt) predecessors[i] |= 1UL << j;
        var state = initial is null ? new Dictionary<int, int>() : new Dictionary<int, int>(initial);
        var seen = new HashSet<(ulong Completed, string State)>();
        var all = (1UL << operations.Length) - 1;
        long visited = 0;
        var exhausted = false;
        var valid = Search(0);
        return new(valid, exhausted, visited);

        bool Search(ulong completed)
        {
            if (completed == all) return true;
            if (++visited > maxStates) { exhausted = true; return false; }
            var signature = string.Join(';', state.OrderBy(pair => pair.Key).Select(pair => $"{pair.Key}:{pair.Value}"));
            if (!seen.Add((completed, signature))) return false;
            for (var i = 0; i < operations.Length; i++)
            {
                var bit = 1UL << i;
                if ((completed & bit) != 0 || (predecessors[i] & ~completed) != 0) continue;
                var op = operations[i];
                if (op.Outcome == HistoryOutcome.Unknown && Search(completed | bit)) return true; // never took effect
                if (op.IsRead)
                {
                    if (state.GetValueOrDefault(op.Key) == op.Observed && Search(completed | bit)) return true;
                }
                else
                {
                    var existed = state.TryGetValue(op.Key, out var previous);
                    state[op.Key] = op.Value;
                    if (Search(completed | bit)) return true;
                    if (existed) state[op.Key] = previous; else state.Remove(op.Key);
                }
                if (exhausted) return false;
            }
            return false;
        }
    }
}
