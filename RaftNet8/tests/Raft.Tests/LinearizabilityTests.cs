using Raft.Simulation;

namespace Raft.Tests;

public sealed class LinearizabilityTests
{
    [Fact]
    public void A_read_after_a_completed_write_cannot_return_the_old_value()
    {
        var result = LinearizabilityChecker.Check([
            new(1, 0, 1, false, 0, 42, null, HistoryOutcome.Success),
            new(2, 2, 3, true, 0, 0, 0, HistoryOutcome.Success)]);
        Assert.False(result.IsLinearizable);
        Assert.False(result.SearchExhausted);
    }
    [Theory]
    [InlineData(0)]
    [InlineData(42)]
    public void An_overlapping_read_can_precede_or_follow_the_write(int observed)
    {
        Assert.True(LinearizabilityChecker.Check([
            new(1, 0, 10, false, 0, 42, null, HistoryOutcome.Success),
            new(2, 2, 3, true, 0, 0, observed, HistoryOutcome.Success)]).IsLinearizable);
    }
    [Fact]
    public void Unknown_write_can_commit_after_its_timeout_while_rejected_write_cannot()
    {
        HistoryOperation[] history = [
            new(1, 0, 1, false, 0, 42, null, HistoryOutcome.Unknown),
            new(2, 2, 3, false, 0, 7, null, HistoryOutcome.Success),
            new(3, 5, 6, true, 0, 0, 42, HistoryOutcome.Success)];
        Assert.True(LinearizabilityChecker.Check(history).IsLinearizable);
        history[0] = history[0] with { Outcome = HistoryOutcome.Rejected };
        Assert.False(LinearizabilityChecker.Check(history).IsLinearizable);
    }
    [Fact]
    public void A_future_unknown_write_cannot_explain_an_earlier_completed_read()
    {
        Assert.False(LinearizabilityChecker.Check([
            new(1, 10, null, false, 0, 42, null, HistoryOutcome.Unknown),
            new(2, 0, 5, true, 0, 0, 42, HistoryOutcome.Success)]).IsLinearizable);
    }
    [Fact]
    public void Generated_concurrent_valid_histories_have_a_real_time_consistent_serialization()
    {
        for (var seed = 0; seed < 200; seed++)
        {
            var random = new Random(seed);
            var state = new Dictionary<int, int>();
            var history = new List<HistoryOperation>();
            for (var step = 0; step < 18; step++)
            {
                var key = random.Next(3);
                var read = random.Next(2) == 0;
                var value = random.Next(1000);
                if (!read) state[key] = value;
                history.Add(new(step, step * 10, step * 10 + random.Next(1, 30), read, key, value,
                    read ? state.GetValueOrDefault(key) : null, HistoryOutcome.Success));
            }
            var result = LinearizabilityChecker.Check(history);
            Assert.True(result.IsLinearizable, $"seed={seed}, states={result.States}, exhausted={result.SearchExhausted}");
        }
    }
}
