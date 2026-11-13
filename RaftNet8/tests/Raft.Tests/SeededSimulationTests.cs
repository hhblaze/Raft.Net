using Raft.Simulation;

namespace Raft.Tests;

public sealed class SeededSimulationTests
{
    [Theory]
    [InlineData(SimulationScenario.Elections)]
    [InlineData(SimulationScenario.Partitions)]
    [InlineData(SimulationScenario.CrashRecovery)]
    [InlineData(SimulationScenario.Reads)]
    [InlineData(SimulationScenario.Membership)]
    [InlineData(SimulationScenario.Snapshots)]
    [InlineData(SimulationScenario.Mixed)]
    public void Real_reducer_preserves_invariants_and_linearizable_histories_under_seeded_faults(SimulationScenario scenario)
    {
        foreach (var voters in new[] { 3, 5 })
            for (var seed = 1; seed <= 8; seed++)
            {
                var result = SimulationRunner.Run(seed, scenario, steps: 1500, voters);
                Assert.True(result.SuccessfulWrites > 0 && result.SuccessfulReads > 0, $"scenario={scenario}, voters={voters}, seed={seed}");
            }
    }

    [Fact]
    public void A_recorded_seed_replays_the_exact_scheduler_history_and_result()
    {
        var first = SimulationRunner.Run(17, SimulationScenario.Mixed, 2000, 3);
        var second = SimulationRunner.Run(17, SimulationScenario.Mixed, 2000, 3);
        Assert.Equal(first, second);
    }
}
