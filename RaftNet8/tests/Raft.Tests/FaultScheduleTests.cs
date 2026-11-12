namespace Raft.Tests;

public sealed class FaultScheduleTests
{
    [Theory]
    [InlineData(7)]
    [InlineData(19)]
    [InlineData(73)]
    [InlineData(431)]
    [InlineData(2026)]
    [InlineData(5417)]
    [InlineData(19391)]
    [InlineData(65537)]
    public void Seeded_partitions_restarts_loss_duplicates_and_reordering_preserve_safety(int seed)
    {
        using var cluster = new SimCluster(5, seed) { Reorder = true, DropRate = 0.12, DuplicateRate = 0.15 };
        var random = new Random(seed);
        var ids = cluster.Nodes.Keys.ToArray();
        for (var step = 0; step < 500; step++)
        {
            if (step % 50 == 0)
            {
                var shuffled = ids.OrderBy(_ => random.Next()).ToArray();
                cluster.Partition(shuffled[..2], shuffled[2..]);
            }
            if (step % 50 == 25) cluster.Heal();
            if (step % 37 == 0) cluster.Restart(ids[random.Next(ids.Length)]);
            if (step % 7 == 0)
                foreach (var leader in cluster.Leaders.ToArray()) cluster.Propose(leader, BitConverter.GetBytes(step));
            cluster.Tick(pump: false);
            cluster.Pump(random.Next(1, 15));
            cluster.AssertSafety();
        }
        cluster.Heal();
        cluster.DropRate = cluster.DuplicateRate = 0;
        cluster.Pump();
        cluster.Tick(150);
        var final = cluster.Propose(cluster.Leader, "final-fence"u8.ToArray());
        cluster.Tick(30);
        foreach (var node in cluster.Nodes.Values) Assert.Equal(final, node.CommitIndex);
        cluster.AssertSafety();
    }
}
