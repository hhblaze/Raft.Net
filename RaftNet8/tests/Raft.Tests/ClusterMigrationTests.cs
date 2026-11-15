using System.Security.Cryptography;
using Raft.Cluster;
using Raft.Cluster.Server;
using Raft.Storage;

namespace Raft.Tests;

[Collection("OS process clusters")]
public sealed class ClusterMigrationTests
{
    [Fact]
    public async Task Stopped_v1_node_copies_history_and_images_then_v2_restarts_and_snapshots_again()
    {
        await using var cluster = new ClientTestCluster(1) { ApplicationVersion = ClusterLedgerVersion.V1 };
        await cluster.StartAsync();
        var original = await cluster.LeaderAsync();
        await using var client = cluster.Client();
        Assert.Equal(1, await client.AddLogEntryAsync(new byte[] { 1, 2, 3 }, "history"));
        Assert.Equal(2, await client.AddLogEntryAsync(new byte[] { 4, 5 }, "history"));
        await client.CreateEntityAsync("cache", EntityMode.HighSpeedCache);
        await client.AddLogEntryAsync(new byte[] { 6 }, "cache");
        await client.AddLogEntryAsync(new byte[] { 7, 8 }, "cache");
        var image = await original.Node.CreateSnapshotAsync();
        var options = original.Options;
        var checkpoint = original.Ledger.Checkpoint;
        var source = cluster.DataPath("n1");
        await cluster.StopAsync("n1");
        var hashes = Directory.EnumerateFiles(source, "*", SearchOption.AllDirectories)
            .ToDictionary(path => Path.GetRelativePath(source, path), path => Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path))));
        using var destination = new ClusterTestDirectory();
        var target = Path.Combine(destination.Path, "node-v2");
        var result = ClusterStorageMigration.CopyNodeV1ToV2(source, target, options);
        Assert.Equal(1, result.Images);
        Assert.Equal(2, result.Entities);
        Assert.Equal(checkpoint, result.Checkpoint);
        foreach (var pair in hashes)
            Assert.Equal(pair.Value, Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(Path.Combine(source, pair.Key)))));
        using (var migrated = new MessageLedger(Path.Combine(target, "messages"), version: ClusterLedgerVersion.V2))
        {
            Assert.Equal(5, migrated.GetEntity("history")!.RetainedBytes);
            Assert.Equal(2, migrated.GetEntity("history")!.RetainedMessages);
            Assert.Equal(2, migrated.GetEntity("cache")!.RetainedBytes);
            Assert.Equal(1, migrated.GetEntity("cache")!.RetainedMessages);
            Assert.Equal(new byte[] { 7, 8 }, migrated.GetMessage("cache", 2)!.Data.ToArray());
        }
        await using var restarted = await RaftClusterNode.StartAsync(new()
        {
            Raft = options with { SnapshotDirectory = Path.Combine(target, "images") },
            Peer = cluster.Peer("n1"), DataDirectory = target,
            ClientServer = new() { Endpoint = cluster.Endpoint("n1") }
        });
        await RuntimeCluster.EventuallyAsync(() => restarted.Node.Status.Role == RaftRole.Leader && restarted.Node.Status.Term > image.LastIncludedTerm &&
            restarted.Node.Status.LastApplied > checkpoint.Index, 20_000);
        var next = await restarted.Node.CreateSnapshotAsync();
        Assert.True(next.LastIncludedIndex > image.LastIncludedIndex);
        Assert.Equal(new byte[] { 1, 2, 3 }, restarted.Ledger.GetMessage("history", 1)!.Data.ToArray());
    }

    [Fact]
    public async Task Live_source_is_rejected_before_creating_a_destination()
    {
        await using var cluster = new ClientTestCluster(1) { ApplicationVersion = ClusterLedgerVersion.V1 };
        await cluster.StartAsync();
        var node = await cluster.LeaderAsync();
        using var destination = new ClusterTestDirectory();
        var target = Path.Combine(destination.Path, "must-not-exist");
        Assert.ThrowsAny<IOException>(() => ClusterStorageMigration.CopyNodeV1ToV2(cluster.DataPath("n1"), target, node.Options));
        Assert.False(Directory.Exists(target));
    }
}
