namespace Raft.Tests;

public sealed class OptionsTests
{
    [Fact]
    public void Rejects_duplicate_missing_self_and_unsafe_identifiers()
    {
        Assert.Throws<ArgumentException>(() => new RaftOptions { NodeId = "a", Members = new[] { "a", "a" } }.Freeze());
        Assert.Throws<ArgumentException>(() => new RaftOptions { NodeId = "a", Members = new[] { "b" } }.Freeze());
        Assert.Throws<ArgumentException>(() => new RaftOptions { NodeId = "../a", Members = new[] { "../a" } }.Freeze());
    }

    [Fact]
    public void Rejects_limits_that_cannot_fit_a_command_or_allow_an_rpc_deadline()
    {
        var options = new RaftOptions { NodeId = "a", Members = new[] { "a" } };
        Assert.Throws<ArgumentOutOfRangeException>(() => (options with { MaxAppendBytes = options.MaxCommandBytes }).Freeze());
        Assert.Throws<ArgumentOutOfRangeException>(() => (options with { RpcTimeout = TimeSpan.FromSeconds(5) }).Freeze());
        Assert.Throws<ArgumentOutOfRangeException>(() => (options with { HeartbeatTicks = options.ElectionMinTicks }).Freeze());
    }

    [Fact]
    public void Membership_is_copied_to_prevent_mutation_after_startup()
    {
        var members = new[] { "a", "b" };
        var options = new RaftOptions { NodeId = "a", Members = members }.Freeze();
        members[1] = "c";
        Assert.Equal(new[] { "a", "b" }, options.Members);
    }
}
