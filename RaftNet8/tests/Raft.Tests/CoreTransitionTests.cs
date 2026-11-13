using System.Collections.Immutable;
using System.Text.Json;
using Raft.Core;
using Raft.Storage;
using Reducer = Raft.Core.RaftCore;

namespace Raft.Tests;

public sealed class CoreTransitionTests
{
    private static RaftOptions Options(bool solo = false) => new()
    {
        NodeId = "n1", Members = solo ? ["n1"] : ["n1", "n2", "n3"], MaxAppendEntries = 16
    };
    private static RaftState Initial(bool solo = false) => Reducer.Create(Options(solo), new(new(), 0, 0), 12);
    private static CoreTransition Persisted(CoreTransition previous)
    {
        var pending = Assert.IsType<CoreEffect.Persist>(previous.State.Pending!.Effect);
        return Reducer.Step(previous.State, new CoreEvent.StorageCompleted(pending.Id));
    }
    private static CoreTransition Sampled(CoreTransition previous)
    {
        var pending = Assert.IsType<CoreEffect.ResetTimer>(previous.State.Pending!.Effect);
        return Reducer.Step(previous.State, new CoreEvent.TimeoutSampled(pending.Id, 13));
    }
    private static void NoExternalSuccess(CoreTransition transition) =>
        Assert.DoesNotContain(transition.Effects, e => e is CoreEffect.Send or CoreEffect.Reply or CoreEffect.Apply or CoreEffect.Proposed);

    [Fact]
    public void Higher_term_and_vote_each_complete_durably_before_reply()
    {
        var original = Initial();
        var before = Describe(original);
        var term = Reducer.Step(original, new CoreEvent.Request("n2", new VoteRequest(4, 0, 0)));
        Assert.Equal(new HardState(4), Assert.IsType<LogWrite.HardStateUpdate>(
            Assert.IsType<CoreEffect.Persist>(term.State.Pending!.Effect).Write).State);
        Assert.Equal(0, term.State.HardState.Term); // not published before durability
        NoExternalSuccess(term);
        var vote = Sampled(Persisted(term));
        Assert.Equal(new HardState(4, "n2"), Assert.IsType<LogWrite.HardStateUpdate>(
            Assert.IsType<CoreEffect.Persist>(vote.State.Pending!.Effect).Write).State);
        Assert.Null(vote.State.HardState.VotedFor);
        NoExternalSuccess(vote);
        var reply = Sampled(Persisted(vote));
        Assert.Equal(new VoteResponse(4, true), Assert.IsType<CoreEffect.Reply>(Assert.Single(reply.Effects)).Message);
        Assert.Equal(before, Describe(original));
    }

    [Fact]
    public void Append_and_commit_complete_before_reply_or_application()
    {
        var original = Reducer.Create(Options(), new(new(2), 0, 0), 12);
        var request = new AppendRequest(2, 11, 0, 0, 1, [new LogEntry(1, 2, "owned"u8)]);
        var append = Sampled(Reducer.Step(original, new CoreEvent.Request("n2", request)));
        var atomic = Assert.IsType<LogWrite.Batch>(Assert.IsType<CoreEffect.Persist>(append.State.Pending!.Effect).Write);
        Assert.Collection(atomic.Writes, w => Assert.IsType<LogWrite.Append>(w), w => Assert.IsType<LogWrite.HardStateUpdate>(w));
        Assert.Equal(0, append.State.LastIndex);
        NoExternalSuccess(append);
        Assert.Equal(0, append.State.HardState.CommitIndex);
        var complete = Persisted(append);
        Assert.True(complete.State.IsIdle);
        Assert.Collection(complete.Effects,
            effect => Assert.Equal(1, Assert.IsType<CoreEffect.Apply>(effect).ThroughIndex),
            effect => Assert.Equal(new AppendResponse(2, 11, true, 1), Assert.IsType<CoreEffect.Reply>(effect).Message));
        Assert.Equal(0, original.LastIndex);
    }

    [Fact]
    public void Late_and_wrong_kind_completions_cannot_release_another_effect()
    {
        var pending = Reducer.Step(Initial(), new CoreEvent.Campaign());
        var effect = Assert.IsType<CoreEffect.Persist>(pending.State.Pending!.Effect);
        foreach (CoreEvent wrong in new CoreEvent[]
        {
            new CoreEvent.StorageCompleted(effect.Id + 1),
            new CoreEvent.TimeoutSampled(effect.Id, 13),
            new CoreEvent.StorageFailed(effect.Id + 1, new("IOException", "stale"))
        })
        {
            var ignored = Reducer.Step(pending.State, wrong);
            Assert.Same(pending.State, ignored.State);
            Assert.Empty(ignored.Effects);
        }
        var timer = Persisted(pending);
        var duplicate = Reducer.Step(timer.State, new CoreEvent.StorageCompleted(effect.Id));
        Assert.Same(timer.State, duplicate.State);
        Assert.Empty(duplicate.Effects);
    }

    [Fact]
    public void Failed_persistence_is_terminal_without_reply_send_or_apply()
    {
        var pending = Reducer.Step(Initial(), new CoreEvent.Campaign());
        var effect = Assert.IsType<CoreEffect.Persist>(pending.State.Pending!.Effect);
        var error = new CoreFailure("ArgumentException", "uncertain disk outcome");
        var failed = Reducer.Step(pending.State, new CoreEvent.StorageFailed(effect.Id, error));
        Assert.Same(error, failed.State.Fault);
        Assert.Equal(0, failed.State.HardState.Term);
        Assert.Collection(failed.Effects, e => Assert.IsType<CoreEffect.ClearOutbound>(e),
            e => Assert.Same(error, Assert.IsType<CoreEffect.Halt>(e).Error));
        var later = Reducer.Step(failed.State, new CoreEvent.StorageCompleted(effect.Id));
        Assert.Same(failed.State, later.State);
        Assert.Empty(later.Effects);
    }

    [Fact]
    public void Historical_log_access_is_an_explicit_bounded_query_not_a_full_log_load()
    {
        var initial = Reducer.Create(Options(), new(new(5), 1_000_000, 4), 12);
        var entries = Enumerable.Range(1, 16).Select(i => new LogEntry(500_000 + i, 4, "x"u8)).ToArray();
        var request = new AppendRequest(5, 9, 500_000, 3, 0, entries);
        var previous = Sampled(Reducer.Step(initial, new CoreEvent.Request("n2", request)));
        var read = Assert.IsType<CoreEffect.ReadLog>(Assert.Single(previous.Effects));
        Assert.Equal(new LogQuery.Term(500_000), read.Query);
        var matching = Reducer.Step(previous.State, new CoreEvent.StorageCompleted(read.Id, new(Number: 3)));
        read = Assert.IsType<CoreEffect.ReadLog>(Assert.Single(matching.Effects));
        Assert.Equal(new LogQuery.Terms(500_001, 16), read.Query);
        var done = Reducer.Step(matching.State, new CoreEvent.StorageCompleted(read.Id,
            new(Terms: Enumerable.Repeat(4L, 16).ToImmutableArray())));
        Assert.Equal(1_000_000, done.State.LastIndex);
        Assert.IsType<CoreEffect.Reply>(Assert.Single(done.Effects));
    }

    [Fact]
    public void State_does_not_retain_mutable_membership_or_request_lists()
    {
        var members = new[] { "n1", "n2", "n3" };
        var initial = Reducer.Create(Options() with { Members = members }, new(new(2), 0, 0), 12);
        members[1] = "different";
        Assert.Equal("n2", initial.Options.Members[1]);
        var entries = new[] { new LogEntry(1, 2, "first"u8) };
        var request = Reducer.Step(initial, new CoreEvent.Request("n2", new AppendRequest(2, 1, 0, 0, 1, entries)));
        entries[0] = new LogEntry(1, 2, "changed"u8);
        var append = Sampled(request);
        var batch = Assert.IsType<LogWrite.Batch>(Assert.IsType<CoreEffect.Persist>(append.State.Pending!.Effect).Write);
        var write = Assert.IsType<LogWrite.Append>(batch.Writes[0]);
        Assert.Equal("first"u8.ToArray(), write.Entries[0].Command.ToArray());
    }

    [Fact]
    public void Stale_snapshot_completion_is_ignored_and_recovered_snapshot_is_an_owned_boundary()
    {
        var initial = Initial();
        var before = Describe(initial);
        var config = Reducer.Step(initial, new CoreEvent.ConfigChange(["n1", "n2"], ["n3"]));
        Assert.Equal("NotLeader", Assert.IsType<ControlResult.Configuration>(Assert.IsType<CoreEffect.Control>(Assert.Single(config.Effects)).Result).Error!.Type);
        var snapshot = new SnapshotMetadata(1, 1, ["n1", "n2", "n3"], [], "staged", System.Collections.Immutable.ImmutableArray.Create(new byte[32]));
        Assert.Empty(Reducer.Step(initial, new CoreEvent.SnapshotCompleted(1, snapshot)).Effects);
        var recovered = Reducer.Create(Options(), new(new(1, CommitIndex: 1), 1, 1, snapshot) { FirstIndex = 2 }, 12);
        Assert.Same(snapshot, recovered.Snapshot);
        Assert.Equal(2, recovered.FirstIndex);
        Assert.Equal(before, Describe(initial));
    }

    [Fact]
    public void Replaying_the_same_recorded_events_reproduces_states_and_effects_without_mutating_history()
    {
        var original = Initial(solo: true);
        var events = new List<CoreEvent>();
        var trace = new List<string>();
        var history = new List<(RaftState State, string Before)>();
        var state = original;
        Run(new CoreEvent.Campaign());
        Complete();
        Run(new CoreEvent.Propose(ImmutableArray.Create(new ProposedCommand("value"u8.ToArray()))));
        Complete();
        Assert.Equal(2, state.LastIndex);
        Assert.Equal(2, state.HardState.CommitIndex);
        Assert.Equal(RaftRole.Leader, state.Role);
        var replay = original;
        var replayTrace = new List<string>();
        foreach (var input in events)
        {
            var transition = Reducer.Step(replay, input);
            replay = transition.State;
            replayTrace.Add(Describe(transition));
        }
        Assert.Equal(trace, replayTrace);
        foreach (var item in history) Assert.Equal(item.Before, Describe(item.State));
        return;

        void Run(CoreEvent input)
        {
            events.Add(input);
            history.Add((state, Describe(state)));
            var transition = Reducer.Step(state, input);
            trace.Add(Describe(transition));
            state = transition.State;
        }
        void Complete()
        {
            for (var i = 0; !state.IsIdle && i < 20; i++)
                Run(state.Pending!.Effect switch
                {
                    CoreEffect.Persist persist => new CoreEvent.StorageCompleted(persist.Id),
                    CoreEffect.ResetTimer timer => new CoreEvent.TimeoutSampled(timer.Id, 13),
                    _ => throw new InvalidOperationException("This single-node trace needs no storage reads.")
                });
            Assert.True(state.IsIdle);
        }
    }

    private static string Describe(CoreTransition t) => Describe(t.State) + "|" + string.Join('|', t.Effects.Select(Describe));
    private static string Describe(RaftState s) => JsonSerializer.Serialize(new
    {
        s.Role, s.LeaderId, s.HardState, s.LastIndex, s.LastTerm, s.ElectionDeadline, s.ElectionElapsed,
        s.HeartbeatElapsed, s.QuorumElapsed, s.RequestId, s.EffectId,
        Votes = s.Votes.Order(StringComparer.Ordinal).ToArray(),
        Peers = s.Peers.Select(p => new { p.Id, p.NextIndex, p.MatchIndex, p.RecentActive,
            InFlight = p.InFlight is { } r ? $"{r.Term}/{r.RequestId}/{r.PreviousIndex}/{r.PreviousTerm}/{r.LeaderCommit}/" + Entries(r.Entries) : null }),
        Agenda = s.Agenda.Select(a => a.ToString()).ToArray(),
        Pending = s.Pending is { } p ? Describe(p.Effect) + "/" + p.Continuation : null
    });
    private static string Describe(CoreEffect effect) => effect switch
    {
        CoreEffect.Persist { Write: LogWrite.Append append } p => $"append/{p.Id}/" + Entries(append.Entries),
        CoreEffect.Persist { Write: LogWrite.ReplaceSuffix suffix } p => $"replace/{p.Id}/{suffix.FromIndex}/" + Entries(suffix.Entries),
        _ => effect.ToString()
    };
    private static string Entries(IEnumerable<LogEntry> entries) => string.Join(';',
        entries.Select(e => $"{e.Index}/{e.Term}/{e.IsNoOp}/{Convert.ToHexString(e.Command.Span)}"));
}
