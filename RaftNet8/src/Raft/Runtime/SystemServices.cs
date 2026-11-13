namespace Raft.Runtime;

public sealed class SystemRaftRandom : IRandom
{
    private readonly Random _random;
    public SystemRaftRandom(int? seed = null) => _random = seed is { } value ? new Random(value) : Random.Shared;
    internal SystemRaftRandom(Random random) => _random = random;
    public int Next(int minInclusive, int maxExclusive) => _random.Next(minInclusive, maxExclusive);
}

public sealed class SystemRaftClock : IClock
{
    public ITicker CreateTicker(TimeSpan interval) => new Ticker(interval);

    private sealed class Ticker(TimeSpan interval) : ITicker
    {
        private readonly PeriodicTimer _timer = new(interval);
        public ValueTask<bool> WaitForNextTickAsync(CancellationToken cancellationToken) =>
            _timer.WaitForNextTickAsync(cancellationToken);
        public ValueTask DisposeAsync() { _timer.Dispose(); return ValueTask.CompletedTask; }
    }
}
