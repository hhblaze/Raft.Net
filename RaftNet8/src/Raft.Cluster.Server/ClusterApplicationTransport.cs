using System.Text;
using Raft.Transport;

namespace Raft.Cluster.Server;

/// <summary>Connection preamble outside the unchanged core wire3 frames. Old applications cannot replay v2 state.</summary>
public sealed class ClusterApplicationTransport(RaftTransportSecurity? inner) : RaftTransportSecurity
{
    private static readonly byte[] Version = Encoding.ASCII.GetBytes("RCC2cluster-v2\0\0\0");
    public override async ValueTask<Stream> AuthenticateClientAsync(Stream stream, string peerId, CancellationToken token)
    {
        var authenticated = inner is null ? stream : await inner.AuthenticateClientAsync(stream, peerId, token).ConfigureAwait(false);
        try
        {
            await authenticated.WriteAsync(Version, token).ConfigureAwait(false);
            await VerifyAsync(authenticated, token).ConfigureAwait(false);
            return authenticated;
        }
        catch { await authenticated.DisposeAsync().ConfigureAwait(false); throw; }
    }
    public override async ValueTask<Stream> AuthenticateServerAsync(Stream stream, CancellationToken token)
    {
        var authenticated = inner is null ? stream : await inner.AuthenticateServerAsync(stream, token).ConfigureAwait(false);
        try
        {
            await VerifyAsync(authenticated, token).ConfigureAwait(false);
            await authenticated.WriteAsync(Version, token).ConfigureAwait(false);
            return authenticated;
        }
        catch { await authenticated.DisposeAsync().ConfigureAwait(false); throw; }
    }
    private static async Task VerifyAsync(Stream stream, CancellationToken token)
    {
        var received = new byte[Version.Length];
        await stream.ReadExactlyAsync(received, token).ConfigureAwait(false);
        if (!received.AsSpan().SequenceEqual(Version)) throw new InvalidDataException("Incompatible Raft cluster application; expected cluster-v2.");
    }
    public override ValueTask AuthorizePeerAsync(Stream stream, string peerId, CancellationToken token) =>
        inner?.AuthorizePeerAsync(stream, peerId, token) ?? ValueTask.CompletedTask;
}
