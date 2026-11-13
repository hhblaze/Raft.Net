using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace Raft.Transport;

/// <summary>Connection security and authenticated peer-ID binding, performed outside the consensus core.</summary>
public abstract class RaftTransportSecurity
{
    public abstract ValueTask<Stream> AuthenticateClientAsync(Stream stream, string peerId, CancellationToken token);
    public abstract ValueTask<Stream> AuthenticateServerAsync(Stream stream, CancellationToken token);
    public abstract ValueTask AuthorizePeerAsync(Stream stream, string peerId, CancellationToken token);
}

/// <summary>Mutual TLS with chain/name validation plus an application-supplied certificate-to-member binding.</summary>
public sealed class TlsRaftTransportSecurity : RaftTransportSecurity
{
    private readonly X509Certificate2 _certificate;
    private readonly Func<string, X509Certificate2, bool> _authorize;
    private readonly RemoteCertificateValidationCallback? _validateCertificate;
    private readonly bool _checkRevocation;

    /// <param name="certificate">Caller-owned certificate with its private key.</param>
    /// <param name="authorizePeer">Binds the claimed Raft ID to the verified remote certificate.</param>
    /// <param name="validateCertificate">Optional explicit private-PKI policy. Default uses platform chain/name validation.</param>
    public TlsRaftTransportSecurity(X509Certificate2 certificate, Func<string, X509Certificate2, bool> authorizePeer,
        RemoteCertificateValidationCallback? validateCertificate = null, bool checkRevocation = true)
    {
        _certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
        if (!certificate.HasPrivateKey) throw new ArgumentException("TLS certificate needs a private key.", nameof(certificate));
        _authorize = authorizePeer ?? throw new ArgumentNullException(nameof(authorizePeer));
        _validateCertificate = validateCertificate;
        _checkRevocation = checkRevocation;
    }

    public override async ValueTask<Stream> AuthenticateClientAsync(Stream stream, string peerId, CancellationToken token)
    {
        var tls = new SslStream(stream, leaveInnerStreamOpen: false, _validateCertificate);
        try
        {
            await tls.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = peerId, ClientCertificates = new X509CertificateCollection { _certificate },
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                CertificateRevocationCheckMode = _checkRevocation ? X509RevocationMode.Online : X509RevocationMode.NoCheck
            }, token).ConfigureAwait(false);
            return tls;
        }
        catch { await tls.DisposeAsync().ConfigureAwait(false); throw; }
    }

    public override async ValueTask<Stream> AuthenticateServerAsync(Stream stream, CancellationToken token)
    {
        var tls = new SslStream(stream, leaveInnerStreamOpen: false, _validateCertificate);
        try
        {
            await tls.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
            {
                ServerCertificate = _certificate, ClientCertificateRequired = true,
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                CertificateRevocationCheckMode = _checkRevocation ? X509RevocationMode.Online : X509RevocationMode.NoCheck
            }, token).ConfigureAwait(false);
            return tls;
        }
        catch { await tls.DisposeAsync().ConfigureAwait(false); throw; }
    }

    public override ValueTask AuthorizePeerAsync(Stream stream, string peerId, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        if (stream is not SslStream { IsAuthenticated: true, IsEncrypted: true, IsMutuallyAuthenticated: true } tls ||
            tls.RemoteCertificate is null) throw new AuthenticationException("Mutual TLS is required.");
        using var certificate = new X509Certificate2(tls.RemoteCertificate);
        if (!_authorize(peerId, certificate)) throw new AuthenticationException("Certificate is not authorized for this Raft peer ID.");
        return ValueTask.CompletedTask;
    }
}
