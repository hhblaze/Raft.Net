using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Raft.Transport;

namespace Raft.Cluster.Demo;

/// <summary>Private local laboratory PKI. Never installs a machine root or disables TLS identity validation.</summary>
public sealed class DemoSecurity : IDisposable
{
    private readonly string _directory, _password;
    private readonly X509Certificate2 _root, _web, _diagnostic;
    private readonly Dictionary<string, X509Certificate2> _peers = new(StringComparer.Ordinal);
    public DemoSecurity(string directory)
    {
        _directory = Path.GetFullPath(directory);
        _password = File.ReadAllText(Path.Combine(_directory, "pfx.password"));
        _root = new(Path.Combine(_directory, "ca.cer"));
        _web = Load("http"); _diagnostic = Load("dashboard");
    }
    public string DiagnosticThumbprint => _diagnostic.Thumbprint;
    public string WebThumbprint => _web.Thumbprint;
    // Windows SChannel requires an OS-backed temporary key. DefaultKeySet removes it on Dispose.
    private X509Certificate2 Load(string name) => new(Path.Combine(_directory, name + ".pfx"), _password,
        OperatingSystem.IsWindows() ? X509KeyStorageFlags.DefaultKeySet : X509KeyStorageFlags.EphemeralKeySet);
    public RaftTransportSecurity Peer(string id)
    {
        if (!_peers.TryGetValue(id, out var certificate)) _peers[id] = certificate = Load("peer-" + id);
        return new TlsRaftTransportSecurity(certificate, (peerId, remote) => remote.GetNameInfo(X509NameType.DnsName, false) == peerId && Trusted(remote, false),
            (_, remote, _, errors) => remote is not null && (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) == SslPolicyErrors.None && Trusted(remote, false), false);
    }
    private bool Trusted(X509Certificate remote, bool server)
    {
        using var leaf = new X509Certificate2(remote);
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(_root);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck; // This ephemeral lab CA has no CRL service.
        chain.ChainPolicy.ApplicationPolicy.Add(new(server ? "1.3.6.1.5.5.7.3.1" : "1.3.6.1.5.5.7.3.2"));
        return chain.Build(leaf);
    }
    public HttpClient HttpClient(bool diagnostic = false)
    {
        var handler = new SocketsHttpHandler { AllowAutoRedirect = false, ConnectTimeout = TimeSpan.FromSeconds(2), MaxConnectionsPerServer = 128 };
        handler.SslOptions.RemoteCertificateValidationCallback = (_, remote, _, errors) => remote is not null &&
            (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) == SslPolicyErrors.None && Trusted(remote, true);
        if (diagnostic) handler.SslOptions.ClientCertificates = new X509CertificateCollection { _diagnostic };
        return new(handler) { Timeout = Timeout.InfiniteTimeSpan };
    }
    public void Configure(WebApplicationBuilder builder, Uri endpoint, bool management)
    {
        builder.Configuration["AllowedHosts"] = "127.0.0.1;localhost";
        builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, endpoint.Port, listener => listener.UseHttps(_web, https =>
        {
            if (!management) return;
            https.ClientCertificateMode = ClientCertificateMode.AllowCertificate;
            https.ClientCertificateValidation = (certificate, _, _) => Trusted(certificate, false);
        })));
    }
    public static void Generate(string directory)
    {
        directory = Path.GetFullPath(directory);
        if (Directory.Exists(directory) && Directory.EnumerateFileSystemEntries(directory).Any()) throw new IOException("Certificate output must be empty.");
        Directory.CreateDirectory(directory);
        var password = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
        File.WriteAllText(Path.Combine(directory, "pfx.password"), password);
        using var key = RSA.Create(3072);
        var request = new CertificateRequest("CN=Raft local laboratory CA", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        using var ca = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(30));
        File.WriteAllBytes(Path.Combine(directory, "ca.cer"), ca.Export(X509ContentType.Cert));
        for (var i = 1; i <= 9; i++) Issue("peer-n" + i, "n" + i, true, true);
        Issue("http", "localhost", true, false);
        Issue("dashboard", "raft-dashboard", false, true);
        void Issue(string file, string name, bool server, bool client)
        {
            using var leafKey = RSA.Create(2048);
            var leaf = new CertificateRequest("CN=" + name, leafKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            leaf.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
            leaf.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            var eku = new OidCollection(); if (server) eku.Add(new("1.3.6.1.5.5.7.3.1")); if (client) eku.Add(new("1.3.6.1.5.5.7.3.2"));
            leaf.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(eku, true));
            var san = new SubjectAlternativeNameBuilder(); san.AddDnsName(name); if (file == "http") san.AddIpAddress(IPAddress.Loopback);
            leaf.CertificateExtensions.Add(san.Build());
            using var signed = leaf.Create(ca, DateTimeOffset.UtcNow.AddMinutes(-2), DateTimeOffset.UtcNow.AddDays(14), RandomNumberGenerator.GetBytes(16));
            using var owned = signed.CopyWithPrivateKey(leafKey);
            File.WriteAllBytes(Path.Combine(directory, file + ".pfx"), owned.Export(X509ContentType.Pfx, password));
        }
    }
    public void Dispose() { foreach (var certificate in _peers.Values) certificate.Dispose(); _diagnostic.Dispose(); _web.Dispose(); _root.Dispose(); }
}
