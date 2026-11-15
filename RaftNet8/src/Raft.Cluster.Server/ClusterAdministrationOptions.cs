using Microsoft.AspNetCore.Builder;

namespace Raft.Cluster.Server;

public sealed record ClusterSecurityOptions
{
    private bool? _requireClientTokens;
    /// <summary>Defaults to true for secure transport, false for trusted-network transport; an explicit host value overrides it.</summary>
    public bool RequireClientTokens { get => _requireClientTokens ?? SecureTransport; init => _requireClientTokens = value; }
    public bool SecureTransport { get; init; }
}

public sealed record ClusterManagementOptions
{
    public Uri Endpoint { get; init; } = new("http://127.0.0.1:7201/");
    public bool AllowInsecureRemote { get; init; }
    /// <summary>At least32 random characters, supplied from an ACL-protected host file. Bootstrap is loopback-only and one-time.</summary>
    public string? BootstrapSecret { get; init; }
    public IReadOnlyList<ManagedNodeEndpoint> InitialEndpoints { get; init; } = [];
    public Action<WebApplicationBuilder>? ConfigureHttpServer { get; init; }
    /// <summary>Caller-owned outbound client for private PKI and node-local read authorization barriers. Defaults to platform certificate validation.</summary>
    public HttpClient? HttpClient { get; init; }
    /// <summary>Verified certificate thumbprints allowed to read only local diagnostics without quorum. TLS chain validation is still required.</summary>
    public IReadOnlyList<string> DiagnosticCertificateThumbprints { get; init; } = [];
    public string? DataDirectory { get; init; }
    public string HostId { get; init; } = Environment.MachineName;
    public PeerDescriptor? LocalPeer { get; init; }
    public bool EnableTelemetry { get; init; } = true;
    /// <summary>Collect simultaneous management observations for2ms before a fresh read. Does not change proposal batching or cache read fences.</summary>
    public bool BatchObservationReads { get; init; } = true;
    public int SampleIntervalSeconds { get; init; } = 1;
    public int FileScanIntervalSeconds { get; init; } = 30;
    public IReadOnlyDictionary<string, string> EffectiveConfiguration { get; init; } = new Dictionary<string, string>();
}
