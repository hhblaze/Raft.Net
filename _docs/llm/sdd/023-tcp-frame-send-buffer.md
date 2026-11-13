# Size outbound TCP send space for bounded frames

Status: rejected and reverted. Focused24 transport/security tests passed, but diagnostic885/s and275–276follower writes versusR12 877/s and273 did not demonstrate useful benefit. Sources/runtime/raw remain archived. The OS send buffer is unchanged in the current runtime. R13 selected-window policy was rejected: its diagnostic recorded172 leader append writes versus166 in R12, and274–276 follower appends versus273. No useful I/O reduction was demonstrated; do not retain extra collection latency. R13 source/runtime/tests/raw remain archived separately.

## Observation and scope

This host reports a new TCP socket send/receive buffer of65536 bytes; global receive auto-tuning is normal. The default Raft append frame can exceed245KiB, and default maximum bounded frame is346560B. The per-session write gate stays held until NetworkStream.WriteAsync completes. Test whether fitting one bounded frame in requested send space lets the configured replication pipeline reach the receiver more effectively.

Request an outbound socket SendBufferSize of clamp(MaxFrameBytes,64KiB,1MiB) before connect. Change only per-connection send space. Preserve OS receive auto-tuning, all global settings, logical flight/frame/admission bounds, framing/correlation and TLS. Set properties inside the existing disposal-protected connect try block. This is a requested kernel buffer, not a managed allocation or an exact cross-platform physical-memory bound; OS values/accounting can differ. It adds bounded per-open-connection kernel capacity and must be described in any retained benchmark result.

Run existing TCP/wire/security/bounds tests, then an actual disk/TCP diagnostic and fresh reference trials. Keep only demonstrated behavior; no throughput claim from API documentation. If retained, complete full318-test/integration validation before a final comparison. Preserve common business work and frozen baseline bytes, all earlier reports and raw data.

## Sources

- Microsoft .NET8 [TcpClient.SendBufferSize](https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.tcpclient.sendbuffersize?view=net-8.0), remarks on application/send buffer size.
- Microsoft [Winsock compatibility notes](https://learn.microsoft.com/en-us/windows/win32/winsock/sio-set-compatibility-mode): setting receive space can disable receive window auto-tuning. No receive option is changed here.
