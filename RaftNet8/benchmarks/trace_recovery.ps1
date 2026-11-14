param(
    [Parameter(Mandatory)][string]$Prepared,
    [Parameter(Mandatory)][string]$Out,
    [Parameter(Mandatory)][string]$DataRoot,
    [string]$ClientAssembly = "$env:USERPROFILE/.nuget/packages/microsoft.diagnostics.netcore.client/0.2.510501/lib/net6.0/Microsoft.Diagnostics.NETCore.Client.dll",
    [string]$Case = 'Current-3n-64B-Pipeline',
    [int]$Operations = 16384,
    [int]$Warmup = 2048
)
$ErrorActionPreference = 'Stop'
[Reflection.Assembly]::LoadFrom((Resolve-Path -LiteralPath $ClientAssembly).Path) | Out-Null
$traceOutput = [IO.Path]::GetFullPath($Out)
if (Test-Path -LiteralPath $traceOutput) { throw 'Use a fresh trace directory.' }
[IO.Directory]::CreateDirectory($traceOutput) | Out-Null
$preparedRoot = (Resolve-Path -LiteralPath $Prepared).Path
$previousData = $env:RAFT_BENCHMARK_ROOT
$env:RAFT_BENCHMARK_ROOT = [IO.Path]::GetFullPath($DataRoot)
try {
    foreach ($variant in @('e06','before','after')) {
        $runtime = Join-Path $preparedRoot "$variant-runtime/Raft.Comparison.dll"
        $metadata = Join-Path $preparedRoot "$variant-source"
        $raw = Join-Path $traceOutput "$variant.json"
        $arguments = @('"' + $runtime + '"','--case',$Case,'--operations',$Operations,
            '--warmup',$Warmup,'--repeats','1','--concurrency','64','--diagnose','--out','"' + $raw + '"')
        if ($variant -eq 'e06') { $arguments += '--unified' } else { $arguments += '--hot-membership' }
        $child = Start-Process -FilePath 'dotnet' -ArgumentList $arguments -WorkingDirectory $metadata -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput (Join-Path $traceOutput "$variant.log") -RedirectStandardError (Join-Path $traceOutput "$variant.errors.log")
        $session = $null
        $stream = $null
        try {
            $client = [Microsoft.Diagnostics.NETCore.Client.DiagnosticsClient]::new($child.Id)
            $providers = [Microsoft.Diagnostics.NETCore.Client.EventPipeProvider[]]@(
                [Microsoft.Diagnostics.NETCore.Client.EventPipeProvider]::new('Microsoft-Windows-DotNETRuntime',[Diagnostics.Tracing.EventLevel]::Verbose,1,$null),
                [Microsoft.Diagnostics.NETCore.Client.EventPipeProvider]::new('Microsoft-DotNETCore-SampleProfiler',[Diagnostics.Tracing.EventLevel]::Informational,0,$null),
                [Microsoft.Diagnostics.NETCore.Client.EventPipeProvider]::new('Raft.Comparison.Measurements',[Diagnostics.Tracing.EventLevel]::Informational,0,$null),
                [Microsoft.Diagnostics.NETCore.Client.EventPipeProvider]::new('Raft.Net8.QueueDiagnostics',[Diagnostics.Tracing.EventLevel]::Informational,0,$null)
            )
            for ($attempt=0; $attempt -lt 50 -and $null -eq $session; $attempt++) {
                try { $session=$client.StartEventPipeSession($providers,$true,128) }
                catch { if ($child.HasExited) { throw }; Start-Sleep -Milliseconds 100 }
            }
            if ($null -eq $session) { throw 'EventPipe attachment failed.' }
            $stream=[IO.File]::Create((Join-Path $traceOutput "$variant.nettrace"))
            $copy=$session.EventStream.CopyToAsync($stream)
            $deadline=[DateTime]::UtcNow.AddSeconds(120)
            $completed=$false
            while (-not $child.HasExited -and [DateTime]::UtcNow -lt $deadline) {
                if (Test-Path -LiteralPath $raw) {
                    try { $completed=(Get-Content -LiteralPath $raw -Raw | ConvertFrom-Json).Complete }
                    catch { $completed=$false }
                    if ($completed) { break }
                }
                Start-Sleep -Milliseconds 200
            }
            if (-not $completed) { throw 'No completed measurement before trace deadline.' }
            $session.Stop() # while the diagnostic harness holds for rundown, outside timing
            $copy.GetAwaiter().GetResult()
            $stream.Flush()
            $child.WaitForExit()
            if ($child.ExitCode -ne 0) { throw "$variant diagnostic exited $($child.ExitCode)" }
            Write-Output "$variant trace complete (timed Start/Stop events included)."
        }
        finally {
            if ($null -ne $session) { $session.Dispose() }
            if ($null -ne $stream) { $stream.Dispose() }
            if (-not $child.HasExited) { $child.Kill(); $child.WaitForExit() }
            $child.Dispose()
        }
    }
}
finally { $env:RAFT_BENCHMARK_ROOT=$previousData }
