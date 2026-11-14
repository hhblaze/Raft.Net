param(
    [Parameter(Mandatory)][string]$Directory,
    [string]$PackageRoot = "$env:USERPROFILE/.nuget/packages"
)
$ErrorActionPreference='Stop'
# These cached tool dependencies are not production package references.
foreach ($relative in @(
    'microsoft.diagnostics.netcore.client/0.2.510501/lib/net6.0/Microsoft.Diagnostics.NETCore.Client.dll',
    'microsoft.diagnostics.tracing.traceevent/3.1.21/lib/netstandard2.0/Microsoft.Diagnostics.FastSerialization.dll',
    'microsoft.diagnostics.tracing.traceevent/3.1.21/lib/netstandard2.0/Microsoft.Diagnostics.Tracing.TraceEvent.dll'
)) { [Reflection.Assembly]::LoadFrom((Join-Path $PackageRoot $relative)) | Out-Null }
$results=@()
foreach ($file in Get-ChildItem -LiteralPath $Directory -Filter '*.nettrace') {
    $converted=[IO.Path]::ChangeExtension($file.FullName,'.etlx')
    [Microsoft.Diagnostics.Tracing.Etlx.TraceLog]::CreateFromEventPipeDataFile($file.FullName,$converted,
        [Microsoft.Diagnostics.Tracing.Etlx.TraceLogOptions]::new()) | Out-Null
    $log=[Microsoft.Diagnostics.Tracing.Etlx.TraceLog]::new($converted)
    try {
        $start=$null; $end=$null; $startQpc=$null; $endQpc=$null
        foreach ($event in $log.Events) {
            if ($event.ProviderName -ne 'Raft.Comparison.Measurements') { continue }
            if ([int]$event.ID -eq 1) { $start=$event.TimeStampRelativeMSec; $startQpc=$event.PayloadByName('timestamp') }
            if ([int]$event.ID -eq 2) { $end=$event.TimeStampRelativeMSec; $endQpc=$event.PayloadByName('timestamp') }
        }
        if ($null -eq $start -or $null -eq $end -or $end -le $start) { throw "Missing timed fences in $($file.Name)" }
        $samples=0; $blocked=0; $withOwnedFrame=0; $types=@{}; $frames=@{}
        foreach ($event in $log.Events) {
            if ($event.TimeStampQPC -lt $startQpc -or $event.TimeStampQPC -gt $endQpc) { continue }
            if ($event.EventName -eq 'GC/AllocationTick') {
                $name=[string]$event.PayloadByName('TypeName')
                $amount=[long]$event.PayloadByName('AllocationAmount64')
                if ($name) { $types[$name]=[long]$types[$name]+$amount }
            }
            if ($event.ProviderName -ne 'Microsoft-DotNETCore-SampleProfiler') { continue }
            $samples++
            $stack=[Microsoft.Diagnostics.Tracing.Etlx.TraceLogExtensions]::CallStack($event)
            $isBlocked=$false; $owned=$null
            while ($null -ne $stack) {
                $method=$stack.CodeAddress.FullMethodName
                if ($method -match 'WaitForOperation|Monitor\.Wait|WaitForSingleObject|SpinThenBlockingWait|WaitHandle|Thread\.Sleep') { $isBlocked=$true }
                if ($null -eq $owned -and $method -match 'Raft\.|DBreeze\.') { $owned=$method }
                $stack=$stack.Caller
            }
            if ($isBlocked) { $blocked++ }
            if ($null -ne $owned) { $withOwnedFrame++; $frames[$owned]=[long]$frames[$owned]+1 }
        }
        $raw=Get-Content -LiteralPath ([IO.Path]::ChangeExtension($file.FullName,'.json')) -Raw | ConvertFrom-Json
        $diagnostics=$raw.Results[0].Diagnostics
        $cpuMs=($diagnostics.RuntimeAfter.CpuTicks-$diagnostics.RuntimeBefore.CpuTicks)/10000
        $results+=@{
            Variant=$file.BaseName; StartMilliseconds=$start; StopMilliseconds=$end; StartTimestamp=$startQpc; StopTimestamp=$endQpc
            ProcessCpuMilliseconds=$cpuMs; ProcessCpuCoreEquivalent=$cpuMs/($end-$start)
            StackSamples=$samples; SamplesWithRecognizedWait=$blocked; SamplesWithOwnedFrame=$withOwnedFrame
            SampleInterpretation='Stack observations include blocked threads. Counts are NOT CPU time; wait recognition is heuristic. Process CPU is measured separately.'
            SampledAllocationBytes=$types
            AllocationInterpretation='GC allocation tick weights are sampled estimates, not exact per-type allocations.'
            FirstOwnedStackFrameSamples=$frames; Diagnostics=$diagnostics
        }
        Write-Output "$($file.BaseName): explicit timed fences, process CPU=$cpuMs ms, samples=$samples, recognized waits=$blocked"
    }
    finally { $log.Dispose() }
}
$results | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $Directory 'trace-summary.json') -Encoding utf8
