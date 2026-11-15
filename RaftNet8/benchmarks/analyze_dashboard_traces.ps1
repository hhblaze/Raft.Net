#requires -Version 7.0
param([Parameter(Mandatory)][string]$Directory)
$ErrorActionPreference='Stop'
foreach($relative in @(
    'microsoft.diagnostics.netcore.client/0.2.510501/lib/net6.0/Microsoft.Diagnostics.NETCore.Client.dll',
    'microsoft.diagnostics.tracing.traceevent/3.1.21/lib/netstandard2.0/Microsoft.Diagnostics.FastSerialization.dll',
    'microsoft.diagnostics.tracing.traceevent/3.1.21/lib/netstandard2.0/Microsoft.Diagnostics.Tracing.TraceEvent.dll'
)) { [Reflection.Assembly]::LoadFrom((Join-Path "$env:USERPROFILE/.nuget/packages" $relative)) | Out-Null }
$results=@()
foreach($file in Get-ChildItem -LiteralPath $Directory -Filter '*.nettrace' -Recurse -File) {
    $converted=[IO.Path]::ChangeExtension($file.FullName,'.etlx')
    [Microsoft.Diagnostics.Tracing.Etlx.TraceLog]::CreateFromEventPipeDataFile($file.FullName,$converted,
        [Microsoft.Diagnostics.Tracing.Etlx.TraceLogOptions]::new()) | Out-Null
    $log=[Microsoft.Diagnostics.Tracing.Etlx.TraceLog]::new($converted)
    try {
        $first=$null; $last=$null; $begin=$null; $end=$null
        foreach($event in $log.Events) {
            if($event.ProviderName -ne 'Raft.Cluster.Measurements') { continue }
            if([int]$event.ID -eq 1) { $first=$event.TimeStampQPC; $begin=$event.TimeStampRelativeMSec }
            if([int]$event.ID -eq 2) { $last=$event.TimeStampQPC; $end=$event.TimeStampRelativeMSec }
        }
        if($null -eq $first -or $null -eq $last) { throw "Missing measurement markers: $file" }
        $samples=0; $waiting=0; $frames=@{}; $waitFrames=@{}; $allocation=@{}; $eventCounts=@{}; $adjustments=@()
        foreach($event in $log.Events) {
            if($event.TimeStampQPC -lt $first -or $event.TimeStampQPC -gt $last) { continue }
            if($event.ProviderName -eq 'Microsoft-Windows-DotNETRuntime') {
                $eventCounts[$event.EventName]=[long]$eventCounts[$event.EventName]+1
                if($event.EventName -eq 'GC/AllocationTick') {
                    $name=[string]$event.PayloadByName('TypeName')
                    if($name) { $allocation[$name]=[long]$allocation[$name]+[long]$event.PayloadByName('AllocationAmount64') }
                }
                if($event.EventName -match 'ThreadPool.*Adjustment') {
                    $payload=@{}; foreach($name in $event.PayloadNames) { $payload[$name]=[string]$event.PayloadByName($name) }
                    $adjustments+=@{At=$event.TimeStampRelativeMSec;Event=$event.EventName;Payload=$payload}
                }
            }
            if($event.ProviderName -ne 'Microsoft-DotNETCore-SampleProfiler') { continue }
            $samples++
            $stack=[Microsoft.Diagnostics.Tracing.Etlx.TraceLogExtensions]::CallStack($event)
            $wait=$false; $owned=$null
            while($null -ne $stack) {
                $method=$stack.CodeAddress.FullMethodName
                if($method -match 'WaitForOperation|Monitor\.(Wait|Enter)|WaitForSingleObject|SpinThenBlockingWait|WaitHandle|Thread\.Sleep|NtWaitFor') { $wait=$true }
                if($null -eq $owned -and $method -match 'Raft\.|DBreeze\.') { $owned=$method }
                $stack=$stack.Caller
            }
            if($wait) { $waiting++ }
            if($owned) {
                if($wait) { $waitFrames[$owned]=[long]$waitFrames[$owned]+1 }
                else { $frames[$owned]=[long]$frames[$owned]+1 }
            }
        }
        $top={ param($map) @($map.GetEnumerator() | Sort-Object Value -Descending | Select-Object -First 30 | ForEach-Object { @{Name=$_.Key;Count=$_.Value} }) }
        $results+=@{File=$file.FullName;Sha256=(Get-FileHash -LiteralPath $file.FullName).Hash;WindowMs=$end-$begin;
            SampleStacks=$samples;StacksWithWaitFrames=$waiting;OwnedTopStacks=(& $top $frames);OwnedWaitStacks=(& $top $waitFrames);
            AllocationTicks=(& $top $allocation);RuntimeEvents=$eventCounts;ThreadPoolAdjustments=$adjustments}
    } finally { $log.Dispose() }
}
$results | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $Directory 'analysis.json') -Encoding utf8
Write-Host "Analyzed $($results.Count) traces. Stack samples are diagnostic estimates, not measured CPU or I/O duration percentages."
