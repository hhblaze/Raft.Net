#requires -Version 7.0
param(
    [Parameter(Mandatory)][string]$Harness,
    [Parameter(Mandatory)][string]$DashboardAssembly,
    [string]$BeforeHarness,
    [string]$BeforeDashboardAssembly,
    [string]$ExperimentRoot='D:\Temp\RaftNet8',
    [ValidateRange(1,10)][int]$Rounds=5,
    [ValidateSet(3,5)][int]$Voters=5,
    [ValidateSet(64,16384)][int]$PayloadBytes=16384,
    [string[]]$Variants=@('managed','telemetry','dashboard'),
    [switch]$Trace
)
$ErrorActionPreference='Stop'
$root=Join-Path ([IO.Path]::GetFullPath($ExperimentRoot)) ('dashboard-isolation-'+[guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($root) | Out-Null
$manifest=[Collections.Generic.List[object]]::new()
if($Trace) { [Reflection.Assembly]::LoadFrom("$env:USERPROFILE/.nuget/packages/microsoft.diagnostics.netcore.client/0.2.510501/lib/net6.0/Microsoft.Diagnostics.NETCore.Client.dll") | Out-Null }
for($round=1; $round -le $Rounds; $round++) {
    $order=@($Variants)
    if($round % 2 -eq 0) { [array]::Reverse($order) }
    foreach($variant in $order) {
        $folder=Join-Path $root "r$round-$variant"
        [IO.Directory]::CreateDirectory($folder) | Out-Null
        $assembly=if($variant -in @('uncached','old-off','old-on')) { if(!$BeforeHarness) { throw 'Supply BeforeHarness' }; [IO.Path]::GetFullPath($BeforeHarness) } else { [IO.Path]::GetFullPath($Harness) }
        $arguments=@($assembly,'benchmark','--rounds','1','--requests','4096','--root',$folder,
            '--only-voters',"$Voters",'--only-payload',"$PayloadBytes",'--management',$(if($variant -in @('old-off','result-off')){'false'}else{'true'}),'--telemetry',$(if($variant -in @('managed','old-off','result-off')){'false'}else{'true'}))
        if($variant -in @('dashboard','direct','batched','cached','uncached','four','parallel','old-on','result-on')) {
            $dashboard=if($variant -eq 'four') { if(!$BeforeDashboardAssembly) { throw 'Supply BeforeDashboardAssembly' }; $BeforeDashboardAssembly } else { $DashboardAssembly }
            $arguments+=@('--dashboard',[IO.Path]::GetFullPath($dashboard))
        }
        if($variant -eq 'direct') { $arguments+=@('--batch-observation-reads','false') }
        if($Trace) { $arguments+=@('--diagnose','true','--trace-hold','true') }
        $start=[Diagnostics.ProcessStartInfo]::new((Get-Command dotnet).Source)
        $start.UseShellExecute=$false; $start.CreateNoWindow=$true; $start.WindowStyle=[Diagnostics.ProcessWindowStyle]::Hidden
        $start.RedirectStandardInput=$true; $start.RedirectStandardOutput=$true; $start.RedirectStandardError=$true
        foreach($argument in $arguments) { $start.ArgumentList.Add($argument) }
        $child=[Diagnostics.Process]::Start($start)
        $stdout=$child.StandardOutput.ReadToEndAsync(); $stderr=$child.StandardError.ReadToEndAsync()
        $session=$null; $stream=$null
        try {
            if($Trace) {
                $client=[Microsoft.Diagnostics.NETCore.Client.DiagnosticsClient]::new($child.Id)
                $providers=[Microsoft.Diagnostics.NETCore.Client.EventPipeProvider[]]@(
                    [Microsoft.Diagnostics.NETCore.Client.EventPipeProvider]::new('Microsoft-Windows-DotNETRuntime',[Diagnostics.Tracing.EventLevel]::Verbose,0x14001,$null),
                    [Microsoft.Diagnostics.NETCore.Client.EventPipeProvider]::new('Microsoft-DotNETCore-SampleProfiler',[Diagnostics.Tracing.EventLevel]::Informational,0,$null),
                    [Microsoft.Diagnostics.NETCore.Client.EventPipeProvider]::new('Raft.Cluster.Measurements',[Diagnostics.Tracing.EventLevel]::Informational,0,$null))
                for($attempt=0; $attempt -lt 50 -and $null -eq $session; $attempt++) {
                    try { $session=$client.StartEventPipeSession($providers,$true,128) }
                    catch { if($child.HasExited) { throw }; Start-Sleep -Milliseconds 100 }
                }
                if($null -eq $session) { throw 'Cannot attach EventPipe' }
                $stream=[IO.File]::Create((Join-Path $folder 'runtime.nettrace'))
                $copy=$session.EventStream.CopyToAsync($stream)
                $completed=$false; $deadline=[DateTimeOffset]::UtcNow.AddMinutes(4)
                while(!$child.HasExited -and [DateTimeOffset]::UtcNow -lt $deadline) {
                    $file=Get-ChildItem -LiteralPath $folder -Directory -Filter 'client-bench-*' | ForEach-Object { Join-Path $_.FullName 'results.json' } | Select-Object -First 1
                    if($file -and (Test-Path -LiteralPath $file)) { try { $completed=(Get-Content -LiteralPath $file -Raw | ConvertFrom-Json).Completed } catch { $completed=$false } }
                    if($completed) { break }
                    Start-Sleep -Milliseconds 200
                }
                if(!$completed) { throw 'Profile did not complete' }
                $session.Stop(); $null=$copy.GetAwaiter().GetResult(); $stream.Dispose(); $stream=$null
                $child.StandardInput.WriteLine('quit'); $child.StandardInput.Flush()
            }
            if(!$child.WaitForExit(240000)) { throw 'Isolation sample exceeded4minutes' }
        } finally {
            if(!$child.HasExited) { $child.Kill($true); $child.WaitForExit() }
            if($session) { $session.Dispose() }; if($stream) { $stream.Dispose() }
            [IO.File]::WriteAllText((Join-Path $folder 'process.log'),$stdout.GetAwaiter().GetResult())
            [IO.File]::WriteAllText((Join-Path $folder 'process.err'),$stderr.GetAwaiter().GetResult())
        }
        $record=[pscustomobject]@{Round=$round;Variant=$variant;Folder=$folder;ExitCode=$child.ExitCode;Trace=[bool]$Trace;HarnessSha256=(Get-FileHash -LiteralPath $assembly).Hash}
        $manifest.Add($record); $manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $root 'runs.json') -Encoding utf8
        $exit=$child.ExitCode; $child.Dispose()
        if($exit -ne 0) { throw "Failed $variant; inspect $folder" }
        Write-Host "Isolated round$round $variant trace=$Trace"
    }
}
Write-Host "ISOLATION ROOT $root"
