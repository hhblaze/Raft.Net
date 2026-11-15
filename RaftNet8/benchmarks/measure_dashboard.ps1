#requires -Version 7.0
param(
    [Parameter(Mandatory)][string]$BaselineDirectory,
    [Parameter(Mandatory)][string]$DashboardAssembly,
    [string]$CurrentHarness,
    [string]$BeforeHarness,
    [string]$BeforeDashboardAssembly,
    [string]$ExperimentRoot='D:\Temp\RaftNet8',
    [ValidateRange(1,10)][int]$Rounds=5,
    [ValidateRange(1,10)][int]$StartRound=1,
    [ValidateRange(512,100000)][int]$Requests=4096,
    [string[]]$Variants=@('baseline','off','on','tokens','tls')
)
$ErrorActionPreference='Stop'
$solutionRoot=Split-Path $PSScriptRoot
$runRoot=Join-Path ([IO.Path]::GetFullPath($ExperimentRoot)) ('dashboard-overhead-'+[guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($runRoot) | Out-Null
$current=if($CurrentHarness) { [IO.Path]::GetFullPath($CurrentHarness) } else { Join-Path $solutionRoot 'HumanTest/Raft.Cluster.Demo/bin/Release/net8.0/Raft.Cluster.Demo.dll' }
$baseline=Join-Path ([IO.Path]::GetFullPath($BaselineDirectory)) 'RaftNet8/HumanTest/Raft.Cluster.Demo/bin/Release/net8.0/Raft.Cluster.Demo.dll'
foreach($file in @($current,$baseline,$DashboardAssembly)) { if(!(Test-Path -LiteralPath $file -PathType Leaf)) { throw "Build/publish required: $file" } }
$manifest=[Collections.Generic.List[object]]::new()
$variantsAllowed=@('baseline','off','on','before','tokens','tls')
if(@($Variants | Where-Object { $_ -notin $variantsAllowed }).Count) { throw 'Unknown variant' }
if('before' -in $Variants) { foreach($file in @($BeforeHarness,$BeforeDashboardAssembly)) { if(!$file -or !(Test-Path -LiteralPath $file -PathType Leaf)) { throw 'Supply frozen before harness/dashboard binaries' } } }
if($StartRound -gt $Rounds) { throw 'StartRound cannot exceed the final round number' }
for($round=$StartRound; $round -le $Rounds; $round++) {
    $order=@($Variants)
    if($round % 2 -eq 0) { [array]::Reverse($order) }
    foreach($variant in $order) {
        $folder=Join-Path $runRoot ("r$round-$variant")
        [IO.Directory]::CreateDirectory($folder) | Out-Null
        $assembly=if($variant -eq 'baseline') { $baseline } elseif($variant -eq 'before') { [IO.Path]::GetFullPath($BeforeHarness) } else { $current }
        $arguments=@($assembly,'benchmark','--rounds','1','--requests',"$Requests",'--root',$folder)
        switch($variant) {
            'off' { $arguments+=@('--telemetry','false') }
            'on' { $arguments+=@('--management','true','--telemetry','true','--dashboard',[IO.Path]::GetFullPath($DashboardAssembly)) }
            'before' { $arguments+=@('--management','true','--telemetry','true','--dashboard',[IO.Path]::GetFullPath($BeforeDashboardAssembly)) }
            'tokens' { $arguments+=@('--management','true','--tokens','true','--telemetry','false') }
            'tls' { $arguments+=@('--management','true','--tokens','true','--tls','true','--telemetry','false') }
        }
        $start=[Diagnostics.ProcessStartInfo]::new((Get-Command dotnet).Source)
        $start.UseShellExecute=$false; $start.CreateNoWindow=$true; $start.WindowStyle=[Diagnostics.ProcessWindowStyle]::Hidden
        $start.RedirectStandardOutput=$true; $start.RedirectStandardError=$true
        foreach($argument in $arguments) { $start.ArgumentList.Add($argument) }
        $started=[DateTimeOffset]::UtcNow
        $child=[Diagnostics.Process]::Start($start)
        $stdout=$child.StandardOutput.ReadToEndAsync(); $stderr=$child.StandardError.ReadToEndAsync()
        try { $child.WaitForExit() }
        finally { if(!$child.HasExited) { $child.Kill($true); $child.WaitForExit() } }
        [IO.File]::WriteAllText((Join-Path $folder 'process.log'),$stdout.GetAwaiter().GetResult())
        [IO.File]::WriteAllText((Join-Path $folder 'process.err'),$stderr.GetAwaiter().GetResult())
        $record=[pscustomobject]@{Round=$round; Variant=$variant; Started=$started; Ended=[DateTimeOffset]::UtcNow; ProcessId=$child.Id; ExitCode=$child.ExitCode;
            Assembly=$assembly; AssemblySha256=(Get-FileHash -LiteralPath $assembly -Algorithm SHA256).Hash; Folder=$folder}
        $manifest.Add($record)
        $manifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $runRoot 'runs.json') -Encoding utf8
        $exit=$child.ExitCode; $child.Dispose()
        if($exit -ne 0) { throw "Measurement failed: $variant round$round; inspect $folder" }
        $result=Get-ChildItem -LiteralPath $folder -File -Recurse -Filter results.json | Select-Object -First 1
        if(!$result -or !(Get-Content -LiteralPath $result.FullName -Raw | ConvertFrom-Json).Completed) { throw 'Incomplete measurement result' }
        Write-Host "Measured round$round $variant ; $($result.FullName)"
    }
}
Write-Host "MEASUREMENT ROOT $runRoot"
