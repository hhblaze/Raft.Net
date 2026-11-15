#requires -Version 7.0
param(
    [ValidateSet(3,5)][int]$Voters = 3,
    [switch]$Secure,
    [string]$ExperimentRoot = 'D:\Temp\RaftNet8',
    [int]$DashboardPort = 7400,
    [int]$PeerBasePort = 6101,
    [int]$ClientBasePort = 7101,
    [int]$ManagementBasePort = 7201,
    [switch]$SkipBuild
)
$ErrorActionPreference = 'Stop'
$solutionRoot = Split-Path $PSScriptRoot
$runRoot = Join-Path ([IO.Path]::GetFullPath($ExperimentRoot)) ('dashboard-stand-' + [guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($runRoot) | Out-Null
if ($IsWindows) {
    $acl = [Security.AccessControl.DirectorySecurity]::new()
    $acl.SetAccessRuleProtection($true, $false)
    foreach ($sid in @([Security.Principal.WindowsIdentity]::GetCurrent().User, [Security.Principal.SecurityIdentifier]::new('S-1-5-18'))) {
        $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid, [Security.AccessControl.FileSystemRights]::FullControl,
            ([Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [Security.AccessControl.InheritanceFlags]::ObjectInherit),
            [Security.AccessControl.PropagationFlags]::None, [Security.AccessControl.AccessControlType]::Allow))
    }
    Set-Acl -LiteralPath $runRoot -AclObject $acl
} else { [IO.File]::SetUnixFileMode($runRoot, ([IO.UnixFileMode]::UserRead -bor [IO.UnixFileMode]::UserWrite -bor [IO.UnixFileMode]::UserExecute)) }
$dotnet = (Get-Command dotnet).Source
$demo = Join-Path $solutionRoot 'HumanTest/Raft.Cluster.Demo/bin/Release/net8.0/Raft.Cluster.Demo.dll'
$published = Join-Path $runRoot 'published'
$dashboard = Join-Path $published 'RaftClusterDashboard.dll'
function Invoke-Checked([string[]]$Arguments) {
    & $dotnet @Arguments
    if ($LASTEXITCODE -ne 0) { throw "dotnet command failed with exit code $LASTEXITCODE" }
}
if (!$SkipBuild) { Invoke-Checked @('build', (Join-Path $solutionRoot 'HumanTest/Raft.Cluster.Demo'), '-c', 'Release') }
Invoke-Checked @('publish', (Join-Path $solutionRoot 'src/RaftClusterDashboard'), '-c', 'Release', '-o', $published)
$password = $env:RAFT_DASHBOARD_BOOTSTRAP_PASSWORD
if (!$password) { $password = [Net.NetworkCredential]::new('', (Read-Host 'New dashboard admin password (12+ characters)' -AsSecureString)).Password }
$processes = [Collections.Generic.List[object]]::new()
function Start-Owned([string]$Name, [string[]]$Arguments, [bool]$Wait = $false, [string]$BootstrapPassword = '') {
    $start = [Diagnostics.ProcessStartInfo]::new($dotnet)
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.WindowStyle = [Diagnostics.ProcessWindowStyle]::Hidden
    $start.WorkingDirectory = Split-Path $Arguments[0]
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.RedirectStandardInput = $true
    foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
    $start.Environment.Remove('RAFT_DASHBOARD_BOOTSTRAP_PASSWORD') | Out-Null
    if ($BootstrapPassword) { $start.Environment['RAFT_DASHBOARD_BOOTSTRAP_PASSWORD'] = $BootstrapPassword }
    if ($Arguments[0] -eq $dashboard) { $start.Environment['ASPNETCORE_ENVIRONMENT'] = 'Stand' }
    $child = [Diagnostics.Process]::Start($start)
    # Asynchronous stream copies avoid filling redirected pipes in long-lived node processes.
    $output = [IO.FileStream]::new((Join-Path $runRoot ($Name + '.log')), [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::Read)
    $errorOutput = [IO.FileStream]::new((Join-Path $runRoot ($Name + '.err')), [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::Read)
    $outTask = $child.StandardOutput.BaseStream.CopyToAsync($output)
    $errTask = $child.StandardError.BaseStream.CopyToAsync($errorOutput)
    if ($Wait) {
        $child.WaitForExit(); $outTask.GetAwaiter().GetResult() | Out-Null; $errTask.GetAwaiter().GetResult() | Out-Null
        $output.Dispose(); $errorOutput.Dispose()
        if ($child.ExitCode -ne 0) { throw "$Name failed. See $runRoot\$Name.err" }
        $child.Dispose()
    } else {
        $processes.Add([pscustomobject]@{ Name=$Name; Process=$child; OutTask=$outTask; ErrTask=$errTask; Output=$output; ErrorOutput=$errorOutput })
    }
}
$nodeRoot = Join-Path $runRoot 'nodes'
$pki = Join-Path $runRoot 'pki'
$scheme = if ($Secure) { 'https' } else { 'http' }
$url = "${scheme}://127.0.0.1:$DashboardPort"
try {
    if ($Secure) { Start-Owned 'pki-generation' @($demo, 'create-lab-pki', '--pki', $pki) $true }
    $configuration = @{
        urls=$url; AllowedHosts='127.0.0.1;localhost'; Logging=@{LogLevel=@{Default='Warning'}}
        Dashboard=@{DataDirectory=(Join-Path $runRoot 'dashboard-data'); SecureProfile=[bool]$Secure}
    }
    if ($Secure) {
        $pfxPassword = [IO.File]::ReadAllText((Join-Path $pki 'pfx.password'))
        $configuration.Kestrel = @{Certificates=@{Default=@{Path=(Join-Path $pki 'http.pfx'); Password=$pfxPassword}}}
        $configuration.Dashboard.DiagnosticCertificates = @{lab=@{Path=(Join-Path $pki 'dashboard.pfx'); Password=$pfxPassword; TrustedRootPath=(Join-Path $pki 'ca.cer')}}
    }
    $settings = Join-Path $runRoot 'dashboard-settings.json'
    $configuration | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $settings -Encoding utf8
    for ($index = 1; $index -le $Voters + 2; $index++) {
        $arguments = @($demo, 'node', "n$index", '--daemon', '--control-stdin', '--root', $nodeRoot, '--voters', "$Voters", '--group', 'movies', '--management', 'true', '--tokens', "$([bool]$Secure)".ToLowerInvariant(),
            '--peer-base-port', "$PeerBasePort", '--client-base-port', "$ClientBasePort", '--management-base-port', "$ManagementBasePort")
        if ($Secure) { $arguments += @('--pki', $pki) }
        Start-Owned "n$index" $arguments
    }
    $bootstrapArguments = @($demo, 'bootstrap-stand', '--root', $nodeRoot, '--voters', "$Voters", '--management-base-port', "$ManagementBasePort")
    if ($Secure) { $bootstrapArguments += @('--pki', $pki) }
    Start-Owned 'cluster-bootstrap' $bootstrapArguments $true
    # ASP.NET configuration is loaded explicitly; private files stay inside this experiment directory.
    Copy-Item -LiteralPath $settings -Destination (Join-Path $published 'appsettings.Stand.json')
    Start-Owned 'dashboard-bootstrap' @($dashboard, 'bootstrap-admin') $true $password
    $password = $null
    Start-Owned 'dashboard-import' @($dashboard, 'import-cluster', '--connection-file', (Join-Path $nodeRoot 'dashboard-connection.json')) $true
    Start-Owned 'dashboard' @($dashboard, '--control-stdin', 'true')
    $manifest = [pscustomobject]@{ Root=$runRoot; Url=$url; Secure=[bool]$Secure; Processes=@($processes | ForEach-Object { @{Name=$_.Name; Id=$_.Process.Id} }) }
    $manifest | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $runRoot 'processes.json') -Encoding utf8
    Write-Host "Dashboard: $url ; user: admin"
    Write-Host "Artifacts: $runRoot"
    if ($Secure) { Write-Host 'Private lab CA: pki/ca.cer. Browser trust must be configured explicitly; Admin/Operator MFA enrollment is required.' }
    Write-Host 'Two joining processes are available for Add. Press Ctrl+C to stop this owned stand.'
    while ($true) { foreach ($item in $processes) { if ($item.Process.HasExited) { throw "$($item.Name) exited. Inspect its log." } }; Start-Sleep -Seconds 1 }
} finally {
    foreach ($item in $processes) {
        if (!$item.Process.HasExited) {
            try { $item.Process.StandardInput.WriteLine('quit'); $item.Process.StandardInput.Flush() } catch [IO.IOException] { }
            if (!$item.Process.WaitForExit(7000)) { $item.Process.Kill($true) }
        }
        $item.Process.WaitForExit()
        $item.OutTask.GetAwaiter().GetResult() | Out-Null; $item.ErrTask.GetAwaiter().GetResult() | Out-Null
        $item.Output.Dispose(); $item.ErrorOutput.Dispose(); $item.Process.Dispose()
    }
}
