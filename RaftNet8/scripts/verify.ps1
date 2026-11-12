param([switch]$SkipScenario)
$ErrorActionPreference = 'Stop'
$solutionRoot = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $solutionRoot
try {
    dotnet restore Raft.Net8.sln --locked-mode
    if ($LASTEXITCODE -ne 0) { throw 'Package restore failed.' }
    dotnet build Raft.Net8.sln -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw 'Release build failed.' }
    dotnet test Raft.Net8.sln -c Release --no-build
    if ($LASTEXITCODE -ne 0) { throw 'Tests failed.' }
    if (-not $SkipScenario) {
        dotnet run --project HumanTest/Raft.Demo -c Release --no-build -- --scenario
        if ($LASTEXITCODE -ne 0) { throw 'Human-test scenario failed.' }
    }
}
finally { Pop-Location }
