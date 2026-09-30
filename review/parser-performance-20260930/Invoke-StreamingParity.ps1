[CmdletBinding()]
param(
    [string]$RepositoryRoot = (Join-Path $PSScriptRoot '../..'),
    [string]$DotNetPath,
    [string]$AugustXer,
    [string]$JulyXer,
    [int]$Iterations = 2,
    [switch]$FixturesOnly,
    [switch]$PhaseTelemetry,
    [string]$BaselineCoreProject,
    [string]$EvidenceSuffix = ''
)

# Frozen sources are extracted from artifacts/parser-performance-20260930/frozen-current.zip.
# The harness sources are reviewable; schedules, extracted CSV/diagnostic evidence and
# reports remain under the repository's ignored artifacts directory.
# Example: ./review/parser-performance-20260930/Invoke-StreamingParity.ps1 -FixturesOnly
$taskRepository = (Resolve-Path -LiteralPath $RepositoryRoot).Path
if (-not $DotNetPath) { $DotNetPath = Join-Path $taskRepository 'local-dotnet-sdk/dotnet.exe' }
$taskProject = Join-Path $PSScriptRoot 'ParserParity.csproj'
$taskArtifacts = Join-Path $taskRepository 'artifacts/parser-performance-20260930'
$taskBaselineCore = if ($BaselineCoreProject) { $BaselineCoreProject } else {
    Join-Path $taskArtifacts 'baseline/XerToCsvConverter.Core/XerToCsvConverter.Core.csproj'
}
if (-not (Test-Path -LiteralPath $taskBaselineCore)) { throw "Frozen Core project is missing: $taskBaselineCore" }
if (-not (Test-Path -LiteralPath $DotNetPath)) { throw "dotnet runtime is missing: $DotNetPath" }
if ($Iterations -lt 1) { throw 'Iterations must be at least one.' }

foreach ($taskVariant in @('baseline', 'candidate')) {
    $taskCore = if ($taskVariant -eq 'baseline') { $taskBaselineCore } else {
        Join-Path $taskRepository 'XerToCsvConverter.Core/XerToCsvConverter.Core.csproj'
    }
    $taskTimingProperty = if ($PhaseTelemetry) { 'true' } else { 'false' }
    & $DotNetPath build $taskProject --configuration Release "-p:CoreProjectPath=$taskCore" "-p:HarnessVariant=$taskVariant" "-p:EnablePhaseTelemetry=$taskTimingProperty" --verbosity minimal
    if ($LASTEXITCODE -ne 0) { throw "$taskVariant harness build failed." }
    $taskExecutable = Join-Path $PSScriptRoot "bin/$taskVariant/Release/net8.0/ParserStreamingParity.dll"
    $taskRunArguments = @($taskExecutable, '--repo', $taskRepository, '--label', $taskVariant,
        '--output', (Join-Path $taskArtifacts "native-$taskVariant$EvidenceSuffix"), '--iterations', $Iterations.ToString())
    if ($FixturesOnly) { $taskRunArguments += @('--fixtures-only', 'true') }
    if ($AugustXer) { $taskRunArguments += @('--august', $AugustXer) }
    if ($JulyXer) { $taskRunArguments += @('--july', $JulyXer) }
    & $DotNetPath @taskRunArguments
    if ($LASTEXITCODE -ne 0) { throw "$taskVariant harness run failed." }
}

$taskCandidateExecutable = Join-Path $PSScriptRoot 'bin/candidate/Release/net8.0/ParserStreamingParity.dll'
& $DotNetPath $taskCandidateExecutable --compare (Join-Path $taskArtifacts "native-baseline$EvidenceSuffix/report.json") `
    --against (Join-Path $taskArtifacts "native-candidate$EvidenceSuffix/report.json") --output (Join-Path $taskArtifacts "native-parity$EvidenceSuffix.json")
if ($LASTEXITCODE -ne 0) { throw 'Frozen-versus-candidate streaming parity failed; inspect native-parity.json.' }
