[CmdletBinding()]
param(
    [string]$RepositoryRoot = (Join-Path $PSScriptRoot '../..'),
    [string]$DotNetPath,
    [hashtable]$StageRefs = @{},
    [string]$ResumeEvidence,
    [string]$FixtureRoot,
    [switch]$Run,
    [switch]$NativeParity,
    [switch]$FixturesOnly,
    [string]$AugustXer,
    [string]$JulyXer,
    [string]$BaselineReport,
    [ValidateRange(1, 10)][int]$Iterations = 2
)

# Default invocation only resolves commits and checks tracked dependencies. Use -Run
# after browser measurements finish. Each stage is built in its own fresh archive;
# nothing is checked out, staged, deleted or changed in the working tree.
# -NativeParity also runs the current review harness against each archived Core,
# reading schedules from FixtureRoot rather than copying large/private XER inputs.
# Historical Web surface tests include three frozen source-text failures that were
# replaced in D. This script runs strict Core/CLI checks; final D surface behaviour
# is verified separately, rather than relaxing or misreporting historical failures.
# -ResumeEvidence reuses only passing stages with an identical full commit SHA,
# rechecking their original TRX, fixture locator and requested native parity files.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$taskRepository = (Resolve-Path -LiteralPath $RepositoryRoot).Path
if (-not $DotNetPath) { $DotNetPath = Join-Path $taskRepository 'local-dotnet-sdk/dotnet.exe' }
$taskDotNet = (Resolve-Path -LiteralPath $DotNetPath).Path
if (-not $FixtureRoot) { $FixtureRoot = $taskRepository }
$taskFixtureRoot = (Resolve-Path -LiteralPath $FixtureRoot).Path
$taskArtifacts = Join-Path $taskRepository 'artifacts/parser-performance-20260930'
if (-not $BaselineReport) { $BaselineReport = Join-Path $taskArtifacts 'native-baseline/report.json' }
$taskPin = Get-Content -LiteralPath (Join-Path $taskRepository 'global.json') -Raw | ConvertFrom-Json
$taskSdkVersion = [string]$taskPin.sdk.version
$taskPreviousEvidence = $null
if ($ResumeEvidence) { $taskPreviousEvidence = Get-Content -LiteralPath $ResumeEvidence -Raw | ConvertFrom-Json }
if ($taskPin.sdk.rollForward -ne 'disable') { throw 'The source SDK pin must disable roll-forward.' }
if ($NativeParity -and -not $Run) { throw '-NativeParity requires -Run; the default mode does not execute code.' }
if ($NativeParity -and -not (Test-Path -LiteralPath $BaselineReport -PathType Leaf)) {
    throw "Native baseline report is missing: $BaselineReport"
}

$taskStages = @(
    @{ Id = 'A'; Title = 'fix: harden streaming ZIP results, hashing and progress' },
    @{ Id = 'C'; Title = 'perf: index working calendars and share export calendar lookups' },
    @{ Id = 'B'; Title = 'perf: reduce review conversions, row lookups and CSV work' },
    @{ Id = 'E'; Title = 'perf: add bounded parsed-source sessions and incremental exports' }
)
$taskArchivePaths = @(
    'XerToCsvConverter.Core', 'XerToCsvConverter.Core.Tests',
    'XerToCsvConverter.RelationshipAudit.Cli', 'review/fixtures'
)
$taskProjects = [ordered]@{
    Core = 'XerToCsvConverter.Core.Tests/XerToCsvConverter.Core.Tests.csproj'
    RelationshipCli = 'XerToCsvConverter.RelationshipAudit.Cli/Tests/XerToCsvConverter.RelationshipAudit.Cli.Tests.csproj'
}
$taskRequiredPaths = @($taskProjects.Values) + @(
    'XerToCsvConverter.Core/XerToCsvConverter.Core.csproj',
    'XerToCsvConverter.RelationshipAudit.Cli/XerToCsvConverter.RelationshipAudit.Cli.csproj',
    'review/fixtures/J5001_C_BL01_2026-01-31.xer',
    'review/fixtures/J5001_C_BL01_Retained_2026-01-31.xer'
)

function Invoke-TaskGit {
    param([string[]]$Arguments)
    $taskOutput = @(& git -C $taskRepository @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "git $($Arguments[0]) failed: $($taskOutput -join [Environment]::NewLine)" }
    return $taskOutput
}

function Assert-TaskChildPath {
    param([string]$Path, [string]$Parent)
    $taskFullPath = [IO.Path]::GetFullPath($Path)
    $taskParentPrefix = [IO.Path]::GetFullPath($Parent).TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar
    if (-not $taskFullPath.StartsWith($taskParentPrefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Evidence path is outside the intended directory: $taskFullPath"
    }
    return $taskFullPath
}

$taskHistory = @(Invoke-TaskGit @('log', '--format=%H%x09%s'))
$taskPlan = foreach ($taskStage in $taskStages) {
    if ($StageRefs.ContainsKey($taskStage.Id)) {
        $taskRef = [string]$StageRefs[$taskStage.Id]
    }
    else {
        $taskFixupTitle = 'fixup! ' + $taskStage.Title
        if ($taskHistory | Where-Object { ($_ -split "`t", 2)[1] -eq $taskFixupTitle }) {
            throw "Stage $($taskStage.Id) has an unsquashed fixup; squash it or supply an explicit -StageRefs override."
        }
        $taskMatches = @($taskHistory | Where-Object { ($_ -split "`t", 2)[1] -eq $taskStage.Title })
        if ($taskMatches.Count -ne 1) { throw "Expected one commit titled '$($taskStage.Title)', found $($taskMatches.Count)." }
        $taskRef = ($taskMatches[0] -split "`t", 2)[0]
    }
    $taskCommit = [string](@(Invoke-TaskGit @('rev-parse', '--verify', ($taskRef + '^{commit}')))[0])
    foreach ($taskRequired in $taskRequiredPaths) {
        $null = Invoke-TaskGit @('cat-file', '-e', ('{0}:{1}' -f $taskCommit, $taskRequired))
    }
    # A's stream result contract must already contain the manifest-aware file count.
    # This catches the historical A/C compile gap before executing a build.
    $taskMetadata = Invoke-TaskGit @('show', ('{0}:XerToCsvConverter.Core/ProgrammeReview/ProgrammeReviewMetadata.cs' -f $taskCommit))
    if (-not ($taskMetadata -match 'public int FileCount => CsvSha256ByFile.Count \+ 1;')) {
        throw "Stage $($taskStage.Id) is missing the streaming FileCount fixup."
    }
    [pscustomobject]@{ Stage = $taskStage.Id; Commit = $taskCommit; Title = $taskStage.Title }
}
if (-not $Run) {
    [pscustomobject]@{
        Mode = 'PlanOnly'; PinnedSdk = $taskSdkVersion; DotNetPath = $taskDotNet
        Stages = @($taskPlan); Projects = @($taskProjects.Values)
        ArchivePaths = $taskArchivePaths; FixtureRoot = $taskFixtureRoot
        HistoricalSurfaceTests = 'Not run: three frozen source-text failures were replaced by behaviour tests in D. Final D surface evidence is separate.'
        Note = 'No archives, builds, tests or parity comparisons executed. Supply -Run only after browser timings finish.'
    } | ConvertTo-Json -Depth 6
    return
}

$taskActualSdk = (& $taskDotNet --version).Trim()
if ($LASTEXITCODE -ne 0 -or $taskActualSdk -ne $taskSdkVersion) {
    throw "Pinned SDK mismatch: expected $taskSdkVersion, received $taskActualSdk."
}
$taskRunRoot = Assert-TaskChildPath (Join-Path $taskArtifacts ('stage-validation/' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ') + '-' + [Guid]::NewGuid().ToString('N').Substring(0, 8))) $taskArtifacts
$null = New-Item -ItemType Directory -Path $taskRunRoot
$taskSummaryPath = Join-Path $taskRunRoot 'stage-validation.json'
$taskSummary = [ordered]@{
    Status = 'running'; StartedUtc = [DateTime]::UtcNow.ToString('O')
    PinnedSdk = $taskActualSdk; DotNetPath = $taskDotNet
    FixtureRoot = $taskFixtureRoot; NativeParity = [bool]$NativeParity
    HistoricalSurfaceTests = 'Not run: three frozen source-text failures were replaced by behaviour tests in D. Final D surface evidence is separate.'
    Stages = [Collections.Generic.List[object]]::new()
}

function Save-TaskSummary {
    $taskSummary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $taskSummaryPath -Encoding UTF8
}

function Invoke-TaskDotNet {
    param([string[]]$Arguments, [string]$WorkingDirectory, [string]$LogPath)
    Push-Location -LiteralPath $WorkingDirectory
    try {
        & $taskDotNet @Arguments 2>&1 | Tee-Object -FilePath $LogPath | ForEach-Object { Write-Host $_ }
        if ($LASTEXITCODE -ne 0) { throw "dotnet $($Arguments[0]) failed; inspect $LogPath" }
    }
    finally { Pop-Location }
}

function Read-TaskTestCounters {
    param([string]$TrxPath)
    [xml]$taskTrx = Get-Content -LiteralPath $TrxPath -Raw
    $taskCounters = $taskTrx.SelectSingleNode('//*[local-name()="Counters"]')
    if ($null -eq $taskCounters -or [int]$taskCounters.executed -le 0 -or [int]$taskCounters.failed -ne 0 -or [int]$taskCounters.error -ne 0) {
        throw "Missing/empty/failing TRX counters: $TrxPath"
    }
    return [pscustomobject]@{
        Passed = [int]$taskCounters.passed; Failed = [int]$taskCounters.failed
        Executed = [int]$taskCounters.executed; Total = [int]$taskCounters.total
        SkippedOrNotExecuted = [Math]::Max([int]$taskCounters.notExecuted, [int]$taskCounters.total - [int]$taskCounters.executed)
    }
}

function Import-TaskPassedStage {
    param($PassedStage)
    if ($PassedStage.FixtureLocatorIsolation.Passed -ne $true -or
        $PassedStage.FixtureLocatorIsolation.FixedRelativeRoot -ne $PassedStage.FixtureLocatorIsolation.SourceRoot) {
        throw "Resume stage $($PassedStage.Stage) has invalid fixture locator evidence."
    }
    foreach ($taskFixture in $PassedStage.FixtureLocatorIsolation.AncestorLookup) {
        $null = Assert-TaskChildPath $taskFixture.ResolvedPath $PassedStage.FixtureLocatorIsolation.SourceRoot
        if ((Get-FileHash -LiteralPath $taskFixture.ResolvedPath -Algorithm SHA256).Hash -ne $taskFixture.Sha256) {
            throw "Resume stage $($PassedStage.Stage) fixture bytes changed."
        }
    }
    foreach ($taskProject in $taskProjects.Values) {
        $taskMatchingTests = @($PassedStage.Tests | Where-Object { $_.Project -eq $taskProject })
        if ($taskMatchingTests.Count -ne 1) { throw "Resume stage $($PassedStage.Stage) lacks exact project evidence: $taskProject" }
        $taskTest = $taskMatchingTests[0]
        $taskCounts = Read-TaskTestCounters $taskTest.Trx
        foreach ($taskProperty in $taskCounts.PSObject.Properties) {
            $taskTest | Add-Member -NotePropertyName $taskProperty.Name -NotePropertyValue $taskProperty.Value -Force
        }
    }
    if ($NativeParity) {
        $taskNative = Get-Content -LiteralPath $PassedStage.NativeParityEvidence -Raw | ConvertFrom-Json
        $taskNativeReportPath = Join-Path (Split-Path -Parent $PassedStage.NativeParityEvidence) 'native/report.json'
        $taskNativeReport = Get-Content -LiteralPath $taskNativeReportPath -Raw | ConvertFrom-Json
        $taskExpectedCases = if ($FixturesOnly) { 24 } else { 32 }
        if ($taskNative.Cases -ne $taskExpectedCases -or $taskNative.Failures -ne 0 -or
            @($taskNative.Results | Where-Object { $_.Passed -ne $true }).Count -ne 0 -or
            $taskNativeReport.Iterations -lt $Iterations -or
            @($taskNativeReport.Cases).Count -ne $taskExpectedCases * $taskNativeReport.Iterations) {
            throw "Resume stage $($PassedStage.Stage) has invalid native parity evidence."
        }
        $PassedStage | Add-Member -NotePropertyName NativeIterations -NotePropertyValue $taskNativeReport.Iterations -Force
    }
    $PassedStage | Add-Member -NotePropertyName ReusedFromEvidence -NotePropertyValue ([IO.Path]::GetFullPath($ResumeEvidence)) -Force
    return $PassedStage
}

try {
    Save-TaskSummary
    foreach ($taskStage in $taskPlan) {
        if ($null -ne $taskPreviousEvidence) {
            $taskPreviousMatches = @($taskPreviousEvidence.Stages | Where-Object {
                $_.Stage -eq $taskStage.Stage -and $_.Commit -eq $taskStage.Commit -and $_.Status -eq 'passed'
            })
            if ($taskPreviousMatches.Count -gt 1) { throw "Duplicate resume evidence for stage $($taskStage.Stage)." }
            if ($taskPreviousMatches.Count -eq 1) {
                $taskSummary.Stages.Add((Import-TaskPassedStage $taskPreviousMatches[0]))
                Save-TaskSummary
                Write-Host "Stage $($taskStage.Stage): reused exact passing commit/TRX/locator/native evidence."
                continue
            }
        }
        $taskStageRoot = Assert-TaskChildPath (Join-Path $taskRunRoot ($taskStage.Stage + '-' + $taskStage.Commit.Substring(0, 7))) $taskRunRoot
        $null = New-Item -ItemType Directory -Path $taskStageRoot
        $taskArchive = Join-Path $taskStageRoot 'sources.zip'
        $taskSources = Join-Path $taskStageRoot 'source'
        $null = Invoke-TaskGit (@('archive', '--format=zip', ('--output=' + $taskArchive), $taskStage.Commit, '--') + $taskArchivePaths)
        Expand-Archive -LiteralPath $taskArchive -DestinationPath $taskSources
        # Historic stages predate the repository-level SDK pin. The evidence copy
        # supplies that same pin without changing any archived production source.
        $taskPin | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $taskSources 'global.json') -Encoding UTF8
        $taskStageResult = [ordered]@{
            Stage = $taskStage.Stage; Commit = $taskStage.Commit; Title = $taskStage.Title
            Status = 'running'; Tests = [Collections.Generic.List[object]]::new()
        }
        # Core's fixed relative lookup and its ancestor lookup must both resolve
        # the archived fixtures before reaching any original repository parent.
        # Core/CLI have no .git, solution or root-project marker lookup.
        $taskTestBase = Join-Path $taskSources 'XerToCsvConverter.Core.Tests/bin/Release/net8.0'
        $taskRelativeFixtureRoot = [IO.Path]::GetFullPath((Join-Path $taskTestBase '../../../..'))
        if ($taskRelativeFixtureRoot -ne [IO.Path]::GetFullPath($taskSources)) {
            throw "The fixed-relative fixture lookup escaped stage $($taskStage.Stage)."
        }
        $taskLocatorEvidence = foreach ($taskFixtureName in @('J5001_C_BL01_2026-01-31.xer', 'J5001_C_BL01_Retained_2026-01-31.xer')) {
            $taskAncestor = [IO.DirectoryInfo]::new($taskTestBase)
            while ($null -ne $taskAncestor -and -not [IO.File]::Exists((Join-Path $taskAncestor.FullName ('review/fixtures/' + $taskFixtureName)))) {
                $taskAncestor = $taskAncestor.Parent
            }
            if ($null -eq $taskAncestor -or $taskAncestor.FullName -ne [IO.Path]::GetFullPath($taskSources)) {
                throw "The ancestor fixture lookup escaped stage $($taskStage.Stage)."
            }
            $taskFixturePath = Assert-TaskChildPath (Join-Path $taskAncestor.FullName ('review/fixtures/' + $taskFixtureName)) $taskSources
            [pscustomobject]@{ Fixture = $taskFixtureName; ResolvedPath = $taskFixturePath; Sha256 = (Get-FileHash -LiteralPath $taskFixturePath -Algorithm SHA256).Hash }
        }
        $taskStageResult['FixtureLocatorIsolation'] = [pscustomobject]@{
            Passed = $true; SourceRoot = $taskSources; FixedRelativeRoot = $taskRelativeFixtureRoot
            AncestorLookup = @($taskLocatorEvidence)
        }
        $taskSummary.Stages.Add($taskStageResult)
        Save-TaskSummary
        foreach ($taskProject in $taskProjects.GetEnumerator()) {
            $taskEvidence = Join-Path $taskStageRoot ('tests/' + $taskProject.Key)
            $null = New-Item -ItemType Directory -Path $taskEvidence
            $taskLog = Join-Path $taskEvidence 'test.log'
            Invoke-TaskDotNet @('test', (Join-Path $taskSources $taskProject.Value), '--configuration', 'Release',
                '--verbosity', 'minimal', '--logger', 'trx;LogFileName=tests.trx', '--results-directory', $taskEvidence) $taskSources $taskLog
            $taskCounters = Read-TaskTestCounters (Join-Path $taskEvidence 'tests.trx')
            $taskStageResult.Tests.Add([pscustomobject]@{
                Project = $taskProject.Value; Passed = $taskCounters.Passed
                Failed = $taskCounters.Failed; Executed = $taskCounters.Executed; Total = $taskCounters.Total
                SkippedOrNotExecuted = $taskCounters.SkippedOrNotExecuted
                Trx = (Join-Path $taskEvidence 'tests.trx'); Log = $taskLog
            })
            Save-TaskSummary
        }
        if ($NativeParity) {
            $taskHarnessRoot = Join-Path $taskSources 'review/parser-performance-20260930'
            $null = New-Item -ItemType Directory -Path $taskHarnessRoot -Force
            foreach ($taskHarnessFile in @('ParserParity.csproj', 'Program.cs')) {
                Copy-Item -LiteralPath (Join-Path $PSScriptRoot $taskHarnessFile) -Destination (Join-Path $taskHarnessRoot $taskHarnessFile)
            }
            $taskHarnessProject = Join-Path $taskHarnessRoot 'ParserParity.csproj'
            Invoke-TaskDotNet @('build', $taskHarnessProject, '--configuration', 'Release', '--verbosity', 'minimal',
                '-p:HarnessVariant=stage', '-p:EnablePhaseTelemetry=false', '-p:EnableCacheMeasurements=false') $taskSources (Join-Path $taskStageRoot 'parity-build.log')
            $taskHarnessDll = Join-Path $taskHarnessRoot 'bin/stage/Release/net8.0/ParserStreamingParity.dll'
            $taskParityRoot = Join-Path $taskStageRoot 'native'
            $taskRunArguments = @($taskHarnessDll, '--repo', $taskFixtureRoot, '--label', ('stage-' + $taskStage.Stage),
                '--output', $taskParityRoot, '--iterations', $Iterations.ToString())
            if ($FixturesOnly) { $taskRunArguments += @('--fixtures-only', 'true') }
            if ($AugustXer) { $taskRunArguments += @('--august', $AugustXer) }
            if ($JulyXer) { $taskRunArguments += @('--july', $JulyXer) }
            Invoke-TaskDotNet $taskRunArguments $taskSources (Join-Path $taskStageRoot 'parity-run.log')
            $taskComparison = Join-Path $taskStageRoot 'native-parity.json'
            $taskComparisonBaseline = $BaselineReport
            if ($FixturesOnly) {
                $taskFixtureBaseline = Get-Content -LiteralPath $BaselineReport -Raw | ConvertFrom-Json
                $taskFixtureBaseline.Cases = @($taskFixtureBaseline.Cases | Where-Object { $_.Name.StartsWith('fixture-', [StringComparison]::Ordinal) })
                if ($taskFixtureBaseline.Cases.Count -eq 0) { throw 'The baseline report contains no fixture cases.' }
                $taskComparisonBaseline = Join-Path $taskStageRoot 'baseline-fixtures.json'
                $taskFixtureBaseline | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath $taskComparisonBaseline -Encoding UTF8
            }
            Invoke-TaskDotNet @($taskHarnessDll, '--compare', $taskComparisonBaseline, '--against', (Join-Path $taskParityRoot 'report.json'),
                '--output', $taskComparison) $taskSources (Join-Path $taskStageRoot 'parity-compare.log')
            $taskStageResult['NativeParityEvidence'] = $taskComparison
            $taskStageResult['NativeIterations'] = $Iterations
        }
        $taskStageResult.Status = 'passed'
        Save-TaskSummary
    }
    $taskSummary.Status = 'passed'
}
catch {
    $taskSummary.Status = 'failed'
    $taskSummary['Error'] = $_.Exception.Message
    throw
}
finally {
    $taskSummary['FinishedUtc'] = [DateTime]::UtcNow.ToString('O')
    Save-TaskSummary
    Write-Host "Stage evidence: $taskSummaryPath"
}
