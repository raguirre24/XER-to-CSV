[CmdletBinding()]
param(
    [string]$RepositoryRoot = (Join-Path $PSScriptRoot '../..')
)

# The primary browser baseline must remain unchanged. This separate package is
# for phase attribution only; observer overhead is never credited as a speedup.
$taskRepository = (Resolve-Path -LiteralPath $RepositoryRoot).Path
$taskArtifacts = Join-Path $taskRepository 'artifacts/parser-performance-20260930'
$taskFrozen = Join-Path $taskArtifacts 'baseline'
$taskOriginalWeb = Join-Path $taskFrozen 'XerToCsvConverter.Web'
$taskObserverWeb = Join-Path $taskFrozen 'XerToCsvConverter.Web.Telemetry'
$taskObserverCore = Join-Path $taskFrozen 'XerToCsvConverter.Core.Telemetry'
if (-not (Test-Path -LiteralPath (Join-Path $taskObserverCore 'phase-overlay-evidence.json'))) {
    throw 'Apply-FrozenPhaseTelemetry.ps1 must first create the separate Core.Telemetry clone.'
}
if (Test-Path -LiteralPath $taskObserverWeb) { throw "Observer Web clone already exists: $taskObserverWeb" }
Copy-Item -LiteralPath $taskOriginalWeb -Destination $taskObserverWeb -Recurse

function Replace-Once([string]$Text, [string]$Old, [string]$New) {
    if ($Text.Split($Old, [StringSplitOptions]::None).Length -ne 2) {
        throw "Expected exactly one frozen hook: $Old"
    }
    return $Text.Replace($Old, $New)
}
function Write-Utf8([string]$Path, [string]$Content) {
    [IO.File]::WriteAllText($Path, $Content, [Text.UTF8Encoding]::new($false))
}

$taskHelper = Join-Path $taskObserverCore 'ParserOperationTelemetry.cs'
$taskHelperText = [IO.File]::ReadAllText($taskHelper)
$taskHelperText = Replace-Once $taskHelperText '[assembly: InternalsVisibleTo("ParserStreamingParity")]' @'
[assembly: InternalsVisibleTo("ParserStreamingParity")]
[assembly: InternalsVisibleTo("XerToCsvConverter.Web")]
'@
Write-Utf8 $taskHelper $taskHelperText

$taskProject = Join-Path $taskObserverWeb 'XerToCsvConverter.Web.csproj'
$taskProjectText = [IO.File]::ReadAllText($taskProject)
$taskProjectText = Replace-Once $taskProjectText '..\XerToCsvConverter.Core\XerToCsvConverter.Core.csproj' '..\XerToCsvConverter.Core.Telemetry\XerToCsvConverter.Core.csproj'
Write-Utf8 $taskProject $taskProjectText

$taskIndex = Join-Path $taskObserverWeb 'Pages/Index.razor'
$taskIndexText = [IO.File]::ReadAllText($taskIndex).Replace("`r`n", "`n")
$taskIndexText = Replace-Once $taskIndexText '    private DownloadService? _downloadService;' @'
    private DownloadService? _downloadService;
    private ParserOperationTelemetry.Operation? _phaseOperation;
    private string _phaseLabel = string.Empty;
'@
if ($taskIndexText.Split('ExportedAtUtc = DateTimeOffset.UtcNow,', [StringSplitOptions]::None).Length -ne 3) {
    throw 'Expected exactly two review export timestamp fixtures.'
}
# A fixture clock makes extracted manifest CSV parity reviewable. This clone-only
# clock is never used by either package in the primary browser speed comparison.
$taskIndexText = $taskIndexText.Replace('ExportedAtUtc = DateTimeOffset.UtcNow,',
    'ExportedAtUtc = new DateTimeOffset(2026, 9, 30, 0, 0, 0, TimeSpan.Zero),')
$taskIndexText = Replace-Once $taskIndexText '        if (batch is null) return;' @'
        if (batch is null) return;
        using var uploadPhases = ParserOperationTelemetry.BeginOperation();
'@
$taskIndexText = Replace-Once $taskIndexText @'
                await using (Stream stream = browserFile.OpenReadStream(maximumFileBytes, batch.Token))
                    await stream.ReadExactlyAsync(data, batch.Token);
                DateOnly? detectedDataDate = await ProgrammeReviewXerMetadataReader
                    .ReadSingleProjectDataDateAsync(data, batch.Token);
'@ @'
                using (ParserOperationTelemetry.Measure(ParserOperationPhase.FileReading, browserFile.Name))
                {
                    await using Stream stream = browserFile.OpenReadStream(maximumFileBytes, batch.Token);
                    await stream.ReadExactlyAsync(data, batch.Token);
                }
                DateOnly? detectedDataDate;
                using (ParserOperationTelemetry.Measure(ParserOperationPhase.Metadata, "upload data date"))
                    detectedDataDate = await ProgrammeReviewXerMetadataReader.ReadSingleProjectDataDateAsync(data, batch.Token);
'@
$taskIndexText = Replace-Once $taskIndexText '            _uploads.Complete(batch);' @'
            _uploads.Complete(batch);
            ParserOperationTimingSnapshot uploadTiming = uploadPhases.Snapshot();
            uploadPhases.Dispose();
            await RecordFrozenPhasesAsync("upload", uploadTiming);
'@
$taskIndexText = Replace-Once $taskIndexText @'
    private void BeginOperation(string message, bool resetFileStatuses = true)
    {
'@ @'
    private void BeginOperation(string message, bool resetFileStatuses = true)
    {
        _phaseOperation = ParserOperationTelemetry.BeginOperation();
        _phaseLabel = message;
'@
$taskIndexText = Replace-Once $taskIndexText @'
    private void EndOperation()
    {
'@ @'
    private void EndOperation()
    {
        if (_phaseOperation is { } phaseOperation)
        {
            ParserOperationTimingSnapshot timing = phaseOperation.Snapshot();
            phaseOperation.Dispose();
            _phaseOperation = null;
            _ = RecordFrozenPhasesAsync(_phaseLabel, timing);
        }
'@
$taskIndexText = Replace-Once $taskIndexText '    private void SetOperationError(string message)' @'
    private async Task RecordFrozenPhasesAsync(string label, ParserOperationTimingSnapshot timing)
    {
        var metrics = new Dictionary<string, double>(StringComparer.Ordinal)
        {
            ["operationElapsedMs"] = timing.ElapsedMs,
            ["hasOverlaps"] = timing.HasOverlaps ? 1 : 0
        };
        foreach (ParserOperationPhase phase in Enum.GetValues<ParserOperationPhase>())
            metrics["exclusive." + phase] = 0;
        foreach (ParserPhaseTiming phase in timing.Phases)
        {
            metrics["exclusive." + phase.Phase] = phase.ExclusiveMs;
            metrics["inclusive." + phase.Phase] = phase.InclusiveMs;
            metrics["scopes." + phase.Phase] = phase.Scopes;
            metrics["bufferedCalls." + phase.Phase] = phase.BufferedCalls;
        }
        foreach (var group in timing.Scopes.Where(scope => scope.Detail is not null)
                     .GroupBy(scope => scope.Phase + ":" + scope.Detail, StringComparer.Ordinal))
        {
            metrics["detailExclusive." + group.Key] = group.Sum(scope => scope.ExclusiveMs);
            metrics["detailInclusive." + group.Key] = group.Sum(scope => scope.InclusiveMs);
        }
        try { await JS.InvokeVoidAsync("recordFrozenParserPhases", label, metrics); }
        catch (JSException) { }
        catch (JSDisconnectedException) { }
    }

    private void SetOperationError(string message)
'@
Write-Utf8 $taskIndex $taskIndexText

$taskHtml = Join-Path $taskObserverWeb 'wwwroot/index.html'
$taskHtmlText = [IO.File]::ReadAllText($taskHtml)
$taskHtmlText = Replace-Once $taskHtmlText '</body>' @'
    <script>
        // Observer evidence is kept separate from the uninstrumented timed baseline.
        window.__frozenParserPhaseRuns = [];
        window.recordFrozenParserPhases = (label, metrics) => {
            window.__frozenParserPhaseRuns.push({ label, metrics, recordedAt: performance.now() });
        };
    </script>
</body>
'@
Write-Utf8 $taskHtml $taskHtmlText

$taskEvidence = [ordered]@{
    purpose = 'Observer-only frozen browser; excluded from primary speed comparisons'
    frozenCommit = '90b98d3'
    frozenArchiveSha256 = (Get-FileHash -LiteralPath (Join-Path $taskArtifacts 'frozen-current.zip') -Algorithm SHA256).Hash
    originalWebProjectSha256 = (Get-FileHash -LiteralPath (Join-Path $taskOriginalWeb 'XerToCsvConverter.Web.csproj') -Algorithm SHA256).Hash
    originalWebIndexSha256 = (Get-FileHash -LiteralPath (Join-Path $taskOriginalWeb 'Pages/Index.razor') -Algorithm SHA256).Hash
    observerWebProjectSha256 = (Get-FileHash -LiteralPath $taskProject -Algorithm SHA256).Hash
    observerWebIndexSha256 = (Get-FileHash -LiteralPath $taskIndex -Algorithm SHA256).Hash
    observerWebHtmlSha256 = (Get-FileHash -LiteralPath $taskHtml -Algorithm SHA256).Hash
    observerHelperSha256 = (Get-FileHash -LiteralPath $taskHelper -Algorithm SHA256).Hash
    coreOverlayEvidence = 'baseline/XerToCsvConverter.Core.Telemetry/phase-overlay-evidence.json'
    port = 8768
    reviewFixtureExportedAtUtc = '2026-09-30T00:00:00Z'
}
Write-Utf8 (Join-Path $taskObserverWeb 'observer-web-evidence.json') ($taskEvidence | ConvertTo-Json -Depth 5)
$taskEvidence | ConvertTo-Json -Depth 5
