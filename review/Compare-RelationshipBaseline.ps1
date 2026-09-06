param(
    [Parameter(Mandatory)][ValidateSet('Capture','Compare')][string]$Mode,
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string[]]$Paths,
    [Parameter(Mandatory)][string]$CoreAssemblyPath,
    [Parameter(Mandatory)][string]$BaselineHashes,
    [Parameter(Mandatory)][string]$BaselineRows,
    [string]$DeltaOutput
)

# Run each mode in a fresh PowerShell process. Capture accepts a prior assembly
# only if its complete table 06 bytes match the independently captured baseline.
# Writes diagnostic JSON only, never normal export files or source XER changes.
$ErrorActionPreference = 'Stop'
$cancel = [Threading.CancellationToken]::None
$assemblyBytes = [IO.File]::ReadAllBytes((Resolve-Path -LiteralPath $CoreAssemblyPath).Path)
$assembly = [Reflection.Assembly]::Load($assemblyBytes)
if ([XerToCsvConverter.XerTransformer].Assembly.ManifestModule.ModuleVersionId -ne $assembly.ManifestModule.ModuleVersionId) {
    throw 'A different Core build is already loaded. Run in a fresh PowerShell process.'
}
function Hash-Bytes([byte[]]$Bytes) { [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($Bytes)) }
function Assert-Equal($Actual, $Expected, [string]$Message) { if ($Actual -cne $Expected) { throw $Message } }
function Assert-SourceHashes {
    for ($index = 0; $index -lt $inputPaths.Count; $index++) {
        Assert-Equal (Get-FileHash -LiteralPath $inputPaths[$index]).Hash $baseline.InputSha256[$index] 'Original source bytes changed during comparison.'
    }
}
function Write-NewDiagnostic([string]$Path, $Value) {
    $stream = [IO.File]::Open([IO.Path]::GetFullPath($Path), [IO.FileMode]::CreateNew, [IO.FileAccess]::Write)
    try {
        $bytes = [Text.Encoding]::UTF8.GetBytes(($Value | ConvertTo-Json -Depth 8))
        $stream.Write($bytes, 0, $bytes.Length)
    } finally { $stream.Dispose() }
}
$baseline = Get-Content -LiteralPath $BaselineHashes -Raw | ConvertFrom-Json
Assert-Equal $Paths.Count $baseline.InputSha256.Count 'Baseline input count differs.'
$inputPaths = [Collections.Generic.List[string]]::new()
for ($i = 0; $i -lt $Paths.Count; $i++) {
    $path = (Resolve-Path -LiteralPath $Paths[$i]).Path
    Assert-Equal (Get-FileHash -LiteralPath $path).Hash $baseline.InputSha256[$i] 'Baseline input content or order differs.'
    $inputPaths.Add($path)
}
$service = [XerToCsvConverter.ProcessingService]::new()
$store = $service.ParseMultipleXerFilesAsync($inputPaths, $null, $cancel).GetAwaiter().GetResult()
$names = [Collections.Generic.List[string]]::new(); $names.Add('06_XER_PREDECESSOR')
$export = $service.ExportTablesToMemoryWithDiagnosticsAsync($store, $names, $null, $cancel).GetAwaiter().GetResult()
$csvBytes = $export.Files['06_XER_PREDECESSOR']
$csvHash = Hash-Bytes $csvBytes
$rows = @([Text.Encoding]::UTF8.GetString($csvBytes).TrimStart([char]0xFEFF) | ConvertFrom-Csv)
$assemblyHash = Hash-Bytes $assemblyBytes
if ($Mode -eq 'Capture') {
    Assert-Equal $csvHash $baseline.Hashes.'06_XER_PREDECESSOR' 'Prior assembly output does not match the captured pre-change table 06 hash.'
    $diagnostic = [ordered]@{
        RecoveredAssemblySha256 = $assemblyHash
        OriginallyCapturedAssemblySha256 = $baseline.AssemblySha256
        VerifiedFullCsvSha256 = $csvHash
        InputSha256 = $baseline.InputSha256
        Rows = @($rows | Select-Object FileName, task_pred_id, free_float)
    }
    Assert-SourceHashes
    Write-NewDiagnostic $BaselineRows $diagnostic
    [pscustomobject]@{ Mode = $Mode; Rows = $rows.Count; NumericRows = @($rows | Where-Object { $_.free_float -ne '' }).Count; RecoveredAssemblySha256 = $assemblyHash; VerifiedFullCsvSha256 = $csvHash; DiagnosticPath = [IO.Path]::GetFullPath($BaselineRows) } | ConvertTo-Json
    return
}
if (-not $DeltaOutput) { throw 'Compare requires a new explicit DeltaOutput diagnostic JSON path.' }
$old = Get-Content -LiteralPath $BaselineRows -Raw | ConvertFrom-Json
Assert-Equal $old.VerifiedFullCsvSha256 $baseline.Hashes.'06_XER_PREDECESSOR' 'Recovered baseline CSV hash differs from the original capture.'
Assert-Equal ($old.InputSha256 -join ',') ($baseline.InputSha256 -join ',') 'Recovered baseline source content/order differs.'
Assert-Equal $rows.Count $old.Rows.Count 'Relationship row count changed.'
$keptColumns = @($rows[0].PSObject.Properties.Name | Where-Object { $_ -cne 'free_float' })
$canonical = ($rows | Select-Object -Property $keptColumns | ConvertTo-Csv -NoTypeInformation) -join "`n"
Assert-Equal (Hash-Bytes ([Text.Encoding]::UTF8.GetBytes($canonical))) $baseline.Hashes.'06_WITHOUT_FREE_FLOAT' 'A non-float table 06 cell changed.'
$assessments = ([XerToCsvConverter.XerTransformer]::new($store)).AssessRelationships($cancel)
Assert-Equal $assessments.Count $rows.Count 'Assessment row count differs.'
$deltas = [Collections.Generic.List[object]]::new()
$oldNumeric = 0; $unchangedNumeric = 0
for ($i = 0; $i -lt $rows.Count; $i++) {
    $before = $old.Rows[$i]; $after = $rows[$i]; $assessment = $assessments[$i]
    Assert-Equal $after.FileName $before.FileName 'Source filename changed at a relationship row.'
    Assert-Equal $after.task_pred_id $before.task_pred_id 'Relationship order or identity changed.'
    Assert-Equal $assessment.RelationshipId $after.task_pred_id 'Assessment identity differs from table 06.'
    Assert-Equal $assessment.FormattedDays $after.free_float 'Assessment allowance differs from table 06.'
    if ($before.free_float -ne '') {
        $oldNumeric++
        if ($before.free_float -ceq $after.free_float) { $unchangedNumeric++ }
    }
    if ($before.free_float -cne $after.free_float) {
        $deltas.Add([pscustomobject]@{ SourceNamespace = $assessment.SourceNamespace; SourceRowNumber = $assessment.SourceRowNumber; FileName = $after.FileName; RelationshipId = $after.task_pred_id; OldFreeFloat = $before.free_float; NewFreeFloat = $after.free_float; Classification = $assessment.Classification.ToString(); ReasonCode = $assessment.ReasonCode })
    }
}
$summary = [ordered]@{
    Mode = $Mode; Rows = $rows.Count; OriginalNumericRows = $oldNumeric; UnchangedOriginalNumericRows = $unchangedNumeric
    ChangedRows = $deltas.Count; CurrentAssemblySha256 = $assemblyHash
    RecoveredAssemblySha256 = $old.RecoveredAssemblySha256; VerifiedOldCsvSha256 = $old.VerifiedFullCsvSha256
    CurrentCsvSha256 = $csvHash
    ChangedReasonCounts = @($deltas | Group-Object Classification, ReasonCode | ForEach-Object { [pscustomobject]@{ Classification = $_.Group[0].Classification; ReasonCode = $_.Group[0].ReasonCode; Count = $_.Count } })
    Deltas = $deltas.ToArray()
}
Assert-SourceHashes
Write-NewDiagnostic $DeltaOutput $summary
Assert-Equal $unchangedNumeric $oldNumeric 'At least one previously numeric allowance changed; inspect the diagnostic deltas.'
$summary.Remove('Deltas')
$summary.DiagnosticPath = [IO.Path]::GetFullPath($DeltaOutput)
$summary | ConvertTo-Json -Depth 5
