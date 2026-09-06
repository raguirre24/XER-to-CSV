param([string]$RepositoryRoot = (Split-Path -Parent $PSScriptRoot))

$ErrorActionPreference = 'Stop'
$skillRoot = Join-Path $RepositoryRoot 'skills/p6-numbered-xer-reporting'
$assemblyPath = Join-Path $RepositoryRoot 'XerToCsvConverter.Core/bin/Debug/net8.0/XerToCsvConverter.Core.dll'
if (-not (Test-Path -LiteralPath $assemblyPath)) { throw 'Build Core before checking the documented contracts.' }
$null = [Reflection.Assembly]::Load([IO.File]::ReadAllBytes($assemblyPath))

$contractText = Get-Content -LiteralPath (Join-Path $skillRoot 'references/profile-contracts.md') -Raw
$sharedMatch = [regex]::Match($contractText, '(?s)## Eight shared review table headers(.*?)## Manifests')
if (-not $sharedMatch.Success) { throw 'Shared review contract section is missing.' }
$sharedHeaders = @{}
foreach ($match in [regex]::Matches($sharedMatch.Groups[1].Value, '(?m)^([0-9]{2}_XER_[A-Z_]+):\r?\n([^\r\n]+)')) {
    $sharedHeaders[$match.Groups[1].Value] = $match.Groups[2].Value
}
if ($sharedHeaders.Count -ne 8) { throw 'Expected exactly eight shared review table headers.' }

$profiles = @(
    @{ Name = 'Programme Review'; Pattern = '(?s)## Exact Programme Review headers(.*?)## Exact Tender Review headers'; Tables = [XerToCsvConverter.ProgrammeReview.ProgrammeReviewContract]::Tables },
    @{ Name = 'Tender Review'; Pattern = '(?s)## Exact Tender Review headers(.*?)## Eight shared review table headers'; Tables = [XerToCsvConverter.TenderReview.TenderReviewContract]::Tables }
)
$checkedContracts = 0
foreach ($profile in $profiles) {
    $section = [regex]::Match($contractText, $profile.Pattern).Groups[1].Value
    $task = [regex]::Match($section, '(?m)^status_code,[^\r\n]+').Value
    $project = [regex]::Match($section, '(?m)^last_recalc_date,[^\r\n]+').Value
    foreach ($table in $profile.Tables) {
        $documented = switch ($table.TableName) {
            '01_XER_TASK' { $task }
            '02_XER_PROJECT' { $project }
            default { $sharedHeaders[$table.TableName] }
        }
        $actual = ($table.Columns | ForEach-Object Name) -join ','
        if ($documented -cne $actual) { throw "Documented header mismatch: $($profile.Name) $($table.TableName)" }
        $checkedContracts++
    }
}

$dictionaryText = Get-Content -LiteralPath (Join-Path $skillRoot 'references/table-dictionary.md') -Raw
$diagnosticHeader = [regex]::Match($dictionaryText, '(?m)^diagnostic_schema_version,[^\r\n]+').Value
$actualDiagnosticHeader = (@([XerToCsvConverter.XerDataQuality]::Columns) + 'FileName') -join ','
if ($diagnosticHeader -cne $actualDiagnosticHeader) { throw 'Documented diagnostic companion header mismatch.' }
$legacyDiagnosticColumns = 'diagnostic_schema_version,severity,issue_code,table_name,source_namespace,source_row_number,proj_id_key,task_id_key,rsrc_id_key,taskrsrc_id_key,taskrsrc_id,task_code,rsrc_name,rsrc_type,unit,status_code,act_start_date,act_end_date,project_data_date,act_reg_qty,act_ot_qty,unallocated_actual_quantity,message'.Split(',')
$appendedDiagnosticColumns = 'allocation_portion,restart_date,reend_date,remain_qty,curv_id,remain_crv,unallocated_remaining_quantity'.Split(',')
$generalEvidenceColumns = 'source_table,column_name,raw_value,raw_row_json'.Split(',')
if ([XerToCsvConverter.XerDataQuality]::SchemaVersion -cne '1.2' -or
    $actualDiagnosticHeader -cne (($legacyDiagnosticColumns + $appendedDiagnosticColumns + $generalEvidenceColumns + 'FileName') -join ',')) {
    throw 'Diagnostic 1.2 must preserve all former 30 data columns and append the four general source-evidence fields.'
}

$linkCount = 0
$files = Get-ChildItem -LiteralPath $skillRoot -Recurse -File
foreach ($file in $files) {
    $content = Get-Content -LiteralPath $file.FullName -Raw
    if ($content -match '(?m)[ \t]+\r?$') { throw "Trailing whitespace in $($file.FullName)" }
    if ($file.Extension -ne '.md') { continue }
    foreach ($match in [regex]::Matches($content, '\]\(([^)]+)\)')) {
        $target = $match.Groups[1].Value
        if ($target -match '^(https?://|#)') { continue }
        $target = ($target -split '#', 2)[0]
        if (-not (Test-Path -LiteralPath (Join-Path $file.DirectoryName $target))) {
            throw "Broken relative skill link in $($file.Name): $target"
        }
        $linkCount++
    }
}
Write-Output "Passed: $checkedContracts exact numbered review table contracts, additive 35-column diagnostic 1.2 companion header, $linkCount relative reference links, and skill whitespace."
