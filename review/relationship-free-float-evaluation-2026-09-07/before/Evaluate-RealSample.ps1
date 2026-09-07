param(
    [string]$InputPath = (Join-Path $PSScriptRoot '../../XER Sample/2608-EBA_PAA_8.0 (draft).xer'),
    [string]$CoreAssemblyPath = (Join-Path $PSScriptRoot '../../XerToCsvConverter.Core/bin/Debug/net8.0/XerToCsvConverter.Core.dll')
)
$ErrorActionPreference = 'Stop'
$source = (Resolve-Path -LiteralPath $InputPath).Path
$assembly = (Resolve-Path -LiteralPath $CoreAssemblyPath).Path
$beforeHash = (Get-FileHash -LiteralPath $source).Hash
$null = [Reflection.Assembly]::Load([IO.File]::ReadAllBytes($assembly))
$cancel = [Threading.CancellationToken]::None
$inputs = [Collections.Generic.List[string]]::new()
$inputs.Add($source)
$service = [XerToCsvConverter.ProcessingService]::new()
$store = $service.ParseMultipleXerFilesAsync($inputs, $null, $cancel).GetAwaiter().GetResult()
$transformer = [XerToCsvConverter.XerTransformer]::new($store)
$assessments = $transformer.AssessRelationships($cancel)
$selection = [Collections.Generic.List[string]]::new()
$selection.Add('06_XER_PREDECESSOR')
$export = $service.ExportTablesToMemoryWithDiagnosticsAsync($store, $selection, $null, $cancel).GetAwaiter().GetResult()
$table06 = @([Text.Encoding]::UTF8.GetString($export.Files['06_XER_PREDECESSOR']).TrimStart([char]0xFEFF) | ConvertFrom-Csv)
if ($assessments.Count -ne $store.GetTable('TASKPRED').RowCount -or $table06.Count -ne $assessments.Count) {
    throw 'Source, assessment and exported table 06 counts differ.'
}
$rows = for ($i = 0; $i -lt $assessments.Count; $i++) {
    $a = $assessments[$i]
    if ($a.RelationshipId -cne $table06[$i].task_pred_id -or $a.FormattedDays -cne $table06[$i].free_float) {
        throw "Table 06 disagrees with assessment at row $($i+1)."
    }
    if ($a.Classification.ToString() -ne 'Calculated' -and ($null -ne $a.FloatHours -or $null -ne $a.FloatDays -or $a.FormattedDays -ne '')) {
        throw 'A noncalculated assessment has a numeric allowance.'
    }
    if ($a.Classification.ToString() -eq 'Calculated' -and $a.FloatDays -ne $a.FloatHours / $a.PredecessorHoursPerDay) {
        throw 'Hours/day conversion differs.'
    }
    [pscustomobject]@{
        source_row_number = $a.SourceRowNumber; relationship_id = $a.RelationshipId
        predecessor_id = $a.PredecessorIdKey; successor_id = $a.SuccessorIdKey
        relationship_type = $a.RelationshipType
        predecessor_status = $a.PredecessorStatus; successor_status = $a.SuccessorStatus
        predecessor_type = $a.InputEvidence['predecessor.task_type'].RawValue
        successor_type = $a.InputEvidence['successor.task_type'].RawValue
        scheduling_mode = $a.SchedulingMode; lag_hours = $a.EffectiveLagHours
        lag_calendar_setting = $a.LagCalendarSetting; lag_calendar_key = $a.LagCalendarKey
        predecessor_calendar = $a.PredecessorCalendarKey
        predecessor_endpoint = $a.PredecessorEndpoint; successor_endpoint = $a.SuccessorEndpoint
        predecessor_endpoint_field = $a.PredecessorEndpointField; successor_endpoint_field = $a.SuccessorEndpointField
        predecessor_hours_per_day = $a.PredecessorHoursPerDay
        free_float_hours = $a.FloatHours; free_float = $a.FormattedDays
        classification = $a.Classification.ToString(); reason_code = $a.ReasonCode
    }
}
$rows | Export-Csv -LiteralPath (Join-Path $PSScriptRoot 'real-sample-relationships.csv') -NoTypeInformation -Encoding utf8
$matrix = $rows | Group-Object relationship_type,predecessor_status,successor_status,classification,reason_code | ForEach-Object {
    $first = $_.Group[0]
    [pscustomobject]@{
        relationship_type = $first.relationship_type; predecessor_status = $first.predecessor_status
        successor_status = $first.successor_status; classification = $first.classification
        reason_code = $first.reason_code; count = $_.Count
    }
} | Sort-Object relationship_type,predecessor_status,successor_status,classification,reason_code
$matrix | Export-Csv -LiteralPath (Join-Path $PSScriptRoot 'real-sample-matrix.csv') -NoTypeInformation -Encoding utf8
$numeric = @($rows | Where-Object classification -eq 'Calculated')
$inconsistentNumeric = @($assessments | Where-Object {
    if ($_.Classification.ToString() -ne 'Calculated') { return $false }
    $a = $_
    foreach ($endpoint in @('predecessor','successor')) {
        if ($a.InputEvidence[($endpoint + '.status_code')].RawValue -ieq 'TK_Active') {
            $actual = [XerToCsvConverter.DateParser]::TryParse($a.InputEvidence[($endpoint + '.act_start_date')].RawValue)
            $finish = [XerToCsvConverter.DateParser]::TryParse($a.InputEvidence[($endpoint + '.reend_date')].RawValue)
            if ($null -eq $actual -or ($null -ne $a.ProjectDataDate -and $actual -gt $a.ProjectDataDate) -or ($null -ne $finish -and $actual -gt $finish)) { return $true }
        }
    }
    return $false
})
$afterHash = (Get-FileHash -LiteralPath $source).Hash
if ($beforeHash -cne $afterHash) { throw 'Input file changed during evaluation.' }
$summary = [ordered]@{
    input_file = $source; input_sha256 = $beforeHash; core_sha256 = (Get-FileHash -LiteralPath $assembly).Hash
    source_relationship_rows = $assessments.Count; exported_rows_verified = $table06.Count
    classifications = @($rows | Group-Object classification | ForEach-Object { @{classification=$_.Name; count=$_.Count} })
    reasons = @($rows | Group-Object reason_code | ForEach-Object { @{reason_code=$_.Name; count=$_.Count} })
    types = @($rows | Group-Object relationship_type | ForEach-Object { @{relationship_type=$_.Name; count=$_.Count} })
    numeric_positive = @($numeric | Where-Object { [decimal]$_.free_float -gt 0 }).Count
    numeric_zero = @($numeric | Where-Object { [decimal]$_.free_float -eq 0 }).Count
    numeric_negative = @($numeric | Where-Object { [decimal]$_.free_float -lt 0 }).Count
    numeric_tiny_nonzero = @($numeric | Where-Object { [decimal]$_.free_float -ne 0 -and [math]::Abs([decimal]$_.free_float) -lt 0.000001 }).Count
    numeric_resource_dependent = @($numeric | Where-Object { $_.predecessor_type -ieq 'TT_Rsrc' -or $_.successor_type -ieq 'TT_Rsrc' }).Count
    numeric_inconsistent_active_evidence = $inconsistentNumeric.Count
    source_unchanged = $true
    evidence_scope = 'Current-source tests and row-for-row export reconciliation; no native P6 reschedule comparison.'
}
$summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $PSScriptRoot 'real-sample-summary.json') -Encoding utf8
$summary | ConvertTo-Json -Depth 6
