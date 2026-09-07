param()
$ErrorActionPreference = 'Stop'
$before = @(Import-Csv -LiteralPath (Join-Path $PSScriptRoot 'before/real-sample-relationships.csv'))
$after = @(Import-Csv -LiteralPath (Join-Path $PSScriptRoot 'real-sample-relationships.csv'))
$beforeSummary = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'before/real-sample-summary.json') -Raw | ConvertFrom-Json
$afterSummary = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'real-sample-summary.json') -Raw | ConvertFrom-Json
if ($beforeSummary.input_sha256 -cne $afterSummary.input_sha256) { throw 'The before/after XER inputs differ.' }

# The sample contains one input occurrence. Retain source TASKPRED ordinal and all native/qualified
# endpoint identity fields, rather than treating a relationship ID or filename alone as unique.
function Get-Identity($row) {
    @($row.source_row_number, $row.relationship_id, $row.predecessor_id, $row.successor_id) | ConvertTo-Json -Compress
}
$index = [Collections.Generic.Dictionary[string,object]]::new([StringComparer]::Ordinal)
foreach ($row in $before) {
    $key = Get-Identity $row
    if (-not $index.TryAdd($key, $row)) { throw "Duplicate before identity: $key" }
}
$seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
$unexpected = [Collections.Generic.List[string]]::new()
$comparison = foreach ($row in $after) {
    $key = Get-Identity $row
    if (-not $seen.Add($key)) { throw "Duplicate after identity: $key" }
    if (-not $index.ContainsKey($key)) { throw "New or changed relationship identity: $key" }
    $prior = $index[$key]
    $changed = [Collections.Generic.List[string]]::new()
    foreach ($field in @('relationship_type','predecessor_status','successor_status','predecessor_type','successor_type','scheduling_mode','lag_hours','lag_calendar_setting','lag_calendar_key','predecessor_calendar','predecessor_endpoint','successor_endpoint','predecessor_endpoint_field','successor_endpoint_field','predecessor_hours_per_day','free_float_hours','free_float','classification','reason_code')) {
        if ($prior.$field -cne $row.$field) { $changed.Add($field) }
    }
    $numericChanged = $prior.free_float_hours -cne $row.free_float_hours -or $prior.free_float -cne $row.free_float
    $explanation = if ($changed.Count -eq 0) {
        if ($row.allowance_status -eq 'Estimated') { 'Existing numeric result preserved; resource-dependent TASK-calendar estimate is now explicit.' }
        else { 'Existing identity, value, classification and reason preserved; allowance status/basis added.' }
    }
    elseif ($row.reason_code -eq 'CalculatedRetainedStartToFinish' -and $prior.classification -eq 'Unsupported' -and $prior.free_float -eq '') {
        'New supported unstarted predecessor SF to active successor under Retained Logic.'
    }
    elseif ($row.reason_code -eq 'FixedActualPredecessorStart' -and $row.free_float -eq '' -and $prior.free_float -eq '') {
        'Already-actual predecessor start is now explicitly FixedEvent; no movable start allowance is invented.'
    }
    elseif ($row.reason_code -eq 'IgnoredSummaryRelationship' -and $row.free_float -eq '' -and $prior.free_float -eq '') {
        'Direct WBS-summary relationship is now explicitly ignored/NoFiniteBound; summary rollups remain separate.'
    }
    elseif ($row.calculation_basis -match 'SuspensionAdjusted' -and $prior.reason_code -match 'Suspension') {
        'A valid closed predecessor suspension is now excluded from activity movement; lag calendar remains independent.'
    }
    elseif ($prior.classification -eq 'Calculated' -and $row.allowance_status -in @('MissingData','InvalidData','RequiresContext') -and $row.free_float -eq '') {
        'Stricter activity-state/actual-date evidence validation now withholds a previously numeric result.'
    }
    elseif (-not $numericChanged -and $row.allowance_status -in @('MissingData','InvalidData','RequiresContext') -and $row.free_float -eq '') {
        'Blank numeric result retained; more specific activity-state/context validation changes the decisive reason.'
    }
    else {
        $unexpected.Add($key)
        'UNEXPLAINED: review this transition before accepting the implementation.'
    }
    [pscustomobject]@{
        source_row_number = $row.source_row_number; relationship_id = $row.relationship_id
        predecessor_id = $row.predecessor_id; successor_id = $row.successor_id; relationship_type = $row.relationship_type
        predecessor_status = $row.predecessor_status; successor_status = $row.successor_status
        before_hours = $prior.free_float_hours; after_hours = $row.free_float_hours
        before_free_float = $prior.free_float; after_free_float = $row.free_float
        before_classification = $prior.classification; after_classification = $row.classification
        before_reason = $prior.reason_code; after_reason = $row.reason_code
        before_allowance_status = 'NotExportedInPriorSchema'; after_allowance_status = $row.allowance_status
        before_calculation_basis = 'NotExportedInPriorSchema'; after_calculation_basis = $row.calculation_basis
        changed_existing_fields = $changed -join ';'; numeric_changed = $numericChanged; explanation = $explanation
    }
}
if ($seen.Count -ne $index.Count) { throw 'One or more before relationship identities disappeared.' }
$comparison | Export-Csv -LiteralPath (Join-Path $PSScriptRoot 'real-sample-before-after.csv') -NoTypeInformation -Encoding utf8
$result = [ordered]@{
    input_sha256 = $afterSummary.input_sha256; before_core_sha256 = $beforeSummary.core_sha256; after_core_sha256 = $afterSummary.core_sha256
    before_rows = $before.Count; after_rows = $after.Count; identities_matched = $seen.Count
    unchanged_existing_values_and_reasons = @($comparison | Where-Object changed_existing_fields -eq '').Count
    changed_existing_values_or_reasons = @($comparison | Where-Object changed_existing_fields -ne '').Count
    numeric_changes = @($comparison | Where-Object numeric_changed -eq $true).Count
    changes_by_explanation = @($comparison | Group-Object explanation | ForEach-Object { @{explanation=$_.Name; count=$_.Count} })
    unexplained_changes = $unexpected.Count
}
$result | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $PSScriptRoot 'implementation-comparison-summary.json') -Encoding utf8
$result | ConvertTo-Json -Depth 8
if ($unexpected.Count -gt 0) { throw "$($unexpected.Count) unexplained relationship transitions." }
