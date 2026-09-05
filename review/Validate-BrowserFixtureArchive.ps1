param(
    [Parameter(Mandatory)][string]$ArchivePath,
    [Parameter(Mandatory)][ValidateSet('Standard', 'Programme', 'Tender')][string]$Profile,
    [Parameter(Mandatory)][int]$SourceCount
)

# Read-only verification of saved browser exports from the synthetic review fixture.
# These maps index output tables, never input occurrences by filename/path/hash.
$ErrorActionPreference = 'Stop'
$fixturePath = Join-Path $PSScriptRoot 'fixtures/J5001_C_BL01_2026-01-31.xer'
$fixtureHash = (Get-FileHash -LiteralPath $fixturePath -Algorithm SHA256).Hash.ToLowerInvariant()
$archive = [IO.Compression.ZipFile]::OpenRead((Resolve-Path -LiteralPath $ArchivePath).Path)
try {
    $tables = @{}
    $hashes = @{}
    foreach ($entry in $archive.Entries) {
        if (-not $entry.Name.EndsWith('.csv', [StringComparison]::Ordinal)) { throw "Unexpected entry: $($entry.FullName)" }
        $tableName = [IO.Path]::GetFileNameWithoutExtension($entry.Name)
        if ($tables.ContainsKey($tableName)) { throw "Duplicate CSV entry: $tableName" }
        $stream = $entry.Open()
        $buffer = [IO.MemoryStream]::new()
        try { $stream.CopyTo($buffer); $bytes = $buffer.ToArray() }
        finally { $stream.Dispose(); $buffer.Dispose() }
        $text = [Text.UTF8Encoding]::new($false, $true).GetString($bytes).TrimStart([char]0xFEFF)
        [object[]]$rows = ConvertFrom-Csv -InputObject $text
        $tables[$tableName] = $rows
        $hashes[$tableName] = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
    }

    $expected = @('01_XER_TASK','02_XER_PROJECT','03_XER_PROJWBS','06_XER_PREDECESSOR',
        '07_XER_ACTVTYPE','08_XER_ACTVCODE','09_XER_TASKACTV','10_XER_CALENDAR','12_XER_RSRC','15_XER_RESOURCE_DISTRIBUTION')
    if ($Profile -eq 'Standard') {
        $expected += @('04_XER_BASELINE','11_XER_CALENDAR_DETAILED','13_XER_TASKRSRC','14_XER_UMEASURE')
    } else { $expected += 'XER_CSV_MANIFEST' }
    if ($tables.Count -ne $expected.Count) { throw "Unexpected archive file count: $($tables.Count)" }
    foreach ($name in $expected) { if (-not $tables.ContainsKey($name)) { throw "Missing CSV: $name" } }

    foreach ($pair in @(@('01_XER_TASK',2),@('02_XER_PROJECT',1),@('06_XER_PREDECESSOR',1),@('10_XER_CALENDAR',1),@('15_XER_RESOURCE_DISTRIBUTION',1))) {
        if ($tables[$pair[0]].Count -ne ([int]$pair[1] * $SourceCount)) { throw "Unexpected row count: $($pair[0])" }
    }
    $taskKeys = @($tables['01_XER_TASK'] | ForEach-Object task_id_key)
    if (@($taskKeys | Sort-Object -Unique).Count -ne $taskKeys.Count) { throw 'Duplicate exported activity key.' }
    foreach ($row in $tables['06_XER_PREDECESSOR']) {
        if ($row.task_id_key -notin $taskKeys -or $row.pred_task_id_key -notin $taskKeys) { throw 'Unresolved relationship endpoint.' }
        if ($row.free_float -eq '' -or [decimal]$row.free_float -ne 0) { throw 'Expected zero fixture relationship free float.' }
    }
    $quantity = [decimal]0
    foreach ($row in $tables['15_XER_RESOURCE_DISTRIBUTION']) {
        if ($row.task_id_key -notin $taskKeys) { throw 'Unresolved resource task.' }
        $quantity += [decimal]::Parse($row.monthly_quantity, [Globalization.CultureInfo]::InvariantCulture)
    }
    if ($quantity -ne (100 * $SourceCount)) { throw 'Resource quantity did not reconcile.' }

    if ($Profile -eq 'Standard') {
        if ($tables['04_XER_BASELINE'].Count -ne 0) { throw 'Expected header-only baseline for this filename convention.' }
        if ($tables['11_XER_CALENDAR_DETAILED'].Count -ne (7 * $SourceCount)) { throw 'Expected seven calendar rules per occurrence.' }
        foreach ($row in $tables['01_XER_TASK']) {
            if ($row.FileName -cne 'J5001_C_BL01_2026-01-31.xer') { throw 'Original filename provenance changed.' }
        }
    } else {
        $manifest = $tables['XER_CSV_MANIFEST']
        if ($manifest.Count -ne (10 * $SourceCount)) { throw 'Unexpected manifest coverage.' }
        foreach ($row in $manifest) {
            if ($row.csv_sha256 -cne $hashes[$row.table_name]) { throw "Manifest CSV hash mismatch: $($row.table_name)" }
            if ($row.source_sha256 -cne $fixtureHash) { throw 'Manifest source hash mismatch.' }
            if ($row.data_date -cne '2026-01-30') { throw 'P6 Data Date changed.' }
            if ($row.original_xer_filename -cne 'J5001_C_BL01_2026-01-31.xer') { throw 'Manifest original filename changed.' }
            if ($Profile -eq 'Programme') {
                if ($row.schema_version -cne '3.0' -or $row.bundle_status -cne 'complete') { throw 'Programme contract changed.' }
                if ($row.monthupdate -cne '2026-01-30') { throw 'Baseline MonthUpdate did not use the P6 Data Date.' }
                $prefix = "CSV::$($row.project_code)::$($row.programme_type)::$($row.snapshot_tag)::"
            } else {
                if ($row.schema_version -cne '1.0' -or $row.bundle_status -cne 'COMPLETE' -or $row.bundle_profile -cne 'tender_review') { throw 'Tender contract changed.' }
                $prefix = "CSV::$($row.project_code)::TENDER::$($row.status_date.Replace('-', ''))::"
            }
            $sourceRows = 0
            foreach ($dataRow in $tables[$row.table_name]) {
                $keyProperty = $dataRow.PSObject.Properties | Where-Object Name -Like '*_id_key' | Select-Object -First 1
                if ($keyProperty -and $keyProperty.Value.StartsWith($prefix, [StringComparison]::Ordinal)) { $sourceRows++ }
            }
            if ($sourceRows -ne [int]$row.row_count) { throw "Manifest source/table row mismatch: $($row.table_name) $prefix" }
        }
        if ($Profile -eq 'Tender') {
            if (@($manifest | ForEach-Object status_date | Sort-Object -Unique).Count -ne $SourceCount) { throw 'Tender stage identity collapsed.' }
        }
    }
    Write-Output "Passed $Profile saved archive: $($tables.Count) CSVs, $SourceCount source occurrence(s), unique activity keys, valid relationships and $quantity resource units; review manifest hashes/counts checked when present."
} finally { $archive.Dispose() }
