param(
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string[]]$Paths,
    [string]$CoreAssemblyPath = (Join-Path $PSScriptRoot '../XerToCsvConverter.Core/bin/Debug/net8.0/XerToCsvConverter.Core.dll'),
    [switch]$ExcludeResourceDistribution
)

# Read-only integration audit of the shared Web stream/memory export path.
# Inputs are an ordered list of occurrences, never a filename/path/hash dictionary.
# No original files are changed and no XER/CSV content is persisted or uploaded.
# This checks parser/reporting contracts, not native P6 scheduling parity.
$ErrorActionPreference = 'Stop'
$null = [Reflection.Assembly]::Load([IO.File]::ReadAllBytes((Resolve-Path -LiteralPath $CoreAssemblyPath).Path))
Add-Type -AssemblyName Microsoft.VisualBasic.Core
$culture = [Globalization.CultureInfo]::InvariantCulture
$numberStyle = [Globalization.NumberStyles]::Float

function Assert-Audit([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}

function Read-Number([string]$Value, [string]$Context, [switch]$BlankIsZero) {
    if ([string]::IsNullOrWhiteSpace($Value) -and $BlankIsZero) { return [decimal]0 }
    $parsed = [decimal]0
    Assert-Audit ([decimal]::TryParse($Value, $numberStyle, $culture, [ref]$parsed)) "$Context is not a finite invariant decimal."
    return $parsed
}

function Read-CsvTable([byte[]]$Bytes, [string]$Name) {
    $text = [Text.UTF8Encoding]::new($false, $true).GetString($Bytes).TrimStart([char]0xFEFF)
    $reader = [Microsoft.VisualBasic.FileIO.TextFieldParser]::new([IO.StringReader]::new($text))
    try {
        $reader.SetDelimiters([string[]]@(','))
        $reader.HasFieldsEnclosedInQuotes = $true
        $reader.TrimWhiteSpace = $false
        [string[]]$headers = $reader.ReadFields()
        Assert-Audit ($null -ne $headers -and $headers.Length -gt 0) "$Name has no CSV header."
        $indexes = [Collections.Generic.Dictionary[string,int]]::new([StringComparer]::OrdinalIgnoreCase)
        for ($i = 0; $i -lt $headers.Length; $i++) {
            Assert-Audit (-not [string]::IsNullOrWhiteSpace($headers[$i]) -and -not $indexes.ContainsKey($headers[$i])) "$Name has a blank or duplicate CSV column."
            $indexes.Add($headers[$i], $i)
        }
        $rows = [Collections.Generic.List[string[]]]::new()
        while (-not $reader.EndOfData) {
            [string[]]$row = $reader.ReadFields()
            Assert-Audit ($row.Length -eq $headers.Length) "$Name CSV row $($rows.Count + 1) has an incorrect field count."
            $rows.Add($row)
        }
        return [pscustomobject]@{ Name = $Name; Headers = $headers; Indexes = $indexes; Rows = $rows }
    } finally { $reader.Dispose() }
}

function Assert-Headers($Table, [string[]]$Expected) {
    Assert-Audit ($Table.Headers.Length -eq $Expected.Length) "$($Table.Name) header count changed."
    for ($i = 0; $i -lt $Expected.Length; $i++) {
        Assert-Audit ($Table.Headers[$i] -ceq $Expected[$i]) "$($Table.Name) header $i changed."
    }
}

function Get-Key([string]$Namespace, [string]$NativeId) {
    if ([string]::IsNullOrEmpty($Namespace) -or [string]::IsNullOrEmpty($NativeId)) { return '' }
    return $Namespace.Trim() + '.' + $NativeId.Trim()
}

function Get-SourceCell($Table, $Row, [string]$Column) {
    if (-not $Table.FieldIndexes.ContainsKey($Column)) { return '' }
    return [XerToCsvConverter.XerTable]::GetFieldValueSafe($Row, $Table.FieldIndexes[$Column])
}

function New-KeySet($Table, [string]$Column) {
    $set = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $index = $Table.Indexes[$Column]
    foreach ($row in $Table.Rows) {
        Assert-Audit (-not [string]::IsNullOrWhiteSpace($row[$index]) -and $set.Add($row[$index])) "$($Table.Name).$Column has a blank or duplicate key."
    }
    return ,$set
}

function Assert-References($Table, [string]$Column, $Keys, [switch]$AllowBlank) {
    $index = $Table.Indexes[$Column]
    foreach ($row in $Table.Rows) {
        if ($AllowBlank -and $row[$index].Length -eq 0) { continue }
        Assert-Audit ($Keys.Contains($row[$index])) "$($Table.Name).$Column contains an unresolved output reference."
    }
}

function Add-Quantity($Totals, $Key, [decimal]$Value) {
    if ($Totals.ContainsKey($Key)) { $Totals[$Key] += $Value }
    else { $Totals.Add($Key, $Value) }
}

function Get-OptionalNumber([string]$Value, [switch]$BlankIsZero) {
    if ($BlankIsZero -and [string]::IsNullOrWhiteSpace($Value)) { return [decimal]0 }
    $parsed = [decimal]0
    if ([decimal]::TryParse($Value, $numberStyle, $culture, [ref]$parsed)) { return $parsed }
    return $null
}

# Independent source-quantity interpretation. Keep unknown separate from zero and
# preserve signed amounts for reconciliation; never call Core allocation helpers.
function Get-SourcePortionQuantity($Table, $Row, [string]$Portion) {
    $fields = if ($Portion -ceq 'Actual') { @('act_reg_qty','act_ot_qty') } else { @('remain_qty') }
    $total = [decimal]0
    $invalid = $false
    $known = $true
    foreach ($field in $fields) {
        $number = Get-OptionalNumber (Get-SourceCell $Table $Row $field) -BlankIsZero
        if ($null -eq $number) { $known = $false; $invalid = $true; continue }
        if ($number -lt 0) { $invalid = $true }
        try { $total = [decimal]::Add($total, $number) }
        catch [OverflowException] { $known = $false; $invalid = $true }
    }
    return [pscustomobject]@{ Known = $known; Value = $(if ($known) { $total } else { $null }); Invalid = $invalid }
}

function Get-ExpectedDays([string]$Hours, $HoursPerDay, [int]$Decimals = 2) {
    $number = Get-OptionalNumber $Hours
    if ($null -eq $number -or $null -eq $HoursPerDay -or $HoursPerDay -le 0) { return '' }
    try { return ([decimal]$number / [decimal]$HoursPerDay).ToString("F$Decimals", $culture) }
    catch [OverflowException] { return '' }
}

function Get-ExpectedDate([string]$Raw) {
    if ([string]::IsNullOrWhiteSpace($Raw)) { return '' }
    $date = [DateTime]::MinValue
    $formats = [string[]]@('d/M/yyyy','dd/MM/yyyy','M/d/yyyy','MM/dd/yyyy','yyyy-MM-dd','dd-MMM-yy','dd-MMM-yyyy','d/M/yyyy H:mm:ss','dd/MM/yyyy HH:mm:ss','M/d/yyyy h:mm:ss tt','yyyy-MM-dd HH:mm:ss')
    $valid = [DateTime]::TryParseExact($Raw, $formats, $culture, [Globalization.DateTimeStyles]::None, [ref]$date)
    if (-not $valid) { $valid = [DateTime]::TryParse($Raw, $culture, [Globalization.DateTimeStyles]::None, [ref]$date) }
    if ($valid -and $date.Year -gt 1900 -and $date.Year -lt 2200) { return $date.ToString('yyyy-MM-dd HH:mm:ss', $culture) }
    return ''
}

function Get-ExpectedPercentage($Table, $Row, [string]$Status) {
    if ($Status -ieq 'TK_Complete') { return '100.00' }
    if ($Status -ieq 'TK_NotStart') { return '0.00' }
    if ($Status -ine 'TK_Active') { return '' }
    try {
        switch ((Get-SourceCell $Table $Row 'complete_pct_type').Trim().ToUpperInvariant()) {
            'CP_PHYS' { $value = Get-OptionalNumber (Get-SourceCell $Table $Row 'phys_complete_pct'); if ($null -eq $value -or $value -lt 0) { return '' } }
            'CP_DRTN' {
                $target = Get-OptionalNumber (Get-SourceCell $Table $Row 'target_drtn_hr_cnt')
                $remaining = Get-OptionalNumber (Get-SourceCell $Table $Row 'remain_drtn_hr_cnt')
                if ($null -eq $target -or $target -le 0 -or $null -eq $remaining -or $remaining -lt 0) { return '' }
                $value = ([decimal]1 - $remaining / $target) * 100
            }
            'CP_UNITS' {
                $values = [Collections.Generic.List[decimal]]::new()
                foreach ($field in @('act_work_qty','act_equip_qty','remain_work_qty','remain_equip_qty')) {
                    $number = Get-OptionalNumber (Get-SourceCell $Table $Row $field) -BlankIsZero
                    if ($null -eq $number -or $number -lt 0) { return '' }
                    $values.Add($number)
                }
                $actual = $values[0] + $values[1]
                $total = $actual + $values[2] + $values[3]
                $value = if ($total -eq 0) { [decimal]0 } else { $actual / $total * 100 }
            }
            default { return '' }
        }
        return ([Math]::Clamp([decimal]$value, [decimal]0, [decimal]100)).ToString('F2', $culture)
    } catch [OverflowException] { return '' }
}

$tableSources = [ordered]@{
    '01_XER_TASK' = 'TASK'; '02_XER_PROJECT' = 'PROJECT'; '03_XER_PROJWBS' = 'PROJWBS'
    '04_XER_BASELINE' = 'TASK'; '06_XER_PREDECESSOR' = 'TASKPRED'; '07_XER_ACTVTYPE' = 'ACTVTYPE'
    '08_XER_ACTVCODE' = 'ACTVCODE'; '09_XER_TASKACTV' = 'TASKACTV'; '10_XER_CALENDAR' = 'CALENDAR'
    '11_XER_CALENDAR_DETAILED' = 'CALENDAR'; '12_XER_RSRC' = 'RSRC'; '13_XER_TASKRSRC' = 'TASKRSRC'
    '14_XER_UMEASURE' = 'UMEASURE'; '15_XER_RESOURCE_DISTRIBUTION' = 'TASKRSRC'
}
if ($ExcludeResourceDistribution) { $tableSources.Remove('15_XER_RESOURCE_DISTRIBUTION') }
$added = @{
    '02_XER_PROJECT' = @('proj_id_key','MonthUpdate')
    '03_XER_PROJWBS' = @('wbs_id_key','parent_wbs_id_key','MonthUpdate')
    '06_XER_PREDECESSOR' = @('task_id_key','pred_task_id_key','calendar_id_key','predecessor_clndr_id_key','status_code','predecessor_status_code','task_type','predecessor_task_type','lag','time_period_hours_per_day','Start','Finish','predecessor_start','predecessor_finish','free_float','total_float','MonthUpdate')
    '07_XER_ACTVTYPE' = @('actv_code_type_id_key','MonthUpdate')
    '08_XER_ACTVCODE' = @('actv_code_id_key','actv_code_type_id_key','MonthUpdate')
    '09_XER_TASKACTV' = @('actv_code_id_key','task_id_key','MonthUpdate')
    '10_XER_CALENDAR' = @('clndr_id_key','MonthUpdate')
    '12_XER_RSRC' = @('rsrc_id_key','clndr_id_key','unit_id_key','MonthUpdate')
    '13_XER_TASKRSRC' = @('rsrc_id_key','task_id_key','MonthUpdate')
    '14_XER_UMEASURE' = @('unit_id_key','MonthUpdate')
}
$taskColumns = 'task_id,proj_id,wbs_id,clndr_id,task_type,status_code,task_code,task_name,rsrc_id,act_start_date,act_end_date,early_start_date,early_end_date,late_start_date,late_end_date,target_start_date,target_end_date,cstr_type,cstr_date,priority_type,float_path,float_path_order,driving_path_flag,remain_drtn_hr_cnt,phys_complete_pct,Start,Finish,ID_Name,Remaining Duration,Original Duration,total_float,Free Float,%,Data Date,wbs_id_key,task_id_key,calendar_id_key,proj_id_key,MonthUpdate'.Split(',')
$calendarColumns = 'clndr_id,clndr_name,clndr_type,date,day_of_week,working_day,work_hours,exception_type,clndr_id_key,MonthUpdate,day_of_week_num,working_day_int'.Split(',')
$distributionColumns = 'task_id_key,rsrc_id_key,clndr_id_key,proj_id_key,distribution_month,month_start_date,month_end_date,monthly_quantity,distribution_type,month_working_hours,total_working_hours,calendar_hours_per_day,month_working_days,total_working_days,month_calendar_days,total_calendar_days,Start,Finish,is_actual,status_code,Unit,task_code,rsrc_short_name,rsrc_name,rsrc_type,MonthUpdate'.Split(',')
$numericColumns = @{
    '01_XER_TASK' = @('Remaining Duration','Original Duration','total_float','Free Float','%')
    '04_XER_BASELINE' = @('Remaining Duration','Original Duration','total_float','Free Float','%')
    '06_XER_PREDECESSOR' = @('lag','time_period_hours_per_day','free_float','total_float')
    '11_XER_CALENDAR_DETAILED' = @('work_hours','day_of_week_num','working_day_int')
    '15_XER_RESOURCE_DISTRIBUTION' = @('monthly_quantity','month_working_hours','total_working_hours','calendar_hours_per_day','month_working_days','total_working_days','month_calendar_days','total_calendar_days','is_actual')
}
$keyColumns = @{
    '01_XER_TASK' = @(@('task_id_key','task_id'),@('proj_id_key','proj_id'),@('wbs_id_key','wbs_id'),@('calendar_id_key','clndr_id'))
    '02_XER_PROJECT' = ,@('proj_id_key','proj_id')
    '03_XER_PROJWBS' = @(@('wbs_id_key','wbs_id'),@('parent_wbs_id_key','parent_wbs_id'))
    '06_XER_PREDECESSOR' = @(@('task_id_key','task_id'),@('pred_task_id_key','pred_task_id'))
    '07_XER_ACTVTYPE' = ,@('actv_code_type_id_key','actv_code_type_id')
    '08_XER_ACTVCODE' = @(@('actv_code_id_key','actv_code_id'),@('actv_code_type_id_key','actv_code_type_id'))
    '09_XER_TASKACTV' = @(@('actv_code_id_key','actv_code_id'),@('task_id_key','task_id'))
    '10_XER_CALENDAR' = ,@('clndr_id_key','clndr_id')
    '12_XER_RSRC' = @(@('rsrc_id_key','rsrc_id'),@('clndr_id_key','clndr_id'),@('unit_id_key','unit_id'))
    '13_XER_TASKRSRC' = @(@('rsrc_id_key','rsrc_id'),@('task_id_key','task_id'))
    '14_XER_UMEASURE' = ,@('unit_id_key','unit_id')
}

$inputs = [Collections.Generic.List[ValueTuple[IO.Stream,string]]]::new()
$opened = [Collections.Generic.List[IO.Stream]]::new()
try {
    foreach ($path in $Paths) {
        $resolved = (Resolve-Path -LiteralPath $path).Path
        $stream = [IO.File]::OpenRead($resolved)
        $opened.Add($stream)
        $inputs.Add([ValueTuple[IO.Stream,string]]::new($stream, [IO.Path]::GetFileName($resolved)))
    }
    $service = [XerToCsvConverter.ProcessingService]::new()
    $store = $service.ParseXerStreamsAsync($inputs, $null, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
} finally { foreach ($stream in $opened) { $stream.Dispose() } }

$requested = [Collections.Generic.List[string]]::new()
foreach ($name in $tableSources.Keys) { $requested.Add($name) }
$export = $service.ExportTablesToMemoryWithDiagnosticsAsync($store, $requested, $null, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
$bytesByTable = $export.Files
$expectedFileCount = $tableSources.Count + 1
Assert-Audit ($bytesByTable.Count -eq $expectedFileCount) 'The Enhanced export did not return every selected table and required companion.'
Assert-Audit ($bytesByTable.ContainsKey('XER_DATA_QUALITY')) 'Enhanced selection is missing its data-quality companion.'
$quality = Read-CsvTable $bytesByTable['XER_DATA_QUALITY'] 'XER_DATA_QUALITY'
Assert-Headers $quality (@([XerToCsvConverter.XerDataQuality]::Columns) + 'FileName')
Assert-Audit ($quality.Rows.Count -eq $export.WarningCount) 'Completion warning count disagrees with the companion.'
$generalWarningCount = 0
$checkedDiagnosticEvidenceCells = 0
$diagnosticSourceRows = [Collections.Generic.Dictionary[ValueTuple[string,string],object]]::new()
foreach ($warning in $quality.Rows) {
    Assert-Audit ($warning[$quality.Indexes['diagnostic_schema_version']] -ceq '1.2' -and
        $warning[$quality.Indexes['severity']] -ceq 'Warning') 'Companion schema/severity is invalid.'
    Assert-Audit ($tableSources.Contains($warning[$quality.Indexes['table_name']])) 'Companion diagnoses an unselected numbered table.'
    Assert-Audit (-not [string]::IsNullOrWhiteSpace($warning[$quality.Indexes['issue_code']]) -and
        -not [string]::IsNullOrWhiteSpace($warning[$quality.Indexes['message']])) 'Companion issue lacks a code or explanation.'
    $portion = $warning[$quality.Indexes['allocation_portion']]
    if ($portion -cin @('Actual','Remaining')) { continue }
    Assert-Audit ($portion -ceq '' -and $warning[$quality.Indexes['unallocated_actual_quantity']] -ceq '' -and
        $warning[$quality.Indexes['unallocated_remaining_quantity']] -ceq '') 'General warning invented an allocation portion or quantity.'
    $generalWarningCount++
    $sourceTableName = $warning[$quality.Indexes['source_table']]
    $sourceTable = $store.GetTable($sourceTableName)
    $ordinalText = $warning[$quality.Indexes['source_row_number']]
    if ([string]::IsNullOrEmpty($ordinalText)) {
        Assert-Audit ($null -eq $sourceTable -or $sourceTable.RowCount -eq 0) 'A populated source table warning has no source row ordinal.'
        continue
    }
    $ordinal = [int]$ordinalText
    $namespace = $warning[$quality.Indexes['source_namespace']]
    $sourceKey = [ValueTuple[string,string]]::new($sourceTableName, $namespace)
    if (-not $diagnosticSourceRows.ContainsKey($sourceKey)) {
        $diagnosticSourceRows.Add($sourceKey, @($sourceTable.Rows | Where-Object { $_.SourceFilename -ceq $namespace }))
    }
    $sourceRows = $diagnosticSourceRows[$sourceKey]
    Assert-Audit ($ordinal -gt 0 -and $ordinal -le $sourceRows.Count) 'General warning cannot be resolved to its source occurrence/row.'
    $sourceRow = $sourceRows[$ordinal - 1]
    Assert-Audit ($warning[$quality.Indexes['FileName']] -ceq $sourceRow.OriginalSourceFilename) 'General warning lost original filename provenance.'
    $evidence = @($warning[$quality.Indexes['raw_row_json']] | ConvertFrom-Json)
    Assert-Audit ($evidence.Count -eq $sourceTable.Headers.Count) 'General warning did not retain every source column.'
    for ($index = 0; $index -lt $sourceTable.Headers.Count; $index++) {
        $column = $sourceTable.Headers[$index]
        Assert-Audit ($evidence[$index].column -ceq $column -and
            [string]$evidence[$index].value -ceq (Get-SourceCell $sourceTable $sourceRow $column)) 'General warning changed its raw source field/value evidence.'
        Assert-Audit ($evidence[$index].presence -ceq $sourceRow.GetRawField($column).State.ToString()) 'General warning changed source field presence.'
        $checkedDiagnosticEvidenceCells++
    }
}
$tables = @{} # Output tables, not an input collection.
$checkedRawCells = [long]0
$checkedNumericCells = [long]0
$sourceTokens = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
foreach ($name in $tableSources.Keys) {
    Assert-Audit ($bytesByTable.ContainsKey($name)) "Requested output $name is absent."
    $table = Read-CsvTable $bytesByTable[$name] $name
    $tables[$name] = $table
    $source = $store.GetTable($tableSources[$name])
    if ($name -in @('01_XER_TASK','04_XER_BASELINE')) { Assert-Headers $table ($taskColumns + 'FileName') }
    elseif ($name -eq '11_XER_CALENDAR_DETAILED') { Assert-Headers $table ($calendarColumns + 'FileName') }
    elseif ($name -eq '15_XER_RESOURCE_DISTRIBUTION') { Assert-Headers $table ($distributionColumns + 'FileName') }
    else { Assert-Headers $table (@($source.Headers) + $added[$name] + 'FileName') }

    if ($name -notin @('04_XER_BASELINE','11_XER_CALENDAR_DETAILED','15_XER_RESOURCE_DISTRIBUTION')) {
        Assert-Audit ($table.Rows.Count -eq $source.RowCount) "$name lost or duplicated source rows."
        for ($i = 0; $i -lt $source.RowCount; $i++) {
            $original = $source.Rows[$i]
            $row = $table.Rows[$i]
            $null = $sourceTokens.Add($original.SourceToken)
            Assert-Audit ($row[$table.Indexes['FileName']] -ceq $original.OriginalSourceFilename) "$name changed original filename provenance."
            foreach ($mapping in $keyColumns[$name]) {
                $value = $row[$table.Indexes[$mapping[0]]]
                # Legacy 03 clears an absent parent; 06 permits unresolved endpoints.
                if ($value.Length -eq 0 -and (($name -eq '03_XER_PROJWBS' -and $mapping[0] -eq 'parent_wbs_id_key') -or $name -eq '06_XER_PREDECESSOR')) { continue }
                $expectedKey = Get-Key $original.SourceFilename (Get-SourceCell $source $original $mapping[1])
                Assert-Audit ($value -ceq $expectedKey) "$name changed the source-qualified meaning of $($mapping[0])."
            }
            for ($column = 0; $column -lt $source.Headers.Length; $column++) {
                $header = $source.Headers[$column]
                if (-not $table.Indexes.ContainsKey($header)) { continue }
                if ($name -eq '01_XER_TASK' -and ($header -in @('status_code','act_start_date','act_end_date','early_start_date','early_end_date','late_start_date','late_end_date','target_start_date','target_end_date','cstr_date'))) { continue }
                Assert-Audit ($row[$table.Indexes[$header]] -ceq [XerToCsvConverter.XerTable]::GetFieldValueSafe($original, $column)) "$name changed retained source field '$header' at row $($i + 1)."
                $checkedRawCells++
            }
        }
    }
    foreach ($column in $numericColumns[$name]) {
        $index = $table.Indexes[$column]
        foreach ($row in $table.Rows) {
            if ($row[$index].Length -eq 0) { continue }
            $null = Read-Number $row[$index] "$name.$column"
            $checkedNumericCells++
        }
    }
}
Assert-Audit ($sourceTokens.Count -eq $Paths.Count) 'Input occurrence identity was collapsed or an input contained no source-backed Enhanced rows.'

# Keys remain source-qualified across repeated paths and names.
$keySets = @{}
foreach ($pair in @(@('01_XER_TASK','task_id_key'),@('02_XER_PROJECT','proj_id_key'),@('03_XER_PROJWBS','wbs_id_key'),@('07_XER_ACTVTYPE','actv_code_type_id_key'),@('08_XER_ACTVCODE','actv_code_id_key'),@('10_XER_CALENDAR','clndr_id_key'),@('12_XER_RSRC','rsrc_id_key'),@('14_XER_UMEASURE','unit_id_key'))) {
    $keySets[$pair[0]] = New-KeySet $tables[$pair[0]] $pair[1]
}
foreach ($join in @(@('01_XER_TASK','proj_id_key','02_XER_PROJECT'),@('01_XER_TASK','wbs_id_key','03_XER_PROJWBS'),@('01_XER_TASK','calendar_id_key','10_XER_CALENDAR'),@('08_XER_ACTVCODE','actv_code_type_id_key','07_XER_ACTVTYPE'),@('09_XER_TASKACTV','task_id_key','01_XER_TASK'),@('09_XER_TASKACTV','actv_code_id_key','08_XER_ACTVCODE'),@('13_XER_TASKRSRC','task_id_key','01_XER_TASK'),@('13_XER_TASKRSRC','rsrc_id_key','12_XER_RSRC'),@('11_XER_CALENDAR_DETAILED','clndr_id_key','10_XER_CALENDAR'),@('15_XER_RESOURCE_DISTRIBUTION','task_id_key','01_XER_TASK'),@('15_XER_RESOURCE_DISTRIBUTION','rsrc_id_key','12_XER_RSRC'),@('15_XER_RESOURCE_DISTRIBUTION','clndr_id_key','10_XER_CALENDAR'),@('15_XER_RESOURCE_DISTRIBUTION','proj_id_key','02_XER_PROJECT'))) {
    if (-not $tables.ContainsKey($join[0])) { continue }
    Assert-References $tables[$join[0]] $join[1] $keySets[$join[2]]
}
foreach ($join in @(@('03_XER_PROJWBS','parent_wbs_id_key','03_XER_PROJWBS'),@('06_XER_PREDECESSOR','task_id_key','01_XER_TASK'),@('06_XER_PREDECESSOR','pred_task_id_key','01_XER_TASK'),@('06_XER_PREDECESSOR','calendar_id_key','10_XER_CALENDAR'),@('06_XER_PREDECESSOR','predecessor_clndr_id_key','10_XER_CALENDAR'),@('12_XER_RSRC','clndr_id_key','10_XER_CALENDAR'),@('12_XER_RSRC','unit_id_key','14_XER_UMEASURE'))) {
    Assert-References $tables[$join[0]] $join[1] $keySets[$join[2]] -AllowBlank
}

# Independent field arithmetic and lookup checks, without invoking the production
# transformer helpers or relationship free-float solver. HPD converts hours to days;
# it does not provide shift availability or establish native P6 driving status.
$checkedDerivedValues = [long]0
$rawCalendar = $store.GetTable('CALENDAR')
$rawProjects = $store.GetTable('PROJECT')
$rawActivities = $store.GetTable('TASK')
$calendarHpd = [Collections.Generic.Dictionary[ValueTuple[string,string],object]]::new()
$projectDates = [Collections.Generic.Dictionary[ValueTuple[string,string],string]]::new()
$activityValues = [Collections.Generic.Dictionary[ValueTuple[string,string],object]]::new()
foreach ($sourceRow in $rawCalendar.Rows) {
    $key = [ValueTuple[string,string]]::new($sourceRow.SourceToken, (Get-SourceCell $rawCalendar $sourceRow 'clndr_id').Trim())
    $hpd = Get-OptionalNumber (Get-SourceCell $rawCalendar $sourceRow 'day_hr_cnt')
    $calendarHpd.Add($key, $(if ($null -ne $hpd -and $hpd -gt 0) { $hpd } else { $null }))
}
foreach ($sourceRow in $rawProjects.Rows) {
    $key = [ValueTuple[string,string]]::new($sourceRow.SourceToken, (Get-SourceCell $rawProjects $sourceRow 'proj_id').Trim())
    $projectDates.Add($key, (Get-ExpectedDate (Get-SourceCell $rawProjects $sourceRow 'last_recalc_date')))
}
$activityOutput = $tables['01_XER_TASK']
for ($i = 0; $i -lt $rawActivities.RowCount; $i++) {
    $sourceRow = $rawActivities.Rows[$i]
    $outputRow = $activityOutput.Rows[$i]
    $status = Get-SourceCell $rawActivities $sourceRow 'status_code'
    $calendarId = (Get-SourceCell $rawActivities $sourceRow 'clndr_id').Trim()
    $hpd = $calendarHpd[[ValueTuple[string,string]]::new($sourceRow.SourceToken, $calendarId)]
    $projectId = (Get-SourceCell $rawActivities $sourceRow 'proj_id').Trim()
    $startField = if ($status.Trim() -ieq 'TK_NotStart') { 'restart_date' } else { 'act_start_date' }
    $finishField = if ($status.Trim() -ieq 'TK_Complete') { 'act_end_date' } else { 'reend_date' }
    $start = Get-SourceCell $rawActivities $sourceRow $startField
    $finish = Get-SourceCell $rawActivities $sourceRow $finishField
    if ($startField -eq 'restart_date' -and [string]::IsNullOrWhiteSpace($start)) { $start = Get-SourceCell $rawActivities $sourceRow 'early_start_date' }
    if ($finishField -eq 'reend_date' -and [string]::IsNullOrWhiteSpace($finish)) { $finish = Get-SourceCell $rawActivities $sourceRow 'early_end_date' }
    if ($status.Trim() -in @('TK_NotStart','TK_Active','TK_Complete')) { $start = Get-ExpectedDate $start; $finish = Get-ExpectedDate $finish }
    else { $start = ''; $finish = '' }
    $expected = @{
        'status_code' = $(switch -CaseSensitive ($status) { 'TK_Complete' { 'Complete' }; 'TK_NotStart' { 'Not Started' }; 'TK_Active' { 'In Progress' }; default { $status } })
        'Start' = $start; 'Finish' = $finish
        'Remaining Duration' = (Get-ExpectedDays (Get-SourceCell $rawActivities $sourceRow 'remain_drtn_hr_cnt') $hpd)
        'Original Duration' = (Get-ExpectedDays (Get-SourceCell $rawActivities $sourceRow 'target_drtn_hr_cnt') $hpd)
        'total_float' = $(if ($status -ceq 'TK_Complete') { '' } else { Get-ExpectedDays (Get-SourceCell $rawActivities $sourceRow 'total_float_hr_cnt') $hpd })
        'Free Float' = $(if ($status -ceq 'TK_Complete') { '' } else { Get-ExpectedDays (Get-SourceCell $rawActivities $sourceRow 'free_float_hr_cnt') $hpd })
        '%' = (Get-ExpectedPercentage $rawActivities $sourceRow $status)
        'ID_Name' = ((Get-SourceCell $rawActivities $sourceRow 'task_code') + ' - ' + (Get-SourceCell $rawActivities $sourceRow 'task_name'))
        'Data Date' = [string]$projectDates[[ValueTuple[string,string]]::new($sourceRow.SourceToken, $projectId)]
    }
    foreach ($column in @('act_start_date','act_end_date','early_start_date','early_end_date','late_start_date','late_end_date','target_start_date','target_end_date','cstr_date')) { $expected[$column] = Get-ExpectedDate (Get-SourceCell $rawActivities $sourceRow $column) }
    foreach ($pair in $expected.GetEnumerator()) {
        Assert-Audit ($outputRow[$activityOutput.Indexes[$pair.Key]] -ceq $pair.Value) "01.$($pair.Key) differs from its independent source-field calculation at row $($i + 1)."
        $checkedDerivedValues++
    }
    $key = [ValueTuple[string,string]]::new($sourceRow.SourceToken, (Get-SourceCell $rawActivities $sourceRow 'task_id').Trim())
    $activityValues.Add($key, [pscustomobject]@{ Row = $sourceRow; Project = $projectId; Calendar = $calendarId; Hpd = $hpd; Start = $start; Finish = $finish })
}
$relationships = $store.GetTable('TASKPRED')
$relationshipOutput = $tables['06_XER_PREDECESSOR']
for ($i = 0; $i -lt $relationships.RowCount; $i++) {
    $sourceRow = $relationships.Rows[$i]
    $outputRow = $relationshipOutput.Rows[$i]
    $expected = @{}
    foreach ($endpoint in @(@('task_id','proj_id','task_id_key','calendar_id_key','status_code','task_type','Start','Finish'),@('pred_task_id','pred_proj_id','pred_task_id_key','predecessor_clndr_id_key','predecessor_status_code','predecessor_task_type','predecessor_start','predecessor_finish'))) {
        $taskId = (Get-SourceCell $relationships $sourceRow $endpoint[0]).Trim()
        $projectId = (Get-SourceCell $relationships $sourceRow $endpoint[1]).Trim()
        $task = $activityValues[[ValueTuple[string,string]]::new($sourceRow.SourceToken, $taskId)]
        if ($null -ne $task -and ($task.Project.Length -eq 0 -or ($projectId.Length -gt 0 -and $projectId -cne $task.Project))) { $task = $null }
        $expected[$endpoint[2]] = $(if ($null -eq $task) { '' } else { Get-Key $sourceRow.SourceFilename $taskId })
        $expected[$endpoint[3]] = $(if ($null -eq $task) { '' } else { Get-Key $sourceRow.SourceFilename $task.Calendar })
        $expected[$endpoint[4]] = $(if ($null -eq $task) { '' } else { (Get-SourceCell $rawActivities $task.Row 'status_code').Trim() })
        $expected[$endpoint[5]] = $(if ($null -eq $task) { '' } else { Get-SourceCell $rawActivities $task.Row 'task_type' })
        $expected[$endpoint[6]] = $(if ($null -eq $task) { '' } else { $task.Start })
        $expected[$endpoint[7]] = $(if ($null -eq $task) { '' } else { $task.Finish })
        if ($endpoint[0] -eq 'task_id') { $successor = $task } else { $predecessor = $task }
    }
    $expected['time_period_hours_per_day'] = $(if ($null -eq $predecessor.Hpd) { '' } else { $predecessor.Hpd.ToString('F2', $culture) })
    $rawLag = Get-SourceCell $relationships $sourceRow 'lag_hr_cnt'
    $lag = Get-OptionalNumber $rawLag -BlankIsZero
    # Preserve the documented legacy display default; absent lag does NOT establish free_float.
    $expected['lag'] = $(if ($null -eq $lag) { '' } elseif ($lag -eq 0) { '0' } else { Get-ExpectedDays $rawLag $predecessor.Hpd })
    $expected['total_float'] = $(if ($null -eq $successor -or (Get-SourceCell $rawActivities $successor.Row 'status_code') -ieq 'TK_Complete') { '' } else { Get-ExpectedDays (Get-SourceCell $rawActivities $successor.Row 'total_float_hr_cnt') $successor.Hpd 1 })
    foreach ($pair in $expected.GetEnumerator()) {
        Assert-Audit ($outputRow[$relationshipOutput.Indexes[$pair.Key]] -ceq $pair.Value) "06.$($pair.Key) differs from its independent endpoint/HPD lookup at row $($i + 1)."
        $checkedDerivedValues++
    }
}

# Baseline is the exact ordered subset at the globally earliest filename month.
$taskTable = $tables['01_XER_TASK']
$monthIndex = $taskTable.Indexes['MonthUpdate']
$monthSet = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
foreach ($row in $taskTable.Rows) { if ($row[$monthIndex].Length -gt 0) { $null = $monthSet.Add($row[$monthIndex]) } }
$months = @($monthSet | Sort-Object)
$baselineRows = [Collections.Generic.List[string[]]]::new()
if ($months.Count -gt 0) {
    foreach ($row in $taskTable.Rows) { if ($row[$monthIndex] -ceq $months[0]) { $baselineRows.Add($row) } }
}
$baseline = $tables['04_XER_BASELINE']
Assert-Audit ($baseline.Rows.Count -eq $baselineRows.Count) '04 baseline does not contain the global earliest-month rows.'
for ($i = 0; $i -lt $baselineRows.Count; $i++) {
    for ($j = 0; $j -lt $baseline.Headers.Length; $j++) {
        Assert-Audit ($baseline.Rows[$i][$j] -ceq $baselineRows[$i][$j]) '04 baseline changed a selected activity cell.'
    }
}

# Detailed calendars must form seven distinct standard days plus dated replacements.
$detail = $tables['11_XER_CALENDAR_DETAILED']
$standardDays = [Collections.Generic.Dictionary[string,Collections.Generic.HashSet[int]]]::new([StringComparer]::Ordinal)
$detailIdentities = [Collections.Generic.HashSet[ValueTuple[string,string]]]::new()
foreach ($row in $detail.Rows) {
    $key = $row[$detail.Indexes['clndr_id_key']]
    $date = $row[$detail.Indexes['date']]
    $day = [int](Read-Number $row[$detail.Indexes['day_of_week_num']] '11.day_of_week_num')
    $hours = Read-Number $row[$detail.Indexes['work_hours']] '11.work_hours'
    Assert-Audit ($day -ge 1 -and $day -le 7 -and $hours -ge 0 -and $hours -le 24) '11 has an invalid weekday or daily work-hour total.'
    $working = $hours -gt 0
    Assert-Audit ($row[$detail.Indexes['working_day']] -ceq $(if ($working) { 'Y' } else { 'N' })) '11 working_day disagrees with hours.'
    Assert-Audit ($row[$detail.Indexes['working_day_int']] -ceq $(if ($working) { '1' } else { '0' })) '11 working_day_int disagrees with hours.'
    $expectedDay = if ($day -eq 7) { [DayOfWeek]::Sunday } else { [DayOfWeek]$day }
    Assert-Audit ($row[$detail.Indexes['day_of_week']] -ceq $expectedDay.ToString()) '11 weekday name and number disagree.'
    if ($date.Length -eq 0) {
        if (-not $standardDays.ContainsKey($key)) { $standardDays.Add($key, [Collections.Generic.HashSet[int]]::new()) }
        Assert-Audit ($standardDays[$key].Add($day)) '11 repeats a standard weekday.'
        Assert-Audit ($row[$detail.Indexes['exception_type']] -ceq 'Standard') '11 standard rule is mislabeled.'
        $identity = 'weekday-' + $day
    } else {
        $parsedDate = [DateTime]::ParseExact($date, 'yyyy-MM-dd', $culture)
        Assert-Audit ($parsedDate.DayOfWeek -eq $expectedDay) '11 dated exception weekday is incorrect.'
        $expectedType = if ($working) { 'Exception - Working' } else { 'Exception - Non-Working' }
        Assert-Audit ($row[$detail.Indexes['exception_type']] -ceq $expectedType) '11 dated exception is mislabeled.'
        $identity = $date
    }
    Assert-Audit ($detailIdentities.Add([ValueTuple[string,string]]::new($key, $identity))) '11 repeats a calendar rule/exception identity.'
}
Assert-Audit ($standardDays.Count -eq $keySets['10_XER_CALENDAR'].Count) '11 omitted a calendar.'
foreach ($days in $standardDays.Values) { Assert-Audit ($days.Count -eq 7) '11 omitted a standard weekday.' }

# Reconcile against rounded contributions per raw assignment, before aggregation.
# The aggregate key contains the internal occurrence token, not a filename.
$expectedQuantities = [Collections.Generic.Dictionary[ValueTuple[string,string,string,string],decimal]]::new()
$fallbackRows = 0
if (-not $ExcludeResourceDistribution) {
$rawTasks = $store.GetTable('TASK')
$rawResources = $store.GetTable('RSRC')
$taskByPublicKey = [Collections.Generic.Dictionary[string,object]]::new([StringComparer]::Ordinal)
$resourceByPublicKey = [Collections.Generic.Dictionary[string,object]]::new([StringComparer]::Ordinal)
foreach ($row in $rawTasks.Rows) { $taskByPublicKey.Add((Get-Key $row.SourceFilename (Get-SourceCell $rawTasks $row 'task_id')), $row) }
foreach ($row in $rawResources.Rows) { $resourceByPublicKey.Add((Get-Key $row.SourceFilename (Get-SourceCell $rawResources $row 'rsrc_id')), $row) }
$actualQuantities = [Collections.Generic.Dictionary[ValueTuple[string,string,string,string],decimal]]::new()
$assignments = $store.GetTable('TASKRSRC')
$assignmentByOccurrence = [Collections.Generic.Dictionary[ValueTuple[string,int],object]]::new()
$sourceOrdinals = [Collections.Generic.Dictionary[string,int]]::new([StringComparer]::Ordinal)
$expectedIssueRows = [Collections.Generic.Dictionary[ValueTuple[string,int,string],string]]::new()
$actualWarningCount = 0
$remainingWarningCount = 0
$unknownQuantityWarnings = 0
foreach ($row in $assignments.Rows) {
    $ordinal = 1
    if ($sourceOrdinals.ContainsKey($row.SourceToken)) { $ordinal = $sourceOrdinals[$row.SourceToken] + 1 }
    $sourceOrdinals[$row.SourceToken] = $ordinal
    $occurrence = [ValueTuple[string,int]]::new($row.SourceFilename, $ordinal)
    $assignmentByOccurrence.Add($occurrence, $row)
    $taskId = (Get-SourceCell $assignments $row 'task_id').Trim()
    $resourceId = (Get-SourceCell $assignments $row 'rsrc_id').Trim()
    $task = $taskByPublicKey[(Get-Key $row.SourceFilename $taskId)]
    $status = (Get-SourceCell $rawTasks $task 'status_code').Trim()
    foreach ($portion in @('Actual','Remaining')) {
        $quantity = Get-SourcePortionQuantity $assignments $row $portion
        if ($quantity.Known -and $quantity.Value -eq 0 -and -not $quantity.Invalid) { continue }
        $issue = ''
        if ($quantity.Invalid) { $issue = 'Quantity' }
        elseif ($status -notin @('TK_NotStart','TK_Active','TK_Complete')) { $issue = 'Status' }
        elseif (($portion -ceq 'Actual' -and $status -ieq 'TK_NotStart') -or
            ($portion -ceq 'Remaining' -and $status -ieq 'TK_Complete')) { $issue = 'Status' }
        else {
            $startField = if ($portion -ceq 'Actual') { 'act_start_date' } else { 'restart_date' }
            $endField = if ($portion -ceq 'Actual') { 'act_end_date' } else { 'reend_date' }
            $start = Get-ExpectedDate (Get-SourceCell $assignments $row $startField)
            $endRaw = Get-SourceCell $assignments $row $endField
            $end = if (-not [string]::IsNullOrWhiteSpace($endRaw)) { Get-ExpectedDate $endRaw }
                elseif ($portion -ceq 'Actual' -and $status -ieq 'TK_Active') { $projectDates[[ValueTuple[string,string]]::new($row.SourceToken, (Get-SourceCell $rawTasks $task 'proj_id').Trim())] }
                else { '' }
            if (-not $start -or -not $end) { $issue = 'PeriodMissingOrMalformed' }
            elseif ($end -clt $start -or ($portion -ceq 'Remaining' -and $end -ceq $start)) { $issue = 'PeriodOrder' }
        }
        if ($issue) { $expectedIssueRows.Add([ValueTuple[string,int,string]]::new($row.SourceFilename, $ordinal, $portion), $issue) }
        if ($quantity.Known) {
            $flag = if ($portion -ceq 'Actual') { '1' } else { '0' }
            $key = [ValueTuple[string,string,string,string]]::new($row.SourceToken, $taskId, $resourceId, $flag)
            Add-Quantity $expectedQuantities $key ([decimal]::Round($quantity.Value, 4, [MidpointRounding]::ToEven))
        }
    }
}
$distribution = $tables['15_XER_RESOURCE_DISTRIBUTION']
foreach ($row in $distribution.Rows) {
    $task = $taskByPublicKey[$row[$distribution.Indexes['task_id_key']]]
    $resource = $resourceByPublicKey[$row[$distribution.Indexes['rsrc_id_key']]]
    Assert-Audit ($task.SourceToken -ceq $resource.SourceToken) '15 combined different input occurrences.'
    Assert-Audit ($row[$distribution.Indexes['FileName']] -ceq $task.OriginalSourceFilename) '15 changed original filename provenance.'
    foreach ($column in @('task_code','status_code')) {
        Assert-Audit ($row[$distribution.Indexes[$column]] -ceq (Get-SourceCell $rawTasks $task $column)) "15 changed TASK descriptive field $column."
    }
    foreach ($column in @('rsrc_name','rsrc_short_name','rsrc_type')) {
        Assert-Audit ($row[$distribution.Indexes[$column]] -ceq (Get-SourceCell $rawResources $resource $column)) "15 changed RSRC descriptive field $column."
    }
    $projectKey = Get-Key $task.SourceFilename (Get-SourceCell $rawTasks $task 'proj_id')
    Assert-Audit ($row[$distribution.Indexes['proj_id_key']] -ceq $projectKey) '15 changed assignment project meaning.'
    $calendarId = if ((Get-SourceCell $rawTasks $task 'task_type').Trim() -ieq 'TT_Rsrc') { Get-SourceCell $rawResources $resource 'clndr_id' } else { Get-SourceCell $rawTasks $task 'clndr_id' }
    Assert-Audit ($row[$distribution.Indexes['clndr_id_key']] -ceq (Get-Key $task.SourceFilename $calendarId)) '15 selected the wrong task/resource calendar.'
    $actualFlag = $row[$distribution.Indexes['is_actual']]
    Assert-Audit ($actualFlag -cin @('0','1')) '15 is_actual must be 0 or 1.'
    $key = [ValueTuple[string,string,string,string]]::new($task.SourceToken, (Get-SourceCell $rawTasks $task 'task_id').Trim(), (Get-SourceCell $rawResources $resource 'rsrc_id').Trim(), $actualFlag)
    $quantity = Read-Number $row[$distribution.Indexes['monthly_quantity']] '15.monthly_quantity'
    Assert-Audit ($quantity -ge 0) '15 contains a negative monthly quantity.'
    Add-Quantity $actualQuantities $key $quantity
    $hours = Read-Number $row[$distribution.Indexes['month_working_hours']] '15.month_working_hours'
    $totalHours = Read-Number $row[$distribution.Indexes['total_working_hours']] '15.total_working_hours'
    Assert-Audit ($hours -ge 0 -and $totalHours -ge $hours) '15 monthly work hours exceed period work hours.'
    $month = [DateTime]::ParseExact($row[$distribution.Indexes['distribution_month']], 'yyyy-MM-dd', $culture)
    Assert-Audit ($month.Day -eq 1) '15 distribution month is not a month-start bucket.'
    $kind = $row[$distribution.Indexes['distribution_type']]
    Assert-Audit ($kind -cin @('Working Hours','Resource Curve','Remaining Units Profile','Actual Recorded Date','Actual Elapsed Time')) '15 has an unrecognized distribution type.'
    if ($kind -in @('Actual Recorded Date','Actual Elapsed Time')) {
        Assert-Audit ($actualFlag -ceq '1' -and $hours -eq 0 -and $totalHours -eq 0) '15 actual fallback invented calendar work hours or affected remaining units.'
        $fallbackRows++
    }
}
foreach ($row in $quality.Rows) {
    if ($row[$quality.Indexes['allocation_portion']] -ceq '') { continue }
    $occurrence = [ValueTuple[string,int]]::new($row[$quality.Indexes['source_namespace']], [int]$row[$quality.Indexes['source_row_number']])
    $portion = $row[$quality.Indexes['allocation_portion']]
    Assert-Audit ($portion -cin @('Actual','Remaining')) 'Companion has no explicit allocation portion.'
    $issueKey = [ValueTuple[string,int,string]]::new($occurrence.Item1, $occurrence.Item2, $portion)
    Assert-Audit ($expectedIssueRows.ContainsKey($issueKey)) "Companion warning $($row[$quality.Indexes['issue_code']]) at $issueKey was not independently predicted by source quantity/status/period checks. Add a dedicated independent oracle before extending this harness to another unsupported case."
    $issueKind = $expectedIssueRows[$issueKey]
    $null = $expectedIssueRows.Remove($issueKey)
    Assert-Audit (-not [string]::IsNullOrWhiteSpace($row[$quality.Indexes['issue_code']]) -and
        -not [string]::IsNullOrWhiteSpace($row[$quality.Indexes['message']])) 'Companion warning lacks its classification or explanation.'
    if ($portion -ceq 'Actual') { $actualWarningCount++ } else { $remainingWarningCount++ }
    $assignment = $assignmentByOccurrence[$occurrence]
    Assert-Audit ($null -ne $assignment) 'Companion cannot be matched to its source assignment occurrence.'
    Assert-Audit ($row[$quality.Indexes['diagnostic_schema_version']] -ceq '1.2' -and $row[$quality.Indexes['severity']] -ceq 'Warning') 'Companion schema/severity is invalid.'
    Assert-Audit ($row[$quality.Indexes['table_name']] -ceq '15_XER_RESOURCE_DISTRIBUTION') 'Companion names the wrong affected table.'
    Assert-Audit ($row[$quality.Indexes['FileName']] -ceq $assignment.OriginalSourceFilename) 'Companion lost original filename provenance.'
    foreach ($column in @('taskrsrc_id','act_start_date','act_end_date','act_reg_qty','act_ot_qty','restart_date','reend_date','remain_qty','curv_id','remain_crv')) {
        Assert-Audit ($row[$quality.Indexes[$column]] -ceq (Get-SourceCell $assignments $assignment $column)) "Companion changed source $column."
    }
    $taskId = (Get-SourceCell $assignments $assignment 'task_id').Trim()
    $resourceId = (Get-SourceCell $assignments $assignment 'rsrc_id').Trim()
    $task = $taskByPublicKey[(Get-Key $assignment.SourceFilename $taskId)]
    $projectId = (Get-SourceCell $rawTasks $task 'proj_id').Trim()
    $status = (Get-SourceCell $rawTasks $task 'status_code').Trim()
    $expectedIssueCode = switch ($issueKind) {
        'Quantity' { if ($portion -ceq 'Actual') { 'ACTUAL_QUANTITY_INVALID' } else { 'REMAINING_QUANTITY_INVALID' } }
        'Status' {
            if ($status -notin @('TK_NotStart','TK_Active','TK_Complete')) { 'ASSIGNMENT_CONTEXT_INVALID' }
            elseif ($portion -ceq 'Actual') { 'ACTUAL_ON_UNSTARTED' } else { 'REMAINING_ON_COMPLETED' }
        }
        'PeriodMissingOrMalformed' { if ($portion -ceq 'Actual') { 'ACTUAL_PERIOD_INVALID' } else { 'REMAINING_PERIOD_INVALID' } }
        'PeriodOrder' { if ($portion -ceq 'Actual') { 'ACTUAL_FINISH_BEFORE_START' } else { 'REMAINING_PERIOD_INVALID' } }
    }
    Assert-Audit ($row[$quality.Indexes['issue_code']] -ceq $expectedIssueCode) "Companion issue code does not match independently detected source $issueKind."
    Assert-Audit ($row[$quality.Indexes['status_code']] -ceq $status) 'Companion changed the source activity state.'
    foreach ($pair in @(@('task_id_key',$taskId),@('rsrc_id_key',$resourceId),@('proj_id_key',$projectId),@('taskrsrc_id_key',(Get-SourceCell $assignments $assignment 'taskrsrc_id')))) {
        Assert-Audit ($row[$quality.Indexes[$pair[0]]] -ceq (Get-Key $assignment.SourceFilename $pair[1])) 'Companion key is not source-qualified.'
    }
    $sourceProject = @($rawProjects.Rows | Where-Object { $_.SourceToken -ceq $assignment.SourceToken -and (Get-SourceCell $rawProjects $_ 'proj_id').Trim() -ceq $projectId })
    Assert-Audit ($sourceProject.Count -eq 1 -and $row[$quality.Indexes['project_data_date']] -ceq (Get-SourceCell $rawProjects $sourceProject[0] 'last_recalc_date')) 'Companion changed the raw project Data Date.'
    $quantity = Get-SourcePortionQuantity $assignments $assignment $portion
    $quantityColumn = if ($portion -ceq 'Actual') { 'unallocated_actual_quantity' } else { 'unallocated_remaining_quantity' }
    $otherQuantityColumn = if ($portion -ceq 'Actual') { 'unallocated_remaining_quantity' } else { 'unallocated_actual_quantity' }
    Assert-Audit ($row[$quality.Indexes[$otherQuantityColumn]] -ceq '') 'Companion populated both portion totals on a single warning.'
    if ($quantity.Known) {
        $unallocated = Read-Number $row[$quality.Indexes[$quantityColumn]] "Companion.$quantityColumn"
        Assert-Audit ($unallocated -eq [decimal]::Round($quantity.Value, 4, [MidpointRounding]::ToEven)) 'Companion did not preserve the rounded signed source portion quantity.'
        $flag = if ($portion -ceq 'Actual') { '1' } else { '0' }
        $key = [ValueTuple[string,string,string,string]]::new($assignment.SourceToken, $taskId, $resourceId, $flag)
        Add-Quantity $actualQuantities $key $unallocated
    } else {
        Assert-Audit ($row[$quality.Indexes[$quantityColumn]] -ceq '' -and $issueKind -ceq 'Quantity') 'An unrepresentable amount must have a quantity warning and a blank numeric total.'
        $unknownQuantityWarnings++
    }
}
Assert-Audit ($expectedIssueRows.Count -eq 0) 'An invalid source quantity, status or period was not represented in the companion.'
Assert-Audit ($actualQuantities.Count -eq $expectedQuantities.Count) '15 omitted or invented a source/task/resource/actual allocation group.'
foreach ($pair in $expectedQuantities.GetEnumerator()) {
    Assert-Audit ($actualQuantities.ContainsKey($pair.Key) -and $actualQuantities[$pair.Key] -eq $pair.Value) 'Distributed plus unallocated units failed source assignment quantity reconciliation.'
}
}

foreach ($name in $tableSources.Keys) {
    [pscustomobject]@{ Table = $name; Rows = $tables[$name].Rows.Count; Columns = $tables[$name].Headers.Length }
}
[pscustomobject]@{
    Validation = 'Passed shared Web stream/memory export contracts (not native P6 parity)'
    OrderedInputOccurrences = $sourceTokens.Count
    Tables = $tables.Count
    FilesIncludingCompanion = $bytesByTable.Count
    DataQualityWarnings = $export.WarningCount
    GeneralWarnings = $generalWarningCount
    DiagnosticEvidenceCells = $checkedDiagnosticEvidenceCells
    AllocationWarnings = $(if ($ExcludeResourceDistribution) { $null } else { $actualWarningCount + $remainingWarningCount })
    ActualAllocationWarnings = $(if ($ExcludeResourceDistribution) { $null } else { $actualWarningCount })
    RemainingAllocationWarnings = $(if ($ExcludeResourceDistribution) { $null } else { $remainingWarningCount })
    UnknownQuantityWarnings = $(if ($ExcludeResourceDistribution) { $null } else { $unknownQuantityWarnings })
    ResourceDistributionScope = $(if ($ExcludeResourceDistribution) { 'Explicitly excluded; NOT validated' } else { 'Included and reconciled' })
    ExactRetainedSourceCells = $checkedRawCells
    FiniteDerivedNumericCells = $checkedNumericCells
    IndependentDerivedValues = $checkedDerivedValues
    ReconciledResourceGroups = $(if ($ExcludeResourceDistribution) { $null } else { $expectedQuantities.Count })
    ActualFallbackRows = $(if ($ExcludeResourceDistribution) { $null } else { $fallbackRows })
} | ConvertTo-Json -Compress
