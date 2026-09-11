param(
    [string]$ProgrammeDefinition = 'C:\Users\ricar\Documents\Code\Programme Review\Programme-Review\Project Review - Programme (datalake).SemanticModel\definition',
    [string]$TenderDefinition = 'C:\Users\ricar\Documents\Code\Tender-Review\Project Review - Tender Programme (datalake).SemanticModel\definition',
    [string]$TomDirectory = 'C:\Users\ricar\.nuget\packages\microsoft.analysisservices\19.114.8\lib\net8.0'
)

# Offline regression: literal M contracts, TOM metadata, and independently translated
# row-context cases. This does not execute M/DAX, query sources, or prove runtime RLS.
$ErrorActionPreference = 'Stop'
$checks = 0
$graphChecks = 0
$scenarioChecks = 0
function Assert-Calendar([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
    $script:checks++
}
function Assert-CalendarGraph([bool]$Condition, [string]$Message) {
    Assert-Calendar $Condition $Message
    $script:graphChecks++
}
function Read-CalendarExpression([string]$Content, [string]$Name) {
    $match = [regex]::Match($Content, '(?ms)^expression ' + [regex]::Escape($Name) + ' =\s*(.*?)^\tlineageTag:')
    Assert-Calendar $match.Success "Missing expression $Name"
    $match.Groups[1].Value
}
function Get-CalendarPaths($Edges, [string]$Current, [string]$Target, [string[]]$Visited) {
    if ($Current -ceq $Target) { return 1 }
    if ($Visited -ccontains $Current) { return 0 }
    $count = 0
    foreach ($edge in @($Edges | Where-Object { $_.From -ceq $Current })) {
        $count += Get-CalendarPaths $Edges $edge.To $Target ($Visited + $Current)
    }
    $count
}

foreach ($dll in @('Microsoft.AnalysisServices.Core.dll', 'Microsoft.AnalysisServices.Tabular.dll', 'Microsoft.AnalysisServices.Tabular.Json.dll')) {
    [Reflection.Assembly]::LoadFrom((Join-Path $TomDirectory $dll)) | Out-Null
}
$business = @('clndr_name','clndr_type','date','day_of_week','working_day','work_hours','exception_type','clndr_id_key','MonthUpdate','day_of_week_num','working_day_int')
$modelResults = @()
foreach ($profile in @(@{Name='Programme';Root=$ProgrammeDefinition;Prefix='Xer';Audit='XER CSV Refresh Audit'},@{Name='Tender';Root=$TenderDefinition;Prefix='Tender';Audit='Tender CSV Refresh Audit'})) {
    $root = (Resolve-Path -LiteralPath $profile.Root).Path
    $model = [Microsoft.AnalysisServices.Tabular.TmdlSerializer]::DeserializeModelFromFolder($root)
    $text = Get-Content -LiteralPath (Join-Path $root 'expressions.tmdl') -Raw
    $contracts = Read-CalendarExpression $text ($profile.Prefix + 'CsvTableContracts')
    $contractMatch = [regex]::Match($contracts, '(?s)#"11_XER_CALENDAR_DETAILED"\s*=\s*\[(.*?)\],\s*#"12_XER_RSRC"')
    Assert-Calendar $contractMatch.Success 'Missing ordered table 11 contract between 10 and12.'
    $contract = $contractMatch.Groups[1].Value
    $columns = [regex]::Match($contract, '(?s)\bColumns\s*=\s*\{([^{}]*)\}')
    $actual = @([regex]::Matches($columns.Groups[1].Value, '"([^"]+)"') | ForEach-Object { $_.Groups[1].Value })
    Assert-Calendar (($actual -join '|') -ceq ($business -join '|')) 'Table11 business headers/order/case changed.'
    Assert-Calendar ($contract -match 'RequiredColumns = \{"MonthUpdate"\}') 'Blank-key or invalid calendar evidence became required.'
    Assert-Calendar ($contract -match 'UniqueKeys = \{\}') 'Table11 must not deduplicate calendar keys.'
    Assert-Calendar ($contract.Contains('{"working_day", type nullable text}') -and $contract.Contains('{"date", type nullable date}') -and $contract.Contains('{"work_hours", type nullable number}')) 'Nullable table11 types changed.'
    $helper = Read-CalendarExpression $text 'fnXerCsvValidateCalendarDetails'
    foreach ($literal in @('current[MonthUpdate] = null','KnownCountsValid','MonthCountsValid','List.RemoveNulls(source[clndr_id_key])','"10_XER_CALENDAR"','else source')) {
        Assert-Calendar ($helper.Contains($literal)) "Missing calendar context validator clause $literal"
    }
    Assert-Calendar (-not $helper.Contains('Table.Buffer')) 'Do not buffer all table11 rows as records.'
    $readName = if ($profile.Name -ceq 'Tender') { 'fnTenderCsvReadTable' } else { 'fnXerCsvReadBundleTable' }
    $reader = Read-CalendarExpression $text $readName
    Assert-Calendar ($reader.Contains('then fnXerCsvValidateCalendarDetails(') -and $reader.Contains('Table.AddColumn(CalendarChecked, "ProjectKey"') -and $reader.Contains('Table.AddColumn(WithProject, "IsCsvSource"')) 'Calendar validator/project identity is not on the output path.'
    Assert-Calendar ($reader.Contains('bundle[ProjectCode]')) 'Manual calendar project identity must come from bundle metadata.'
    $table = $model.Tables.Find('11 XER_CALENDAR_DETAILED')
    Assert-Calendar ($null -ne $table -and $table.Columns.Count -eq 13) 'Table11 needs11 business columns and two hidden technical columns.'
    foreach ($name in $business) {
        Assert-Calendar ($null -ne $table.Columns.Find($name)) "Missing table11 model column $name"
        Assert-Calendar ($table.Columns.Find($name).SourceColumn -ceq $name) "Source-column casing mismatch for $name"
    }
    Assert-Calendar ($table.Columns.Find('IsCsvSource').IsHidden -and $table.Columns.Find('ProjectKey').IsHidden) 'Technical source/security columns must be hidden.'
    Assert-Calendar ($table.Columns.Find('working_day').DataType.ToString() -ceq 'String') 'Y/N must not become a Boolean.'
    Assert-Calendar (-not $table.Columns.Find('clndr_id_key').IsKey -and -not $table.Columns.Find('date').IsKey) 'Detail calendar/date is not a unique key.'
    $partition = $table.Partitions[0].Source.Expression
    Assert-Calendar ($partition.Contains('fnAthenaSource("11_xer_calendar_detailed"') -and $partition.Contains('t.""monthupdate"" AS ""MonthUpdate""')) 'Athena11 table or MonthUpdate alias is missing.'
    Assert-Calendar ($partition.Contains('CsvRouting[HasAthenaProjects] then AthenaFinal else Empty') -and $partition.Contains('CsvRouting[IsActive] then Table.Combine')) 'CSV-only/Athena-only lazy ownership branches changed.'
    Assert-Calendar (-not $partition.Contains('Table.Distinct') -and -not $partition.Contains('Table.SelectRows') -and -not $partition.Contains('otherwise 0')) 'Calendar evidence may be filtered/deduplicated/zero-filled.'
    $audit = $model.Tables.Find($profile.Audit).Partitions[0].Source.Expression
    Assert-Calendar ($audit.Contains('"Check", "11_XER_CALENDAR_DETAILED"')) 'Missing table11 refresh audit count.'
    $relationships = @($model.Relationships | Where-Object { $_.FromTable.Name -ceq $table.Name -or $_.ToTable.Name -ceq $table.Name })
    Assert-CalendarGraph ($relationships.Count -eq 2) 'Table11 requires exactly its project and calendar relationships.'
    foreach ($relationship in $relationships) {
        Assert-CalendarGraph ($relationship.FromTable.Name -ceq $table.Name -and $relationship.IsActive -and $relationship.FromCardinality.ToString() -ceq 'Many' -and $relationship.ToCardinality.ToString() -ceq 'One') 'Table11 relationship direction/cardinality is wrong.'
        Assert-CalendarGraph ($relationship.CrossFilteringBehavior.ToString() -ceq 'OneDirection' -and $relationship.SecurityFilteringBehavior.ToString() -ceq 'OneDirection') 'Table11 must not create bidirectional/security filter propagation.'
    }
    Assert-CalendarGraph (@($relationships | Where-Object { $_.FromColumn.Name -ceq 'ProjectKey' -and $_.ToTable.Name -ceq 'Project_Dimension' -and $_.ToColumn.Name -ceq 'ProjectKey' }).Count -eq 1) 'Missing direct project protection independent of task calendar usage.'
    Assert-CalendarGraph (@($relationships | Where-Object { $_.FromColumn.Name -ceq 'clndr_id_key' -and $_.ToTable.Name -ceq '10 XER_CALENDARS' -and $_.ToColumn.Name -ceq 'clndr_id_key' }).Count -eq 1) 'Missing calendar10→11 FK.'
    foreach ($security in @($false,$true)) {
        $edges = foreach ($relationship in @($model.Relationships | Where-Object IsActive)) {
            [pscustomobject]@{From=$relationship.ToTable.Name;To=$relationship.FromTable.Name}
            $direction = if ($security) { $relationship.SecurityFilteringBehavior.ToString() } else { $relationship.CrossFilteringBehavior.ToString() }
            if ($direction -ceq 'BothDirections') { [pscustomobject]@{From=$relationship.FromTable.Name;To=$relationship.ToTable.Name} }
        }
        Assert-CalendarGraph ((Get-CalendarPaths $edges 'Project_Dimension' $table.Name @()) -eq 1) 'Multiple project-filter paths to11 are ambiguous.'
        Assert-CalendarGraph ((Get-CalendarPaths $edges $table.Name '01 XER_TASK' @()) -eq 0) 'Table11 unexpectedly filters existing task data.'
    }
    $modelResults += @{Profile=$profile.Name;Tables=$model.Tables.Count;Relationships=$model.Relationships.Count}
}

# Independent translation of the row-context rules. Namespace/type conversion and
# bundle totals happen upstream in M; include the same preconditions explicitly here.
function Test-CalendarContext($Rows, $Metadata, [string[]]$CalendarKeys, [string]$NamespacePrefix='CSV::P::TENDER::') {
    $total = ($Metadata | Measure-Object -Property Count -Sum).Sum
    if (@($Rows).Count -ne $total) { return $false }
    foreach ($row in $Rows) {
        if (-not $row.Month) { return $false }
        if ($row.Key) {
            if (-not $row.Key.StartsWith($NamespacePrefix,[StringComparison]::Ordinal) -or $CalendarKeys -cnotcontains $row.Key) { return $false }
            $token = $row.Key.Split('::')[3]
            if (@($Metadata | Where-Object { $_.Token -ceq $token -and $_.Month -ceq $row.Month }).Count -eq 0) { return $false }
        } elseif (@($Metadata | Where-Object { $_.Month -ceq $row.Month }).Count -eq 0) { return $false }
    }
    foreach ($entry in $Metadata) {
        $known = @($Rows | Where-Object { $_.Key -and $_.Key.Split('::')[3] -ceq $entry.Token }).Count
        if ($known -gt $entry.Count) { return $false }
    }
    foreach ($month in @($Metadata.Month | Sort-Object -Unique)) {
        $expected = ($Metadata | Where-Object { $_.Month -ceq $month } | Measure-Object -Property Count -Sum).Sum
        if (@($Rows | Where-Object { $_.Month -ceq $month }).Count -ne $expected) { return $false }
    }
    return $true
}
function New-CalendarRow([AllowNull()][string]$Key,[string]$Month) { @{Key=$Key;Month=$Month;Date=$null;Hours=$null;WorkingDay=$null;DayNumber=$null;WorkingDayInt=$null} }
function Assert-CalendarScenario([string]$Name, $Rows, $Metadata, [string[]]$Keys, [bool]$Expected) {
    Assert-Calendar ((Test-CalendarContext $Rows $Metadata $Keys) -eq $Expected) "Translated case failed: $Name"
    $script:scenarioChecks++
}
$a = 'CSV::P::TENDER::20260910::4962'
$b = 'CSV::P::TENDER::20260911::4962'
$m1 = '2026-08-01'
$m2 = '2026-09-01'
$ma = @{Token='20260910';Month=$m1;Count=2;Filename='repeated.xer';Hash='same'}
$mb = @{Token='20260911';Month=$m1;Count=2;Filename='repeated.xer';Hash='same'}
$mb2 = @{Token='20260911';Month=$m2;Count=2;Filename='repeated.xer';Hash='same'}
$ra = New-CalendarRow $a $m1
$rb = New-CalendarRow $b $m1
$rb2 = New-CalendarRow $b $m2
$blank1 = New-CalendarRow $null $m1
$blank2 = New-CalendarRow $null $m2
Assert-CalendarScenario 'same filename/hash/native ID independent sources' @($ra,$ra,$rb,$rb) @($ma,$mb) @($a,$b) $true
Assert-CalendarScenario 'reverse repeated input order' @($rb,$rb,$ra,$ra) @($mb,$ma) @($b,$a) $true
Assert-CalendarScenario 'header-only10/11' @() @(@{Token='20260910';Month=$m1;Count=0}) @() $true
Assert-CalendarScenario 'single-source unknown calendar marker remains nullable' @($blank1,$blank1) @($ma) @() $true
Assert-CalendarScenario 'all blank-key rows across same-date sources unassigned' @($blank1,$blank1,$blank1,$blank1) @($ma,$mb) @() $true
Assert-CalendarScenario 'same-date residual source attribution unknown' @($ra,$rb,$blank1,$blank1) @($ma,$mb) @($a,$b) $true
Assert-CalendarScenario 'different-date residuals reconcile' @($ra,$blank1,$rb2,$blank2) @($ma,$mb2) @($a,$b) $true
Assert-CalendarScenario 'known-key source overrepresented' @($ra,$ra,$ra,$rb) @($ma,$mb) @($a,$b) $false
Assert-CalendarScenario 'known source key paired with other source MonthUpdate' @($ra,(New-CalendarRow $a $m2),$rb2,$blank2) @($ma,$mb2) @($a,$b) $false
Assert-CalendarScenario 'wrong blank-key date bucket distribution' @($ra,$blank1,$blank1,$rb2) @($ma,$mb2) @($a,$b) $false
Assert-CalendarScenario 'blank-key MonthUpdate absent from manifest' @($ra,(New-CalendarRow $null '2020-01-01')) @($ma) @($a) $false
Assert-CalendarScenario 'missing nonblank10 foreign key' @($ra,$ra) @($ma) @() $false
Assert-CalendarScenario 'blank MonthUpdate rejected' @($ra,(New-CalendarRow $null '')) @($ma) @($a) $false
Assert-CalendarScenario 'cross-project namespace rejected' @((New-CalendarRow 'CSV::OTHER::TENDER::20260910::4962' $m1),$ra) @($ma) @($a,'CSV::OTHER::TENDER::20260910::4962') $false
Assert-CalendarScenario 'truncated total rejected' @($ra) @($ma) @($a) $false

[ordered]@{Result='PASS';Assertions=$checks;GraphAssertions=$graphChecks;TranslatedScenarios=$scenarioChecks;Models=$modelResults;Scope='Literal contracts, TOM graph, and independent translated row-context cases only; not M/DAX execution, source refresh, or runtime RLS proof.';ManualGates=@('Athena source11 schema/types','Desktop and Service refresh','View-as allowed/denied projects including unused/resource and blank-key calendars','Existing invalid10 calendar-identity boundary is not repaired by table11')} | ConvertTo-Json -Depth 5
