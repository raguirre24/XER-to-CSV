param(
    [string]$DefinitionRoot = 'C:\Users\ricar\Documents\Code\Tender-Review\Project Review - Tender Programme (datalake).SemanticModel\definition',
    [string]$CoreAssemblyPath = (Join-Path $PSScriptRoot '../XerToCsvConverter.Core/bin/Debug/net8.0/XerToCsvConverter.Core.dll'),
    [string]$TomDirectory = 'C:\Users\ricar\.nuget\packages\microsoft.analysisservices\19.114.8\lib\net8.0'
)

# Offline only: actual Core execution, literal M/DAX inspection, TOM relationship
# metadata, and an independently translated OR truth table. No M/DAX evaluation,
# connector access, Power BI refresh, or proof of runtime RLS behaviour.
$ErrorActionPreference = 'Stop'
$checks = 0
$graphChecks = 0
function Assert-Tender([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
    $script:checks++
}
function Assert-Graph([bool]$Condition, [string]$Message) {
    Assert-Tender $Condition $Message
    $script:graphChecks++
}
function Read-ModelFile([string]$RelativePath) {
    Get-Content -LiteralPath (Join-Path $DefinitionRoot $RelativePath) -Raw
}
function Read-Expression([string]$Text, [string]$Name) {
    $match = [regex]::Match($Text, '(?ms)^expression ' + [regex]::Escape($Name) + ' =\s*(.*?)^\tlineageTag:')
    Assert-Tender $match.Success "Missing expression $Name."
    $match.Groups[1].Value
}

[Reflection.Assembly]::LoadFrom((Resolve-Path -LiteralPath $CoreAssemblyPath).Path) | Out-Null
foreach ($dll in @('Microsoft.AnalysisServices.Core.dll', 'Microsoft.AnalysisServices.Tabular.dll', 'Microsoft.AnalysisServices.Tabular.Json.dll')) {
    [Reflection.Assembly]::LoadFrom((Join-Path $TomDirectory $dll)) | Out-Null
}
$model = [Microsoft.AnalysisServices.Tabular.TmdlSerializer]::DeserializeModelFromFolder((Resolve-Path -LiteralPath $DefinitionRoot).Path)
$expressions = Read-ModelFile 'expressions.tmdl'
$dimension = Read-ModelFile 'tables/Project_Dimension.tmdl'
$role = Read-ModelFile 'roles/Project Access.tmdl'
$audit = Read-ModelFile 'tables/Tender CSV Refresh Audit.tmdl'
$project = Read-ModelFile 'tables/02 XER_PROJECT.tmdl'
$catalogue = Read-ModelFile 'tables/dbo_project.tmdl'
$normalizer = Read-Expression $expressions 'fnTenderCsvNormalizeState'
Assert-Tender ($normalizer.Contains('Text.Upper(Text.Trim(value), "")')) 'M State normalizer must use outer trim and invariant uppercase.'
Assert-Tender ($normalizer.Contains('if Token = "" then null else Record.FieldOrDefault(Names, Token, Token)')) 'M State normalizer must preserve blank and custom labels.'
$nameBlock = [regex]::Match($normalizer, '(?s)Names\s*=\s*\[(.*?)\]').Groups[1].Value
$aliases = [Collections.Generic.Dictionary[string,string]]::new([StringComparer]::Ordinal)
foreach ($match in [regex]::Matches($nameBlock, '(?:#"([^"]+)"|([A-Z]+))\s*=\s*"([A-Z]+)"')) {
    $name = if ($match.Groups[1].Success) { $match.Groups[1].Value } else { $match.Groups[2].Value }
    $aliases.Add($name, $match.Groups[3].Value)
}
Assert-Tender ($aliases.Count -eq 8) 'Expected exactly the eight reviewed full-State-name mappings.'
function Convert-TranslatedState($Value) {
    if ($null -eq $Value) { return '' }
    $token = ([string]$Value).Trim().ToUpperInvariant()
    if ($aliases.ContainsKey($token)) { return $aliases[$token] }
    return $token
}
$stateVectors = @(
    $null, '', " `t`r`n ", 'New South Wales', ' Queensland ', 'South Australia', 'Tasmania',
    'Victoria', 'Western Australia', 'Australian Capital Territory', 'Northern Territory', ' qld ',
    ' Waikato / Tāmaki 工程 ', 'North  Region', 'North Region', "North`nRegion", 'State, "A"|B', 'ßẞİı'
)
$savedCulture = [Globalization.CultureInfo]::CurrentCulture
try {
    foreach ($culture in @('en-NZ', 'tr-TR')) {
        [Globalization.CultureInfo]::CurrentCulture = [Globalization.CultureInfo]::GetCultureInfo($culture)
        foreach ($value in $stateVectors) {
            $actualCore = [XerToCsvConverter.TenderReview.TenderReviewNaming]::NormalizeState($value)
            Assert-Tender ($actualCore -ceq (Convert-TranslatedState $value)) "Core/literal-M State mapping differs for culture $culture."
        }
    }
} finally { [Globalization.CultureInfo]::CurrentCulture = $savedCulture }
Assert-Tender ((Convert-TranslatedState 'North  Region') -cne (Convert-TranslatedState 'North Region')) 'Internal State spaces collapsed.'

$routing = Read-Expression $expressions 'TenderCsvRouting'
Assert-Tender ($routing.Contains('List.Difference(ProjectFilterList, RoutedCodes)')) 'CSV override is not exact whole-project selection.'
Assert-Tender ($routing.Contains('AthenaExcludedProjectCodes = if IsActive then RoutedCodes else {}')) 'Athena exclusion changes configured business identities.'
Assert-Tender ($expressions.Contains("LIKE 'QAC%'")) 'Athena schedule QAC admission guard is missing.'
Assert-Tender (($expressions + $dimension + $role + $audit + (Read-ModelFile 'tables/01 XER_TASK.tmdl')) -notmatch 'CJ:|AliasVariants|SecurityAltProjectCode|j_project_code|c_project_code|VAR\s+AltProjectCode') 'A Tender C/J identity alias remains.'
Assert-Tender ($dimension -notmatch 'VAR\s+PermissionState|VAR\s+PermState|UserPermission\x27?\[State\]') 'Project State still falls back to permissions.'
Assert-Tender ($dimension.Contains('RETURN IF ( [__IsCsvSource], ManualState, CanonicalAthenaState )')) 'CSV State is not the independent authoritative branch.'
Assert-Tender ($dimension.Contains("'Tender CSV Manifest'[project_state]")) 'Manual State does not originate in manifest metadata.'
Assert-Tender ($dimension.Contains('RETURN IF ( [__IsCsvSource], ManifestName,')) 'CSV project name can be replaced by catalogue enrichment.'
Assert-Tender ($dimension.Contains('VAR CanonicalAthenaState = COALESCE ( DboState, XerState )')) 'Athena State authority is not actual catalogue/source metadata.'
Assert-Tender ($catalogue.Contains('each fnTenderCsvNormalizeState(_)')) 'Catalogue State does not use shared canonical normalization.'
Assert-Tender (([regex]::Matches($project, 'each fnTenderCsvNormalizeState\(_\)')).Count -eq 2) 'Source/model project State normalization diverges.'
Assert-Tender ($role.Contains("EXACT ( 'UserPermission'[State], CurrentProjectState )")) 'State permission matching is not exact.'
Assert-Tender ($role.Contains("EXACT ( 'UserPermission'[ProjectCode], CurrentProjectCode )")) 'Project permission matching is not exact.'
Assert-Tender ($role.Contains('NOT ISBLANK ( CurrentProjectState ) && CurrentProjectState <> ""')) 'Blank project State can match a State grant.'
Assert-Tender ($role.Contains("NOT ISBLANK ( 'UserPermission'[State] ) && 'UserPermission'[State] <> `"`"")) 'Blank permission State can match.'
Assert-Tender ($role -match '(?s)RETURN\s+HasAllAccess\s*\|\| HasStateAccess\s*\|\| HasProjectAccess') 'Permission levels are not independent OR grants.'
foreach ($table in @('Tender CSV Manifest','Tender CSV Refresh Audit')) {
    Assert-Tender ($role -match ("tablePermission '" + [regex]::Escape($table) + "' = FALSE\(\)")) "$table lacks role protection."
}
Assert-Tender (([regex]::Matches($audit, '\bERROR\s*\(')).Count -eq 1 -and $audit.Contains('COUNTROWS ( OwnershipOverlap ) > 0, ERROR')) 'Refresh audit retains a non-ownership fatal registration gate.'
$readTable = Read-Expression $expressions 'fnTenderCsvReadTable'
Assert-Tender ($readTable.Contains('current[state] <> metadata[project_state]')) 'Table 02 State mirror is not validated.'
Assert-Tender ($readTable -match '(?s)ContextChecked\s*=.*InvalidProjectContext.*UniqueChecked\s*=.*ContextChecked.*WithManifestState\s*=.*Table.TransformColumns\(UniqueChecked,.*bundle\[ProjectState\].*Table.AddColumn\(WithManifestState') 'Manifest State overwrite bypasses mirror validation or is not on the returned output path.'
$readBundle = Read-Expression $expressions 'fnTenderCsvReadBundle'
Assert-Tender ($readBundle.Contains('List.Count(ProjectStates) <> 1')) 'Bundle-wide State consistency is not forced by validation.'
Assert-Tender ($readBundle.Contains('RequiredSourceTables = {"02_XER_PROJECT"}')) 'Legitimate optional header-only sources are rejected.'
Assert-Tender ($readBundle.Contains('rows{0}[row_count] = 1')) 'Each admitted stage must retain one project row.'

# Deliberate independent translation of the reviewed OR policy, not DAX execution.
function Test-TranslatedAccess([string]$ProjectCode, $State, [object[]]$Grants) {
    $canonicalProject = $ProjectCode.Trim().ToUpperInvariant()
    $canonicalState = Convert-TranslatedState $State
    foreach ($grant in $Grants) {
        $grantState = Convert-TranslatedState $grant.State
        $grantCode = if ($null -eq $grant.Project) { '' } else { ([string]$grant.Project).Trim().ToUpperInvariant() }
        if ($grant.Level -eq 1) { return $true }
        if ($grant.Level -eq 2 -and $canonicalState -cne '' -and $grantState -cne '' -and $canonicalState -ceq $grantState) { return $true }
        if ($grant.Level -eq 3 -and $grantCode -cne '' -and $canonicalProject -ceq $grantCode) { return $true }
    }
    return $false
}
function Grant([int]$Level, $State = $null, $Project = $null) { [pscustomobject]@{ Level=$Level; State=$State; Project=$Project } }
$accessCases = @(
    @{ Name='All without State/project registration'; Project='QAC11111'; State=$null; Grants=@((Grant 1)); Expected=$true },
    @{ Name='Canonical full-name State grant'; Project='CIVIL'; State='Queensland'; Grants=@((Grant 2 ' qld ')); Expected=$true },
    @{ Name='Wrong State denied'; Project='CIVIL'; State='NSW'; Grants=@((Grant 2 'QLD')); Expected=$false },
    @{ Name='Blank State never matches blank'; Project='CIVIL'; State=$null; Grants=@((Grant 2)); Expected=$false },
    @{ Name='Exact project without State'; Project='C5001'; State=$null; Grants=@((Grant 3 $null 'c5001')); Expected=$true },
    @{ Name='C does not grant J'; Project='J5001'; State=$null; Grants=@((Grant 3 $null 'C5001')); Expected=$false },
    @{ Name='Numeric code is separate'; Project='5001'; State=$null; Grants=@((Grant 3 $null 'C5001')); Expected=$false },
    @{ Name='CIVIL does not grant JIVIL'; Project='JIVIL'; State=$null; Grants=@((Grant 3 $null 'CIVIL')); Expected=$false },
    @{ Name='Project internal spaces are distinct'; Project='NORTH  SOUTH'; State=$null; Grants=@((Grant 3 $null 'NORTH SOUTH')); Expected=$false },
    @{ Name='Custom State internal spaces are distinct'; Project='NORTH'; State='NORTH  REGION'; Grants=@((Grant 2 'NORTH REGION')); Expected=$false },
    @{ Name='Custom State exact match'; Project='NORTH'; State='WAIKATO / TĀMAKI 工程'; Grants=@((Grant 2 ' Waikato / Tāmaki 工程 ')); Expected=$true },
    @{ Name='One matching grant is sufficient'; Project='NORTH'; State='QLD'; Grants=@((Grant 2 'NSW'), (Grant 3 $null 'NORTH')); Expected=$true },
    @{ Name='No applicable grant denied'; Project='NORTH'; State='QLD'; Grants=@(); Expected=$false }
)
foreach ($case in $accessCases) {
    Assert-Tender ((Test-TranslatedAccess $case.Project $case.State $case.Grants) -eq $case.Expected) ('Translated policy case failed: ' + $case.Name)
}

$active = @($model.Relationships | Where-Object IsActive)
function Get-ActiveEdge([string]$From, [string]$To) { @($active | Where-Object { $_.FromTable.Name -ceq $From -and $_.ToTable.Name -ceq $To }) }
$projectEdge = Get-ActiveEdge '02 XER_PROJECT' 'Project_Dimension'
Assert-Graph ($projectEdge.Count -eq 1) 'Expected one active Project_Dimension-to-project relationship.'
Assert-Graph ($projectEdge[0].FromColumn.Name -ceq 'ProjectKey' -and $projectEdge[0].ToColumn.Name -ceq 'ProjectKey') 'Project security route uses the wrong identity.'
Assert-Graph ((Get-ActiveEdge '01 XER_TASK' '02 XER_PROJECT').Count -eq 1) 'Project-to-task security route is missing.'
Assert-Graph ((Get-ActiveEdge '03 XER_PROJWBS' 'Project_Dimension').Count -eq 0) 'A competing direct project-to-WBS path remains.'
$wbsEdge = Get-ActiveEdge '01 XER_TASK' '03 XER_PROJWBS'
Assert-Graph ($wbsEdge.Count -eq 1) 'Expected one existing activity/WBS relationship.'
Assert-Graph ($wbsEdge[0].CrossFilteringBehavior.ToString() -eq 'BothDirections' -and $wbsEdge[0].SecurityFilteringBehavior.ToString() -eq 'BothDirections') 'Activity/WBS filtering and reverse security must both be enabled.'
$securityGraph = @{}
foreach ($edge in $active) {
    $from = $edge.FromTable.Name; $to = $edge.ToTable.Name
    if (-not $securityGraph.ContainsKey($to)) { $securityGraph[$to] = @() }
    $securityGraph[$to] += $from
    if ($edge.SecurityFilteringBehavior.ToString() -eq 'BothDirections') {
        if (-not $securityGraph.ContainsKey($from)) { $securityGraph[$from] = @() }
        $securityGraph[$from] += $to
    }
}
function Count-SecurityPaths([string]$Current, [string]$Target, [string[]]$Visited) {
    if ($Current -ceq $Target) { return 1 }
    if ($Current -cin $Visited) { return 0 }
    $count = 0
    foreach ($next in @($securityGraph[$Current] | Select-Object -Unique)) {
        $count += Count-SecurityPaths $next $Target (@($Visited) + $Current)
    }
    return $count
}
foreach ($target in @('02 XER_PROJECT', '01 XER_TASK', '03 XER_PROJWBS', '06 XER_PREDECESSOR', '06a XER_SUCCESSOR', '15_XER_RESOURCE_DISTRIBUTION')) {
    Assert-Graph ((Count-SecurityPaths 'Project_Dimension' $target @()) -eq 1) "Expected one unambiguous simple security path to $target."
}

[pscustomobject]@{
    Result = 'PASS'; Checks = $checks; StateVectors = $stateVectors.Count; Cultures = 2
    TranslatedAccessCases = $accessCases.Count; GraphAssertions = $graphChecks
    Scope = 'Real Core normalization compared with mappings parsed from M; static M/DAX policy and TOM graph assertions; translated OR truth table. No M/DAX execution or live RLS proof.'
    ManualGates = @('Desktop full refresh and Viewer/View-as', 'Blank/orphan WBS tasks survive security filtering', 'Ancestor-only WBS flattened labels remain available', 'Downstream 06/15 and stage/project/State slicers', 'Mixed Athena/CSV exact source override')
} | ConvertTo-Json -Depth 4
