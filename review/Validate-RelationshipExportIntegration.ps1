param(
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string[]]$Paths,
    [string]$BaselineHashes,
    [switch]$RecordProfileValidationFailures,
    [string]$ProfileFixture = (Join-Path $PSScriptRoot 'fixtures/J5001_C_BL01_2026-01-31.xer'),
    [string]$ProfileProjectCode,
    [string]$ExpectedFixtureReason,
    [AllowEmptyString()][string]$ExpectedFixtureFloat,
    [string]$CoreAssemblyPath = (Join-Path $PSScriptRoot '../XerToCsvConverter.Core/bin/Debug/net8.0/XerToCsvConverter.Core.dll'),
    [string]$OutputRoot = (Join-Path $PSScriptRoot '../artifacts/relcheck')
)

# Read originals locally; only review-fixture bundles are written beneath a new
# run directory. This compares implementations and contracts, not native P6.
$ErrorActionPreference = 'Stop'
$assemblyBytes = [IO.File]::ReadAllBytes((Resolve-Path -LiteralPath $CoreAssemblyPath).Path)
$loadedAssemblyHash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($assemblyBytes))
$loadedCoreAssembly = [Reflection.Assembly]::Load($assemblyBytes)
if ([XerToCsvConverter.XerTransformer].Assembly.ManifestModule.ModuleVersionId -ne $loadedCoreAssembly.ManifestModule.ModuleVersionId) {
    throw 'A different Core build is already loaded. Run this validation in a fresh PowerShell process.'
}
Add-Type -AssemblyName Microsoft.VisualBasic.Core
$culture = [Globalization.CultureInfo]::InvariantCulture
$cancel = [Threading.CancellationToken]::None
$names = [Collections.Generic.List[string]]::new()
foreach ($name in @('01_XER_TASK','02_XER_PROJECT','03_XER_PROJWBS','04_XER_BASELINE','06_XER_PREDECESSOR','07_XER_ACTVTYPE','08_XER_ACTVCODE','09_XER_TASKACTV','10_XER_CALENDAR','11_XER_CALENDAR_DETAILED','12_XER_RSRC','13_XER_TASKRSRC','14_XER_UMEASURE','15_XER_RESOURCE_DISTRIBUTION')) { $names.Add($name) }

function Assert-Check([bool]$Condition, [string]$Message) { if (-not $Condition) { throw $Message } }
function Get-BytesHash([byte[]]$Bytes) { return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($Bytes)) }
function Get-CsvRows([byte[]]$Bytes) { return ,@([Text.Encoding]::UTF8.GetString($Bytes).TrimStart([char]0xFEFF) | ConvertFrom-Csv) }
function Get-CsvHeaders([byte[]]$Bytes) {
    $reader = [Microsoft.VisualBasic.FileIO.TextFieldParser]::new([IO.StringReader]::new([Text.Encoding]::UTF8.GetString($Bytes).TrimStart([char]0xFEFF)))
    try { $reader.SetDelimiters([string[]]@(',')); $reader.TrimWhiteSpace = $false; return ,$reader.ReadFields() }
    finally { $reader.Dispose() }
}
function Assert-FilesEqual($First, $Second, [string]$Context) {
    Assert-Check ($First.Count -eq $Second.Count) "$Context file envelopes differ."
    foreach ($name in $First.Keys) {
        Assert-Check ($Second.ContainsKey($name)) "$Context omitted $name."
        Assert-Check ((Get-BytesHash $First[$name]) -ceq (Get-BytesHash $Second[$name])) "$Context changed $name bytes."
    }
}
function Parse-Streams($Service, [string[]]$InputPaths) {
    $inputs = [Collections.Generic.List[ValueTuple[IO.Stream,string]]]::new()
    try {
        foreach ($path in $InputPaths) { $inputs.Add([ValueTuple[IO.Stream,string]]::new([IO.File]::OpenRead($path), [IO.Path]::GetFileName($path))) }
        return $Service.ParseXerStreamsAsync($inputs, $null, $cancel).GetAwaiter().GetResult()
    } finally { foreach ($input in $inputs) { $input.Item1.Dispose() } }
}
function Get-AuditText($Assessments) {
    $writer = [IO.StringWriter]::new($culture)
    try { [XerToCsvConverter.RelationshipAuditCsv]::Write($Assessments, $writer, $cancel); return $writer.ToString() }
    finally { $writer.Dispose() }
}
function Add-Quantity($Totals, [string]$Key, [decimal]$Value) {
    if ($Totals.ContainsKey($Key)) { $Totals[$Key] += $Value } else { $Totals.Add($Key, $Value) }
}

$fullPaths = [Collections.Generic.List[string]]::new()
foreach ($path in $Paths) { $fullPaths.Add((Resolve-Path -LiteralPath $path).Path) }
$sourceHashes = @($fullPaths | ForEach-Object { (Get-FileHash -LiteralPath $_).Hash })
$service = [XerToCsvConverter.ProcessingService]::new()
$disk = $service.ParseMultipleXerFilesAsync($fullPaths, $null, $cancel).GetAwaiter().GetResult()
$stream = Parse-Streams $service $fullPaths.ToArray()
$diskExport = $service.ExportTablesToMemoryWithDiagnosticsAsync($disk, $names, $null, $cancel).GetAwaiter().GetResult()
$streamExport = $service.ExportTablesToMemoryWithDiagnosticsAsync($stream, $names, $null, $cancel).GetAwaiter().GetResult()
Assert-Check ($diskExport.Files.Count -eq 15) 'All 14 Standard tables plus Enhanced data-quality companion must be present.'
Assert-FilesEqual $diskExport.Files $streamExport.Files 'Original file/stream Standard export'
Assert-Check ($diskExport.WarningCount -eq $streamExport.WarningCount) 'File/stream data-quality warning counts differ.'

$diskAssessments = ([XerToCsvConverter.XerTransformer]::new($disk)).AssessRelationships($cancel)
$streamAssessments = ([XerToCsvConverter.XerTransformer]::new($stream)).AssessRelationships($cancel)
Assert-Check ($diskAssessments.Count -eq $disk.GetTable('TASKPRED').RowCount) 'Assessment omitted or invented a raw relationship row.'
Assert-Check ((Get-AuditText $diskAssessments) -ceq (Get-AuditText $streamAssessments)) 'File/stream relationship audit differs.'
$rows06 = Get-CsvRows $diskExport.Files['06_XER_PREDECESSOR']
Assert-Check ($rows06.Count -eq $diskAssessments.Count) 'Standard table 06 row count differs from its audit.'
$ordinals = [Collections.Generic.Dictionary[string,int]]::new([StringComparer]::Ordinal)
for ($index = 0; $index -lt $diskAssessments.Count; $index++) {
    $assessment = $diskAssessments[$index]
    $ordinal = if ($ordinals.ContainsKey($assessment.SourceNamespace)) { $ordinals[$assessment.SourceNamespace] + 1 } else { 1 }
    $ordinals[$assessment.SourceNamespace] = $ordinal
    Assert-Check ($assessment.SourceRowNumber -eq $ordinal) 'Audit source relationship ordinal is not contiguous.'
    Assert-Check ($assessment.RelationshipId -ceq $rows06[$index].task_pred_id) 'Audit row order/relationship identity differs from table 06.'
    Assert-Check ($assessment.FileName -ceq $rows06[$index].FileName) 'Audit changed original filename provenance.'
    Assert-Check ($assessment.FormattedDays -ceq $rows06[$index].free_float) 'Audit free_float differs from the normal export.'
    Assert-Check ($assessment.AllowanceStatus.ToString() -ceq $rows06[$index].free_float_status) 'Audit allowance status differs from the normal export.'
    Assert-Check ($assessment.CalculationBasis -ceq $rows06[$index].free_float_basis) 'Audit allowance basis differs from the normal export.'
    Assert-Check ($assessment.ReasonCode -ceq $rows06[$index].free_float_reason) 'Audit allowance reason differs from the normal export.'
    Assert-Check (-not [string]::IsNullOrWhiteSpace($assessment.ReasonCode)) 'Audit omitted an explanation code.'
    if ($assessment.Classification.ToString() -ne 'Calculated') {
        Assert-Check ($null -eq $assessment.FloatHours -and $null -eq $assessment.FloatDays) 'A noncalculated audit result carries a numeric allowance.'
    }
}
Assert-Check ($ordinals.Count -eq $fullPaths.Count) 'An input relationship occurrence was collapsed.'

$comparedBaseline = $false
if ($BaselineHashes) {
    $baseline = Get-Content -LiteralPath $BaselineHashes -Raw | ConvertFrom-Json
    Assert-Check ($baseline.InputSha256.Count -eq $sourceHashes.Count) 'Baseline input count differs.'
    for ($index = 0; $index -lt $sourceHashes.Count; $index++) { Assert-Check ($sourceHashes[$index] -ceq $baseline.InputSha256[$index]) 'Baseline input content/order differs.' }
    foreach ($name in $diskExport.Files.Keys) {
        if ($name -in @('06_XER_PREDECESSOR','XER_DATA_QUALITY')) { continue }
        Assert-Check ((Get-BytesHash $diskExport.Files[$name]) -ceq $baseline.Hashes.$name) "Nonrelationship output changed from the recorded baseline: $name."
    }
    $keptColumns = @((Get-CsvHeaders $diskExport.Files['06_XER_PREDECESSOR']) | Where-Object { $_ -cnotin @('free_float','free_float_status','free_float_basis','free_float_reason') })
    $canonical = ($rows06 | Select-Object -Property $keptColumns | ConvertTo-Csv -NoTypeInformation) -join "`n"
    Assert-Check ((Get-BytesHash ([Text.Encoding]::UTF8.GetBytes($canonical))) -ceq $baseline.Hashes.'06_WITHOUT_FREE_FLOAT') 'A table 06 field other than free_float changed.'
    $comparedBaseline = $true
}

# Exercise both fixed profiles with an explicit local fixture; never rewrite the originals.
$fixture = (Resolve-Path -LiteralPath $ProfileFixture).Path
$fixturePaths = [Collections.Generic.List[string]]::new(); $fixturePaths.Add($fixture)
$fixtureHash = (Get-FileHash -LiteralPath $fixture).Hash
$fixtureStore = $service.ParseMultipleXerFilesAsync($fixturePaths, $null, $cancel).GetAwaiter().GetResult()
$projectTable = $fixtureStore.GetTable('PROJECT')
Assert-Check ($projectTable.RowCount -eq 1) 'The profile fixture must contain exactly one project.'
$project = $projectTable.Rows[0]
$projectCode = [XerToCsvConverter.XerTable]::GetFieldValueSafe($project, $projectTable.FieldIndexes['proj_short_name'])
if ($ProfileProjectCode) { $projectCode = $ProfileProjectCode }
$dataDate = [DateTime]::Parse([XerToCsvConverter.XerTable]::GetFieldValueSafe($project, $projectTable.FieldIndexes['last_recalc_date']), $culture)
$isoDate = $dataDate.ToString('yyyy-MM-dd', $culture)
$fixtureName = [IO.Path]::GetFileName($fixture)
$fixture06 = ([XerToCsvConverter.XerTransformer]::new($fixtureStore)).AssessRelationships($cancel)
if ($ExpectedFixtureReason) {
    Assert-Check ($fixture06.Count -eq 1 -and $fixture06[0].ReasonCode -ceq $ExpectedFixtureReason) 'The profile fixture did not exercise the requested relationship assessment case.'
    if ($PSBoundParameters.ContainsKey('ExpectedFixtureFloat')) { Assert-Check ($fixture06[0].FormattedDays -ceq $ExpectedFixtureFloat) 'The profile fixture allowance differs from its explicit expected value.' }
}
$fixtureStandard = $service.ExportTablesToMemoryWithDiagnosticsAsync($fixtureStore, $names, $null, $cancel).GetAwaiter().GetResult()
$fixture15 = Get-CsvRows $fixtureStandard.Files['15_XER_RESOURCE_DISTRIBUTION']
$runRoot = Join-Path ([IO.Path]::GetFullPath($OutputRoot)) ([Guid]::NewGuid().ToString('N').Substring(0, 12))
$null = New-Item -ItemType Directory -Path $runRoot
$jsonOptions = [Text.Json.JsonSerializerOptions]::new()
$jsonOptions.PropertyNamingPolicy = [Text.Json.JsonNamingPolicy]::SnakeCaseLower
$profileResults = [Collections.Generic.List[object]]::new()
foreach ($profile in @('Programme','Tender')) {
    $common = @{ project_code = $projectCode; project_name = 'Local relationship integration fixture'; parser_version = 'integration-check'; exported_at_utc = '2026-09-06T00:00:00Z' }
    if ($profile -eq 'Programme') {
        $common.programme_type = 'C'
        $common.snapshots = @(@{ original_xer_filename = $fixtureName; xer_file_path = $fixture; snapshot_kind = 0; snapshot_tag = 'BL01'; month_update = $isoDate; data_date = $isoDate })
        $request = [Text.Json.JsonSerializer]::Deserialize(($common | ConvertTo-Json -Depth 6), [XerToCsvConverter.ProgrammeReview.ProgrammeReviewBundleRequest], $jsonOptions)
        $bundleService = [XerToCsvConverter.ProgrammeReview.ProgrammeReviewBundleService]::new()
        $inputBytes = [Collections.Generic.Dictionary[string,byte[]]]::new([StringComparer]::Ordinal)
        $inputBytes.Add($fixtureName, [IO.File]::ReadAllBytes($fixture))
        $contracts = [XerToCsvConverter.ProgrammeReview.ProgrammeReviewContract]::Tables
        $prefix = 'CSV::' + [XerToCsvConverter.ReviewProjectIdentity]::EncodeComponent([XerToCsvConverter.ReviewProjectIdentity]::NormalizeCode($projectCode)) + '::C::BL01::'
    } else {
        $common.sources = @(@{ source_token = 'fixture-000001'; original_xer_filename = $fixtureName; xer_file_path = $fixture; status_date = $isoDate })
        $request = [Text.Json.JsonSerializer]::Deserialize(($common | ConvertTo-Json -Depth 6), [XerToCsvConverter.TenderReview.TenderReviewBundleRequest], $jsonOptions)
        $bundleService = [XerToCsvConverter.TenderReview.TenderReviewBundleService]::new()
        $inputBytes = [Collections.Generic.List[XerToCsvConverter.TenderReview.TenderReviewSourceBytes]]::new()
        $upload = [XerToCsvConverter.TenderReview.TenderReviewSourceBytes]::new()
        $upload.SourceToken = 'fixture-000001'; $upload.Content = [IO.File]::ReadAllBytes($fixture); $inputBytes.Add($upload)
        $contracts = [XerToCsvConverter.TenderReview.TenderReviewContract]::Tables
        $prefix = 'CSV::' + [XerToCsvConverter.ReviewProjectIdentity]::EncodeComponent([XerToCsvConverter.ReviewProjectIdentity]::NormalizeCode($projectCode)) + '::TENDER::' + $dataDate.ToString('yyyyMMdd', $culture) + '::'
    }
    try {
        $memoryResult = $bundleService.BuildFromXerBytesAsync($request, $inputBytes, $null, $cancel).GetAwaiter().GetResult()
        $fileResult = $bundleService.BuildFromXerFilesAsync($request, (Join-Path $runRoot $profile), $null, $cancel).GetAwaiter().GetResult()
    } catch {
        $cause = $_.Exception.GetBaseException()
        $validationTypes = @('XerToCsvConverter.ProgrammeReview.ProgrammeReviewValidationException', 'XerToCsvConverter.TenderReview.TenderReviewValidationException')
        if (-not $RecordProfileValidationFailures -or $cause.GetType().FullName -notin $validationTypes) { throw }
        $profileResults.Add([pscustomobject]@{ Profile = $profile; Validation = 'Rejected by unchanged profile contract'; Error = $cause.Message })
        continue
    }
    $expectedFiles = @($contracts | ForEach-Object FileName) + 'XER_CSV_MANIFEST.csv'
    Assert-Check ((($memoryResult.Files.Keys | Sort-Object) -join '|') -ceq (($expectedFiles | Sort-Object) -join '|')) "$profile normal bundle envelope changed."
    $published = [Collections.Generic.Dictionary[string,byte[]]]::new([StringComparer]::Ordinal)
    foreach ($file in (Get-ChildItem -LiteralPath $fileResult.BundlePath -File)) { $published.Add($file.Name, [IO.File]::ReadAllBytes($file.FullName)) }
    Assert-FilesEqual $memoryResult.Files $published "$profile file/byte bundle"
    foreach ($contract in $contracts) {
        $headers = Get-CsvHeaders $memoryResult.Files[$contract.TableName + '.csv']
        Assert-Check (($headers -join ',') -ceq (($contract.Columns | ForEach-Object Name) -join ',')) "$profile changed $($contract.TableName) headers."
    }
    $profile06 = Get-CsvRows $memoryResult.Files['06_XER_PREDECESSOR.csv']
    $expectedFloats = [Collections.Generic.Dictionary[string,string]]::new([StringComparer]::Ordinal)
    foreach ($assessment in $fixture06) { $expectedFloats.Add($prefix + $assessment.RelationshipId, $assessment.FormattedDays) }
    $expectedAssessments = [Collections.Generic.Dictionary[string,object]]::new([StringComparer]::Ordinal)
    foreach ($assessment in $fixture06) { $expectedAssessments.Add($prefix + $assessment.RelationshipId, $assessment) }
    Assert-Check ($profile06.Count -eq $expectedFloats.Count) "$profile changed the relationship count."
    foreach ($row in $profile06) {
        Assert-Check ($expectedFloats.ContainsKey($row.task_pred_id_key) -and $expectedFloats[$row.task_pred_id_key] -ceq $row.free_float) "$profile free_float differs from the shared assessment."
        $expectedAssessment = $expectedAssessments[$row.task_pred_id_key]
        Assert-Check ($row.free_float_status -ceq $expectedAssessment.AllowanceStatus.ToString()) "$profile allowance status differs from the shared assessment."
        Assert-Check ($row.free_float_basis -ceq $expectedAssessment.CalculationBasis) "$profile allowance basis differs from the shared assessment."
        Assert-Check ($row.free_float_reason -ceq $expectedAssessment.ReasonCode) "$profile allowance reason differs from the shared assessment."
    }
    $expectedQuantities = [Collections.Generic.Dictionary[string,decimal]]::new([StringComparer]::Ordinal)
    foreach ($row in $fixture15) {
        $taskId = $row.task_id_key.Substring($fixtureName.Length + 1)
        $resourceId = $row.rsrc_id_key.Substring($fixtureName.Length + 1)
        $key = @(($prefix + $taskId), ($prefix + $resourceId), $row.is_actual, $row.distribution_month) -join '|'
        Add-Quantity $expectedQuantities $key ([decimal]::Parse($row.monthly_quantity, $culture))
    }
    $actualQuantities = [Collections.Generic.Dictionary[string,decimal]]::new([StringComparer]::Ordinal)
    foreach ($row in (Get-CsvRows $memoryResult.Files['15_XER_RESOURCE_DISTRIBUTION.csv'])) {
        $actualFlag = if ($row.is_actual -ceq 'true') { '1' } elseif ($row.is_actual -ceq 'false') { '0' } else { throw "$profile has an invalid actual boolean." }
        $key = @($row.task_id_key, $row.rsrc_id_key, $actualFlag, $row.distribution_month) -join '|'
        Add-Quantity $actualQuantities $key ([decimal]::Parse($row.monthly_quantity, $culture))
    }
    Assert-Check ($expectedQuantities.Count -eq $actualQuantities.Count) "$profile changed resource distribution groups."
    foreach ($key in $expectedQuantities.Keys) { Assert-Check ($actualQuantities.ContainsKey($key) -and $actualQuantities[$key] -eq $expectedQuantities[$key]) "$profile resource distribution differs from shared Standard quantities." }
    # Review warnings remain available on the result but are outside the 11-file bundle.
    $qualityStream = [IO.MemoryStream]::new()
    try {
        ([XerToCsvConverter.CsvExporter]::new()).WriteTableToStream($memoryResult.DataQualityTable, $qualityStream)
        $profileQuality = Get-CsvRows $qualityStream.ToArray()
    } finally { $qualityStream.Dispose() }
    $standardQuality = Get-CsvRows $fixtureStandard.Files['XER_DATA_QUALITY']
    Assert-Check ($profileQuality.Count -eq $memoryResult.WarningCount -and $standardQuality.Count -eq $fixtureStandard.WarningCount) "$profile warning totals disagree with companion rows."
    $allocationEvidence = @('FileName','source_row_number','allocation_portion','issue_code','taskrsrc_id',
        'act_start_date','act_end_date','act_reg_qty','act_ot_qty','restart_date','reend_date','remain_qty',
        'curv_id','remain_crv','unallocated_actual_quantity','unallocated_remaining_quantity')
    $expectedWarnings = @($standardQuality | Where-Object allocation_portion -In @('Actual','Remaining') |
        Sort-Object FileName,source_row_number,allocation_portion | Select-Object -Property $allocationEvidence)
    $actualWarnings = @($profileQuality | Where-Object allocation_portion -In @('Actual','Remaining') |
        Sort-Object FileName,source_row_number,allocation_portion | Select-Object -Property $allocationEvidence)
    Assert-Check ($expectedWarnings.Count -eq $actualWarnings.Count -and
        ($expectedWarnings | ConvertTo-Json -Depth 4 -Compress) -ceq ($actualWarnings | ConvertTo-Json -Depth 4 -Compress)) "$profile changed source allocation warning evidence."
    $methodCodes = @('REMAINING_CURVE_ESTIMATED','REMAINING_CURVE_UNIFORM_FALLBACK')
    $methodEvidence = $allocationEvidence + @('message','project_data_date','source_table','raw_row_json')
    $expectedMethods = @($standardQuality | Where-Object issue_code -In $methodCodes |
        Sort-Object FileName,source_row_number,issue_code | Select-Object -Property $methodEvidence)
    $actualMethods = @($profileQuality | Where-Object issue_code -In $methodCodes |
        Sort-Object FileName,source_row_number,issue_code | Select-Object -Property $methodEvidence)
    Assert-Check ($expectedMethods.Count -eq $actualMethods.Count -and
        ($expectedMethods | ConvertTo-Json -Depth 4 -Compress) -ceq ($actualMethods | ConvertTo-Json -Depth 4 -Compress)) "$profile changed successful resource forecast method evidence."
    # Review-only field/identity diagnostics legitimately differ from Standard:
    # numeric relationship agreement above remains exact for every relationship.
    $profileResults.Add([pscustomobject]@{ Profile = $profile; Files = $memoryResult.Files.Count; Relationships = $profile06.Count;
        ResourceGroups = $actualQuantities.Count; DataQualityWarnings = $profileQuality.Count; AllocationWarnings = $actualWarnings.Count;
        GeneralWarnings = $profileQuality.Count - $actualWarnings.Count; ByteIdenticalFileAndMemory = $true })
}

for ($index = 0; $index -lt $fullPaths.Count; $index++) { Assert-Check ((Get-FileHash -LiteralPath $fullPaths[$index]).Hash -ceq $sourceHashes[$index]) 'An original input changed during validation.' }
Assert-Check ((Get-FileHash -LiteralPath $fixture).Hash -ceq $fixtureHash) 'Profile fixture changed during validation.'
[pscustomobject]@{
    Validation = $(if (@($profileResults | Where-Object { $_.Error }).Count -gt 0) { 'Standard integration passed; review profile validation failures recorded; not native P6 parity' } else { 'Passed local relationship/export integration; not native P6 parity' })
    OrderedInputOccurrences = $fullPaths.Count
    StandardFiles = $diskExport.Files.Count
    RelationshipAssessments = $diskAssessments.Count
    Classifications = @($diskAssessments | Group-Object Classification | Select-Object Name,Count)
    DataQualityWarnings = $diskExport.WarningCount
    ResourceWarnings = @((Get-CsvRows $diskExport.Files['XER_DATA_QUALITY']) | Where-Object allocation_portion -In @('Actual','Remaining')).Count
    NonFloatBaselineCompared = $comparedBaseline
    ByteIdenticalFileAndStream = $true
    ProfileFixture = $fixtureName
    ProfileProjectCode = $projectCode
    CoreAssemblySha256 = $loadedAssemblyHash
    CoreModuleVersionId = $loadedCoreAssembly.ManifestModule.ModuleVersionId.ToString()
    Profiles = $profileResults.ToArray()
    GeneratedFixtureBundleRoot = $runRoot
} | ConvertTo-Json -Depth 6
