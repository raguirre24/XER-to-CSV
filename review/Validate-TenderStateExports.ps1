param(
    [Parameter(Mandatory)][string[]]$Paths,
    [string]$CoreAssemblyPath = (Join-Path $PSScriptRoot '../XerToCsvConverter.Core/bin/Debug/net8.0/XerToCsvConverter.Core.dll'),
    [string]$BaselineSummary,
    [ValidateSet('StateOnly','CalendarDetail')][string]$BaselinePolicy = 'StateOnly',
    [switch]$VerifyCalendarDetail,
    [string]$OutputRoot = (Join-Path $PSScriptRoot '../artifacts/tender-state-access-validation/profile-exports')
)

# Real disk/Web-byte export and pre-change regression comparison, not M execution or native P6 parity.
# Input identity is always an ordered list of occurrences; only emitted output filenames key output hashes.
$ErrorActionPreference = 'Stop'
[Reflection.Assembly]::Load([IO.File]::ReadAllBytes((Resolve-Path -LiteralPath $CoreAssemblyPath).Path)) | Out-Null
$cancel = [Threading.CancellationToken]::None
$jsonOptions = [Text.Json.JsonSerializerOptions]::new()
$jsonOptions.PropertyNamingPolicy = [Text.Json.JsonNamingPolicy]::SnakeCaseLower
$culture = [Globalization.CultureInfo]::InvariantCulture
$runRoot = Join-Path ([IO.Path]::GetFullPath($OutputRoot)) ([guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $runRoot | Out-Null
$checks = 0
$results = [Collections.Generic.List[object]]::new()
$baseline = if ($BaselineSummary) { Get-Content -LiteralPath $BaselineSummary -Raw | ConvertFrom-Json } else { $null }
function Assert-StateExport([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
    $script:checks++
}
function Get-StateHash([byte[]]$Bytes) { [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($Bytes)) }
$calendarHeaders = @('clndr_name','clndr_type','date','day_of_week','working_day','work_hours','exception_type','clndr_id_key','MonthUpdate','day_of_week_num','working_day_int')
function Get-CalendarSignature([string[]]$Fields) {
    # Length-prefix every cell so custom names cannot alias row boundaries.
    ($Fields | ForEach-Object { $_.Length.ToString($culture) + ':' + $_ }) -join ''
}

for ($inputIndex = 0; $inputIndex -lt $Paths.Count; $inputIndex++) {
    $sourcePath = (Resolve-Path -LiteralPath $Paths[$inputIndex]).Path
    $sourceBytes = [IO.File]::ReadAllBytes($sourcePath)
    $sourceHash = Get-StateHash $sourceBytes
    $sourceName = [IO.Path]::GetFileName($sourcePath)
    $pathsForParse = [Collections.Generic.List[string]]::new(); $pathsForParse.Add($sourcePath)
    $store = ([XerToCsvConverter.ProcessingService]::new()).ParseMultipleXerFilesAsync($pathsForParse, $null, $cancel).GetAwaiter().GetResult()
    $project = $store.GetTable('PROJECT')
    Assert-StateExport ($project.RowCount -eq 1) 'Validation needs one native PROJECT per source.'
    $nativeCode = [XerToCsvConverter.XerTable]::GetFieldValueSafe($project.Rows[0], $project.FieldIndexes['proj_short_name'])
    $dataDate = [DateTime]::Parse([XerToCsvConverter.XerTable]::GetFieldValueSafe($project.Rows[0], $project.FieldIndexes['last_recalc_date']), $culture)
    $isoDate = $dataDate.ToString('yyyy-MM-dd', $culture)
    $standardCalendar = if ($VerifyCalendarDetail) { ([XerToCsvConverter.XerTransformer]::new($store)).Create11XerCalendarDetailed() } else { $null }
    foreach ($profile in @('Programme','Tender')) {
        $tender = $profile -ceq 'Tender'
        $common = @{ project_code = $nativeCode; project_name = 'Local State regression validation'; parser_version = 'state-regression-check'; exported_at_utc = '2026-09-10T00:00:00Z' }
        if ($tender) {
            $common.state = ' Queensland '
            $common.sources = @(@{ source_token = 'source-000001'; original_xer_filename = $sourceName; xer_file_path = $sourcePath; status_date = $isoDate })
            $request = [Text.Json.JsonSerializer]::Deserialize(($common | ConvertTo-Json -Depth 8), [XerToCsvConverter.TenderReview.TenderReviewBundleRequest], $jsonOptions)
            $inputs = [Collections.Generic.List[XerToCsvConverter.TenderReview.TenderReviewSourceBytes]]::new()
            $upload = [XerToCsvConverter.TenderReview.TenderReviewSourceBytes]::new(); $upload.SourceToken = 'source-000001'; $upload.Content = $sourceBytes; $inputs.Add($upload)
            $service = [XerToCsvConverter.TenderReview.TenderReviewBundleService]::new()
        } else {
            $common.programme_type = 'C'
            $common.snapshots = @(@{ original_xer_filename = $sourceName; xer_file_path = $sourcePath; snapshot_kind = 0; snapshot_tag = 'BL01'; month_update = $isoDate; data_date = $isoDate })
            $request = [Text.Json.JsonSerializer]::Deserialize(($common | ConvertTo-Json -Depth 8), [XerToCsvConverter.ProgrammeReview.ProgrammeReviewBundleRequest], $jsonOptions)
            $inputs = [Collections.Generic.Dictionary[string,byte[]]]::new([StringComparer]::Ordinal); $inputs.Add($sourceName, $sourceBytes)
            $service = [XerToCsvConverter.ProgrammeReview.ProgrammeReviewBundleService]::new()
        }
        $memory = $service.BuildFromXerBytesAsync($request, $inputs, $null, $cancel).GetAwaiter().GetResult()
        $disk = $service.BuildFromXerFilesAsync($request, (Join-Path $runRoot "$inputIndex-$profile"), $null, $cancel).GetAwaiter().GetResult()
        $tableCount = if ($tender) { [XerToCsvConverter.TenderReview.TenderReviewContract]::Tables.Count } else { [XerToCsvConverter.ProgrammeReview.ProgrammeReviewContract]::Tables.Count }
        Assert-StateExport ($memory.Files.Count -eq ($tableCount + 1) -and @(Get-ChildItem -LiteralPath $disk.BundlePath -File).Count -eq ($tableCount + 1)) "$profile lost a required output."
        $hashes = [ordered]@{}
        foreach ($fileName in $memory.Files.Keys) {
            $hash = Get-StateHash $memory.Files[$fileName]
            Assert-StateExport ((Get-FileHash -LiteralPath (Join-Path $disk.BundlePath $fileName)).Hash -ceq $hash) "$profile disk/Web bytes disagree: $fileName."
            $hashes[$fileName] = $hash
        }
        $manifest = @(Import-Csv -LiteralPath (Join-Path $disk.BundlePath 'XER_CSV_MANIFEST.csv'))
        Assert-StateExport ($manifest.Count -eq $tableCount) "$profile manifest coverage differs."
        foreach ($row in $manifest) {
            $tablePath = Join-Path $disk.BundlePath ($row.table_name + '.csv')
            Assert-StateExport (@(Import-Csv -LiteralPath $tablePath).Count -eq [long]$row.row_count) "$profile table/manifest count mismatch."
            Assert-StateExport ($hashes[$row.table_name + '.csv'] -ceq $row.csv_sha256.ToUpperInvariant()) "$profile table/manifest hash mismatch."
        }
        $version = $manifest[0].schema_version
        if ($tender -and $version -in @('3.0','4.0')) {
            Assert-StateExport (@($manifest | Where-Object { $_.project_state -cne 'QLD' }).Count -eq 0) 'Manifest manual State was not applied.'
            $projectRows = @(Import-Csv -LiteralPath (Join-Path $disk.BundlePath '02_XER_PROJECT.csv'))
            Assert-StateExport (@($projectRows | Where-Object { $_.state -cne 'QLD' }).Count -eq 0) 'Table 02 did not mirror manual State.'
        }
        $calendarRowsChecked = 0
        if ($VerifyCalendarDetail) {
            $calendarBytes = $memory.Files['11_XER_CALENDAR_DETAILED.csv']
            Assert-StateExport ($null -ne $calendarBytes) "$profile omitted detailed calendars."
            $calendarText = [Text.Encoding]::UTF8.GetString($calendarBytes).TrimStart([char]0xFEFF)
            Assert-StateExport (($calendarText -split "`r?`n", 2)[0] -ceq ($calendarHeaders -join ',')) "$profile table11 exact headers differ."
            $actualCalendarRows = @($calendarText | ConvertFrom-Csv)
            $expectedCalendarRows = [Collections.Generic.List[string]]::new()
            $projectToken = [XerToCsvConverter.ReviewProjectIdentity]::EncodeComponent([XerToCsvConverter.ReviewProjectIdentity]::NormalizeCode($nativeCode))
            $calendarPrefix = if ($tender) { 'CSV::' + $projectToken + '::TENDER::' + $dataDate.ToString('yyyyMMdd', $culture) + '::' } else { 'CSV::' + $projectToken + '::C::BL01::' }
            if ($null -ne $standardCalendar) {
                foreach ($rawCalendarRow in $standardCalendar.Rows) {
                    $fields = foreach ($column in $calendarHeaders) {
                        $raw = [XerToCsvConverter.XerTable]::GetFieldValueSafe($rawCalendarRow, $standardCalendar.FieldIndexes[$column])
                        if ($column -ceq 'MonthUpdate') { $isoDate }
                        elseif ($column -ceq 'clndr_id_key' -and $raw -ne '') {
                            $sourcePrefix = $rawCalendarRow.SourceFilename + '.'
                            Assert-StateExport ($raw.StartsWith($sourcePrefix, [StringComparison]::Ordinal)) 'Standard calendar key is not source-local.'
                            $calendarPrefix + $raw.Substring($sourcePrefix.Length)
                        } else { $raw }
                    }
                    $expectedCalendarRows.Add((Get-CalendarSignature $fields))
                }
            }
            [string[]]$actualSignatures = @($actualCalendarRows | ForEach-Object {
                $calendarRecord = $_
                Get-CalendarSignature @($calendarHeaders | ForEach-Object { [string]$calendarRecord.$_ })
            })
            $expectedSignatures = $expectedCalendarRows.ToArray()
            [Array]::Sort($expectedSignatures, [StringComparer]::Ordinal)
            [Array]::Sort($actualSignatures, [StringComparer]::Ordinal)
            Assert-StateExport ($actualSignatures.Count -eq $expectedSignatures.Count) "$profile lost or invented calendar-detail rows."
            for ($calendarIndex = 0; $calendarIndex -lt $expectedSignatures.Count; $calendarIndex++) {
                Assert-StateExport ($actualSignatures[$calendarIndex] -ceq $expectedSignatures[$calendarIndex]) "$profile changed a resolved calendar-detail value."
            }
            $calendarRowsChecked = $expectedSignatures.Count
        }
        $unchanged = 0
        if ($baseline) {
            $prior = @($baseline.Results | Where-Object { $_.InputIndex -eq $inputIndex -and $_.Profile -ceq $profile })
            Assert-StateExport ($prior.Count -eq 1 -and $prior[0].SourceSha256 -ceq $sourceHash) 'Baseline source/order differs.'
            foreach ($fileName in $hashes.Keys) {
                if ($BaselinePolicy -ceq 'CalendarDetail') {
                    if ($fileName -in @('11_XER_CALENDAR_DETAILED.csv','XER_CSV_MANIFEST.csv')) { continue }
                } elseif ($tender -and $fileName -in @('02_XER_PROJECT.csv','XER_CSV_MANIFEST.csv')) { continue }
                Assert-StateExport ($hashes[$fileName] -ceq $prior[0].Hashes.$fileName) "$profile changed a protected pre-change output: $fileName."
                $unchanged++
            }
        }
        $results.Add([pscustomobject]@{ InputIndex=$inputIndex; InputName=$sourceName; SourceSha256=$sourceHash; Profile=$profile; Version=$version; Files=$memory.Files.Count; WarningCount=$memory.WarningCount; PreChangeFilesUnchanged=$unchanged; CalendarRowsCompared=$calendarRowsChecked; Hashes=$hashes })
    }
    Assert-StateExport ((Get-FileHash -LiteralPath $sourcePath).Hash -ceq $sourceHash) 'Original XER changed during validation.'
}
[pscustomobject]@{ Result='PASS'; Checks=$checks; Scope='Real Core disk/Web-byte exports and protected-output comparison; not M runtime, live refresh or native P6 parity'; OutputDirectory=$runRoot; Results=$results.ToArray() } | ConvertTo-Json -Depth 8
