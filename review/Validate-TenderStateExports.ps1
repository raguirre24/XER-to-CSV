param(
    [Parameter(Mandatory)][string[]]$Paths,
    [string]$CoreAssemblyPath = (Join-Path $PSScriptRoot '../XerToCsvConverter.Core/bin/Debug/net8.0/XerToCsvConverter.Core.dll'),
    [string]$BaselineSummary,
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
        Assert-StateExport ($memory.Files.Count -eq 11 -and @(Get-ChildItem -LiteralPath $disk.BundlePath -File).Count -eq 11) "$profile lost a required output."
        $hashes = [ordered]@{}
        foreach ($fileName in $memory.Files.Keys) {
            $hash = Get-StateHash $memory.Files[$fileName]
            Assert-StateExport ((Get-FileHash -LiteralPath (Join-Path $disk.BundlePath $fileName)).Hash -ceq $hash) "$profile disk/Web bytes disagree: $fileName."
            $hashes[$fileName] = $hash
        }
        $manifest = @(Import-Csv -LiteralPath (Join-Path $disk.BundlePath 'XER_CSV_MANIFEST.csv'))
        Assert-StateExport ($manifest.Count -eq 10) "$profile manifest coverage differs."
        foreach ($row in $manifest) {
            $tablePath = Join-Path $disk.BundlePath ($row.table_name + '.csv')
            Assert-StateExport (@(Import-Csv -LiteralPath $tablePath).Count -eq [long]$row.row_count) "$profile table/manifest count mismatch."
            Assert-StateExport ($hashes[$row.table_name + '.csv'] -ceq $row.csv_sha256.ToUpperInvariant()) "$profile table/manifest hash mismatch."
        }
        $version = $manifest[0].schema_version
        if ($tender -and $version -ceq '3.0') {
            Assert-StateExport (@($manifest | Where-Object { $_.project_state -cne 'QLD' }).Count -eq 0) 'Manifest manual State was not applied.'
            $projectRows = @(Import-Csv -LiteralPath (Join-Path $disk.BundlePath '02_XER_PROJECT.csv'))
            Assert-StateExport (@($projectRows | Where-Object { $_.state -cne 'QLD' }).Count -eq 0) 'Table 02 did not mirror manual State.'
        }
        $unchanged = 0
        if ($baseline) {
            $prior = @($baseline.Results | Where-Object { $_.InputIndex -eq $inputIndex -and $_.Profile -ceq $profile })
            Assert-StateExport ($prior.Count -eq 1 -and $prior[0].SourceSha256 -ceq $sourceHash) 'Baseline source/order differs.'
            foreach ($fileName in $hashes.Keys) {
                if ($tender -and $fileName -in @('02_XER_PROJECT.csv','XER_CSV_MANIFEST.csv')) { continue }
                Assert-StateExport ($hashes[$fileName] -ceq $prior[0].Hashes.$fileName) "$profile changed a protected pre-change output: $fileName."
                $unchanged++
            }
        }
        $results.Add([pscustomobject]@{ InputIndex=$inputIndex; InputName=$sourceName; SourceSha256=$sourceHash; Profile=$profile; Version=$version; Files=$memory.Files.Count; WarningCount=$memory.WarningCount; PreChangeFilesUnchanged=$unchanged; Hashes=$hashes })
    }
    Assert-StateExport ((Get-FileHash -LiteralPath $sourcePath).Hash -ceq $sourceHash) 'Original XER changed during validation.'
}
[pscustomobject]@{ Result='PASS'; Checks=$checks; Scope='Real Core disk/Web-byte exports and protected-output comparison; not M runtime, live refresh or native P6 parity'; OutputDirectory=$runRoot; Results=$results.ToArray() } | ConvertTo-Json -Depth 8
