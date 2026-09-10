param(
    [string]$ProgrammeExpressions = 'C:\Users\ricar\Documents\Code\Programme Review\Programme-Review\Project Review - Programme (datalake).SemanticModel\definition\expressions.tmdl',
    [string]$TenderExpressions = 'C:\Users\ricar\Documents\Code\Tender-Review\Project Review - Tender Programme (datalake).SemanticModel\definition\expressions.tmdl',
    [string]$CoreAssemblyPath = (Join-Path $PSScriptRoot '../XerToCsvConverter.Core/bin/Debug/net8.0/XerToCsvConverter.Core.dll')
)

# Offline contract reconciliation: executes the real exporter, reads literal M contracts.
# Does NOT evaluate the M loader or simulate a Power BI/SharePoint refresh.
$ErrorActionPreference = 'Stop'
[Reflection.Assembly]::LoadFrom((Resolve-Path -LiteralPath $CoreAssemblyPath).Path) | Out-Null
Add-Type -AssemblyName Microsoft.VisualBasic.Core
$jsonOptions = [Text.Json.JsonSerializerOptions]::new()
$jsonOptions.PropertyNamingPolicy = [Text.Json.JsonNamingPolicy]::SnakeCaseLower
$cancel = [Threading.CancellationToken]::None
$checks = 0
function Assert-Check([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
    $script:checks++
}
function Get-Expression([string]$Content, [string]$Name) {
    $match = [regex]::Match($Content, '(?ms)^expression ' + [regex]::Escape($Name) + ' =\s*(.*?)^\tlineageTag:')
    if (-not $match.Success) { throw "Missing M expression: $Name" }
    return $match.Groups[1].Value.Trim()
}
function Get-LiteralList([string]$Content, [string]$Name) {
    $match = [regex]::Match($Content, '(?s)\b' + [regex]::Escape($Name) + '\s*=\s*\{([^{}]*)\}')
    if (-not $match.Success) { throw "Missing literal M list: $Name" }
    return ,@([regex]::Matches($match.Groups[1].Value, '"([^"]+)"') | ForEach-Object { $_.Groups[1].Value })
}
function Get-Csv([byte[]]$Bytes) {
    $reader = [Microsoft.VisualBasic.FileIO.TextFieldParser]::new([IO.StringReader]::new([Text.Encoding]::UTF8.GetString($Bytes)))
    try {
        $reader.SetDelimiters([string[]]@(','))
        $reader.TrimWhiteSpace = $false
        $headers = $reader.ReadFields()
        $rows = @()
        while (-not $reader.EndOfData) {
            $fields = $reader.ReadFields()
            Assert-Check ($fields.Count -eq $headers.Count) 'Exporter emitted a nonrectangular CSV.'
            $row = @{}
            for ($i = 0; $i -lt $headers.Count; $i++) { $row[$headers[$i]] = $fields[$i] }
            $rows += ,$row
        }
        return @{ Headers = $headers; Rows = $rows }
    } finally { $reader.Dispose() }
}
$programmeText = Get-Content -LiteralPath $ProgrammeExpressions -Raw
$tenderText = Get-Content -LiteralPath $TenderExpressions -Raw
foreach ($helper in @('fnXerCsvProjectList','fnXerCsvProjectToken','fnXerCsvProjectFileToken','fnXerCsvSha256')) {
    $first = (Get-Expression $programmeText $helper) -replace '\s+', ' '
    $second = (Get-Expression $tenderText $helper) -replace '\s+', ' '
    Assert-Check ($first -ceq $second) "Shared identity helper differs across reports: $helper."
}
Assert-Check ($programmeText -notmatch 'SupportedSchemaVersions|RecommendedSchemaVersion|bundle\[SchemaVersion\]\s*=\s*"[123]\.0"') 'Programme retains a legacy schema path.'
Assert-Check ($programmeText -match 'RequiredSchemaVersion = "4\.0"') 'Programme current-only gate is not 4.0.'
Assert-Check ($programmeText -match 'RawVersions\{0\} = RequiredSchemaVersion') 'Programme does not force raw manifest version validation.'
Assert-Check ($tenderText -match 'SchemaVersions\{0\} <> "3\.0"') 'Tender current-only gate is not 3.0.'
Assert-Check ($tenderText -match 'BundleProfiles\{0\} <> "tender_review"') 'Tender profile discriminator is not required.'
Assert-Check ($tenderText -notmatch 'SchemaVersions\{0\} <> "[12]\.0"') 'Tender retains a legacy version gate.'

$fixture = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'fixtures/J5001_C_BL01_2026-01-31.xer') -Raw
$names = @('J5001', 'C5001', '5001', 'CIVIL', 'JIVIL', 'QAC11111', 'QAC000623-01-02', 'NE Part B', 'NE  Part B', 'Māori 工程 Étage 2', '../North\South:Part|B%25', 'North, "Section B"', ('A' * 260 + ' Māori/Part B'))
$summaries = @()
foreach ($profile in @('Programme','Tender')) {
    $tender = $profile -eq 'Tender'
    $content = if ($tender) { $tenderText } else { $programmeText }
    $contractName = if ($tender) { 'TenderCsvTableContracts' } else { 'XerCsvTableContracts' }
    $manifestName = if ($tender) { 'fnTenderCsvReadBundle' } else { 'fnXerCsvReadBundle' }
    $literalContracts = Get-Expression $content $contractName
    $literalManifest = Get-LiteralList (Get-Expression $content $manifestName) 'ExpectedManifestColumns'
    $contracts = if ($tender) { [XerToCsvConverter.TenderReview.TenderReviewContract]::Tables } else { [XerToCsvConverter.ProgrammeReview.ProgrammeReviewContract]::Tables }
    $version = if ($tender) { [XerToCsvConverter.TenderReview.TenderReviewContract]::SchemaVersion } else { [XerToCsvConverter.ProgrammeReview.ProgrammeReviewContract]::SchemaVersion }
    Assert-Check (([regex]::Matches($literalContracts, '#"\d\d_XER_[A-Z_]+"\s*=\s*\[')).Count -eq $contracts.Count) "$profile has a missing/extra M table contract."
    foreach ($table in $contracts) {
        $block = [regex]::Match($literalContracts, '(?s)#"' + $table.TableName + '"\s*=\s*\[(.*?)\]').Groups[1].Value
        $headers = Get-LiteralList $block 'Columns'
        Assert-Check (($headers -join '|') -ceq (($table.Columns | ForEach-Object Name) -join '|')) "$profile $($table.TableName) header/order disagrees with Core."
        Assert-Check ($block.Contains('FileName = "' + $table.FileName + '"')) "$profile filename differs from Core."
        foreach ($column in $table.Columns) {
            $type = switch ($column.Type.ToString()) { Text {'text'} Date {'date'} Number {'number'} Integer {'Int64.Type'} Boolean {'logical'} }
            $pattern = '\{"' + [regex]::Escape($column.Name) + '",\s*' + $(if ($type -eq 'Int64.Type') { 'Int64\.Type' } else { 'type(?: nullable)? ' + $type }) + '\}'
            Assert-Check ($block -match $pattern) "$profile $($table.TableName).$($column.Name) has an incompatible M type."
        }
    }
    foreach ($code in $names) {
        $normalised = [XerToCsvConverter.ReviewProjectIdentity]::NormalizeCode($code)
        $xer = [Text.Encoding]::UTF8.GetBytes($fixture.Replace("`tJ5001`t", "`t$normalised`t"))
        $common = @{ project_code = $normalised; project_name = 'Synthetic loader contract fixture'; parser_version = 'loader-contract-check'; exported_at_utc = '2026-09-10T00:00:00Z' }
        if ($tender) {
            $common.state = ' Queensland '
            # Repeated filenames and identical bytes must retain two independent stages.
            $common.sources = @(
                @{ source_token = 'first'; original_xer_filename = 'same.xer'; status_date = '2026-01-31' },
                @{ source_token = 'second'; original_xer_filename = 'same.xer'; status_date = '2026-02-01' })
            $request = [Text.Json.JsonSerializer]::Deserialize(($common | ConvertTo-Json -Depth 8), [XerToCsvConverter.TenderReview.TenderReviewBundleRequest], $jsonOptions)
            $inputs = [Collections.Generic.List[XerToCsvConverter.TenderReview.TenderReviewSourceBytes]]::new()
            foreach ($token in @('first','second')) {
                $source = [XerToCsvConverter.TenderReview.TenderReviewSourceBytes]::new()
                $source.SourceToken = $token
                $source.Content = $xer
                $inputs.Add($source)
            }
            $result = ([XerToCsvConverter.TenderReview.TenderReviewBundleService]::new()).BuildFromXerBytesAsync($request, $inputs, $null, $cancel).GetAwaiter().GetResult()
        } else {
            $common.programme_type = 'C'
            $common.snapshots = @(@{ original_xer_filename = 'same.xer'; snapshot_kind = 0; snapshot_tag = 'BL01'; month_update = '2026-01-31'; data_date = '2026-01-30' })
            $request = [Text.Json.JsonSerializer]::Deserialize(($common | ConvertTo-Json -Depth 8), [XerToCsvConverter.ProgrammeReview.ProgrammeReviewBundleRequest], $jsonOptions)
            $inputs = [Collections.Generic.Dictionary[string,byte[]]]::new([StringComparer]::Ordinal)
            $inputs.Add('same.xer', $xer)
            $result = ([XerToCsvConverter.ProgrammeReview.ProgrammeReviewBundleService]::new()).BuildFromXerBytesAsync($request, $inputs, $null, $cancel).GetAwaiter().GetResult()
        }
        Assert-Check ($result.Files.Count -eq 11) "$profile bundle must be ten numbered CSVs and one manifest."
        $manifest = Get-Csv $result.Files['XER_CSV_MANIFEST.csv']
        Assert-Check (($manifest.Headers -join '|') -ceq ($literalManifest -join '|')) "$profile manifest headers differ from M."
        Assert-Check (@($manifest.Rows | Where-Object { $_.schema_version -cne $version }).Count -eq 0) "$profile emitted wrong version."
        $token = [XerToCsvConverter.ReviewProjectIdentity]::EncodeComponent($normalised)
        $fileToken = [XerToCsvConverter.ReviewProjectIdentity]::FileComponent($normalised)
        Assert-Check ($result.BundleId.StartsWith($fileToken + '_', [StringComparison]::Ordinal)) "$profile bundle has unexpected filename identity."
        foreach ($table in $contracts) {
            $csv = Get-Csv $result.Files[$table.FileName]
            Assert-Check (($csv.Headers -join '|') -ceq (($table.Columns | ForEach-Object Name) -join '|')) "$profile $($table.TableName) actual exporter header differs."
            $manifestRows = @($manifest.Rows | Where-Object { $_.table_name -ceq $table.TableName })
            $expectedCount = ($manifestRows | ForEach-Object { [int]$_.row_count } | Measure-Object -Sum).Sum
            Assert-Check ($csv.Rows.Count -eq $expectedCount) "$profile $($table.TableName) manifest count differs."
            $hash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($result.Files[$table.FileName]))
            Assert-Check (@($manifestRows | Where-Object { $_.csv_sha256.ToUpperInvariant() -cne $hash }).Count -eq 0) "$profile manifest hash differs from published bytes."
            foreach ($row in $csv.Rows) {
                if ($row.ContainsKey('ProjectCode')) { Assert-Check ($row.ProjectCode -ceq $normalised) "$profile changed business code." }
                foreach ($key in @($row.Keys | Where-Object { $_.EndsWith('_key') -and $row[$_] -ne '' })) {
                    Assert-Check ($row[$key].StartsWith('CSV::' + $token + '::', [StringComparison]::Ordinal)) "$profile changed encoded key component."
                    Assert-Check ($row[$key].Split('::').Count -eq 5) "$profile current key contains a delimiter collision."
                }
            }
        }
        if ($tender) {
            Assert-Check (@($manifest.Rows | Where-Object { $_.project_state -cne 'QLD' }).Count -eq 0) 'Tender manifest omitted or changed manual State.'
            $projectCsv = Get-Csv $result.Files['02_XER_PROJECT.csv']
            Assert-Check (@($projectCsv.Rows | Where-Object { $_.state -cne 'QLD' }).Count -eq 0) 'Tender table 02 does not mirror manual State.'
            Assert-Check ($manifest.Rows.Count -eq 20) 'Tender collapsed repeated input stages.'
            Assert-Check (@($manifest.Rows.original_xer_filename | Sort-Object -Unique).Count -eq 1) 'Tender renamed original provenance.'
            Assert-Check (@($manifest.Rows.canonical_xer_filename | Sort-Object -Unique).Count -eq 2) 'Tender stages do not have separate canonical identities.'
        }
    }
    $summaries += [pscustomobject]@{ Profile = $profile; Version = $version; Tables = $contracts.Count; ProjectNameCases = $names.Count; Result = 'PASS' }
}
[pscustomobject]@{ Checks = $checks; Profiles = $summaries; Scope = 'Literal M contract reconciliation and real Core exports; not M runtime execution or live refresh.' } | ConvertTo-Json -Depth 6
