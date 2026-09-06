param(
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string[]]$Paths,
    [string]$CoreAssemblyPath = (Join-Path $PSScriptRoot '../XerToCsvConverter.Core/bin/Debug/net8.0/XerToCsvConverter.Core.dll'),
    [string]$OutputRoot = (Join-Path $PSScriptRoot '../artifacts/relclones')
)

# A diagnostic fixture only: change PROJECT.proj_short_name in a new local copy
# to exercise governed review profiles. Preserve every other byte and originals.
# The resulting bundles are not exports of unmodified production sources.
$ErrorActionPreference = 'Stop'
$null = [Reflection.Assembly]::Load([IO.File]::ReadAllBytes((Resolve-Path -LiteralPath $CoreAssemblyPath).Path))
$runRoot = Join-Path ([IO.Path]::GetFullPath($OutputRoot)) ([Guid]::NewGuid().ToString('N').Substring(0, 12))
$null = New-Item -ItemType Directory -Path $runRoot
$results = [Collections.Generic.List[object]]::new()
$ordinal = 0
foreach ($path in $Paths) {
    $ordinal++
    $original = (Resolve-Path -LiteralPath $path).Path
    $originalHash = (Get-FileHash -LiteralPath $original).Hash
    $bytes = [IO.File]::ReadAllBytes($original)
    # Latin1 is deliberately a byte-preserving transport here, not XER decoding.
    # Only ASCII table delimiters and the ASCII project label are interpreted.
    $text = [Text.Encoding]::Latin1.GetString($bytes)
    $lines = [regex]::Split($text, '(\r\n|\n|\r)')
    $inProject = $false
    $projectRows = 0
    $changedFields = 0
    $oldLabel = ''
    $dataDate = ''
    $labelIndex = -1
    $dateIndex = -1
    for ($index = 0; $index -lt $lines.Length; $index += 2) {
        $line = $lines[$index]
        if ($line.StartsWith("%T`t", [StringComparison]::Ordinal)) { $inProject = $line -ceq "%T`tPROJECT"; continue }
        if (-not $inProject) { continue }
        if ($line.StartsWith("%F`t", [StringComparison]::Ordinal)) {
            $headers = $line.Split("`t")
            $labelIndex = [array]::IndexOf($headers, 'proj_short_name')
            $dateIndex = [array]::IndexOf($headers, 'last_recalc_date')
            if ($labelIndex -lt 1 -or $dateIndex -lt 1) { throw 'Required raw project metadata is absent.' }
        } elseif ($line.StartsWith("%R`t", [StringComparison]::Ordinal)) {
            $projectRows++
            $fields = $line.Split("`t")
            if ($labelIndex -lt 1 -or $dateIndex -lt 1 -or $labelIndex -ge $fields.Length -or $dateIndex -ge $fields.Length) { throw 'Required raw project metadata is absent or omitted.' }
            $oldLabel = $fields[$labelIndex]
            $dataDate = [DateTime]::Parse($fields[$dateIndex], [Globalization.CultureInfo]::InvariantCulture).ToString('yyyy-MM-dd')
            if ($oldLabel -ceq 'J5001') { throw 'This clone diagnostic requires a different original label to make the metadata change explicit.' }
            $fields[$labelIndex] = 'J5001'
            $lines[$index] = $fields -join "`t"
            $changedFields++
            $restored = $fields.Clone(); $restored[$labelIndex] = $oldLabel
            if (($restored -join "`t") -cne $line) { throw 'Metadata clone changed another field.' }
        }
    }
    if ($projectRows -ne 1 -or $changedFields -ne 1) { throw 'Each clone diagnostic requires exactly one project row and one metadata change.' }

    $jsonOptions = [Text.Json.JsonSerializerOptions]::new()
    $jsonOptions.PropertyNamingPolicy = [Text.Json.JsonNamingPolicy]::SnakeCaseLower
    $requestJson = @{ project_code = 'J5001'; project_name = 'Diagnostic metadata clone'; exported_at_utc = '2026-09-06T00:00:00Z'; sources = @(@{ source_token = 'original-000001'; original_xer_filename = [IO.Path]::GetFileName($original); status_date = $dataDate }) } | ConvertTo-Json -Depth 5
    $request = [Text.Json.JsonSerializer]::Deserialize($requestJson, [XerToCsvConverter.TenderReview.TenderReviewBundleRequest], $jsonOptions)
    $uploads = [Collections.Generic.List[XerToCsvConverter.TenderReview.TenderReviewSourceBytes]]::new()
    $upload = [XerToCsvConverter.TenderReview.TenderReviewSourceBytes]::new()
    $upload.SourceToken = 'original-000001'; $upload.Content = $bytes; $uploads.Add($upload)
    $originalRejection = ''
    try {
        $null = ([XerToCsvConverter.TenderReview.TenderReviewBundleService]::new()).BuildFromXerBytesAsync($request, $uploads, $null, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
    } catch { $originalRejection = $_.Exception.ToString() }
    if (-not $originalRejection.Contains('PROJECT.proj_short_name is invalid.', [StringComparison]::Ordinal)) { throw "The original input did not show the expected governed project-code rejection: $originalRejection" }

    $cloneFolder = Join-Path $runRoot ('input-' + $ordinal.ToString('D6'))
    $null = New-Item -ItemType Directory -Path $cloneFolder
    $clone = Join-Path $cloneFolder ([IO.Path]::GetFileName($original))
    [IO.File]::WriteAllBytes($clone, [Text.Encoding]::Latin1.GetBytes(($lines -join '')))
    try {
        $validation = & (Join-Path $PSScriptRoot 'Validate-RelationshipExportIntegration.ps1') -Paths @($clone) -ProfileFixture $clone -CoreAssemblyPath $CoreAssemblyPath -OutputRoot (Join-Path $runRoot 'bundles') -RecordProfileValidationFailures
    } finally {
        if ((Get-FileHash -LiteralPath $original).Hash -cne $originalHash) { throw 'Original source changed during metadata-clone validation.' }
    }
    $results.Add([pscustomobject]@{
        OriginalFileName = [IO.Path]::GetFileName($original)
        OriginalSha256 = $originalHash
        OriginalTenderRejection = ($originalRejection -split '\r?\n' | Where-Object { $_ -like '*PROJECT.proj_short_name is invalid.*' } | Select-Object -First 1).Trim()
        DiagnosticOnlyChangedField = 'PROJECT.proj_short_name'
        OriginalLabel = $oldLabel
        CloneLabel = 'J5001'
        ClonePath = $clone
        CloneSha256 = (Get-FileHash -LiteralPath $clone).Hash
        Validation = ($validation | ConvertFrom-Json)
    })
}
[pscustomobject]@{ Scope = 'Metadata-only diagnostic clones; not unchanged original review exports or native P6 parity'; Results = $results.ToArray(); GeneratedRoot = $runRoot } | ConvertTo-Json -Depth 9
