param([string]$Configuration = 'Debug')

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$dotnet = Join-Path $repo 'local-dotnet-sdk/dotnet.exe'
$cli = Join-Path $repo "XerToCsvConverter.TenderReview.Cli/bin/$Configuration/net8.0/XerToCsvConverter.TenderReview.Cli.dll"
if (-not (Test-Path -LiteralPath $cli)) { throw 'Build the Tender CLI before running this smoke check.' }
$fixture = Join-Path $PSScriptRoot 'fixtures/J5001_C_BL01_2026-01-31.xer'
$fixtureHash = (Get-FileHash -LiteralPath $fixture).Hash
$nativeCode = 'QAC000623-01-02'
$reportCode = 'QAC000623'
$content = [IO.File]::ReadAllText($fixture).Replace("`tJ5001`t", "`t$nativeCode`t")
if (-not $content.Contains("`t$nativeCode`t")) { throw 'Fixture project substitution failed.' }
$run = Join-Path $repo ('artifacts/tender-project-mapping-validation/cli-' + [Guid]::NewGuid().ToString('N'))
[void][IO.Directory]::CreateDirectory($run)
$utf8 = [Text.UTF8Encoding]::new($false)
$source = Join-Path $run 'NE Part B Backup74 LIVE.xer'
[IO.File]::WriteAllText($source, $content, $utf8)
$sourceHash = (Get-FileHash -LiteralPath $source).Hash
$config = @{
    project_code = $reportCode
    project_name = 'Synthetic NE Part B'
    exported_at_utc = '2026-09-10T00:00:00Z'
    sources = @(@{ xer_file_path = $source; original_xer_filename = 'NE Part B Backup74 LIVE.xer'; status_date = '2026-09-05' })
}
$configPath = Join-Path $run 'tender.json'
[IO.File]::WriteAllText($configPath, ($config | ConvertTo-Json -Depth 5), $utf8)
$stdout = Join-Path $run 'stdout.log'
$stderr = Join-Path $run 'stderr.log'
# Windows PowerShell treats native stderr as errors under Stop; capture it as expected CLI output.
$ErrorActionPreference = 'Continue'
& $dotnet $cli --config $configPath --output-root (Join-Path $run 'output') 1> $stdout 2> $stderr
$exitCode = $LASTEXITCODE
$ErrorActionPreference = 'Stop'
if ($exitCode -ne 0) { throw "Tender CLI failed ($exitCode); inspect $stderr" }
$diagnostics = [IO.File]::ReadAllText($stderr)
foreach ($expected in @('TENDER_PROJECT_CODE_MAPPED', $nativeCode, "reporting project code '$reportCode'")) {
    if (-not $diagnostics.Contains($expected)) { throw "Missing CLI diagnostic: $expected" }
}
if ($diagnostics.Contains('XER_DATA_QUALITY.csv')) { throw 'Review CLI incorrectly promises a diagnostic CSV.' }
$bundle = ([IO.File]::ReadAllText($stdout)).Trim()
$files = @(Get-ChildItem -LiteralPath $bundle -File)
if ($files.Count -ne 11) { throw "Expected eleven bundle files, got $($files.Count)." }
$manifest = @(Import-Csv -LiteralPath (Join-Path $bundle 'XER_CSV_MANIFEST.csv'))
if ($manifest.Count -ne 10) { throw 'Expected ten manifest rows.' }
foreach ($row in $manifest) {
    if ($row.project_code -ne $reportCode -or $row.schema_version -ne '3.0' -or $row.bundle_profile -ne 'tender_review' -or $row.bundle_status -ne 'COMPLETE') {
        throw 'Manifest reporting identity or contract mismatch.'
    }
    if ($row.canonical_xer_filename -ne 'QAC000623-TENDER-20260905.xer') { throw 'Incorrect canonical reporting name.' }
    if ($row.original_xer_filename -ne 'NE Part B Backup74 LIVE.xer') { throw 'Original filename was not preserved.' }
    if ($row.source_sha256 -ne $sourceHash.ToLowerInvariant()) { throw 'Incorrect source hash.' }
}
foreach ($table in @('01_XER_TASK.csv', '02_XER_PROJECT.csv')) {
    foreach ($row in (Import-Csv -LiteralPath (Join-Path $bundle $table))) {
        if ($row.ProjectCode -ne $reportCode) { throw "Incorrect reporting code in $table" }
    }
}
if ((Get-FileHash -LiteralPath $source).Hash -ne $sourceHash -or (Get-FileHash -LiteralPath $fixture).Hash -ne $fixtureHash) {
    throw 'A source fixture was modified during export.'
}
Write-Output "PASS: explicit Tender project mapping, CLI diagnostics, eleven-file bundle, manifest identity and source preservation. Artifacts: $run"
