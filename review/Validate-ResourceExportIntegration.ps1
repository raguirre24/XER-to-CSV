param(
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string[]]$Paths,
    [string]$BaselineHashes,
    [string]$UnchangedCsvDirectory,
    [string]$CoreAssemblyPath = (Join-Path $PSScriptRoot '../XerToCsvConverter.Core/bin/Debug/net8.0/XerToCsvConverter.Core.dll'),
    [string]$OutputRoot = (Join-Path $PSScriptRoot '../artifacts/resource-nonblocking')
)

# Complete Standard Enhanced export checks. All inputs are ordered occurrences.
# This script persists only new local validation outputs, never original sources.
$ErrorActionPreference = 'Stop'
$assemblyBytes = [IO.File]::ReadAllBytes((Resolve-Path -LiteralPath $CoreAssemblyPath).Path)
$null = [Reflection.Assembly]::Load($assemblyBytes)
$cancel = [Threading.CancellationToken]::None
function Assert-Resource([bool]$Condition, [string]$Message) { if (-not $Condition) { throw $Message } }
function Get-ResourceHash([byte[]]$Bytes) { [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($Bytes)) }

$inputPaths = [Collections.Generic.List[string]]::new()
foreach ($path in $Paths) { $inputPaths.Add((Resolve-Path -LiteralPath $path).Path) }
$originalHashes = @($inputPaths | ForEach-Object { (Get-FileHash -LiteralPath $_).Hash })
$names = [Collections.Generic.List[string]]::new()
foreach ($name in @('01_XER_TASK','02_XER_PROJECT','03_XER_PROJWBS','04_XER_BASELINE','06_XER_PREDECESSOR','07_XER_ACTVTYPE','08_XER_ACTVCODE','09_XER_TASKACTV','10_XER_CALENDAR','11_XER_CALENDAR_DETAILED','12_XER_RSRC','13_XER_TASKRSRC','14_XER_UMEASURE','15_XER_RESOURCE_DISTRIBUTION')) { $names.Add($name) }
$service = [XerToCsvConverter.ProcessingService]::new()
$diskStore = $service.ParseMultipleXerFilesAsync($inputPaths, $null, $cancel).GetAwaiter().GetResult()
$streams = [Collections.Generic.List[ValueTuple[IO.Stream,string]]]::new()
try {
    foreach ($path in $inputPaths) { $streams.Add([ValueTuple[IO.Stream,string]]::new([IO.File]::OpenRead($path), [IO.Path]::GetFileName($path))) }
    $streamStore = $service.ParseXerStreamsAsync($streams, $null, $cancel).GetAwaiter().GetResult()
} finally { foreach ($pair in $streams) { $pair.Item1.Dispose() } }
$diskMemory = $service.ExportTablesToMemoryWithDiagnosticsAsync($diskStore, $names, $null, $cancel).GetAwaiter().GetResult()
$streamMemory = $service.ExportTablesToMemoryWithDiagnosticsAsync($streamStore, $names, $null, $cancel).GetAwaiter().GetResult()
Assert-Resource ($diskMemory.Files.Count -eq 15 -and $streamMemory.Files.Count -eq 15) 'A requested Standard Enhanced table or companion is missing.'
Assert-Resource ($diskMemory.WarningCount -eq $streamMemory.WarningCount) 'Disk/Web-stream warning counts differ.'
$runRoot = Join-Path ([IO.Path]::GetFullPath($OutputRoot)) ([Guid]::NewGuid().ToString('N').Substring(0,12))
$null = New-Item -ItemType Directory -Path $runRoot
$published = $service.ExportTablesWithDiagnosticsAsync($diskStore, $names, $runRoot, $null, $cancel).GetAwaiter().GetResult()
Assert-Resource ($published.Files.Count -eq 15 -and $published.WarningCount -eq $diskMemory.WarningCount) 'Complete disk publication disagrees with memory export.'
$hashes = [ordered]@{}
foreach ($name in $diskMemory.Files.Keys) {
    $hash = Get-ResourceHash $diskMemory.Files[$name]
    $hashes[$name] = $hash
    Assert-Resource ($streamMemory.Files.ContainsKey($name) -and (Get-ResourceHash $streamMemory.Files[$name]) -ceq $hash) "Disk/Web-stream bytes differ for $name."
    Assert-Resource ((Get-FileHash -LiteralPath (Join-Path $runRoot ($name + '.csv'))).Hash -ceq $hash) "Published CSV bytes differ for $name."
}
$baselineComparisons = 0
if ($BaselineHashes) {
    $baseline = Get-Content -LiteralPath $BaselineHashes -Raw | ConvertFrom-Json
    Assert-Resource ($baseline.InputSha256.Count -eq $originalHashes.Count) 'Baseline source occurrence count differs.'
    for ($index = 0; $index -lt $originalHashes.Count; $index++) {
        Assert-Resource ($originalHashes[$index] -ceq $baseline.InputSha256[$index]) 'Baseline source content/order differs.'
    }
    foreach ($name in $names) {
        Assert-Resource ($hashes[$name] -ceq $baseline.Hashes.$name) "Previously valid numbered CSV changed: $name."
        $baselineComparisons++
    }
}
if ($UnchangedCsvDirectory) {
    foreach ($file in (Get-ChildItem -LiteralPath $UnchangedCsvDirectory -File -Filter '*.csv')) {
        $name = [IO.Path]::GetFileNameWithoutExtension($file.Name)
        Assert-Resource ($diskMemory.Files.ContainsKey($name) -and $hashes[$name] -ceq (Get-FileHash -LiteralPath $file.FullName).Hash) "Existing source-check CSV changed: $name."
        $baselineComparisons++
    }
}
for ($index = 0; $index -lt $inputPaths.Count; $index++) {
    Assert-Resource ((Get-FileHash -LiteralPath $inputPaths[$index]).Hash -ceq $originalHashes[$index]) 'An original source changed during validation.'
}
$quality = @(Import-Csv -LiteralPath (Join-Path $runRoot 'XER_DATA_QUALITY.csv'))
Assert-Resource ($quality.Count -eq $published.WarningCount) 'Published companion rows disagree with completion warnings.'
$summary = [ordered]@{
    Validation = 'Passed complete Standard disk/Web-stream memory parity and disk publication; not native P6 parity'
    CoreAssemblySha256 = Get-ResourceHash $assemblyBytes
    InputPaths = $inputPaths.ToArray()
    InputSha256 = $originalHashes
    PublishedFiles = $published.Files.Count
    DataQualityWarnings = $quality.Count
    AllocationWarnings = @($quality | Where-Object allocation_portion -In @('Actual','Remaining')).Count
    GeneralWarnings = @($quality | Where-Object allocation_portion -EQ '').Count
    WarningGroups = @($quality | Group-Object table_name,allocation_portion,issue_code | ForEach-Object { [ordered]@{ Group = $_.Name; Count = $_.Count } })
    BaselineNumberedFilesUnchanged = $baselineComparisons
    OutputDirectory = $runRoot
    Hashes = $hashes
}
$summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $runRoot 'validation-summary.json') -Encoding utf8
$summary | ConvertTo-Json -Depth 6
