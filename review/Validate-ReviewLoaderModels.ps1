param(
    [string[]]$ModelRoots = @(
        'C:\Users\ricar\Documents\Code\Programme Review\Programme-Review\Project Review - Programme (datalake).SemanticModel\definition',
        'C:\Users\ricar\Documents\Code\Tender-Review\Project Review - Tender Programme (datalake).SemanticModel\definition'
    ),
    [string[]]$BaselineModelRoots = @(),
    [string]$TomDirectory,
    [string]$PowerQueryParserDirectory
)

$ErrorActionPreference = 'Stop'

if (-not $TomDirectory) {
    $packageRoot = Join-Path ([Environment]::GetFolderPath('UserProfile')) '.nuget\packages\microsoft.analysisservices'
    $package = Get-ChildItem -LiteralPath $packageRoot -Directory |
        Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName 'lib\net8.0\Microsoft.AnalysisServices.Tabular.dll') } |
        Sort-Object { [version]$_.Name } -Descending |
        Select-Object -First 1
    if (-not $package) { throw 'No cached Microsoft.AnalysisServices net8.0 package is available. Pass -TomDirectory.' }
    $TomDirectory = Join-Path $package.FullName 'lib\net8.0'
}

if (-not $PowerQueryParserDirectory) {
    $cacheRoot = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'npm-cache\_npx'
    $PowerQueryParserDirectory = Get-ChildItem -LiteralPath $cacheRoot -Directory |
        ForEach-Object { Join-Path $_.FullName 'node_modules\@microsoft\powerquery-parser' } |
        Where-Object { Test-Path -LiteralPath (Join-Path $_ 'package.json') } |
        Select-Object -First 1
    if (-not $PowerQueryParserDirectory) { throw 'No cached Microsoft Power Query parser is available. Pass -PowerQueryParserDirectory.' }
}

foreach ($dll in @('Microsoft.AnalysisServices.Core.dll', 'Microsoft.AnalysisServices.Tabular.dll', 'Microsoft.AnalysisServices.Tabular.Json.dll')) {
    [Reflection.Assembly]::LoadFrom((Join-Path $TomDirectory $dll)) | Out-Null
}

$modelResults = @()
$queries = @()
if ($BaselineModelRoots.Count -ne 0 -and $BaselineModelRoots.Count -ne $ModelRoots.Count) {
    throw 'Supply one baseline definition folder per staged model, or an empty baseline array for full models.'
}
$validationTempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$validationTemp = Join-Path $validationTempRoot ('review-loader-validation-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $validationTemp | Out-Null
try {
for ($modelIndex = 0; $modelIndex -lt $ModelRoots.Count; $modelIndex++) {
    $root = $ModelRoots[$modelIndex]
    $resolvedRoot = (Resolve-Path -LiteralPath $root).Path
    $validationRoot = $resolvedRoot
    if ($BaselineModelRoots.Count -ne 0) {
        $baselineRoot = (Resolve-Path -LiteralPath $BaselineModelRoots[$modelIndex]).Path
        $validationRoot = Join-Path $validationTemp ('model-' + $modelIndex)
        New-Item -ItemType Directory -Path $validationRoot | Out-Null
        Get-ChildItem -LiteralPath $baselineRoot -Force | Copy-Item -Destination $validationRoot -Recurse -Force
        Get-ChildItem -LiteralPath $resolvedRoot -Force | Copy-Item -Destination $validationRoot -Recurse -Force
    }
    $model = [Microsoft.AnalysisServices.Tabular.TmdlSerializer]::DeserializeModelFromFolder($validationRoot)
    $sharedCount = 0
    $partitionCount = 0
    foreach ($expression in $model.Expressions) {
        if ($expression.Kind.ToString() -ne 'M') { continue }
        $queries += [pscustomobject]@{ model = $resolvedRoot; name = $expression.Name; kind = 'shared'; expression = $expression.Expression }
        $sharedCount++
    }
    foreach ($table in $model.Tables) {
        foreach ($partition in $table.Partitions) {
            if ($partition.Source -isnot [Microsoft.AnalysisServices.Tabular.MPartitionSource]) { continue }
            $queries += [pscustomobject]@{ model = $resolvedRoot; name = $table.Name + '/' + $partition.Name; kind = 'partition'; expression = $partition.Source.Expression }
            $partitionCount++
        }
    }
    $modelResults += [pscustomobject]@{
        model = $resolvedRoot
        tmdl = 'PASS'
        tables = $model.Tables.Count
        relationships = $model.Relationships.Count
        sharedExpressions = $sharedCount
        mPartitions = $partitionCount
    }
}

$payload = [pscustomobject]@{ models = $modelResults; queries = $queries }
$payload | ConvertTo-Json -Depth 8 -Compress |
    & node (Join-Path $PSScriptRoot 'validate-review-loader-m.cjs') $PowerQueryParserDirectory
if ($LASTEXITCODE -ne 0) { throw 'Microsoft Power Query parser validation failed.' }
}
finally {
    $resolvedTemp = (Resolve-Path -LiteralPath $validationTemp).Path
    if (-not $resolvedTemp.StartsWith($validationTempRoot, [StringComparison]::OrdinalIgnoreCase) -or
        [IO.Path]::GetFileName($resolvedTemp) -notmatch '^review-loader-validation-[a-f0-9]{32}$') {
        throw 'Refusing to remove an unexpected temporary validation path.'
    }
    Remove-Item -LiteralPath $resolvedTemp -Recurse -Force
}
