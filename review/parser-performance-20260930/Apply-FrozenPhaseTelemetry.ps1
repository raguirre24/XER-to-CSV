[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$CoreRoot,
    [string]$SourceCoreRoot = (Join-Path $PSScriptRoot '../../XerToCsvConverter.Core')
)

# Apply only clock/observer boundaries to a COPY of frozen Core. This does not
# copy optimised transformers, cached calendars, CSV writers, sorting, JSON
# serializers, request/worker APIs, or any other candidate implementation.
$targetCore = (Resolve-Path -LiteralPath $CoreRoot).Path
$sourceCore = (Resolve-Path -LiteralPath $SourceCoreRoot).Path
if ($targetCore -eq $sourceCore) { throw 'Choose a copied frozen Core, not the active Core project.' }
$utf8 = [Text.UTF8Encoding]::new($false)

function Get-MethodPosition([string]$Text, [string]$Name) {
    $pattern = '(?m)^[ \t]*(?:public|private|internal|protected)\b[^\r\n]*?\b' + [regex]::Escape($Name) + '\s*\('
    $match = [regex]::Match($Text, $pattern)
    if (-not $match.Success) { throw "Method not found: $Name" }
    $open = $match.Index + $match.Length - 1
    $depth = 1
    $index = $open + 1
    while ($depth -gt 0 -and $index -lt $Text.Length) {
        if ($Text[$index] -eq '(') { $depth++ }
        if ($Text[$index] -eq ')') { $depth-- }
        $index++
    }
    while ([char]::IsWhiteSpace($Text[$index])) { $index++ }
    return @{ Declaration = $match.Index; Body = $index }
}

function Get-MethodSource([string]$Text, [string]$Name) {
    $position = Get-MethodPosition $Text $Name
    if ($Text[$position.Body] -ne '{') { throw "Expected block body for $Name" }
    $depth = 1
    $index = $position.Body + 1
    while ($depth -gt 0 -and $index -lt $Text.Length) {
        if ($Text[$index] -eq '{') { $depth++ }
        if ($Text[$index] -eq '}') { $depth-- }
        $index++
    }
    return $Text.Substring($position.Declaration, $index - $position.Declaration)
}

function Copy-MethodScope([ref]$Text, [string]$Candidate, [string]$Name) {
    $candidatePosition = Get-MethodPosition $Candidate $Name
    $scope = [regex]::Match($Candidate.Substring($candidatePosition.Body + 1),
        '^\s*(using var phase = ParserOperationTelemetry\.Measure\([^;]+;)')
    if (-not $scope.Success) { throw "Candidate scope missing: $Name" }
    $position = Get-MethodPosition $Text.Value $Name
    $scopeLine = $scope.Groups[1].Value
    if ($Text.Value.Substring($position.Body).StartsWith("{`n        $scopeLine") -or
        $Text.Value.Substring($position.Body).StartsWith("{`r`n        $scopeLine")) { return }
    if ($Text.Value[$position.Body] -eq '{') {
        $indent = if ($Name -in @('MergeStore','ParseXerStreamAsync','ResolveRequestedTablesAsync','GenerateEnhancedTable')) { '            ' } else { '        ' }
        $Text.Value = $Text.Value.Insert($position.Body + 1, "`n$indent$scopeLine")
    } elseif ($Text.Value.Substring($position.Body, 2) -eq '=>') {
        # The frozen Tender request resolver is expression-bodied. Preserve its
        # original expression and exception text while inserting the same timer.
        $end = $Text.Value.IndexOf(';', $position.Body)
        $expression = $Text.Value.Substring($position.Body + 2, $end - $position.Body - 2).Trim()
        $Text.Value = $Text.Value.Substring(0, $position.Body) + "{`n        $scopeLine`n        return $expression;`n    }" + $Text.Value.Substring($end + 1)
    } else { throw "Unsupported method body: $Name" }
}

function Replace-Required([ref]$Text, [string]$Old, [string]$New) {
    if (-not $Text.Value.Contains($Old)) { throw "Frozen anchor missing: $Old" }
    $Text.Value = $Text.Value.Replace($Old, $New)
}

$files = @(
    'CoreLogic.cs',
    'ProgrammeReview/ProgrammeReviewTransformer.cs',
    'TenderReview/TenderReviewTransformer.cs',
    'ProgrammeReview/ProgrammeReviewBundleService.cs',
    'TenderReview/TenderReviewBundleService.cs'
)
$before = @{}
foreach ($relative in $files) {
    $path = Join-Path $targetCore $relative
    $before[$relative] = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash
    $text = [IO.File]::ReadAllText($path)
    $candidate = [IO.File]::ReadAllText((Join-Path $sourceCore $relative))

    if ($relative -eq 'CoreLogic.cs') {
        foreach ($name in @('MergeStore','ParseXerStreamAsync','ResolveRequestedTablesAsync','GenerateEnhancedTable')) {
            Copy-MethodScope ([ref]$text) $candidate $name
        }
        Replace-Required ([ref]$text) 'using (var archive = new ZipArchive(zipOutputStream, ZipArchiveMode.Create, leaveOpen: true))' 'using (var archive = ParserOperationTelemetry.CreateZipArchive(zipOutputStream))'
        Replace-Required ([ref]$text) 'using (var entryStream = entry.Open())' 'using (var entryStream = ParserOperationTelemetry.OpenZipEntry(entry))'
        $text = [regex]::Replace($text, '(?m)^(\s*)(_exporter\.WriteTableToStream\(tables\[i\], entryStream(?:, cancellationToken)?\);)',
            '$1using var csvPhase = ParserOperationTelemetry.Measure(ParserOperationPhase.CsvSerialization, tables[i].Name);' + "`n" + '$1$2')
        Replace-Required ([ref]$text) 'foreach (string name in names) StandardExportPublication.ValidateTableName(name);' ('using var preflightPhase = ParserOperationTelemetry.Measure(ParserOperationPhase.StandaloneValidation, "standard source/export preflight");' + "`n            " + 'foreach (string name in names) StandardExportPublication.ValidateTableName(name);')
        Replace-Required ([ref]$text) 'progress?.Report((0, "Generating and validating every requested table..."));' ('preflightPhase.Dispose();' + "`n            " + 'progress?.Report((0, "Generating and validating every requested table..."));')
        Replace-Required ([ref]$text) 'if (table?.Headers is not { Length: > 0 })' ('using var outputValidation = ParserOperationTelemetry.Measure(ParserOperationPhase.StandaloneValidation, requestedName + ":output schema");' + "`n                " + 'if (table?.Headers is not { Length: > 0 })')
        Replace-Required ([ref]$text) '? transformer.CreateDataQualityTable(result.Select(table => table.Name)) : null;' ('? ParserOperationTelemetry.Time(ParserOperationPhase.StandaloneValidation, "standard source quality/diagnostics",' + "`n                    " + '() => transformer.CreateDataQualityTable(result.Select(table => table.Name))) : null;')
    } elseif ($relative.EndsWith('Transformer.cs')) {
        $programme = $relative.StartsWith('Programme')
        $methods = if ($programme) {
            @('ProgrammeReviewTransformer','Build','BuildTaskRows','BuildProjectedRows','ValidateRequiredSnapshotCoverage','ValidateTransformOutcomes','ValidateSingleProjectPerSnapshot','ValidateKeysAndRelationships')
        } else { @('Build','BuildTaskRows','BuildProjectedRows','ResolveProjectMetadata','ValidateTransformOutcomes','ValidateRequiredSourceCoverage','ValidateKeysAndRelationships') }
        foreach ($name in $methods) { Copy-MethodScope ([ref]$text) $candidate $name }
        $builderPattern = '(?m)(?<prefix>^\s*\[EnhancedTableNames\.(?<name>\w+)\]\s*=\s*)(?<call>transformer\.[\s\S]*?)(?=,\s*\r?\n\s*\[|\r?\n\s*};)'
        if ([regex]::Matches($text, $builderPattern).Count -ne 11) { throw 'Frozen enhanced builder count differs.' }
        $builderReplacement = [Text.RegularExpressions.MatchEvaluator] {
            param($builder)
            $builder.Groups['prefix'].Value + 'ParserOperationTelemetry.Time(ParserOperationPhase.Transformation, EnhancedTableNames.' + $builder.Groups['name'].Value + ', () => ' + $builder.Groups['call'].Value + ')'
        }
        $text = [regex]::Replace($text, $builderPattern, $builderReplacement)
        $profile = if ($programme) { 'programme' } else { 'tender' }
        Replace-Required ([ref]$text) 'XerTable warnings = transformer.CreateDataQualityTable();' ('XerTable warnings = ParserOperationTelemetry.Time(ParserOperationPhase.StandaloneValidation, "' + $profile + ' source quality/diagnostics",' + "`n            " + '() => transformer.CreateDataQualityTable());')
    } else {
        $programme = $relative.StartsWith('Programme')
        $profile = if ($programme) { 'ProgrammeReview' } else { 'TenderReview' }
        foreach ($name in @('CreateManifestRows','ComputeSourceSha256')) { Copy-MethodScope ([ref]$text) $candidate $name }
        if (-not $programme) {
            foreach ($name in @('ValidateManifestConsistency','ResolveWithConfiguredHashes','BindGovernedSourceTokens','MergeTenderStoreWithUnionSchema')) { Copy-MethodScope ([ref]$text) $candidate $name }
        }
        Replace-Required ([ref]$text) 'using (var archive = new ZipArchive(zipOutputStream, ZipArchiveMode.Create, leaveOpen: true))' 'using (var archive = ParserOperationTelemetry.CreateZipArchive(zipOutputStream))'
        Replace-Required ([ref]$text) 'using (var entryStream = entry.Open())' 'using (var entryStream = ParserOperationTelemetry.OpenZipEntry(entry))'
        Replace-Required ([ref]$text) 'using (var hashingStream = new HashingWriteStream(entryStream))' ('using (var hashingStream = new HashingWriteStream(entryStream))' + "`n                " + 'using (var csvStream = ParserOperationTelemetry.TrackCsvHashing(hashingStream, entryStream, table.Contract.FileName))')
        $csvBlock = $profile + 'Csv.WriteTableToStream(hashingStream, table, cancellationToken);'
        Replace-Required ([ref]$text) $csvBlock ('using (ParserOperationTelemetry.Measure(ParserOperationPhase.CsvSerialization, table.Contract.FileName))' + "`n                    {`n                        " + $profile + 'Csv.WriteTableToStream(csvStream, table, cancellationToken);')
        Replace-Required ([ref]$text) 'hashingStream.Flush();' "csvStream.Flush();`n                    }"
        Replace-Required ([ref]$text) 'hashes.Add(table.Contract.FileName, hashingStream.GetHash());' ('using (ParserOperationTelemetry.Measure(ParserOperationPhase.CsvHashing, table.Contract.FileName + ":digest"))' + "`n                        " + 'hashes.Add(table.Contract.FileName, hashingStream.GetHash());')
        Replace-Required ([ref]$text) 'using (var manifestStream = manifestEntry.Open())' 'using (var manifestStream = ParserOperationTelemetry.OpenZipEntry(manifestEntry))'
        Replace-Required ([ref]$text) ($profile + 'Csv.WriteManifestToStream(manifestStream, manifestRows, cancellationToken);') ('using var csvPhase = ParserOperationTelemetry.Measure(ParserOperationPhase.CsvSerialization, ' + $profile + 'Contract.ManifestFileName);' + "`n                " + $profile + 'Csv.WriteManifestToStream(manifestStream, manifestRows, cancellationToken);')
        if ($programme) {
            $text = $text.Replace('ProgrammeReviewNaming.Resolve(', 'ResolveRequest(')
            $text = $text.Replace('string computedHash = ProgrammeReviewCsv.ComputeSha256(', 'string computedHash = ComputeSourceSha256(')
            $text = $text.Replace('string afterParseHash = ProgrammeReviewCsv.ComputeSha256(', 'string afterParseHash = ComputeSourceSha256(')
            $requestHelper = Get-MethodSource $candidate 'ResolveRequest'
            $lastBrace = $text.LastIndexOf('}')
            $text = $text.Insert($lastBrace, "`n$requestHelper`n")
        } else {
            $text = $text.Replace('string computedHash = TenderReviewCsv.ComputeSha256(', 'string computedHash = ComputeSourceSha256(')
            $text = $text.Replace('string afterParseHash = TenderReviewCsv.ComputeSha256(', 'string afterParseHash = ComputeSourceSha256(')
        }
        $bytesMethod = [regex]::Match($candidate, '(?s)    private static string ComputeSourceSha256\(ReadOnlySpan<byte> content\).*?\n    }').Value
        if (-not $bytesMethod) { throw 'Byte SHA timer helper missing.' }
        $lastBrace = $text.LastIndexOf('}')
        $text = $text.Insert($lastBrace, "`n$bytesMethod`n")
    }
    [IO.File]::WriteAllText($path, $text, $utf8)
}
Copy-Item -LiteralPath (Join-Path $sourceCore 'ParserOperationTelemetry.cs') -Destination (Join-Path $targetCore 'ParserOperationTelemetry.cs')
$evidence = foreach ($relative in $files) {
    [pscustomobject]@{ File = $relative; BeforeSha256 = $before[$relative]; AfterSha256 = (Get-FileHash -LiteralPath (Join-Path $targetCore $relative) -Algorithm SHA256).Hash }
}
$evidence | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $targetCore 'phase-overlay-evidence.json') -Encoding utf8
Write-Output "Applied timing-only overlay to $targetCore. Original implementations remain in place."
