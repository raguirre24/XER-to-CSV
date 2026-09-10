param([switch]$Apply)

# Publish only the seven reviewed files after checking that their originals have
# not changed since the local backups. No source-control or external data writes.
$ErrorActionPreference = 'Stop'
$stageRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../artifacts/review-loader-migration-20260910'))
$profiles = @(
    @{
        Stage = 'programme'
        Root = 'C:\Users\ricar\Documents\Code\Programme Review\Programme-Review'
        Files = @(
            'Project Review - Programme (datalake).SemanticModel\definition\expressions.tmdl',
            'Project Review - Programme (datalake).SemanticModel\definition\tables\06 XER_PREDECESSOR.tmdl',
            'SHAREPOINT_CSV_SETUP.md')
    },
    @{
        Stage = 'tender'
        Root = 'C:\Users\ricar\Documents\Code\Tender-Review'
        Files = @(
            'Project Review - Tender Programme (datalake).SemanticModel\definition\expressions.tmdl',
            'Project Review - Tender Programme (datalake).SemanticModel\definition\tables\06 XER_PREDECESSOR.tmdl',
            'TENDER_CSV_SETUP.md',
            'XER_TO_CSV_TENDER_PROFILE_SPEC.md')
    }
)
$plan = @()
foreach ($profile in $profiles) {
    $root = (Resolve-Path -LiteralPath $profile.Root).Path
    foreach ($file in $profile.Files) {
        $target = (Resolve-Path -LiteralPath (Join-Path $root $file)).Path
        $source = (Resolve-Path -LiteralPath (Join-Path (Join-Path $stageRoot $profile.Stage) $file)).Path
        $backup = (Resolve-Path -LiteralPath (Join-Path (Join-Path $stageRoot ($profile.Stage + '-original')) $file)).Path
        if (-not $target.StartsWith($root + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase) -or
            -not $source.StartsWith($stageRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase) -or
            -not $backup.StartsWith($stageRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Path escaped the named migration scope: $file"
        }
        $originalHash = (Get-FileHash -LiteralPath $backup -Algorithm SHA256).Hash
        $currentHash = (Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash
        $newHash = (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash
        if ($currentHash -cne $originalHash) { throw "Original changed after backup; preserving it without overwriting: $target" }
        $plan += [pscustomobject]@{ Source = $source; Target = $target; Backup = $backup; OriginalHash = $originalHash; NewHash = $newHash }
    }
}
if ($Apply) {
    foreach ($item in $plan) {
        Copy-Item -LiteralPath $item.Source -Destination $item.Target -Force
        if ((Get-FileHash -LiteralPath $item.Target -Algorithm SHA256).Hash -cne $item.NewHash) { throw "Published bytes differ: $($item.Target)" }
    }
}
[pscustomobject]@{ Mode = $(if ($Apply) { 'Applied' } else { 'Read-only preflight' }); Files = $plan } | ConvertTo-Json -Depth 5
