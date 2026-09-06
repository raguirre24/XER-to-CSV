# Relationship free-float audit

Run this separate command when investigating table `06.free_float`. It does not produce or modify a normal Standard, Programme Review or Tender Review bundle.

```powershell
.\local-dotnet-sdk\dotnet.exe run --project XerToCsvConverter.RelationshipAudit.Cli -- --input "C:\Schedules\July.xer" --input "C:\Schedules\August.xer" --output "C:\Audits\relationships.csv"
```

Each repeatable `--input` is an ordered input occurrence. Repeated paths, filenames, hashes and native IDs are retained independently. The audit uses Standard public namespaces: colliding filenames receive deterministic occurrence suffixes. Internal source tokens are never exported. To compare with review keys, use the supplied profile/source identity and native relationship ID; do not mistake Standard namespaces for governed review namespaces.

The parent output folder must exist. The chosen output must have a safe `.csv` filename. Existing output is protected unless `--overwrite` is explicitly supplied. Input files cannot be replaced, even with that flag. Filesystem-link traversal is refused. Preparation writes a new temporary file in the same folder; the complete file is flushed and renamed to the output only after all rows succeed. A preparation failure or cancellation leaves the prior output intact. A cancellation after the rename cannot undo an already completed publication.

## Interpretation

There is one row for every source `TASKPRED` occurrence, including calculated results. `source_row_number` is its one-based ordinal within that occurrence. Numeric `free_float` is the same signed predecessor-day allowance used by Core's table 06 calculation, not imported activity float, a reschedule or guaranteed native P6 relationship float. `free_float_hours` exposes its hours numerator; `predecessor_hours_per_day` exposes the conversion factor.

`classification`, `reason_code` and `message` explain why a value is calculated, ignored, historical, unsupported, missing or invalid. Blank is not zero and must not be treated as a driving relationship. Some progressed settings combinations remain explicitly unsupported; successful publication does not mean all relationships have a numeric result.

The fixed schema (`audit_schema_version=1.0`) is declared in [RelationshipAuditCsv.cs](../XerToCsvConverter.Core/RelationshipAuditCsv.cs). It includes public endpoint identities, statuses, mode, SS lag basis, raw/effective lag, required calendars, selected endpoint timestamps and source fields. Six successor scheduling options have separate raw/state columns. `input_evidence` is a JSON object containing all captured raw fields and presence/validity states for both activities, projects, scheduling contexts and the relationship. Import evidence and raw date strings as text; do not coerce malformed source values into dates or zeros.

CSV serialization preserves source text, embedded quotes/newlines and invariant numeric precision. The audit is not automatically included in numbered exports or manifests and does not replace the assignment-specific `XER_DATA_QUALITY.csv`.

## Exit codes and tests

- `0`: audit published, including nonnumeric assessments, or `--help` shown.
- `1`: fatal input, calculation or publication failure.
- `2`: invalid command-line usage.
- `130`: cancelled before publication.

```powershell
.\local-dotnet-sdk\dotnet.exe build XerToCsvConverter.RelationshipAudit.Cli\XerToCsvConverter.RelationshipAudit.Cli.csproj
.\local-dotnet-sdk\dotnet.exe test XerToCsvConverter.RelationshipAudit.Cli\Tests\XerToCsvConverter.RelationshipAudit.Cli.Tests.csproj
```
