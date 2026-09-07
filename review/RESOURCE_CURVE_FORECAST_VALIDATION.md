# Active resource-curve forecast validation

Validated on 2026-09-07. The shared Core automatically forecasts remaining named
curves for active activities from assignment elapsed working time, and uses a
labeled uniform fallback when phase cannot be established or the curve tail is
empty. Actual allocation, authoritative remaining profiles, public APIs, CSV
headers and review schema versions are unchanged. This is the agreed exporter
forecast policy, not native P6 timephased parity.

## Calculation and diagnostics

The implementation crops an immutable copy of the named curve at `p=A/(A+R)`.
`A` ends at the project Data Date, `R` follows assignment remaining dates, and the
future restart gap does not consume curve progress. Valid closed suspensions
reduce effective forecast working time; raw calendar diagnostic columns retain
their existing meaning. Linear curves and one-working-month totals need no phase.

New Standard labels are `Resource Curve Forecast` and `Working Hours Fallback`.
Their companion codes are `REMAINING_CURVE_ESTIMATED` and
`REMAINING_CURVE_UNIFORM_FALLBACK`. These successful method warnings have blank
allocation_portion and unallocated quantity fields, so already-distributed units
are never counted twice. Invalid definitions/profiles, unavailable remaining
periods/calendars and other source failures retain unsuccessful portion warnings.

## Automated checks

The complete Core suite passed **1,168 tests, zero failed or skipped**, including
44 added forecast cases and updated assertions for the deliberately changed
active-curve behaviour:

```powershell
dotnet test XerToCsvConverter.Core.Tests/XerToCsvConverter.Core.Tests.csproj --no-restore --verbosity minimal
```

Independent examples include front-loaded 75/25, back-loaded 33.3333/66.6667 and
bell-shaped 66.6667/33.3333 allocations at A=8/R=24. Tests also cover partial
bands, holidays, short shifts, resource calendars, suspensions, raw field presence,
restart gaps, zero actual units, unchanged actual rows, malformed actual units
with valid remaining work, tiny quantities, empty tails, shared curves with
different phases and repeated source filenames/native IDs. Export tests exercise
Standard, Programme and Tender memory/disk paths and diagnostic/manifest mapping.

Windows, Programme CLI and Tender CLI builds passed with zero warnings/errors:

```powershell
dotnet build 'XER to CSV.csproj' --no-restore -p:BuildProjectReferences=false --verbosity minimal
dotnet build XerToCsvConverter.ProgrammeReview.Cli/XerToCsvConverter.ProgrammeReview.Cli.csproj --no-restore -p:BuildProjectReferences=false --verbosity minimal
dotnet build XerToCsvConverter.TenderReview.Cli/XerToCsvConverter.TenderReview.Cli.csproj --no-restore -p:BuildProjectReferences=false --verbosity minimal
```

The initial Web build encountered a cached .NET 10 WebAssembly task-host mismatch
(MSB4216/MSB4027). Restoring with the repository's bundled .NET 8 SDK resolved it;
the complete Web/Blazor build then passed with zero warnings/errors. No project
dependency or SDK configuration was changed:

```powershell
& ./local-dotnet-sdk/dotnet.exe restore XerToCsvConverter.Web/XerToCsvConverter.Web.csproj --ignore-failed-sources -p:NuGetAudit=false --verbosity minimal
& ./local-dotnet-sdk/dotnet.exe build XerToCsvConverter.Web/XerToCsvConverter.Web.csproj --no-restore -p:BuildProjectReferences=false --verbosity minimal
```

## Source and integration checks

The original `XER Sample/2608-EBA_PAA_8.0 (draft).xer` was preserved (SHA-256
`BCD04882C15C96ED94A18F670F6F5F9AD26493993BA5ADD44250A48958E487B7`).
All 14 numbered CSVs and the companion are byte-identical to the pre-change
export. Table 15 retains 5,951 rows, with C110950/Demolition at **20.0000 August
remaining units** and zero table-15 warnings. The 218 existing general warnings
elsewhere in the sample are unchanged.

- Baseline: `artifacts/resource-forecast-baseline/c5ead44c12a4/validation-summary.json`.
- Fresh export: `artifacts/resource-forecast-validation/da5ab78140b5/validation-summary.json`.
- Profile integration: `artifacts/resource-forecast-validation/profile-integration/ce2938b6e493/`.

The profile integration uses an isolated synthetic copy of the retained-logic
fixture: one 100-unit assignment forecasts 75/25, and one 200-unit assignment
with invalid phase evidence falls back to 100/100. Standard exports four rows;
Tender aggregates them to 175/125 after per-assignment calculation. Both review
profiles retain 12 files and two successful method diagnostics, with zero
unallocated portions. The harness compares quantities, full method evidence,
and file/memory bytes across profiles.

```powershell
& ./review/Validate-ResourceExportIntegration.ps1 -Paths @('XER Sample/2608-EBA_PAA_8.0 (draft).xer') -BaselineHashes './artifacts/resource-forecast-baseline/c5ead44c12a4/validation-summary.json' -OutputRoot './artifacts/resource-forecast-validation'
& ./review/Validate-RelationshipExportIntegration.ps1 -Paths @('XER Sample/2608-EBA_PAA_8.0 (draft).xer') -ProfileFixture './artifacts/resource-forecast-validation/curve-forecast-fixture.xer' -OutputRoot './artifacts/resource-forecast-validation/profile-integration'
& ./review/Validate-LocalXerExport.ps1 -Paths @('artifacts/resource-forecast-validation/curve-forecast-fixture.xer')
& ./review/Validate-ReportingSkill.ps1
git diff --check
```

The independent local audit reconciled all 300 synthetic remaining units and
verified raw diagnostic evidence without counting either method notice as an
unallocated amount. Reporting-reference validation passed 20 numbered review
contracts, the 35-column diagnostic header and all relative links. Whitespace
validation passed. No deployment, package publication or native P6 UI verification
was performed.
