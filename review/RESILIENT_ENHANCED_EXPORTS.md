# Resilient Enhanced numbered exports

## Implemented policy

All selected Enhanced numbered tables preserve available source data. A bad source
row, unavailable lookup or unsupported calculation is isolated to its affected
field, calendar definition or allocation portion; it must not abort valid rows in
the same table or unrelated numbered tables. Unknown calculations stay blank,
never an invented zero, eight-hour calendar, date, month or repaired source value.

Standard preserves raw rows and numbered headers. Tables with no available output
have their declared header; diagnostics distinguish unavailable results from
genuinely empty source data. The UI offers tables based on primary source presence
alone: TASK enables 01/04, TASKPRED enables 06, and TASKRSRC enables 13/15 even when
their lookup tables are absent. Ordered repeated inputs remain distinct through
internal occurrence identity; no filename/path/hash deduplication is introduced.

Table 11 retains seven weekday rows per source CALENDAR occurrence. Invalid daily
hours/working flags are blank, including the following day's uncertain incoming
overnight contribution where relevant. Invalid dated exceptions retain their date
as `Exception - Invalid`; unresolved exception dates/inheritance produce an undated
invalid marker. No invalid rule is represented as known nonworking time. Valid
independent rules/overrides survive. Blank/duplicate calendar identities retain
source metadata but cannot acquire a guessed unique calendar key. Tables 06/15
retain their strict complete-calendar calculation checks and do not schedule from
partial table 11 reporting results.

`XER_DATA_QUALITY.csv` is now included whenever any Enhanced numbered table is
selected, header-only when clean. Raw-only exports remain unchanged. Companion
schema **1.2** preserves all 30 former data columns, then adds `source_table`,
`column_name`, `raw_value` and `raw_row_json`, followed by writer `FileName`:
**35 columns**. Raw evidence includes ordered source header/value/presence entries,
not internal source tokens. General warnings have no allocation portion or units;
the row ordinal belongs to the named source table, not always TASKRSRC.

Table 15 retains its independent Actual/Remaining handling and four-decimal
quantity conservation. Filter the companion to `allocation_portion` before
reconciling distributed plus known unallocated quantity; general warnings are not
monthly contributions. Unknown amounts remain blank numerically and visible as
source text. Tender aggregate overflow/conflicting labels preserve separately
valid contributions with warnings instead of dropping or clamping the group.

Programme and Tender retain ten numbered tables, companion and manifest, with
unchanged numbered versions/headers and manifest columns. Missing optional or
nonidentity source tables can be header-only with warnings. Duplicate keys and
orphan references are retained and diagnosed; a completed bundle is not proof
that report one-to-many keys are valid. Programme ambiguous cross-snapshot history
is not matched arbitrarily. Governed PROJECT/request/source/snapshot/stage identity
metadata remains required and is not guessed.

Unreadable input, unsafe filenames/paths, cancellation, genuine file publication
failure and unrecoverable schema/runtime failures remain errors. If a transformation
fails but a safe output schema and raw source evidence remain available, last-resort
recovery publishes that available evidence with `TABLE_GENERATION_FAILED` and
unavailable calculations blank. Expanded calendar/distribution outputs do not
invent dated/monthly rows merely to fill a failed transformation. This warning does
not claim the calculation succeeded or label a programming failure as bad schedule
data. Resilience does not promise export after a disk or decoding failure.
No LongestPathVisual policy, sibling report repository, source XER or live
deployment is modified by this work.

## Final validation result

The frozen production Core SHA-256 is
`FE5A016E2DD7BAECCB6DD5976D16732E8042478A293832106BE489587EE2BEF9`.
Both source sets were re-exported using this exact binary after the final
diagnostic-scope changes. All 14 numbered files still match their prior successful
baselines byte-for-byte, and all 15 output files match across disk, Web-stream,
memory and disk-publication paths. Original XER hashes remain unchanged.

| Frozen-build input | Files | General warnings | Allocation warnings | Total warnings |
| --- | ---: | ---: | ---: | ---: |
| C5064 July | 15 | 158 | 14 | 172 |
| EBA July then August | 15 | 449 | 2 | 451 |

The one extra general warning per source set compared with the earlier integration
checkpoint below is deliberate: selected table 04 now includes the retained
baseline row's existing activity-field warning as an affected output of its own.
There is no changed numbered value. July relationship **8639741**, predecessor
**4806388** to successor **4447632**, still has exact `free_float=0`.

Frozen-build artifacts:

- `artifacts/resilient-enhanced-exports/c5064-frozen/9ecf9e6e602c/validation-summary.json`
- `artifacts/resilient-enhanced-exports/eba-final/ea28994f77a3/validation-summary.json`

Each required build was run separately with zero warnings and errors:

| Build target | Build | Tests |
| --- | --- | --- |
| Core | Passed | Not a test project |
| Windows | Passed | UI smoke exit 0 |
| Core.Tests | Passed | 1,097 passed; zero skipped |
| Web | Passed | Download JavaScript: 5 passed |
| Programme Review CLI | Passed | Shared Core/surface checks |
| Tender Review CLI | Passed | Shared Core/surface checks |
| TenderReview.Surface.Tests | Passed | 51 passed |
| Relationship Audit CLI | Passed | Shared Core/audit checks |
| Relationship Audit CLI.Tests | Passed | 28 passed; two Windows symbolic-link skips |

Reporting contract validation passed 20 exact numbered review headers, the
35-column diagnostic 1.2 schema, 12 relative skill links and whitespace checks.
Skill-creator validation passed for portable and installed skills; changed skill
files are hash-checked when synchronized. `git diff --check` passed with only Git's
line-ending advisories. JavaScript tests required the scoped execution retry after
the sandbox blocked a test-runner child-process spawn; that restriction is not an
application test failure. These are automated checks, not a live Web deployment
or manual Power BI acceptance.

### Independent raw calendar reconciliation

`review/Validate-RawCalendarExports.cjs` reuses the earlier independent raw XER
calendar tree reader and integer-minute occupancy oracle, without loading Core or
calling production calendar calculations. It accepts explicit source, CSV folder
and new JSON result paths, filters combined CSVs to the original filename, requires
both calendar CSVs and refuses to overwrite existing result evidence. The
scheduling/occupancy arithmetic is unchanged from the previous C5064 oracle.

Run it once per original input against a newly exported CSV folder:

```powershell
& 'C:/Program Files/nodejs/node.exe' ./review/Validate-RawCalendarExports.cjs 'C:\Users\ricar\Downloads\2607 C5064-C-2607.xer' './artifacts/resilient-enhanced-exports/c5064-frozen/9ecf9e6e602c' './artifacts/resilient-enhanced-exports/c5064-frozen/new-calendar-check.json'
& 'C:/Program Files/nodejs/node.exe' ./review/Validate-RawCalendarExports.cjs 'C:\Users\ricar\Eastern Busway Alliance\EBA MasterData - Power BI Master Data\06 PC - Performance\Data\XER\Baseline 8.0\2607-EBA_PAA_8.0.xer' './artifacts/resilient-enhanced-exports/eba-final/ea28994f77a3' './artifacts/resilient-enhanced-exports/eba-final/new-july-calendar-check.json'
& 'C:/Program Files/nodejs/node.exe' ./review/Validate-RawCalendarExports.cjs 'C:\Users\ricar\Eastern Busway Alliance\EBA MasterData - Power BI Master Data\06 PC - Performance\Data\XER\Baseline 8.0\2608-EBA_PAA_8.0.xer' './artifacts/resilient-enhanced-exports/eba-final/ea28994f77a3' './artifacts/resilient-enhanced-exports/eba-final/new-august-calendar-check.json'
```

All three runs against frozen-build CSVs passed with **zero mismatches**:

| Original source | Raw shifts | Table 10 calendar rows compared | Table 11 rule/exception rows recomputed |
| --- | ---: | ---: | ---: |
| C5064 July | 114 | 10 | 1,665 |
| EBA July | 485 | 32 | 15,468 |
| EBA August | 502 | 33 | 15,866 |

The oracle compares all raw calendar fields and every expected table 11 date,
weekday, daily hours, working flags and exception label, including inherited
overrides and overnight contributions. Results are saved in
`c5064-frozen/calendar-validation.json`, `eba-final/july-calendar-validation.json`
and `eba-final/august-calendar-validation.json` under the artifact root above.
This raw-calendar arithmetic check is additional to, and distinct from, the
source validator's table 11 schema/weekday/flag/identity checks.

## Regression baselines

The prior successful resource-warning build is preserved in ignored artifacts;
its Core assembly hash was
`FB0EC55CB4185E1A7F48D4AC1DA426AF016C2546A2ECDF9AA21E319B6C758985`.

- July/August ordered pair: `artifacts/resource-nonblocking/adba2c60e4c9/validation-summary.json`.
- C5064: `artifacts/resource-nonblocking/b32e0a55eb69/validation-summary.json`.

Each baseline contains original input hashes and complete hashes for all 14
numbered CSVs. The companion is expected to change because it gains general
warnings and the additive header. Existing valid numbered values should not
change merely because diagnostics expand.

## Reproducible validation

Build in separate commands using the local .NET SDK; avoid concurrent builds of
the shared Core outputs. All nine project targets are checked independently:

```powershell
& ./local-dotnet-sdk/dotnet.exe build ./XerToCsvConverter.Core/XerToCsvConverter.Core.csproj --no-restore --disable-build-servers -m:1 -p:UseSharedCompilation=false
& ./local-dotnet-sdk/dotnet.exe build './XER to CSV.csproj' --no-restore --disable-build-servers -m:1 -p:UseSharedCompilation=false
& ./local-dotnet-sdk/dotnet.exe build ./XerToCsvConverter.Core.Tests/XerToCsvConverter.Core.Tests.csproj --no-restore --disable-build-servers -m:1 -p:UseSharedCompilation=false
& ./local-dotnet-sdk/dotnet.exe build ./XerToCsvConverter.Web/XerToCsvConverter.Web.csproj --no-restore --disable-build-servers -m:1 -p:UseSharedCompilation=false
& ./local-dotnet-sdk/dotnet.exe build ./XerToCsvConverter.ProgrammeReview.Cli/XerToCsvConverter.ProgrammeReview.Cli.csproj --no-restore --disable-build-servers -m:1 -p:UseSharedCompilation=false
& ./local-dotnet-sdk/dotnet.exe build ./XerToCsvConverter.TenderReview.Cli/XerToCsvConverter.TenderReview.Cli.csproj --no-restore --disable-build-servers -m:1 -p:UseSharedCompilation=false
& ./local-dotnet-sdk/dotnet.exe build ./XerToCsvConverter.TenderReview.Cli/Tests/XerToCsvConverter.TenderReview.Surface.Tests.csproj --no-restore --disable-build-servers -m:1 -p:UseSharedCompilation=false
& ./local-dotnet-sdk/dotnet.exe build ./XerToCsvConverter.RelationshipAudit.Cli/XerToCsvConverter.RelationshipAudit.Cli.csproj --no-restore --disable-build-servers -m:1 -p:UseSharedCompilation=false
& ./local-dotnet-sdk/dotnet.exe build ./XerToCsvConverter.RelationshipAudit.Cli/Tests/XerToCsvConverter.RelationshipAudit.Cli.Tests.csproj --no-restore --disable-build-servers -m:1 -p:UseSharedCompilation=false

& ./local-dotnet-sdk/dotnet.exe test ./XerToCsvConverter.Core.Tests/XerToCsvConverter.Core.Tests.csproj --no-restore --no-build --disable-build-servers -m:1
& ./local-dotnet-sdk/dotnet.exe test ./XerToCsvConverter.TenderReview.Cli/Tests/XerToCsvConverter.TenderReview.Surface.Tests.csproj --no-restore --no-build --disable-build-servers -m:1
& ./local-dotnet-sdk/dotnet.exe test ./XerToCsvConverter.RelationshipAudit.Cli/Tests/XerToCsvConverter.RelationshipAudit.Cli.Tests.csproj --no-restore --no-build --disable-build-servers -m:1
& 'C:/Program Files/nodejs/node.exe' --test ./XerToCsvConverter.Web/tests/downloads.test.cjs
& ./local-dotnet-sdk/dotnet.exe './bin/Debug/net8.0-windows/XER to CSV.dll' --ui-smoke-test
& ./review/Validate-ReportingSkill.ps1
git diff --check
```

Run each real-source harness in a fresh PowerShell process after the Core build:

```powershell
& ./review/Validate-ResourceExportIntegration.ps1 -Paths @(
  'C:\Users\ricar\Downloads\2607 C5064-C-2607.xer'
) -BaselineHashes './artifacts/resource-nonblocking/b32e0a55eb69/validation-summary.json' -OutputRoot './artifacts/resilient-enhanced'

& ./review/Validate-ResourceExportIntegration.ps1 -Paths @(
  'C:\Users\ricar\Eastern Busway Alliance\EBA MasterData - Power BI Master Data\06 PC - Performance\Data\XER\Baseline 8.0\2607-EBA_PAA_8.0.xer',
  'C:\Users\ricar\Eastern Busway Alliance\EBA MasterData - Power BI Master Data\06 PC - Performance\Data\XER\Baseline 8.0\2608-EBA_PAA_8.0.xer'
) -BaselineHashes './artifacts/resource-nonblocking/adba2c60e4c9/validation-summary.json' -OutputRoot './artifacts/resilient-enhanced'

& ./review/Validate-LocalXerExport.ps1 -Paths @(
  'C:\Users\ricar\Downloads\2607 C5064-C-2607.xer'
)
& ./review/Validate-LocalXerExport.ps1 -Paths @(
  'C:\Users\ricar\Eastern Busway Alliance\EBA MasterData - Power BI Master Data\06 PC - Performance\Data\XER\Baseline 8.0\2607-EBA_PAA_8.0.xer',
  'C:\Users\ricar\Eastern Busway Alliance\EBA MasterData - Power BI Master Data\06 PC - Performance\Data\XER\Baseline 8.0\2608-EBA_PAA_8.0.xer'
)

& ./review/Validate-RelationshipExportIntegration.ps1 -Paths @(
  'C:\Users\ricar\Eastern Busway Alliance\EBA MasterData - Power BI Master Data\06 PC - Performance\Data\XER\Baseline 8.0\2607-EBA_PAA_8.0.xer',
  'C:\Users\ricar\Eastern Busway Alliance\EBA MasterData - Power BI Master Data\06 PC - Performance\Data\XER\Baseline 8.0\2608-EBA_PAA_8.0.xer'
) -ProfileFixture './review/fixtures/J5001_C_BL01_Retained_2026-01-31.xer' -ExpectedFixtureReason 'CalculatedRetainedRemainingRelationship' -ExpectedFixtureFloat '0' -OutputRoot './artifacts/resilient-enhanced-exports/relationship-integration'
```

The first harness compares disk parsing, Web-style stream parsing, memory
generation and actual disk publication byte-for-byte, verifies every numbered
baseline hash and rechecks unchanged original XER hashes. The source validator
independently checks raw cells, applicable calculations and resource conservation;
general warning evidence is matched to the original ordered source row. Its
resource oracle remains deliberately limited to independently implemented cases,
not a call back into the production allocator. Controlled regression tests cover
malformed rows/calendars, duplicate identities, missing dependencies and profile
fallbacks outside the real schedules' source conditions.

These checks demonstrate parser and publication consistency at the tested inputs,
not native P6 scheduling parity, a saved browser ZIP, a Power BI refresh or a live
Web deployment. Runtime counts and final results are recorded after validation;
the earlier [table 15 checkpoint](NONBLOCKING_RESOURCE_WARNINGS.md) is historical.

## Real-source results

The first completed resilience integration run used Core SHA-256
`B261B313054D0AD1FCEEA2DC0CC80510206CC14A9129C8F2F0B26044A51F3556`.
Both original-source runs published all **14 numbered tables plus the companion**.
Disk parsing, Web-style stream parsing, memory generation and disk publication
produced identical CSV bytes. Original XER hashes were unchanged after validation.

| Ordered input | Numbered CSV regression | General warnings | Allocation warnings |
| --- | --- | ---: | ---: |
| C5064 July | 06/10/11 match the original calendar-validation exports; all 14 match the prior successful resource-warning build | 157 | 14: one Actual, thirteen Remaining |
| EBA July then August | All 14 match the original pre-resource-change baseline exactly | 448 | 2 Actual |

Expanded general diagnostics explain already-existing unsupported relationship
calculations and source issues; they did not change these schedules' numbered
values. C5064 general warnings comprise one unavailable activity percentage, one
unresolved WBS parent and 155 unsupported relationship assessments. EBA general
warnings comprise two malformed activity dates, two unresolved WBS parents and
444 unsupported relationship assessments. Warnings are evidence/coverage limits,
not proof that every flagged schedule record is invalid in native P6.

Fresh ignored artifact summaries:

- `artifacts/resilient-enhanced-exports/c5064/8f900fb3a178/validation-summary.json`
- `artifacts/resilient-enhanced-exports/eba/05109585e7a0/validation-summary.json`

The C5064 all-14 comparison additionally checked the prior
`artifacts/resource-nonblocking/b32e0a55eb69/validation-summary.json` hashes.

The independent C5064 source validator also passed: **812,519** retained source
cells, **218,725** finite derived cells, **382,462** independent derived-value
checks, **4,658** reconciled resource groups, and **1,792** diagnostic source
evidence cells. Table 15 retains **8,981** monthly rows, including 38 actual
fallback rows. All known distributed plus unallocated amounts reconcile; there
are no unknown numeric-quantity warnings in this input. Full output is in
`artifacts/resilient-enhanced-exports/c5064/source-reconciliation.txt`.

The independent EBA July/August source validator passed all 14 tables as well:
**2,236,209** retained source cells, **391,414** finite derived cells,
**909,978** independent derived-value checks, **6,849** reconciled resource
groups and **5,070** diagnostic source evidence cells. Table 15 retains
**12,324** monthly rows, including 128 actual fallback rows. Its two Actual
warnings and zero Remaining warnings reconcile with the known source amounts;
there are no unknown numeric-quantity warnings. Full output is in
`artifacts/resilient-enhanced-exports/eba/source-reconciliation.txt`.

A subsequent repeated-source run used Core SHA-256
`18D77C5F16DC28342C5B8A9739B9D104A95B28DAEE653168C486539830B1DA6F` and supplied
the exact same C5064 filename, path and hash twice as two ordered inputs. It again
published all 15 files with disk/Web-stream/publication parity and unchanged
original source hashes. Both occurrences remained independent: 342 warnings
(314 general, 28 allocation), exactly twice the single-input counts.
Every field in tables **06, 10, 11 and 15** matches the single-input export after
normalizing only the qualified source namespace; rows doubled to 28,276, 20,
3,330 and 17,962 respectively. There was no cross-occurrence calculation leak.
Evidence is saved under
`artifacts/resilient-enhanced-exports/c5064-repeated/1a87470165f0/` and
`artifacts/resilient-enhanced-exports/c5064-repeated/value-reconciliation.json`.

## Relationship/profile agreement

The fresh relationship integration run used Core SHA-256
`B26CB8EEAE351529E1FB7FBAD67DB2D096E603C917875E3A40DABE5B432808EB`.
All **33,100** EBA July/August source relationships were assessed and matched the
exported table 06 values and row order exactly: 4,783 calculated, 27,873 historical
and 444 unsupported. Disk and Web-stream Standard CSVs and full relationship
audit text matched. Unsupported/historical values remained blank rather than
becoming zero.

The explicit retained-logic fixture produced the required **zero** with reason
`CalculatedRetainedRemainingRelationship`. Programme and Tender both matched the
shared assessment and resource quantities, each publishing the unchanged
12-file envelope with exact disk/memory bytes, one relationship and one resource
group. Both fixture bundles had zero warnings. The script reconciles allocation
warning evidence separately from profile-specific general diagnostics; it does
not relax numeric table 06 agreement. Evidence is in
`artifacts/resilient-enhanced-exports/relationship-integration-summary.json` and
its fresh `relationship-integration/bf04465815a6/` fixture bundles.

The C5064 relationship integration also passed, using Core
`43C3FF8158B51CE45FC26842F587305BB616B6FA1C2C71884455F24802F09F9D` before the
final frozen-build parity pass: all **14,138** source relationships agreed with
table 06 exactly (10,687 calculated, 3,296 historical, 155 unsupported), with
identical file/stream audit text and successful zero-allowance retained fixtures
for both review profiles. See
`artifacts/resilient-enhanced-exports/relationship-c5064-summary.json`.
