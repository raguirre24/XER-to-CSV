# Non-blocking table 15 resource warnings

Historical checkpoint: the subsequent [resilient Enhanced export policy](RESILIENT_ENHANCED_EXPORTS.md)
extends warnings to all numbered outputs and supersedes this checkpoint's
table-11/WBS/aggregation fail-fast boundaries and diagnostic 1.1 header. Results
below remain evidence for the earlier table 15 change, not the current whole-export policy.

## Outcome and boundaries

Table 15 no longer treats assignment source-quality or unsupported-allocation
conditions as a failed table. Actual and remaining portions are independent:
every valid portion retains the existing monthly calculation, and each portion
that cannot be justified is preserved as one warning in `XER_DATA_QUALITY.csv`.
Known signed quantities remain unallocated; unrepresentable amounts remain raw
text with a blank numeric total. No source dates, quantities, curves or months are
invented. Completed activities retaining remaining quantities are not zeroed.

The companion schema becomes 1.1, preserving its former 23 data columns and adding
seven fields before final `FileName`: `allocation_portion`, `restart_date`,
`reend_date`, `remain_qty`, `curv_id`, `remain_crv`, and
`unallocated_remaining_quantity`. Numbered schemas, review versions, file counts
and manifest columns do not change. Warning count is an unsuccessful-portion
count, not necessarily a distinct assignment count.

This policy applies to table 15 source data and allocation coverage, not to
unexpected programming failures, cancellation, I/O, a missing requested source,
other numbered-table validation or governed review metadata. Those protections
remain independent. A malformed calendar could therefore produce a table 15
warning while still legitimately preventing a requested table 11 from generating.
Review-specific numeric-range/aggregation constraints are also retained. If
individually representable assignment amounts sum to a review-grain value beyond
the supported decimal range, that review export can still fail. This is a known
boundary, not covered by the portion warning policy or exercised by the supplied
real schedules; no aggregate is clamped or represented as a guessed metric.
This is not a blanket promise that unreadable files or unrelated failed tables
will be published as valid.

## Regression evidence and reproducible checks

Before rebuilding Core, the unmodified July/August input pair was exported through
the previous DLL with SHA-256
`13530175C61AF199B2E7DD2EC8C139E193650504781807B57C06976DC9FF83DE`.
All 14 numbered CSV byte sequences, the old companion and hashes were captured
locally under ignored `artifacts/resource-nonblocking-baseline/`. The prior
export had two actual warnings. This baseline is specific to ordered July then
August; it is not a filename-keyed input collection.

The independent `review/Validate-LocalXerExport.ps1` harness now checks both
portion quantities against raw TASKRSRC values, detects source quantity/status/
period issues independently, preserves raw evidence and reconciles distributed
plus unallocated quantities separately by source occurrence/task/resource/portion.
Unknown totals must have a warning and a blank numeric amount. It does not call
the new Core allocation assessment or distribution helpers for its oracle.
Existing calendar/derived-field/key checks remain in place. This whole-export
harness requires valid dimension keys and independently recognized issue cases;
special malformed identity/calendar/curve cases are covered by dedicated tests,
not silently accepted by the integration oracle.

Run in fresh PowerShell processes after building Core:

```powershell
& ./review/Validate-LocalXerExport.ps1 -Paths @(
  'C:\Users\ricar\Downloads\2607 C5064-C-2607.xer'
)

& ./review/Validate-LocalXerExport.ps1 -Paths @(
  'C:\Users\ricar\Eastern Busway Alliance\EBA MasterData - Power BI Master Data\06 PC - Performance\Data\XER\Baseline 8.0\2607-EBA_PAA_8.0.xer',
  'C:\Users\ricar\Eastern Busway Alliance\EBA MasterData - Power BI Master Data\06 PC - Performance\Data\XER\Baseline 8.0\2608-EBA_PAA_8.0.xer'
)

& ./review/Validate-ResourceExportIntegration.ps1 -Paths @(
  'C:\Users\ricar\Downloads\2607 C5064-C-2607.xer'
) -UnchangedCsvDirectory './artifacts/c5064-validation/export'

& ./review/Validate-ResourceExportIntegration.ps1 -Paths @(
  'C:\Users\ricar\Eastern Busway Alliance\EBA MasterData - Power BI Master Data\06 PC - Performance\Data\XER\Baseline 8.0\2607-EBA_PAA_8.0.xer',
  'C:\Users\ricar\Eastern Busway Alliance\EBA MasterData - Power BI Master Data\06 PC - Performance\Data\XER\Baseline 8.0\2608-EBA_PAA_8.0.xer'
) -BaselineHashes './artifacts/resource-nonblocking-baseline/baseline.json'

& ./review/Validate-ReportingSkill.ps1
git diff --check
```

`Validate-ResourceExportIntegration.ps1` byte-compares all 15 Standard files from
disk parsing, Web-style stream parsing, memory generation and actual disk
publication. Optional baseline checks compare complete numbered CSVs, not just
row counts. Each run writes a fresh ignored output directory and verifies source
hashes afterward; it does not overwrite the original baseline evidence.

## C5064 full export

The original `2607 C5064-C-2607.xer` has SHA-256
`23D4F0EA20B096886C4D9AFE34968F62738393EA23C8524EF3F24E2CCB569561`.
The full independent source validator passed with table 15 included:

| Check | Result |
| --- | ---: |
| Numbered tables / files including companion | 14 / 15 |
| Table 15 monthly rows | 8,981 |
| Independent retained source-cell comparisons | 812,519 |
| Independent derived-field comparisons | 382,462 |
| Finite derived numeric cells | 218,725 |
| Reconciled resource/portion groups | 4,658 |
| Valid recorded-date/off-calendar actual fallback rows | 38 |
| Actual warnings / remaining warnings | 1 / 13 |
| Unknown-quantity warnings | 0 |

The 13 remaining warnings are `REMAINING_ON_COMPLETED`, on seven completed
activities. Their original remaining quantities remain visible and unallocated:
6,068.8000 Design Man-Hours and 184,290.7410 AUD on the separate MAL - EV Cost
resource. These unlike quantities must not be summed together. The additional
`ACTUAL_FINISH_BEFORE_START` warning on activity `LP-PR-6650` preserves 13,058.8750
AUD actual units; its valid remaining portion still distributes independently.

Core build SHA-256
`FB0EC55CB4185E1A7F48D4AC1DA426AF016C2546A2ECDF9AA21E319B6C758985`
passed the new integration script: all 15 files matched between disk, memory and
Web-stream paths and were published successfully. Its table 06, 10 and 11 bytes
match the previous C5064 review artifacts exactly. Outputs and machine-readable
hashes are under ignored `artifacts/resource-nonblocking/b32e0a55eb69/`.

The existing independent raw-calendar oracle was rerun against those newly
published table 10/11 files using a redirected validation output directory. All
10 calendars and 1,665 detailed rows matched; no errors were reported. Genuine
5-hour and 9-hour Saturday rules remain intact. The original oracle and baseline
files were not modified.

The relationship integration harness also passed all 14,138 C5064 assessments,
audit/export numeric equality and the retained-logic zero fixture projected
through both twelve-file Programme/Tender profiles with disk/memory equality.
That controlled profile fixture is distinct from the original C5064 project;
the check does not claim that C5064 bypassed either review profile's governance.
Its output is `artifacts/resource-nonblocking-integration/96f620209b1b/`.

## July/August regression

The ordered original July/August pair passed complete disk, memory and Web-stream
publication comparison with the same Core build. Every one of the 14 numbered
CSV files is byte-identical to the captured pre-change baseline, including tables
06, 10, 11 and 15. The companion alone changes for schema 1.1; it still has the two
pre-existing actual-period warnings. Machine-readable results and the complete
new export are under ignored `artifacts/resource-nonblocking/adba2c60e4c9/`.

The full independent source validator also passed the original ordered pair:
2,236,209 retained source cells, 391,414 finite derived numeric cells, 909,978
independent derived checks and 6,849 reconciled resource/portion groups. Table 15
retains 12,324 monthly rows, with 128 valid actual fallback rows. The companion
contains two actual warnings, zero remaining warnings and no unknown quantities.
Table 11 retains 31,334 detailed rules/exception rows across 65 calendars.

## Automated validation and limits

Reporting-skill validation passed all 20 exact numbered review headers, the
additive 31-column diagnostic schema 1.1 header, 12 relative links and whitespace.
Skill Creator's `quick_validate.py` passed using the existing isolated local
PyYAML validation dependency; no new dependency or global installation was needed.

All nine .NET builds passed separately with zero warnings and errors, using the
local .NET 8 SDK and `--no-restore --disable-build-servers -m:1 -p:UseSharedCompilation=false`:

| Build target | Result |
| --- | --- |
| Core | Passed |
| Windows application | Passed |
| Core.Tests | Passed |
| Web application | Passed |
| Programme Review CLI | Passed |
| Tender Review CLI | Passed |
| TenderReview.Surface.Tests | Passed |
| Relationship Audit CLI | Passed |
| Relationship Audit CLI.Tests | Passed |

The Core test suite passed 1,050/1,050, existing surface tests passed 50/50, audit
CLI tests passed 28 with two explicitly skipped platform symbolic-link scenarios,
and Web download JavaScript tests passed 5/5. The Node test runner needed its
normal child process permission after an initial sandbox `EPERM`; no production
code or test assertion was relaxed for that environment restriction. The skipped
link scenarios are not claimed as live filesystem validation.

The current Windows `--ui-smoke-test` exited zero. Portable and installed skill
copies passed Skill Creator validation; all six installed instruction files were
SHA-256 matched to the portable sources. `git diff --check` passed; Git's LF/CRLF
normalization notices are not whitespace-check failures. These are working-tree
builds and local smoke checks, not a new committed release or published Web build.

No original XERs, sibling repositories, native P6
schedules or reports are modified by these checks. Shared disk/Web-stream byte
equality and automated surface tests establish parser consistency, not native P6,
live browser-saved ZIP integrity or Power BI refresh equivalence.
