# Detailed calendars in Programme and Tender Review

Implementation and local validation: 2026-09-10.

## Delivered contract

Both review exporters now include `11_XER_CALENDAR_DETAILED.csv`. The companion
Programme and Tender semantic models include `11 XER_CALENDAR_DETAILED`, sourced
from that CSV or the scoped Athena `11_xer_calendar_detailed` table.

The exact ordered business header is:

```text
clndr_name,clndr_type,date,day_of_week,working_day,work_hours,exception_type,clndr_id_key,MonthUpdate,day_of_week_num,working_day_int
```

Programme is schema **5.0**; Tender is schema **4.0**. Each bundle contains eleven
numbered CSVs plus `XER_CSV_MANIFEST.csv`: twelve files and eleven manifest rows
per retained source. Manifest column schemas remain unchanged. Review diagnostics
remain on result objects; no diagnostic CSV is added to the bundle.

Windows, Web and the independent Programme/Tender CLIs use these same Core
contracts. The report loaders accept only their new current version. Regenerate
older bundles with the updated parser; editing a version cell does not migrate a
bundle. Existing manual identities, Tender State and Programme snapshot selection
remain unchanged. No source XER, relationship/calendar/resource calculation,
LongestPathVisual source, PBIR layout, existing role, or existing relationship was
changed for this feature.

## Calendar semantics and report integration

- The existing Enhanced decoder supplies the values. Seven undated weekday rules
  plus dated replacement exceptions are not a dense calendar date series.
- Programme `MonthUpdate` is its governed snapshot month; Tender `MonthUpdate` is
  the source P6 Data Date, not its user-selected stage date or filename date.
- Nullable dates, Y/N flags, hours and integer helpers preserve invalid/unknown
  evidence. Blank hours are not zero. Repeated calendar keys are expected; the
  detail table has no unique row/calendar/date key.
- Review keys are qualified using the existing retained source/snapshot identity.
  Ordered internal source tokens remain independent of filenames, paths or hashes.
  Tender repeated source occurrences remain distinct stages.
- Two hidden report-only columns, `IsCsvSource` and `ProjectKey`, are not extra CSV
  columns. CSV project identity comes from bundle metadata, not calendar-key or
  filename guesses. Athena retains each report's existing source-scope policy.
- Each model adds only single-direction `Project_Dimension -> 11` and
  `10 XER_CALENDARS -> 11` relationships. The direct project link covers unused and
  resource calendars without relying on activity usage. Table 11 cannot filter
  activities backwards through a new bidirectional route.
- Report loaders check nonblank calendar references against table 10 and reconcile
  governed dates, attributable source counts and residual blank-key counts by
  `MonthUpdate`. Blank-key rows cannot be assigned individually to sources sharing
  that date using the requested eleven columns; no stage identity is fabricated.
- Existing strict table-10 dimension validation can still reject a bundle with
  blank/duplicate calendar identities. Preserving invalid table-11 evidence does
  not remove that pre-existing model identity requirement.
- CSV-only ownership takes the empty Athena branch. The Athena query uses
  `t."monthupdate" AS "MonthUpdate"`; this follows existing source naming conventions
  but still needs verification against the live source schema.

## Automated validation

Logs and pre-change backups are in the ignored local directory
`artifacts/calendar11-20260910`. Final binaries use Core SHA-256
`9E00264BB566140240E6CB8DE5D8B6744B840ED727D6E6D9EAAEEAB6257B9B29`.

All builds used the repository SDK (`local-dotnet-sdk/dotnet.exe`, 8.0.421),
`build <target> --no-restore -m:1 -p:BuildProjectReferences=false`, separately:

| Build target | Result |
| --- | --- |
| `XerToCsvConverter.Core/XerToCsvConverter.Core.csproj` | PASS, zero warnings/errors |
| `XerToCsvConverter.Core.Tests/XerToCsvConverter.Core.Tests.csproj` | PASS, zero warnings/errors |
| `XER to CSV.csproj` (Windows) | PASS, zero warnings/errors |
| `XerToCsvConverter.Web/XerToCsvConverter.Web.csproj` | PASS, zero warnings/errors |
| `XerToCsvConverter.ProgrammeReview.Cli/XerToCsvConverter.ProgrammeReview.Cli.csproj` | PASS, zero warnings/errors |
| `XerToCsvConverter.TenderReview.Cli/XerToCsvConverter.TenderReview.Cli.csproj` | PASS, zero warnings/errors |
| `XerToCsvConverter.TenderReview.Cli/Tests/XerToCsvConverter.TenderReview.Surface.Tests.csproj` | PASS, zero warnings/errors |
| `XerToCsvConverter.RelationshipAudit.Cli/XerToCsvConverter.RelationshipAudit.Cli.csproj` | PASS, zero warnings/errors |
| `XerToCsvConverter.RelationshipAudit.Cli/Tests/XerToCsvConverter.RelationshipAudit.Cli.Tests.csproj` | PASS, zero warnings/errors |

Separate `dotnet test <test-target> --no-build --no-restore` runs passed:

- Core: 1,363/1,363, including twenty detailed-calendar tests.
- Tender Surface: 80/80.
- Relationship Audit: 36 passed, two existing Windows symlink-capability skips.
- Web download JavaScript: 5/5 after a scoped approved retry for sandbox spawn
  restrictions. Hidden Windows `--ui-smoke-test`: exit 0.

The real July/August EBA XER checks use ordered disk and Web stream/byte inputs:

| XER | Profile | Detailed rows compared | Prior numbered CSVs byte-identical |
| --- | --- | ---: | ---: |
| July | Programme 5.0 | 15,468 | 10/10 |
| July | Tender 4.0 | 15,468 | 10/10 |
| August | Programme 5.0 | 15,866 | 10/10 |
| August | Tender 4.0 | 15,866 | 10/10 |

`Validate-TenderStateExports.ps1 -BaselinePolicy CalendarDetail -VerifyCalendarDetail`
checks 125,544 assertions, including exact projected calendar row multisets,
governed dates/keys, all twelve disk/Web file bytes, manifest hashes/counts and
unchanged original XER hashes. Only the new table and manifest differ from the
prior profile golden outputs. The baselines were retained from the earlier local
State validation after matching its Core DLL and source hashes to the task-start
build; they were not reconstructed with the new exporter.

`Validate-ResourceExportIntegration.ps1` preserves all fourteen Standard numbered
outputs byte-for-byte and checks disk/Web parity and publication. The local-XER
reconciliation covers 31,334 calendar detail rows and 6,849 resource groups, with
distributed plus known unallocated actuals reconciled. Relationship integration
reconciles 33,100 assessments and checks both profiles using each real XER as the
profile fixture. These are parser consistency checks, not native P6 equivalence.

Additional checks:

- `Validate-ReviewLoaderModels.ps1`: both models deserialize through TOM; all 113
  shared/partition M expressions parse with the Microsoft M parser.
- `Validate-ReviewLoaderContracts.ps1`: 5,126 checks across 13 project-name cases,
  exact eleven-table contracts and manifest/version enforcement.
- `Validate-ReviewCalendarDetails.ps1`: 131 assertions, including 22 relationship
  graph assertions and 15 independently translated row-context scenarios.
- Existing Tender State/access: 93 assertions, including 18 State vectors in two
  cultures, 13 translated access cases and 12 graph assertions. Project mapping
  also passes, including exact/manual identity and source ownership. The State
  assertion was adjusted only for the additional validated table-11 output steps.
- Independent raw-calendar oracle: July 32 calendars/485 raw shifts/15,468 detail
  rows and August 33 calendars/502 raw shifts/15,866 detail rows, zero mismatches.
  This parses raw calendar trees and minute occupancy separately from Core.
- Reporting skill: 22 exact review table contracts, current versions, manifest
  and diagnostic headers, relative links and whitespace. Generic skill validation
  passes for the parser, installed, Programme and Tender copies; changed files
  were backed up and hash-verified when synchronized.
- `git diff --check` passes separately in the parser, Programme and Tender
  repositories. The 13 model/setup/specification files were independently reviewed
  in staging and promoted with before/after hash guards. Existing roles, edges,
  report layout and cache files are unchanged.

## Manual acceptance before deployment

1. Open both changed PBIP projects in Desktop and apply/refresh a regenerated
   current-version CSV-only bundle; check header-only table 11 as well.
2. Verify Athena table/column types (especially `monthupdate`) and refresh Athena-only
   and mixed ownership. Confirm table-11 row counts and known working/nonworking
   exceptions. No live Athena connection was queried during this implementation.
3. Check View-as with allowed/denied exact projects, all-project and State grants,
   including unused/resource calendars and nullable detail keys. Static TOM graphs
   and translated permission cases do not execute DAX/RLS.
4. Inspect the existing refresh-audit table's new table-11 CSV count, and confirm
   no legacy bundle/version is admitted. Existing loader hash-format checks are
   not live downloaded-byte SHA-256 verification; offline export tests do verify
   actual bytes.

No upload, report publication, self-contained packaging, commit or push is part
of this change. Keep manifest-last deployment and avoid refresh during replacement.
