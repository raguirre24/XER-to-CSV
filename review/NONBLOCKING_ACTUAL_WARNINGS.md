# Non-blocking actual-date warnings

Historical checkpoint: its actual-date-only warning scope and companion schema
1.0/24-column header are superseded by the
[current non-blocking resource-allocation policy](NONBLOCKING_RESOURCE_WARNINGS.md),
which covers actual and remaining portions with the additive schema 1.1 header.
The validation results below describe that earlier implementation, not the current
coverage limits.

Validated 2026-09-06. This supersedes the strict July rejection recorded in
[the earlier real-XER checkpoint](REAL_XER_VALIDATION.md).

## Export behaviour

An invalid actual date period is a source-data warning, not a reason to discard
every assignment in table 15. When quantities, identities and calendars remain
usable, the parser preserves the original source values and continues exporting:

- Valid actual allocations and valid remaining allocations are retained.
- An actual portion with missing/malformed required dates or finish before start
  has no invented monthly allocation. Its quantity is explicitly unallocated in
  `XER_DATA_QUALITY.csv`, with the original date and quantity strings.
- Remaining work on that same assignment still distributes independently.
- Reconcile distributed actual plus unallocated actual against source actual,
  at compatible source/task/resource/unit scope and four-decimal precision.
- This is scoped actual-date recovery, not an exhaustive scheduling-quality
  audit. Invalid quantities, identities, calendars, remaining periods and
  unsupported remaining curves remain fatal rather than producing fabricated data.

The companion has 24 columns and diagnostic schema version 1.0. Its grain is one
unresolved actual portion per source assignment, not per month. The public source
namespace and one-based source row number distinguish repeated input occurrences;
internal source tokens are never exported as reporting keys. Original `FileName`
is provenance, not unique identity. Diagnostic dates must be imported as text.
See [the exact dictionary](../skills/p6-numbered-xer-reporting/references/table-dictionary.md).

Standard emits the companion whenever table 15 is selected, even when header-only.
Programme and Tender always emit ten unchanged numbered tables, the companion,
and the manifest: twelve files, with eleven manifest rows per retained source.
Manifest hashes include the companion. Existing numbered schema versions remain
Programme 3.0 and Tender 1.0. Consumers enforcing the old eleven-file review
envelope must allow this supplemental artifact.

Windows and Web show completion with warnings only after publication/download
handoff succeeds. Both review CLIs report warnings on stderr while retaining
exit code zero and the completed bundle path on stdout. Manifest `complete` /
`COMPLETE` means publication succeeded, not that the input schedule is clean.

## Original user-file results

Both unmodified originals were read locally. No source rows were deleted or
excluded, and no source files were uploaded.

| Source | Table 15 rows | Warning rows | Unallocated actual |
| --- | ---: | ---: | ---: |
| 2607-EBA_PAA_8.0.xer | 6,388 | 2 | 56.0000 units |
| 2608-EBA_PAA_8.0.xer | 5,936 | 0 | 0.0000 units |

July's assignments `3967175` and `3967176` have actual starts of
`2026-08-03 08:00` and `2026-08-06 08:00`, blank actual finishes, and project Data
Date `2026-07-25 17:00`. The invalid fallback periods produce
`ACTUAL_FINISH_BEFORE_START` warnings with 32.0000 and 24.0000 unallocated actual
units. Both assignments' valid remaining work is retained.

Combined full validation passed for all 14 Enhanced tables plus the companion:
2,236,209 retained source cells, 391,296 finite derived numeric cells, 909,978
independent derived checks, and 6,849 reconciled resource groups. Table 11 contains
31,334 rows; table 15 contains 12,324 rows. There are 128 valid actual fallback
rows. Source totals reconcile after including the 56.0000 unallocated actual units.

File-based and Web stream APIs produced byte-identical CSV output for both
originals together and for July supplied twice as separate ordered occurrences.
This checks the browser's shared parser/export API, not rendered UI behaviour.

## Validation commands and results

All seven builds passed with zero warnings and zero errors using
`local-dotnet-sdk/dotnet.exe build <project> --no-restore`:

- `XerToCsvConverter.Core/XerToCsvConverter.Core.csproj`
- `XER to CSV.csproj`
- `XerToCsvConverter.Core.Tests/XerToCsvConverter.Core.Tests.csproj`
- `XerToCsvConverter.Web/XerToCsvConverter.Web.csproj`
- `XerToCsvConverter.ProgrammeReview.Cli/XerToCsvConverter.ProgrammeReview.Cli.csproj`
- `XerToCsvConverter.TenderReview.Cli/XerToCsvConverter.TenderReview.Cli.csproj`
- `XerToCsvConverter.TenderReview.Cli/Tests/XerToCsvConverter.TenderReview.Surface.Tests.csproj`

The same SDK's `test <project> --no-restore --no-build` passed 745 Core tests
and 50 surface tests. `node --test XerToCsvConverter.Web/tests/downloads.test.cjs`
passed all five download lifecycle tests. The surface suite includes source-wiring
assertions; it is not a substitute for interactive Windows/browser verification.

`review/Validate-LocalXerExport.ps1 -Paths @(<July path>, <August path>)` passed
all checks above. `review/Validate-ReportingSkill.ps1` passed 20 exact numbered
review contracts, the diagnostic header, eight links and whitespace. Independent
forward-use skill QA correctly identified the unallocated quantity, remaining
allocation, profile grains and warning semantics; its raw-date documentation
clarification was incorporated.

Manual Windows/Web visual acceptance and deployed-site acceptance remain separate
checks. This work does not edit the sibling Tender-Review repository, the visual,
or a Power BI report, and does not upload anything to SharePoint.
