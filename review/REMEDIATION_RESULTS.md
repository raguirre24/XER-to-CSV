# Approved review remediation

Date: 2026-09-05. Scope: XER to CSV parser, Core, Windows, Web and both review CLIs.

## Result

All 16 findings in [the original review](PRE_COMMIT_REVIEW.md) are resolved in the
working tree, with regression assertions updated to the corrected contracts.
Additional integration checks closed duplicate-header loss and low-level parser
identity gaps within F01/F02. Empty optional review projections were hardened and
tested; that defensive change is not presented as another confirmed defect.

No commit, push, SharePoint upload, report/visual edit or change to the sibling
Tender-Review repository was performed. This is implementation validation, not
certification of undocumented native P6 scheduling behavior.

## Resolution matrix

| Finding | Corrected behavior | Regression evidence |
| --- | --- | --- |
| F01: schema loss | Deterministic ordered union preserves later-only values and row metadata. Parser/merge preflight rejects blank or case-insensitively duplicate raw headers before loss. Derived row order is deterministic. | `IngestionExportRegressionTests`, `HeaderValidationRegressionTests`, `LegacyTableReviewTests` |
| F02: filename identity | Ordered batch tokens are independent of names/paths/hashes. Low-level parses also mint occurrence tokens. Standard collision-qualified public keys preserve original filenames; unsafe independently merged public namespaces fail explicitly. Tender binds every entry point to its governed ordered tokens without mutating callers. | `IngestionExportRegressionTests`, `ParserOccurrenceIdentityTests`, `TransformerConsistencyRegressionTests`, `ReviewProfileAuditTests` |
| F03: endpoint dates | Shared 01/06 use actual starts for started work, actual finishes for completed work and remaining forecasts for unfinished work. Only absent remaining values can use early dates; malformed values remain unknown and late dates do not substitute. | `LegacyTableReviewTests`, `TransformerConsistencyRegressionTests` |
| F04: incomplete/stale exports | Resolve every requested Standard table before writing; valid empty results produce exact headers, missing/failed requested sources fail. Stage and roll back selected-file publication. Defined optional review tables remain header-only. | `IngestionExportRegressionTests`, `CalendarExportValidationTests`, `ReviewProfileAuditTests` |
| F05: unsafe paths | Raw table names and CSV/ZIP destinations are validated; unsafe names and selected filesystem links are rejected. | `IngestionExportRegressionTests` |
| F06: invented eight hours | Unknown/nonpositive/ambiguous successor HPD leaves 06 total-float conversion blank. | `LegacyTableReviewTests`, `TransformerConsistencyRegressionTests` |
| F07: units percent | Include actual/remaining labor and nonlabor quantities; preserve defined completed/unstarted and active zero-total behavior. | `LegacyTableReviewTests`, `TransformerConsistencyRegressionTests` |
| F08: nonfinite percentages | Derived percentages use finite decimal inputs; malformed/negative/overflowing/unsupported values remain blank. | `LegacyTableReviewTests`, `TransformerConsistencyRegressionTests` |
| F09: discarded baseline inputs | Filter retained Programme sources before shared calculations across parsed/file/byte and disk/memory entry points. | `ReviewProfileAuditTests` |
| F10: false external relationships | Both review profiles validate raw task/project ownership even with blank/zero lag or unsupported float. Standard leaves unresolved endpoint keys/float blank. Duplicate source-local task identity cannot be disambiguated by project filtering, including 15. | `ReviewProfileAuditTests`, `TransformerConsistencyRegressionTests`, `ResourceCalendarCalculationTests` |
| F11: row-order lookups | Calendar/project/task/curve/resource indexes use occurrence-qualified identities and require unambiguous referenced inputs. | `TransformerConsistencyRegressionTests`, calendar/resource/curve suites |
| F12: WBS ancestry | Reject duplicate/blank identities, cycles and cross-project parents. Preserve the explicit missing-parent-as-root policy. | `LegacyTableReviewTests`, `TransformerConsistencyRegressionTests` |
| F13: Web baseline month | Governed filename grammar cannot interpret project digits as a month. Baseline MonthUpdate starts from the detected P6 Data Date. | Web metadata surface tests and browser fixture |
| F14: upload lifecycle | Serial admission, operation generation/cancellation, monotonic identity, staged commit and combined-limit revalidation prevent stale upload state and token reuse. | Web upload-session/metadata surface tests |
| F15: BOM decoding | Declared UTF-8/16/32 uses strict decoding; malformed declared content rejects. Only BOM-less UTF-8 failure can fall back to Windows-1252. | `XerEncodingTests`, parser metadata entry-point checks |
| F16: download cancellation | Prepare keeps the stream alive; cancellation can prevent commit. Browser handoff is irreversible and uncertain acknowledgement is reported honestly. Pending discard prevents late Blob creation. | Five managed download tests and five shipped-JavaScript lifecycle tests |

## Final automated validation

Each build ran individually using the local SDK and `--no-restore` after the final
parser-token/Tender-binding changes. All returned exit 0, zero warnings and errors.

| Target | Build | Full test (`--no-restore --no-build`) |
| --- | --- | --- |
| `XerToCsvConverter.Core/XerToCsvConverter.Core.csproj` | Passed | Not a test project |
| `XER to CSV.csproj` (Windows) | Passed | Not a test project |
| `XerToCsvConverter.Core.Tests/XerToCsvConverter.Core.Tests.csproj` | Passed | 636/636 passed, zero skipped |
| `XerToCsvConverter.Web/XerToCsvConverter.Web.csproj` | Passed | Not a test project |
| `XerToCsvConverter.ProgrammeReview.Cli/XerToCsvConverter.ProgrammeReview.Cli.csproj` | Passed | Not a test project |
| `XerToCsvConverter.TenderReview.Cli/XerToCsvConverter.TenderReview.Cli.csproj` | Passed | Not a test project |
| `XerToCsvConverter.TenderReview.Cli/Tests/XerToCsvConverter.TenderReview.Surface.Tests.csproj` | Passed | 40/40 passed, zero skipped |

Command pattern: `.\local-dotnet-sdk\dotnet.exe build <target> --no-restore`,
then `.\local-dotnet-sdk\dotnet.exe test <test-target> --no-restore --no-build`.

`C:\Program Files\nodejs\node.exe --test XerToCsvConverter.Web/tests/downloads.test.cjs`
passed 5/5. Sandbox worker creation initially returned EPERM; the approved local
rerun passed. Surface restore refreshed stale framework-reference metadata before
the final no-restore sequence; it introduced no new package dependency.

Earlier integration runs exposed old omission/duplicate-ID expectations. Those
assertions now test explicit failure instead of preserving the reviewed defects.
Only the final complete runs above are reported as passing validation.

## Browser and saved archives

The local browser uses only the synthetic
`review/fixtures/J5001_C_BL01_2026-01-31.xer`; no user schedule was transmitted.
`Validate-BrowserFixtureArchive.ps1` independently reads saved ZIP bytes, checks
file coverage, row counts, unique task keys, relationship joins/float, resource
quantity totals and review manifest CSV/source hashes and per-source counts.

After the final build/reload, all three profiles reached their validated browser
handoff state and restored editable controls. Standard accepted two identical
ordered inputs and generated all 14 Enhanced tables. Tender accepted the same
filename/path/bytes as two separate stages dated 2026-09-05 and 2026-09-07; the
duplicate-date rejection was also exercised earlier in this run. Programme inferred
BL01 and baseline MonthUpdate/Data Date 2026-01-30, not a month from project J5001.
Clear and profile-switch workflows did not retain previous table/activity-log state.

Two archives saved earlier in the same remediation run were independently verified:

| Saved archive | Verification |
| --- | --- |
| `C:\Users\ricar\Downloads\XER_Export (1).zip` | 14 CSVs; four unique activities, two relationships with zero free float, two calendars/seven rules each, header-only 04, original filename provenance and 200.0000 resource units |
| `C:\Users\ricar\Downloads\J5001_C_20260905T104701Z_5f6b0d36.zip` | Ten Programme CSVs plus manifest; two activities, one relationship, 100 resource units; exact source/CSV hashes, per-source table counts, schema 3.0 and P6 Data Date/MonthUpdate checked |

These saved archives preceded the final low-level parser-token/Tender-binding
hardening. Later browser handoffs, including Tender on the final build, did not
produce an observable new file in Downloads during this session. Browser warning/
error logs and the page showed no explanation; the cause is not established.
Saved-file delivery and integrity for those final handoffs remain **unverified**.
No browser permission/security settings were changed and no completion log is used
as a substitute for delivery evidence. Final Core/surface tests independently cover
the final byte/file/parsed entry points and repeated-stage calendar/curve parity.

## Reporting skill

Updated portable `skills/p6-numbered-xer-reporting` and the installed copy at
`C:\Users\ricar\.codex\skills\p6-numbered-xer-reporting`. All six installed files
match the portable files by SHA256. The official Skill Creator validator passed;
`Validate-ReportingSkill.ps1` passed 20 exact review header contracts, eight relative
reference links and whitespace checks.

A fresh agent used only the skill and its routed references to correctly answer
the source-identity, joins, nullable/signed float, exception replacement, HPD,
curve/actual spread, optional export and browser handoff scenarios. Its assignment
traceability feedback was incorporated: no table 15 schema exports assignment ID,
so raw-source reconstruction plus aggregate/multiset comparison is required, not
an invented unique row-to-assignment match.

Final `git diff --check` and an additional whitespace scan of untracked source,
test, documentation, script and skill files passed. The synthetic `.xer` fixture
is intentionally excluded from trailing-tab checks because empty XER fields use
tab delimiters. Git emitted only LF-to-CRLF normalization notices.

## Preserved boundaries and next acceptance

- Programme 3.0 and Tender 1.0 schemas, history conventions and their different
  uniqueness rules remain intentional. Standard unique-filename public keys remain
  compatible; collision qualification applies only where needed.
- Table 04 remains a global earliest filename-month snapshot, Programme history
  remains weekday-based, and LongestPathVisual's least-float policy is unchanged.
- Unsupported progressed/resource-dependent relationship float stays blank. Actual
  resource spread is a working-time estimate; unsupported progressed nonlinear
  curves require an explicit remaining profile. No native P6 golden parity claimed.
- Standard publication is rollback-protected and atomic per file, not an atomic
  directory snapshot for concurrent readers. Do not refresh a report mid-publication.
- Windows interactive acceptance, representative large-browser memory/cancellation
  stress, Power BI refresh/visual acceptance and native P6 comparisons remain manual
  checks. Existing CSVs must be regenerated with the rebuilt parser.
- Commit and push remain a separate user decision.
