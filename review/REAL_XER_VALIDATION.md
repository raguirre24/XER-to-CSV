# Real-XER calendar and all-table validation — 2026-09-06

Historical checkpoint: the user subsequently approved non-blocking actual-date
warnings. July is no longer blocked. See [current results](NONBLOCKING_ACTUAL_WARNINGS.md)
for complete exports, preserved unallocated actuals and the companion CSV.

## Outcome

The original table 11 failure is fixed. The two supplied XERs contain legitimate
P6 structured calendar text using DEL/U+007F separators and unnamed root records.
The parser previously rejected this syntax. All 32 July and 33 August calendars
now resolve without editing their raw calendar data.

- **2608-EBA_PAA_8.0.xer:** all 14 Standard/Enhanced tables generate and pass the
  source/column validation described below, including all resource assignments.
- **2607-EBA_PAA_8.0.xer:** the other 13 tables pass. Full export including table 15
  correctly remains blocked by two inconsistent source assignment periods.
  No records are silently omitted from the production export.
- Windows/file and Web/stream parsing produce byte-identical CSVs for August's
  complete 14-table export, the combined July/August 13-table selection, and a
  repeated ordered input containing August twice with all 14 tables.

These are local working-tree results, not a deployment. No commit, push, live
website publication, SharePoint upload, report/visual edit, or original-XER edit
was performed for this correction. Original XERs were read locally only.

## Corrections and table impact

1. Calendar parsing consumes ISO control characters only as structural spacing
   and permits optional record names. Seven-day completeness, invalid clock/date
   rejection, source identity and inheritance safeguards remain. **10** preserves
   every raw source calendar field. **11** can now publish the valid weekday and
   resolved exception rules. **06** and **15** can use those same resolved calendars.
2. Positive recorded actual units outside calendar working time are retained.
   An entirely nonworking valid actual period uses elapsed-time monthly weights,
   labeled `Actual Elapsed Time`; equal actual timestamps put units in their
   recorded month, labeled `Actual Recorded Date`. Working hours remain zero;
   a recorded instant has zero duration diagnostics. Valid working-time actual
   spreads retain their previous weights. Actual curves are not applied.
3. A valid progressed nonlinear remaining curve confined to one monthly bucket
   has an exact monthly quantity independent of its unknown intramonth phase.
   This also covers an exclusive finish at the next month's midnight. Curve,
   calendar and duration-type validation still apply. Cross-month progressed
   nonlinear curves still require an authoritative remaining profile; no curve
   phase is guessed or reset.
4. Generated-table failures retain their underlying cause, original filename and
   calendar/assignment context through Standard, Programme and Tender errors.
   Failed requested tables still prevent publication of a partial export.

No headers or schema versions changed. **01** HPD conversions, dates and
percentages were independently checked, not changed by this syntax correction.
**02/03/04/07/08/09/12/13/14** retain their existing source/projection and baseline
rules. Programme weekday-history policy is unchanged. Review **06/15** consume the
corrected shared values; review **10** remains key/name-only. Programme remains
schema 3.0 and Tender 1.0. Manifests naturally reflect regenerated rows/hashes.

The relationship `06.free_float` formula and the visual's least-float policy were
not changed. Resolving the valid calendars now yields 2,236 known relationship
floats in July (1,156 zero, 1,080 positive) and 2,429 in August (1,420 zero, 1,009
positive). The other 14,009/14,426 relationships remain nullable under the existing
unsupported/unresolved-case contract; blank is not zero or a driving-edge claim.
These counts are not a native P6 scheduling-parity certification.

## Source-data correction required for July

The project Data Date is **2026-07-25 17:00**, but these positive-actual assignments
have no actual finish and start later than that date:

| TASKRSRC ID | TASK ID | RSRC ID | Assignment actual start |
| --- | --- | --- | --- |
| 3967175 | 4602913 | 7916 | 2026-08-03 08:00 |
| 3967176 | 4602914 | 7916 | 2026-08-06 08:00 |

For unfinished actual work, the export uses the project Data Date as the period
end. These would therefore be reversed intervals. Correct the assignment dates
or the intended project status date in P6, based on the authoritative schedule,
then export the XER again. The parser cannot decide which date is wrong, borrow
the task's wider actual dates, or invent a historical monthly spread.

A local **diagnostic in-memory copy only** excluded these two assignments to
check all remaining July assignments. Its 6,386 monthly rows reconciled across
3,589 task/resource/actual groups. This is not a successful complete July export,
not a production exclusion policy, and no such modified XER/CSV was published.

## Per-table results

Column counts include the Standard writer's `FileName` provenance column.

| Table | Columns | July rows | August rows |
| --- | ---: | ---: | ---: |
| 01_XER_TASK | 40 | 10,655 | 11,107 |
| 02_XER_PROJECT | 91 | 1 | 1 |
| 03_XER_PROJWBS | 30 | 2,781 | 2,862 |
| 04_XER_BASELINE | 40 | 10,655 | 11,107 |
| 06_XER_PREDECESSOR | 29 | 16,245 | 16,855 |
| 07_XER_ACTVTYPE | 11 | 46 | 46 |
| 08_XER_ACTVCODE | 12 | 1,088 | 1,091 |
| 09_XER_TASKACTV | 8 | 39,101 | 39,506 |
| 10_XER_CALENDAR | 16 | 32 | 33 |
| 11_XER_CALENDAR_DETAILED | 13 | 15,468 | 15,866 |
| 12_XER_RSRC | 38 | 94 | 98 |
| 13_XER_TASKRSRC | 52 | 10,980 | 11,082 |
| 14_XER_UMEASURE | 7 | 4 | 4 |
| 15_XER_RESOURCE_DISTRIBUTION | 27 | Blocked as described above | 5,936 |

August resource quantities reconcile across 3,256 source/task/resource/actual
groups to the sum of individually rounded assignment quantities. Its 47 elapsed
actual rows and 15 recorded-date actual rows retain zero working-hour diagnostics.
The July diagnostic subset contains 47 elapsed and 19 recorded-date actual rows.

## Independent column validation

The reusable [local validator](Validate-LocalXerExport.ps1) accepts an ordered
`-Paths` array, opens originals read-only, and exercises the shared Web stream and
memory-export entrypoints. It writes aggregate results only, not XER/CSV data.
Full 14-table validation is the default. `-ExcludeResourceDistribution` explicitly
limits the check to 13 tables and declares that table 15 was not validated.

Checks include exact fixed/dynamic headers and CSV widths, retained raw cells,
provenance, unique dimension keys, foreign keys, source occurrence isolation,
global baseline selection, finite derived numerics, and calendar weekday/dated
replacement identity and flags. Independent source-field calculations also verify
01 durations/floats, percentages, dates, status, label and Data Date, and 06 endpoint
metadata/dates, predecessor HPD/lag and successor total float. This audit does not
call the production relationship solver as an allegedly independent oracle.

| Check | July, 13 tables | August, all 14 tables |
| --- | ---: | ---: |
| Exact retained source cells | 1,106,964 | 1,129,245 |
| Finite derived numeric cells | 155,659 | 215,352 |
| Independently checked derived values | 446,120 | 463,858 |

The combined-input audit additionally checked 2,236,209 retained source cells and
280,380 finite numeric cells across the 13-table selection. All raw input rows had
the declared width; primary IDs were unique and required native references resolved.
Each source has one WBS parent outside its exported WBS set, correctly retained as
an exported root with a blank parent key.

## Build, test and skill validation

Each command was run from the repository using `local-dotnet-sdk/dotnet.exe`.
All seven build targets passed with **zero warnings and zero errors**:

```text
build XerToCsvConverter.Core/XerToCsvConverter.Core.csproj --no-restore
build "XER to CSV.csproj" --no-restore
build XerToCsvConverter.Core.Tests/XerToCsvConverter.Core.Tests.csproj --no-restore
build XerToCsvConverter.Web/XerToCsvConverter.Web.csproj --no-restore
build XerToCsvConverter.ProgrammeReview.Cli/XerToCsvConverter.ProgrammeReview.Cli.csproj --no-restore
build XerToCsvConverter.TenderReview.Cli/XerToCsvConverter.TenderReview.Cli.csproj --no-restore
build XerToCsvConverter.TenderReview.Cli/Tests/XerToCsvConverter.TenderReview.Surface.Tests.csproj --no-restore
```

- `test XerToCsvConverter.Core.Tests/XerToCsvConverter.Core.Tests.csproj --no-restore --no-build`: **707 passed**.
- `test XerToCsvConverter.TenderReview.Cli/Tests/XerToCsvConverter.TenderReview.Surface.Tests.csproj --no-restore --no-build`: **40 passed**.
- `node --test XerToCsvConverter.Web/tests/downloads.test.cjs`: **5 passed**.
- The 71 new Core cases cover calendar grammar, strict semantic rejection,
  actual allocation/rounding/boundaries, single-month curve invariance, Standard
  and both review-profile success/failure paths, and preservation of existing
  output files on failure. Existing float/curve/profile regressions also passed.
- Reporting skill: **20 exact review-table headers**, **8 relative reference
  links**, whitespace checks, and Skill Creator validation passed. The five
  changed Markdown files were installed and hash-verified against the portable
  copy; agent metadata was preserved.

Final `git diff --check` passed. An independent six-scenario skill forward test
also found no conflicting guidance. These checks do
not certify a browser-saved ZIP, a currently deployed WebAssembly version, native
P6 Resource Usage/relationship parity, or Power BI report/visual acceptance.
Originals were not uploaded to the live Web parser for this validation.
