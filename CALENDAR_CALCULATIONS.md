# Parser calendar calculations and relationship free float

The shared Core parser supplies calendar calculations to Standard Enhanced,
Programme Review and Tender Review exports. No BI report measures or Programme
history/weekday-variance rules are changed by this correction.

## Real-XER compatibility correction (2026-09-06)

The calendar reader now accepts P6's unnamed structured-text containers and ISO
control separators, notably DEL/U+007F. This changes parsing, not calendar shifts,
conversion factors or the relationship-float formula. Table 10 preserves the raw
`clndr_data` value exactly after text decoding. Invalid semantic calendar definitions
still fail. Shared 06/11/15 calculations can now resolve these valid calendars.

Recorded actuals outside scheduled working time and equal actual timestamps are
handled as described below, without inventing calendar hours. A progressed curve
whose entire remaining period fits one monthly bucket has an exact monthly total
without requiring its unknown intramonth phase. Cross-month restrictions remain.

Generated-table failures retain original causes and input/calendar/assignment
context across Standard, Programme and Tender. Corrected syntax does not make
inconsistent source dates valid. The approved non-blocking policy now preserves
unresolvable actual periods in `XER_DATA_QUALITY.csv`, while valid actual/remaining
allocations continue. See [current warning validation](review/NONBLOCKING_ACTUAL_WARNINGS.md)
and the [earlier calendar checkpoint](review/REAL_XER_VALIDATION.md) for the two
supplied schedules and the distinction between real-file checks, synthetic
regressions and native P6 parity.

## Integrated consistency fixes (2026-09-05)

This section supersedes earlier phase-specific statements below that only
`06.free_float` or table 15 changed. The approved pre-commit remediation also
corrects shared ingestion, derived dates, conversions and export validation.
See [the remediation results](review/REMEDIATION_RESULTS.md) for current validation;
the earlier validation counts below are historical checkpoints.

- Every input occurrence has an internal source token independent of its filename,
  path and hash. All shared calculation lookups use `(source token, native ID)`.
  Standard retains existing keys for unique filenames; collisions receive a
  deterministic occurrence-qualified public namespace. Original filenames remain
  provenance and continue to determine legacy filename-month metadata.
- Input schemas are unioned without losing later-only fields. Blank or duplicate
  case-insensitive raw headers are rejected before parsing/merging calculations.
  Source and derived output order no longer depends on parallel task completion.
- In shared 01 and 06 display dates, unstarted work uses remaining dates, falling
  back to early dates only when the remaining value is absent. Active work retains
  its actual start and forecast finish; completed work uses actual endpoints.
  A malformed preferred value, missing actual endpoint or unknown status stays
  blank. Late dates never silently substitute for these display dates. The stricter
  relationship solver guards documented below still apply independently.
- Table 06 successor total float no longer invents an eight-hour conversion.
  Unknown, nonpositive or ambiguous hours/day leaves derived day values blank.
  Duplicate source-local calendar/project/task identities cannot be selected by
  row order; an extra project field cannot disambiguate a duplicate public task key.
- Table 01 units percent includes actual and remaining labor plus nonlabor units.
  Derived percentages use finite invariant decimals; malformed, negative,
  overflowing or unsupported inputs stay blank. Valid completed/unstarted status
  remains 100/0; active zero-total units remains zero. Raw source fields are retained
  independently and are not proof that a derived value is valid.
- WBS duplicate identities, cycles and cross-project parent links fail validation.
  Missing parents retain the legacy root policy with a blank parent key. Table 04
  deliberately retains its global earliest filename-month baseline policy.
- Programme filters discarded baselines before shared calculations. Both review
  profiles validate raw relationship project ownership before projecting keys,
  including zero/blank-lag and unsupported-float cases. Invalid external edges
  reject a review bundle; Standard leaves unresolved endpoint keys and float blank.
- Standard validates every requested table before publication. A genuinely empty
  generated table writes its exact header, replacing stale selected output; a
  missing requested source or failed calculation aborts explicitly. Disk publication
  stages files and rolls back the selected set on failure, but is not an atomic
  directory snapshot to concurrent readers. Unrelated existing files are untouched.
  Review profiles retain their ten fixed tables plus manifest and optional
  header-only table policy.

Regenerate exports to use these corrections. Existing CSVs do not change in place.
No profile schema/version, report, LongestPathVisual policy or native P6 scheduling
engine was changed or certified by these fixes.

## One working-calendar model

`P6CalendarRepository` reads the balanced `clndr_data` structure, requires all
seven weekday rules and resolves `base_clndr_id` exceptions within the same source
token. A local dated exception overrides its inherited exception. Repeated original
filenames, paths, hashes and native calendar IDs in different source instances do
not combine calendars.

Working intervals, not `day_hr_cnt`, determine availability. Split shifts are
preserved, intervals are sorted and overlaps are counted once. Overnight shifts
are split into civil-day intervals. An explicit exception replaces that whole
date, including incoming night work; affected next-day rules are also resolved.
The resulting model is shared by table 11 and date arithmetic in tables 06/15.
Table 15 selects the resource calendar for resource-dependent activities and the
task calendar for task-dependent work. Month slices meet exactly at midnight, so
continuous calendars do not lose a second at every month boundary.

Table 10 continues to preserve source calendar fields. Table 11 keeps its existing
headers but exports deterministic, invariant-culture standard-week and resolved
exception rows. It is a rule table, not a fully expanded daily date table. A
consumer must replace a standard weekday with its dated exception, not add them.

Missing/malformed workweeks never become invented eight-hour or 24-hour schedules.
Calendar parsing gives contextual errors. Table 11 cannot be generated from an
invalid calendar. Table 06 retains the relationship with blank free float when a
required calendar is unresolved; table 15 cannot distribute on an unresolved
calendar. Positive hours-per-day without shifts do not establish availability.
Requested calendar-dependent tables that fail generation cause an explicit export
error before CSV writing, rather than being silently omitted from a successful export.

Working-time addition/subtraction uses exact shift endpoints, excludes breaks,
preserves the direction of intervals, supports cancellation, and has a bounded
search rather than looping indefinitely over a calendar with no working time.
Calendar numeric/date serialization is invariant and Gregorian.

## Tables 10, 11 and 15: verified calendar and distribution behaviour

Table 10 is the source calendar table, not a newly calculated work schedule. Its
`day_hr_cnt`, `week_hr_cnt`, `month_hr_cnt`, `year_hr_cnt` and raw `clndr_data` remain
unchanged. A period conversion factor must not be replaced by average daily work
availability. Programme Review and Tender Review deliberately project only the
calendar key/name from table 10; their fixed contracts do not contain table 11.

Table 11's shared calendar fixes were already present. New table-level tests lock
its existing headers, all seven weekday rules, working/nonworking flags, inherited
and overridden exceptions, supported exception-section spellings, minute-level
hour precision, invalid period factors, and source-occurrence isolation. Invalid
hours/day do not erase a valid workweek; ambiguous calendar identities cannot
produce a misleading detailed calendar.

Table 15 is a **monthly distribution of assignment units**, normally weighted by
calendar working time.
It uses TASK calendar availability for task-dependent activities and the assigned
RSRC calendar for resource-dependent activities. Actual units include regular and
overtime quantities, but the monthly distribution is an estimate: it does not
reconstruct historical timesheets. Remaining units use the explicit remaining
profile or supported named curve described below; without either they remain
uniformly spread. These are distinct P6 mechanisms documented in Oracle's
[resource curve guidance](https://docs.oracle.com/cd/G48902_01/English/User_Guides/p6_pro_user/resource_curves.htm)
and [future-period planning guidance](https://docs.oracle.com/cd/G18294_01/p6help/en/future_period_bucket_planning.htm).

The table 15 correction:

- Uses source-token/native-ID tuples internally, with assignment project context
  when resolving TASK. Duplicate or missing referenced identities cannot be chosen
  by row order. Public keys and all CSV headers remain unchanged.
- Parses nonnegative quantities as invariant decimals. Blank optional quantities
  retain their zero convention; nonblank malformed, nonfinite, negative or
  overflowing values cause an explicit failed export, not a silently lost row.
- Uses assignment actual/remaining dates, not wider activity dates. For actuals,
  an explicit valid assignment actual finish is used even while the activity is
  active; only a genuinely absent finish on an active activity uses its project's
  data date. A completed allocation requires its assignment actual finish.
- Rejects positive quantities with invalid/missing/reversed periods or no working
  availability for remaining work. Recorded actuals with a valid but entirely
  nonworking period use elapsed-time monthly shares (`Actual Elapsed Time`), with
  zero working-hour diagnostics. Equal actual timestamps place units in that
  instant's month (`Actual Recorded Date`), with zero duration diagnostics.
  Neither exception permits malformed calendars or repairs reversed dates.
  Missing/malformed required actual dates and reversed actual periods (including
  fallback Data Date before actual start) instead produce reportable unallocated
  actual warnings. Valid remaining work on the same assignment continues.
  Unrecoverable failures still discard the complete table and prevent publication.
- Slices periods as adjacent half-open intervals `[start, finish)`, including
  midnight/overnight work. Calendar-day diagnostics count occupied civil dates:
  January 31 00:00 -> February 2 00:00 occupies **two**, not three, calendar days.
- Calculates cumulative proportional quantities from exact working ticks and
  subtracts successive four-decimal rounded cumulative amounts. Monthly rows
  therefore sum to the distributed assignment portion at existing four-decimal
  export precision. Distributed actual plus companion unallocated actual equals
  rounded source actual; remaining units reconcile entirely to monthly rows.
  In working-time mode a zero-work month
  receives no remainder. The actual-only elapsed fallback uses the same cumulative
  rounding rule with elapsed rather than working ticks.
  For a quantity of 1 over 1/28/1 continuous working days, the monthly quantities
  are **0.0333, 0.9334, 0.0333**, summing to 1.0000.
- Divides unrounded working hours by the selected calendar's positive hours/day
  for day diagnostics. Missing/unrepresentable conversions remain blank in Standard;
  existing stricter review-profile validation is retained. Hour/day diagnostics
  keep their existing two-decimal display formatting; they are not inputs to the
  quantity calculation. Repeated total-period fields must not be summed across months.

Programme Review consumes the corrected monthly quantities; Tender consumes and
aggregates them at its existing task/resource/actual/month grain. Contract versions
remain Programme **3.0** and Tender **1.0**. Table 06's relationship calculation and
LongestPathVisual are not changed by this table 10/11/15 work.

### Supplemental actual-date diagnostics

Every selected table 15 export includes `XER_DATA_QUALITY.csv`, header-only when
clean. This avoids leaving stale warnings after a clean rerun. It contains original
assignment dates/quantity strings, raw project Data Date, source-qualified project,
task, resource and assignment keys, per-source assignment row ordinal, known rounded
unallocated actual quantity and the issue reason. Internal source tokens correlate
rows but are not exported; public namespaces preserve repeated-file independence.

No invalid actual period is silently dropped, assigned to a guessed month, zeroed
or repaired using activity dates. The monthly numbered schema remains unchanged.
Only actual-date validation is recoverable; invalid quantities, ambiguous required
identities/calendars and unsupported or invalid remaining allocations retain their
existing failure rules. This is not an exhaustive scheduling-quality audit.

Review bundles contain ten numbered tables, companion and manifest (twelve files).
Manifests retain their columns and publication-complete literals; each source gains
one `XER_DATA_QUALITY` manifest row with warning count and companion hash. Numbered
schema versions stay Programme 3.0 / Tender 1.0; the companion is independently
versioned 1.0. Windows, Web and CLI display completed-with-warnings when its row count
is nonzero. CLI warnings use stderr and successful publication still returns zero.
Consumers with a hard-coded eleven-file review envelope must allow this approved
supplemental file; report/visual repositories were not changed.

### Table 15 remaining resource curves

The shared Core implementation applies the following precedence **per TASKRSRC
assignment**, before any Tender Review task/resource/month aggregation:

1. A nonblank `TASKRSRC.remain_crv` is authoritative for the remaining allocation.
   Supported serialization is semicolon-separated `quantity:working-hours` pairs,
   for example `80:24;20:24`. From assignment `restart_date`, the first 24 calendar
   working hours receive 80 units and the next 24 receive 20 units. Calendar breaks,
   exceptions and holidays do not advance the profile. Zero-quantity bands preserve
   deliberate gaps. This path also supports progressed assignments.
2. Otherwise a nonblank `curv_id` resolves exactly one `RSRCCURVDATA` row in that
   **source-occurrence token**, never another input with the same filename, path,
   hash or native curve ID. The 21 `pct_usage_0` through `pct_usage_20` fields are
   required finite nonnegative percentages, each no greater than 100. Their sum
   must be within 0.001 of 100; only this small serialization/proration difference
   is normalized. Indices 1 through 20 allocate quantities to successive 5% bands
   of the assignment's working duration. They are not cumulative percentages or
   spline heights. Nonzero index 0 requires P6 actuals semantics and is rejected
   unless an explicit remaining profile was supplied.
3. No remaining profile and no curve ID preserves the existing uniform spread.

Named curves require `TASK.duration_type = DT_FixedDrtn` or `DT_FixedDUR2`.
Nonlinear named curves support unstarted activities with unstarted assignments.
A named linear curve is phase-independent and can also be used on active work.
A progressed nonlinear curve whose complete remaining interval fits one calendar
month has an exact total in that bucket, independently of its unknown curve phase.
This includes an exclusive finish at next month's midnight; curve/calendar
validation still applies and no intramonth shape is inferred. Across monthly
buckets, a progressed nonlinear curve without explicit `remain_crv` fails with
source/assignment/curve context: we do not restart its entire shape over the
remaining period or infer a P6 curve phase from activity percent complete.
Manual curve ID 9 without `remain_crv` is also rejected. Opaque `RSRCCURV.curv_data`
alone is not decoded or substituted for the required numeric definition.

Manual-profile quantities must be nonnegative and periods strictly positive and
representable in whole working ticks. Periods must sum exactly to the assignment's
remaining calendar working duration; quantities must sum to `remain_qty` at the
existing four-decimal export precision. Inconsistent, malformed or unsupported
profiles fail rather than being stretched, truncated or replaced by a named curve.
Accepted sub-export-precision quantity differences reconcile to `remain_qty`.

Each month receives the difference between successive cumulative curve/profile
allocations, rounded cumulatively to four decimals. A band crossing a month end is
split in proportion to its working time. Total quantity is conserved, and a zero
allocation/nonworking interval receives no rounding remainder. An assigned linear
curve retains the same quantity arithmetic as the original uniform calculation.
`month_working_hours`, `total_working_hours` and hours/day diagnostics remain actual
calendar measurements, not curve-weighted resource quantities.

Standard's existing `distribution_type` distinguishes `Working Hours`,
`Resource Curve`, `Remaining Units Profile`, `Actual Elapsed Time` and
`Actual Recorded Date`; no columns are added. Programme
Review and Tender Review retain their fixed nine-column table 15 contracts and
existing schema versions. Actual rows use the working-time estimate or explicitly
labeled off-calendar/recorded-date cases above, even when `curv_id` or `actual_crv`
exists. `target_crv` does not substitute for remaining
or actual units. Tables 06, 10, 11 and LongestPathVisual are unchanged.

Curve errors propagate with table/source/assignment context through Standard,
Programme Review and Tender Review, preventing partial requested-table exports.
Unused curve definitions and actual-only assignments do not require a valid
remaining curve. Selecting unrelated raw tables does not invoke this calculation.

References: Oracle's [curve-band and 0% semantics](https://docs.oracle.com/cd/G18296_01/client_help/en_US/modify_resource_curves_dialog_box.htm),
[curve fields](https://docs.oracle.com/cd/F51301_01/English/Mapping_and_Schema/xer_import_export_data_map_project/97898.htm),
and [exported assignment profiles](https://docs.oracle.com/cd/F88968_01/English/Mapping_and_Schema/xer_import_export_data_map_project/97916.htm).
The manual profile serialization is corroborated by the primary
[MPXJ reader](https://raw.githubusercontent.com/joniles/mpxj/master/src/main/java/org/mpxj/primavera/TimephasedHelper.java).
The implementation is independently written; no MPXJ code or runtime dependency
is included. Synthetic tests establish this explicit allocation contract. Native
P6 Resource Usage exports remain necessary to certify version-specific parity,
particularly before extending progressed named curves or special 0% handling.

## `06_XER_PREDECESSOR.free_float`

This column is the **maximum signed predecessor working-time delay allowance in
days for this individual relationship**. It is not successor-calendar relationship
float, the predecessor activity's stored `TASK.free_float_hr_cnt`, or a promise of
equality with P6's displayed relationship-gap metric under mixed calendars/lag.

For a supported relationship:

1. Select Finish->Start for FS, Start->Start for SS, Finish->Finish for FF and
   Start->Finish for SF. Prefer remaining early dates (`restart_date`/`reend_date`).
   A missing remaining date can fall back to a valid early date for an unstarted
   activity; a malformed nonblank remaining date cannot. An active predecessor's
   finish must have an explicit valid `reend_date`.
2. Find the latest predecessor endpoint whose signed lag, applied on the configured
   lag calendar, does not exceed the unchanged successor endpoint. The inverse
   includes nonworking plateaus and signed-lag discontinuities: merely subtracting
   lag is insufficient.
3. Convert that endpoint limit to predecessor working ticks, with moved Start
   events at a working-start boundary and moved Finish events at a working-finish
   boundary. Zero movement preserves the original timestamp.
4. Verify that the resulting displacement is feasible and the next working tick is
   not. Divide the signed hours by the predecessor's positive `day_hr_cnt` only at
   final serialization. Unknown/unprojectable boundaries remain blank.

The previous implementation measured predecessor work between the already-lagged
date and the successor date. That gap is not generally a safe predecessor delay.
For a Monday-Friday 08-12/13-17 predecessor, continuous successor and +8 elapsed
hours FS lag, Monday 17:00 -> Tuesday 17:00 permits **1 working hour (0.125 days)**,
not the previously reported 8 hours. Monday 09:00 -> Tuesday 09:00 permits **7
working hours (0.875 days)**, not 1 hour. Monday 17:00 -> Tuesday 00:00 needs the
predecessor brought forward **1 working hour (-0.125 days)**, not zero.

The supported scheduling tokens are `rcal_Predecessor`, `rcal_Successor`,
`rcal_24Hour` and `rcal_ProjDefault`, with whitespace/case normalization. The earlier
`rcal_Project` spelling remains a compatibility alias. An explicitly
present but blank selection uses P6's successor-calendar default. Missing export
metadata cannot establish a nonzero lag's scheduling calendar and is unresolved.
Zero lag does not require a projection calendar setting, but `lag_hr_cnt` itself
must contain a valid number. Missing/truncated/blank/malformed lag cannot establish
zero. The separate legacy `lag` display column is unchanged.

Source/task/calendar/project lookups for this column use tuple identities with the
stable input-occurrence token, not filename/path/hash or concatenated display keys.
Duplicate endpoint identities are unresolved. Unrelated invalid calendar identities
do not remove table 06. For nonzero cross-project lag, both projects must have
unambiguous exported options resolving to the same lag operation; conflicting or
missing contexts remain blank. No scheduling-run context is guessed.

Signed negative allowances are retained rather than clamped to driving zero. Numeric
precision is retained so a one-minute gap cannot disappear through two-decimal day
rounding. `06.total_float` remains successor activity total float. The `lag` day
column uses predecessor hours-per-day independently of the scheduling calendar;
Tender now follows the same denomination.

### When free float is blank

- Missing/invalid required task, calendar, positive predecessor HPD, endpoint,
  numeric lag, or scheduling metadata; an unprojectable lag.
- A completed endpoint, LOE/WBS summary or unsupported activity/relationship type.
- A resource-dependent endpoint: TASK calendar alone does not establish how its
  resource-driven dates can move. Table 15's resource-calendar selection is a
  separate concern; its distribution corrections are documented above.
- Duplicate/ambiguous task or scheduling identities, mismatched project endpoints,
  contradictory effective task date ranges or incompatible actual/status inputs.
- An active successor, whose incoming relationship may be retained, overridden or
  out of sequence.
- An active predecessor on SS/SF. Original lag and actual start alone do not
  establish remaining relationship lag and P6's internal early-start constraint.

An active predecessor on FS/FF can use its remaining finish with an unstarted
successor. Raw `aref/arls` are not used as universal internal relationship dates:
Oracle defines them as adjusted external-relationship values with type-dependent
semantics. This parser is not a replacement P6 scheduling engine.

The earlier free-float correction changes only derived `06.free_float` values and their input guards.
Existing CSV column names/order, profile schema versions, other 06 values, native
activity floats and table 10/11 contents are retained. Subsequent table 15 changes
are documented in the separate section above.
LongestPathVisual is not changed: its intentional least-float selection consumes
the corrected column. This column does not independently certify P6 driving status.

## Validation

Regression tests cover the counterexamples above, independent minute-by-minute
forward feasibility/maximality checks, split/inherited/overnight calendars,
exception precedence, 24-hour work, malformed input, source separation, locale,
exact boundaries, no-work calendars, signed lag/float, all four relationship types
and lag calendars, nullable progressed cases, sub-day precision and Tender CSV
integration. Historical test counts from the earlier forward-gap implementation
are not evidence for this correction; current validation is recorded after running
all seven .NET builds, both test targets and `git diff --check`.

Validation for the earlier free-float correction on 2026-09-05 used the local .NET 8 SDK:

- All seven builds passed individually with `--no-restore`: Core, Windows, Web,
  Programme Review CLI, Tender Review CLI, Core.Tests and TenderReview.Surface.Tests.
- Full `dotnet test --no-restore --no-build`: Core **353/353**, surface **13/13**;
  no failures or skipped tests. The 22 direct relationship-engine tests include
  540 systematic and 300 seeded-random independent forward-oracle cases.
- `git diff --check` passed. No visual build/package or SharePoint upload was run.

Subsequent table 10/11/15 validation on 2026-09-05:

- All seven `.NET build --no-restore` targets passed individually: Core, Windows,
  Core.Tests, Web, Programme Review CLI, Tender Review CLI and TenderReview.Surface.Tests.
- Full `dotnet test --no-restore --no-build`: Core **411/411**, surface **13/13**;
  zero failed or skipped tests. This includes all existing free-float tests,
  11 new table 10/11 cases and resource-distribution/profile regression cases.
- `git diff --check` passed; new/untracked calculation and test files also have no
  trailing whitespace. The pre-existing Core CS1998 warning remains on recompilation.
- No visual, report, Tender-Review repository, profile schema version or SharePoint
  changes were made. Regenerate CSVs with the rebuilt parser to use the corrections.

Resource-curve validation on 2026-09-05 used `.\local-dotnet-sdk\dotnet.exe`:

| Target | `build --no-restore` | `test --no-restore --no-build` |
| --- | --- | --- |
| `XerToCsvConverter.Core/XerToCsvConverter.Core.csproj` | Passed | Not a test project |
| `XER to CSV.csproj` (Windows) | Passed | Not a test project |
| `XerToCsvConverter.Core.Tests/XerToCsvConverter.Core.Tests.csproj` | Passed | 508/508 passed |
| `XerToCsvConverter.Web/XerToCsvConverter.Web.csproj` | Passed | Not a test project |
| `XerToCsvConverter.ProgrammeReview.Cli/XerToCsvConverter.ProgrammeReview.Cli.csproj` | Passed | Not a test project |
| `XerToCsvConverter.TenderReview.Cli/XerToCsvConverter.TenderReview.Cli.csproj` | Passed | Not a test project |
| `XerToCsvConverter.TenderReview.Cli/Tests/XerToCsvConverter.TenderReview.Surface.Tests.csproj` | Passed | 13/13 passed |

There were zero skipped tests. The 97 additional curve cases cover named/manual
allocations, working-time boundaries, unsupported states, tiny-quantity rounding,
actual/remaining separation, source identity and profile-level atomic failures.
The first integration runs exposed test assumptions about existing Standard
`FileName` and numeric-vs-review-boolean serialization; the assertions were corrected
to the unchanged contracts, with no production schema changes. All final tests pass.
`git diff --check` passed. The pre-existing CS1998 warning appears on Core recompiles.
No native P6 export/UI comparison, SharePoint upload, visual/report build or edits
to the Tender-Review repository were performed.

Oracle references: [lag and progress scheduling settings](https://docs.oracle.com/cd/G18296_01/client_help/en_US/general_tab_-_schedule_options_dialog_box.htm),
[base-calendar exceptions and period factors](https://docs.oracle.com/cd/G48902_01/client_help/en_US/edit_a_project_calendar.htm),
[adjusted relationship fields](https://docs.oracle.com/cd/G48897_01/English/Integration_Documentation/p6_eppm_web_services_reference/42348.htm).
Exact parity for progressed/out-of-sequence schedules still requires golden P6
exports with their scheduling options and displayed relationship values.
The synthetic tests validate the documented predecessor-delay contract, not
undocumented internal relationship-date snapping by a particular P6 version.
