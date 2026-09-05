# Calendar, float, spread and reconciliation rules

These describe this parser's supported calculation contract, not all behavior of every P6 release. The shared Core supplies Windows, Web and both review CLI services. Browser success alone does not prove that a downloaded ZIP is intact or that a live report uses the same parser version.

## Working calendars

Availability comes from complete seven-day rules, split shifts, resolved base-calendar exceptions and local overrides, using source-qualified identities. Overlapping intervals are counted once; breaks do not count. Overnight shifts are split at civil midnight. A dated exception replaces the entire date, including incoming overnight work, rather than adding to it. Period factors do not manufacture missing shifts.

P6 structured calendar text can use unnamed container records and ISO control separators such as DEL/U+007F between records. These are valid syntax, not missing workweek data. The parser consumes structural separators without changing table 10's raw `clndr_data`; malformed clocks, incomplete workweeks and invalid inheritance still fail. This compatibility correction restores shared 06/11/15 calculations without changing the float formula or HPD factors.

Invalid workweeks must not silently become eight-hour or continuous calendars. Missing positive HPD prevents a trustworthy hours-to-days conversion even if shifts are valid. Conversely, invalid HPD does not by itself erase valid shift availability. Keep elapsed days, weekday counts, working hours and HPD-equivalent working days distinct.

Shared lookups are qualified by immutable source occurrence, not public filename or public key concatenation. A calendar/project conversion requires one unambiguous matching row; duplicate IDs do not select the first or last conversion by row order. Unresolved conversion/data-date metadata stays blank where the table allows unknown values. Detailed calendar generation and positive resource allocation reject unusable required definitions instead of emitting partial results.

Table 11 contains daily totals/rules, not enough information for exact intraday lag arithmetic. Both review 10 tables contain only key/name. When a report needs exact calendar calculations beyond the supplied results, obtain raw calendars/options/timestamps or an explicitly expanded export; do not reconstruct an eight-hour calendar from names.

## Relationship `06.free_float`

Interpret as the maximum signed predecessor working-time displacement that this individual relationship permits while its successor endpoint stays fixed, divided by positive predecessor HPD. It is not imported `TASK.free_float_hr_cnt`, successor-calendar slack, elapsed date difference, or a guaranteed match to P6's displayed relationship float on mixed calendars.

| Type | Predecessor endpoint | Successor endpoint |
| --- | --- | --- |
| FS | Finish | Start |
| SS | Start | Start |
| FF | Finish | Finish |
| SF | Start | Finish |

The engine prefers remaining `restart_date`/`reend_date`. Unstarted endpoints can fall back only when the remaining field is absent and a valid early field exists. Malformed nonblank remaining dates cannot use that fallback. Active predecessor finishes require valid explicit remaining finish. The displayed 01/06 Start/Finish policy retains actual dates for started/completed events and remaining forecasts for unfinished events; a display fallback does not broaden the solver's supported progressed cases.

For each candidate predecessor movement, lag is applied on the configured lag calendar and tested against the successor event. The engine solves for the latest feasible movement including nonworking plateaus and signed-lag boundary behavior, then checks maximality at the next working tick. A single subtraction of lag from a working-time gap is not equivalent under mixed calendars. Moved starts and finishes have different working-boundary conventions; zero displacement preserves the original instant.

Example: predecessor works Monday-Friday 08-12/13-17, FS predecessor finish Monday 17:00, successor start Tuesday 17:00, lag +8 elapsed hours. The maximum predecessor delay is one working hour: `0.125` days for HPD=8, not one whole working day. This example requires full timestamps and the elapsed lag setting, not only the date-only review fields.

Lag option tokens include predecessor, successor, continuous 24-hour and project-default calendar; the implementation uses `rcal_Predecessor`, `rcal_Successor`, `rcal_24Hour`, `rcal_ProjDefault` and a compatibility `rcal_Project` alias. An exported explicitly blank selection uses the successor default; missing scheduling metadata does not establish a nonzero lag calendar. A valid zero numeric lag needs no lag-calendar option. Blank/malformed lag is not zero. Cross-project nonzero lag requires unambiguous compatible exported scheduling context from both projects.

Blank free float includes unsupported/missing/ambiguous task/calendar/project identities or dates, invalid HPD/lag, completed endpoints, LOE/WBS summary/resource-dependent endpoints, an active successor, and active predecessor SS/SF relationships. Active predecessor FS/FF to an unstarted successor can use its remaining finish. Blank is a diagnostic state: neither coalesce to zero nor include it as the minimum float edge.

Negative allowances are retained and subday precision matters. `06.total_float` is successor activity float, while `06.lag` and `06.free_float` use predecessor day denomination. Do not combine these fields as if all used one time basis. Candidate edges with different predecessor HPD factors also have different day units: ranking those exported day values is a business policy, not necessarily ranking the same physical delay in hours. Raw `aref/arls` are not universal remaining relationship dates. A least-float chain rule can intentionally select a positive minimum edge; do not change the user's policy to <=0 without authorization. Nor does <=0 alone certify P6 native driving status for unsupported schedule states.

## Table 15 allocation

Calculate per assignment, source and project before Tender aggregation. Use TASK calendar for task-dependent work and assigned RSRC calendar for resource-dependent work. Ambiguous/missing required identities fail the requested table, rather than selecting the last matching ID. Exactly one same-source native task ID must exist before checking its project: filtering duplicate task IDs by project does not make the identity valid. Parsed-store copies retain source tokens, so calendars, project dates and curve definitions cannot leak across repeated filenames.

- Actual quantity is regular plus overtime assignment units. Spread uniformly by working time between assignment actual start and explicit actual finish; only an absent finish for active work can use its project's Data Date. If the entire valid actual period has zero scheduled working time, use elapsed-time monthly shares (`Actual Elapsed Time`), retaining zero working hours. Equal actual timestamps put the rounded quantity in that instant's month (`Actual Recorded Date`), with zero duration diagnostics. These preserve recorded units outside calendar capacity; they do not reconstruct timesheets. Actual curves are not applied. Missing/malformed required actual dates, a missing/malformed fallback Data Date, or finish/Data Date before actual start yield an unallocated-actual warning in `XER_DATA_QUALITY.csv`, not a guessed month or whole-table rejection. Valid remaining units on that same assignment still distribute independently. A valid explicit actual finish is not silently replaced by the Data Date.
- Remaining quantity is assignment `remain_qty` across assignment `restart_date` to `reend_date`, using profile precedence below. Zero optional quantities retain zero convention; malformed, negative, nonfinite or overflowing values are errors, not zero.
- Periods are half-open `[start, finish)` with contiguous midnight month boundaries; the recorded-actual instant case is a dated observation, not a positive-duration interval. Positive remaining units need finish after start and nonzero working availability. Actual fallback never bypasses malformed calendars, invalid quantities or unknown identities. Reversed actual dates are preserved in the companion, not reversed or repaired by the allocator.
- Monthly quantity is a difference of cumulative allocations rounded to four decimals. Monthly totals conserve the source assignment quantity rounded to four decimals, including tiny allocations. Do not redistribute a rounding remainder into a nonworking/zero-allocation month.

### Non-blocking actual-date warnings

This is a scoped source-quality policy, not a general catch-and-ignore rule. Only invalid actual-period dates are recoverable here. Malformed/negative/nonfinite quantities, ambiguous required identities, invalid required calendars, unsupported remaining curves and invalid remaining periods still fail their existing validations. Never silently publish a partially calculated assignment portion after an unexpected exception.

For affected assignments, no actual monthly row is fabricated. The companion preserves original assignment date/quantity strings and raw project Data Date, known rounded `unallocated_actual_quantity`, source-qualified keys, assignment ID, and its per-source TASKRSRC row ordinal. `ACTUAL_PERIOD_INVALID` denotes required missing/malformed dates; `ACTUAL_FINISH_BEFORE_START` denotes reversed endpoints, including a fallback Data Date before actual start. Keep raw diagnostic dates as text so malformed source values remain visible rather than breaking report refresh. Standard table 13 remains unchanged; the companion also supplies this provenance to review profiles that do not export 13.

Reconcile **distributed actual quantity plus unallocated actual quantity equals source actual quantity**, using individually rounded assignment contributions at four decimals and compatible units/source scope. Remaining quantities still reconcile entirely to monthly remaining rows. Do not double-count the companion as distributed monthly work or force it into a blank/fake month in table 15. Header-only companion output means zero detected recoverable actual-date issues, not a complete scheduling-quality certification. UI/CLI completion with warnings is a successful publication; CLI exit code remains zero.

### Remaining profile precedence

1. Nonblank `TASKRSRC.remain_crv` is authoritative. Supported format is `quantity:working-hours;...`, for example `80:24;20:24`. Bands advance through calendar working time, not elapsed time. Zero-unit bands retain gaps; trailing semicolon is accepted. Quantities must reconcile to remaining units at four-decimal precision; positive whole-tick durations must sum exactly to remaining working duration. Malformed data is not stretched or replaced by a named curve.
2. Otherwise `curv_id` must resolve one same-source `RSRCCURVDATA` definition. Required `pct_usage_0`..`pct_usage_20` are finite percentages in 0..100, sum within 0.001 of 100, with index0 zero in the supported contract. Indices1..20 allocate units over successive 5% bands of working duration, not cumulative percentages or spline heights. Normalize only accepted tiny serialization differences.
3. With neither remaining profile nor assigned curve, spread uniformly by working time.

Named curves require fixed duration types `DT_FixedDrtn` or `DT_FixedDUR2`. Nonlinear named curves support unstarted activities/assignments. Progressed nonlinear work needs an explicit remaining profile when its remaining period spans monthly buckets. If the entire valid remaining interval fits one calendar month (including an exclusive finish at next month's midnight), every remaining unit necessarily belongs to that month, so its monthly total is exact without knowing curve phase. The parser validates the curve and calendar but does not infer an intramonth shape in that case. A linear named curve is phase-independent and can support active work across months. Manual curve ID9 without `remain_crv`, nonzero index0 and opaque `RSRCCURV.curv_data`-only definitions are unsupported. Do not infer curve phase from activity percent complete. `target_crv` does not substitute for remaining units; `actual_crv` does not change the current actual spread.

Invalid assigned remaining profiles cause an explicit whole-table/bundle failure before publication. Unused definitions and actual-only assignments need not validate a remaining curve. A curve changes monthly quantities, not underlying available hours or conversion factors. The fixed review schema does not expose which shape was used; obtain assignment/source data when auditing it.

## Programme history is a separate business calculation

The profile's finish/start variances and remaining working-day history use its Monday-Friday weekday convention, not task shifts, exceptions or HPD. Preserve that declared policy unless the user asks to change it. Do not advertise those numbers as calendar-exact P6 working days. `Driven_DataDate` is a profile history heuristic, not the relationship delay solver or a newly calculated native driving flag. Distinguish resolved baseline, adjusted baseline and previous observed activity values before interpreting a variance. Retained source selection occurs before shared calendar/resource calculations; rejected/discarded snapshots are not silently used as historical quantity contributors.

## Report validation checklist

Choose checks relevant to the requested report; do not alter a live model merely to run an audit.

- Confirm profile/version, expected files/header casing, bundle completion and per-source manifest reconciliation. Header-only optional review tables and legitimately empty requested Standard tables are valid. Explicitly requested missing/failed Standard outputs reject the export; missing expected files or stale files from another run are not evidence of zero records.
- Count dimension key duplicates and orphan foreign keys at qualified source/stage grain. Verify relationship endpoint direction, project context, WBS cycles and activity-code fan-out. Preserve multiple assignments.
- Separate snapshot/status date, P6 Data Date, monthly distribution date and activity dates. Never sum repeated snapshots to present one current resource total.
- Preserve decimal precision and nulls; test negative floats and quantities that round below one unit. Distinguish day-denominated floats across differing HPD.
- Reconstruct expected 15 allocations by source/assignment and actual/remaining from raw assignment quantities, periods, calendars and curves when available; add companion unallocated actuals for source-total reconciliation, then compare at the exported reporting grain (including Tender's aggregation). Standard and Programme preserve assignment contributions but do not export an assignment ID in 15: duplicate-looking rows cannot uniquely identify their originating assignment. Use aggregate totals or a multiset comparison of reconstructed contributions, not invented row-to-assignment identity. Without raw assignments, state that full source reconciliation is unavailable.
- Aggregate only compatible units; hours, pieces, tonnes and currency are not interchangeable. Repeated resource calendar hours across assignments are not resource capacity. Do not sum period-total diagnostics across months.
- For a native P6 parity claim, compare controlled P6 exports with known scheduler options, all four relationship types, mixed/split/overnight calendars, holidays, signed lag, progress states and resource-usage curves. Synthetic tests alone prove only the explicit implemented contract.

Primary context: [Oracle activity/units definitions](https://docs.oracle.com/cd/G18294_01/English/User_Guides/p6_eppm_data_dictionary/46503.htm), [resource curve bands](https://docs.oracle.com/cd/G18296_01/client_help/en_US/modify_resource_curves_dialog_box.htm), and [schedule options](https://docs.oracle.com/cd/G18296_01/client_help/en_US/general_tab_-_schedule_options_dialog_box.htm). Parser-specific limits above come from its implementation, not a claim that Oracle rejects every unsupported parser case.
