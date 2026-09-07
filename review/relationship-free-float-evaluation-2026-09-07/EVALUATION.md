# Evaluation of table 06 relationship free float

Evaluation date: 2026-09-07. Scope: the current working-tree Core engine and its table `06_XER_PREDECESSOR.free_float`, all four relationship types, all three endpoint statuses, progressed scheduling modes, signed lag, calendar arithmetic and a local draft XER. Production implementation and original XER files were not edited. Existing resource-forecast changes were preserved.

## Conclusion

The arithmetic is sound for the implemented **remaining endpoint allowance** model at the tested inputs. This is a useful answer to how much an individual predecessor endpoint can move before violating its relationship with a fixed successor endpoint. It is not a universally available or verified P6 rescheduling result for every activity type and status.

Three qualifications matter:

1. Completed events cannot be delayed as remaining work; their results are correctly historical/blank. Started SS/SF cases and several progressed scheduling modes are deliberately unsupported/blank, rather than zero.
2. Resource-dependent activities can receive numeric results using the activity's TASK calendar. That is a task-calendar sensitivity measure; assigned-resource calendars and availability can produce a different actual P6 scheduling response.
3. Actual-start validation is asymmetric. FS/FF from an active predecessor to an unstarted successor can still calculate with missing, malformed or contradictory actual-start evidence. This is a validation gap, not an identified error in the inverse-calendar arithmetic. No affected numeric rows were found in the local draft sample.

Native P6 parity remains unverified: this evaluation did not alter durations/dates and reschedule controlled projects in P6.

## What the number measures

For each individual relationship, the engine selects predecessor endpoint P and successor endpoint S. It finds the largest signed displacement of P, in predecessor working hours, for which applying the signed relationship lag on its configured lag calendar still lands on or before S. It divides that displacement by the predecessor calendar's positive `day_hr_cnt`.

Conceptually:

`free_float = max allowable predecessor working-hour displacement / predecessor hours per day`

The successor endpoint is held fixed during this calculation. The engine does not reschedule a whole activity or its network.

| Relationship | Movable predecessor event | Fixed successor event |
|---|---|---|
| FS | Remaining finish | Remaining start/restart |
| SS | Remaining start | Remaining start |
| FF | Remaining finish | Remaining finish |
| SF | Remaining start | Remaining finish |

These mappings agree with [Oracle's relationship definitions](https://docs.oracle.com/cd/F51301_01/p6help/en/6616.htm). FF and SF protect successor finish; they do not necessarily prevent its start moving.

- Positive: that amount of predecessor working-time displacement is available to this individual edge.
- Zero: no positive working-time displacement is available under this model.
- Negative: the edge is already infeasible against the fixed successor event; advancement is required under the model.
- Blank: no numeric remaining allowance was established. It does not mean zero, infinite float, or no remaining scheduling influence.

This differs from raw `TASK.free_float_hr_cnt`, an activity-level field. Oracle's `PredFreeFloat` relationship display property also refers to predecessor **activity** free float, and Oracle exposes a separate relationship `Driving` property. Neither should be substituted as a universal expected value for this custom per-edge column. See [Oracle relationship fields](https://docs.oracle.com/cd/G48897_01/English/Integration_Documentation/p6_eppm_web_services_reference/42348.htm).

Source: `XerToCsvConverter.Core/XerTransformer.RelationshipAssessment.cs:204` selects endpoints; `:255` calls the inverse solver; `:268` converts to days. `Calendars/RelationshipFreeFloatCalculator.cs:14` implements displacement and `:49` verifies feasibility and one-tick maximality.

## Complete relationship/status matrix

The following table assumes valid supporting evidence and Retained Logic where the successor is active. C = calculated signed allowance; U = unsupported/blank; H = historical/blank. Task/calendar limitations below can turn a C case into an unavailable result.

| Predecessor status | Successor status | FS | SS | FF | SF |
|---|---|---|---|---|---|
| Not started | Not started | C | C | C | C |
| Not started | Active | C | U | C | U |
| Not started | Complete | H | H | H | H |
| Active | Not started | C | U | C | U |
| Active | Active | C | U | C | U |
| Active | Complete | H | H | H | H |
| Complete | Not started | H | H | H | H |
| Complete | Active | H | H | H | H |
| Complete | Complete | H | H | H | H |

For unstarted activities, endpoints prefer `restart_date` and `reend_date`. Empty/missing remaining dates may fall back to valid early dates. A malformed nonblank remaining date cannot use that fallback. Active finishes and retained-logic successor restarts require explicit remaining dates.

For FS into an active successor, the protected event is its **remaining restart**, not its fixed actual start. FF protects its remaining finish. Active predecessor SS/SF has an actual start that cannot be moved as future work. Progressed SS/SF handling also depends on the remaining lag and P6's SS basis option; the current solver does not reconstruct that state.

If either endpoint is complete, the assessment is historical. A completed predecessor may still impose a fixed release date or unexpired lag; its blank movement allowance does not show that the relationship has no effect.

## Scheduling modes and lag

| Context | Actual current outcome |
|---|---|
| Successor not started | Progress mode is not required for the supported endpoint cases. Nonzero lag still requires a resolvable lag-calendar setting. |
| Active successor, Retained Logic | FS/FF can calculate against remaining restart/finish. SS/SF unsupported. |
| Active successor, Progress Override | Zero-lag FS is ignored when the actual start is valid and on/before Data Date. Other types and nonzero-lag FS are unsupported, not assumed to be ignored. |
| Active successor, Actual Dates | Unsupported by the remaining-displacement model. |
| Active successor, missing scheduling flags | MissingData, with `UnresolvedProgressMode`. |
| Active successor, conflicting Y/Y flags | InvalidData, with `UnresolvedProgressMode`. |

The tests exercise lag of -8, 0 and +8 hours across every type/status/mode combination. Other tests cover fractional lag, negative and positive float, nonworking deadlines and calendar boundaries.

Lag can use predecessor, successor, 24-hour or project-default calendars. This is independent of the calendar used to express the answer. An explicitly exported blank lag-calendar setting defaults to successor; absent headers/omitted values do not establish that default. Zero lag does not need a lag calendar. These distinctions align with the settings documented by [Oracle schedule options](https://docs.oracle.com/cd/G48902_01/client_help/en_US/general_tab_-_schedule_options_dialog_box.htm).

Mixed-calendar subtraction cannot generally be reduced to `working-time gap - lag`. For example: a weekday predecessor with 08:00-12:00/13:00-17:00 shifts finishes Monday 17:00; successor deadline is Tuesday 17:00; lag is 8 elapsed hours on a 24-hour calendar. The latest predecessor finish is Tuesday 09:00. Allowance is **1 predecessor working hour / 8 = 0.125 day**.

The solver handles lag plateaus over nonworking gaps and canonicalizes moved starts/finishes differently. It works at 100ns .NET tick resolution. Some exact boundary answers are one tick below an intuitive whole-hour value, and the exported G29 decimal preserves that difference. This is intentional maximal-feasible boundary handling, not a meaningful operational day difference. Display rounding must not silently redefine the underlying value or blank/zero distinction.

## Activity types and additional boundaries

- `TT_Task`: uses the activity calendar; consistent with the selected endpoint-displacement model.
- `TT_Rsrc`: accepted but uses the TASK calendar even when assigned resources have different calendars. The existing `Resource_dependent_relationship_uses_task_calendar_not_assigned_resource_calendar` test explicitly confirms this. Oracle schedules resource-dependent dates using assigned-resource calendars and availability. Therefore the broad claim of actual schedulable delay is not established for these numeric cases. See [Oracle resource-dependent activity](https://docs.oracle.com/cd/G48902_01/client_help/en_US/resource_dependent_activity.htm).
- `TT_Mile` and `TT_FinMile`: accepted with the same type/status endpoint policy. Synthetic activity-type probes use coincident milestone start/finish timestamps.
- `TT_LOE`: unsupported as an independently movable task-calendar endpoint. Its duration follows linked activities. See [Oracle level of effort](https://docs.oracle.com/cd/G48902_01/client_help/en_US/level_of_effort_activity.htm).
- `TT_WBS`: unsupported. Oracle ignores direct WBS-summary relationships during scheduling/leveling. See [Oracle WBS summary](https://docs.oracle.com/cd/G48902_01/client_help/en_US/wbs_summary_activity.htm).
- Missing/ambiguous identities, unknown statuses, contradictory unfinished actual-finish dates, reversed remaining periods, malformed selected dates/lag, invalid calendars and nonpositive/missing hours per day prevent a trustworthy calculation.
- Cross-project progressed relationships require governing scheduling context and remain unsupported. Unstarted cross-project links require resolvable endpoint/lag context; explicitly ignored external links remain ignored.
- Movement intersecting predecessor suspension or unresolved suspension bounds remains unsupported. The engine does not run a suspension-aware project reschedule.
- Other successors, constraints, expected finishes, resource availability and network effects can further restrict actual activity movement. An individual edge's value does not certify total activity float or native P6 driving status.

## Reproduced validation gap

Actual starts are validated inside the `activeSuccessor` branch (`XerTransformer.RelationshipAssessment.cs:165`, `:183-191`). An active predecessor with an unstarted successor skips this validation. The earlier unfinished-state guard checks actual finish and not-started actual start, but does not require a coherent actual start for every active predecessor (`:147-157`).

Synthetic setup: eight-hour weekday calendar, predecessor remaining interval 2026-01-05 08:00-17:00, successor remaining interval 2026-01-06 08:00-17:00, zero lag, Data Date 2026-01-02 17:00. With an active predecessor and unstarted successor:

- FS exports 0 days and FF exports 1 day even if predecessor actual start is blank or malformed.
- Those numeric values are also emitted with an actual start of 2026-01-10 08:00, after both Data Date and its remaining finish.

The endpoint arithmetic is unchanged because actual start is not used in the finish-only equation. The issue is admitting an internally inconsistent activity state into a result labelled Calculated. Missing history may be irrelevant to a narrowly defined finish sensitivity, but the contradictory chronology should not be described as verified schedulable remaining delay.

Recommended follow-up: define and apply active-state evidence requirements consistently before emitting numeric allowances, while retaining separate classifications for missing, invalid and unsupported states. Remaining duration is also not an arithmetic input; malformed/negative duration probes are observations of that boundary, not independently demonstrated errors in the endpoint calculation. Historical classifications precede actual-date validation and therefore are not certificates that completed source records are valid.

## Local draft XER results

Input: `XER Sample/2608-EBA_PAA_8.0 (draft).xer`.

| Type | Relationships | Calculated | Historical | Unsupported |
|---|---:|---:|---:|---:|
| FS | 13,379 | 2,064 | 11,314 | 1 |
| SS | 1,193 | 74 | 1,092 | 27 |
| FF | 2,177 | 263 | 1,728 | 186 |
| SF | 12 | 0 | 11 | 1 |
| Total | **16,761** | **2,401** | **14,145** | **215** |

Of the numeric results, 945 are positive and 1,456 are zero; none are negative or tiny nonzero boundary values. Sixty are retained-logic active-successor calculations. The 215 unsupported rows comprise 198 LOE relationships, one WBS-summary relationship and 16 progressed SS/SF cases. There are no Ignored, MissingData or InvalidData classifications in this sample.

No numeric result involves a resource-dependent endpoint, and none has the active-evidence issue described above. The sample's 12 SF rows do not include a calculable remaining case; SF arithmetic is exercised synthetically.

Every one of the 16,761 audit results matched table 06, including row identity/order, exact float string and blanks. Every calculated days value matched hours divided by predecessor hours per day. This demonstrates export consistency; because the exporter and audit share the evaluator, it is not independent proof of the formula.

Input SHA-256 before and after evaluation:
`BCD04882C15C96ED94A18F670F6F5F9AD26493993BA5ADD44250A48958E487B7`.

Evaluated Core SHA-256:
`7D7BFDD759CC13482053E88DB003D2390768C4A67789FDD3CDF3E4C0D1B45798`.

## Validation and reproducibility

The current-source Core suite passed **1,168 tests, zero failures/skips**. It includes 413 dedicated relationship tests: 257 assessment tests, 130 free-float integration tests, 22 solver tests and four small-calendar enumeration tests.

The separate audit CLI suite passed **28 tests**, with **two skipped** because this host does not permit unprivileged symbolic-link creation. Its first parallel build invocation stalled; the completed result used a serial build with build servers disabled. No skipped test concerns relationship arithmetic.

The standalone harness completed **1,164 probes with zero assertion failures**: 540 status/mode/lag combinations, 504 activity-type cases and 120 malformed-state observations. Nineteen numeric boundary cases differed from simple whole-hour expectations by one time tick; an independent interval walk verified every returned displacement feasible and the next working tick infeasible. Accepting current classification/export behavior in exploratory probes is not a statement that every admitted source state is valid.

Independent numerical reference checks inside that suite cover 540 deterministic minute-resolution calendar combinations, 300 seeded random examples and 224 small-calendar configurations with independently enumerated candidate displacements. They supplement hand-calculated boundary, lag, overnight, exception, negative-float and precision tests. They are finite test coverage, not proof for every possible calendar or native P6 parity.

Artifacts:

- `test-results/core-evaluation.trx`: complete executed Core test results.
- `test-results/relationship-cli-evaluation.trx`: audit CLI test results, including the two platform skips.
- `status/status-mode-lag-matrix.csv`: all 540 relationship/status/mode/signed-lag cases.
- `status/activity-type-matrix.csv`: 504 type/status/endpoint activity-type cases.
- `status/malformed-state-probes.csv`: 120 state/evidence probes; some deliberately record current acceptance rather than assert that it is correct.
- `status/summary.json`: executed harness counts, assertions and assembly identity.
- `real-sample-relationships.csv`: one compact assessment row per real relationship.
- `real-sample-matrix.csv`: grouped real type/status/reason counts.
- `real-sample-summary.json`: row counts, classifications, numeric distribution, evidence checks and hashes.
- `Evaluate-RealSample.ps1` and `status/Program.cs.txt`: reproducible evaluation sources. The latter is explicitly compiled by its standalone project so the desktop application's recursive source glob does not include it.

Commands from the repository root:

```powershell
.\local-dotnet-sdk\dotnet.exe test XerToCsvConverter.Core.Tests\XerToCsvConverter.Core.Tests.csproj --no-restore --logger 'trx;LogFileName=core-evaluation.trx' --results-directory review\relationship-free-float-evaluation-2026-09-07\test-results --verbosity minimal
.\local-dotnet-sdk\dotnet.exe test XerToCsvConverter.RelationshipAudit.Cli\Tests\XerToCsvConverter.RelationshipAudit.Cli.Tests.csproj --no-restore --disable-build-servers -m:1 --logger 'trx;LogFileName=relationship-cli-evaluation.trx' --results-directory review\relationship-free-float-evaluation-2026-09-07\test-results --verbosity minimal
.\local-dotnet-sdk\dotnet.exe run --project review\relationship-free-float-evaluation-2026-09-07\status\StatusEvaluation.csproj -- review\relationship-free-float-evaluation-2026-09-07\status
& ./review/relationship-free-float-evaluation-2026-09-07/Evaluate-RealSample.ps1
git diff --check
```

The standalone status harness references the compiled Core DLL; build/run the current-source Core tests first. The local sample script reads the original and writes only the evaluation outputs. No application packaging, publishing or live Power BI/P6 validation was performed.
