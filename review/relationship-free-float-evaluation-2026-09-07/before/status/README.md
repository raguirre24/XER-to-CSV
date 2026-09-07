# Relationship status evaluation

This standalone harness evaluates the current Core assembly through the real XER parser,
`AssessRelationships`, and `Create06XerPredecessor`. It does not change production code or existing tests.
Its calendar and base schedule follow `RelationshipAssessmentTests.Schedule`: Monday-Friday,
08:00-12:00 and 13:00-17:00, predecessor on 5 January 2026, successor on 6 January 2026,
data date 2 January 2026, and active actual starts on 1 January 2026.

From the repository root, run:

```powershell
dotnet build XerToCsvConverter.Core\XerToCsvConverter.Core.csproj --no-restore
dotnet run --project review\relationship-free-float-evaluation-2026-09-07\status\StatusEvaluation.csproj -- review\relationship-free-float-evaluation-2026-09-07\status
```

The harness references the built Core DLL directly, so its own build cannot rebuild production code.
The audit source is `Program.cs.txt`, explicitly included by the audit project, and all build
outputs live under the hidden `.build/` directory. This keeps audit C# files outside the root
desktop project's recursive source glob. The desktop project's evaluated Compile items were
checked after running the harness and contain no path under this evaluation directory.
`summary.json` records the evaluated DLL SHA-256. Run the Core build first when source changes.

## Executed coverage

- `status-mode-lag-matrix.csv`: 540 cases: four relationship types, three predecessor statuses,
  three successor statuses, five option states, and -8/0/+8 hours of lag. The five option states
  are Retained Logic, Progress Override, Actual Dates, conflicting Y/Y flags, and an absent
  SCHEDOPTIONS table.
- `activity-type-matrix.csv`: 504 cases: four relationship types, all nine status pairs,
  seven activity types, and each endpoint independently changed. Includes task dependent,
  resource dependent, start milestone, finish milestone, LOE, WBS summary, and an unknown type.
  Milestone remaining endpoints coincide with zero remaining duration. Active milestone cases
  deliberately probe unsupported or contradictory source-state combinations; acceptance by the
  engine does not establish that such a state can be scheduled in P6.
- `malformed-state-probes.csv`: 120 probes for status spelling/whitespace, blank or unknown status,
  actual dates contradicting status, absent/malformed/future actual starts, actual start later
  than remaining finish, negative/malformed remaining duration, and completed tasks missing
  or having malformed actual finish.

Every row checks agreement between assessment and exported `free_float`, and verifies that
non-calculated classifications have blank numeric output. The full matrix and activity-type
matrix also assert expected branch classification. Numeric matrix cases use independent simple
calendar arithmetic: FS = 0 hours, FF = 8, SS = 8, SF = 16, less the signed lag. The arithmetic
assertion permits one .NET tick for the documented start/finish boundary canonicalization.
`summary.json` preserves every exact whole-hour difference; CSV retains the raw decimal values.
Each such difference is also checked using an independent interval walk over the fixture calendar:
the returned movement must satisfy the lagged successor deadline, and one additional working tick
must violate it. The summary records both projected endpoints and both feasibility assertions.
Malformed probes labeled `Observation only` are exploratory findings, not correctness assertions.

## Findings

1. All four unstarted-to-unstarted relationship types calculate. With an active predecessor
   and an unstarted successor only FS/FF calculate; SS/SF are Unsupported. With an active
   successor, Retained Logic allows FS/FF only; progressed SS/SF are Unsupported. Progress
   Override returns Ignored only for zero-lag FS; the other progressed relationships are
   Unsupported. Progressed Actual Dates is Unsupported. Missing mode evidence returns
   MissingData and conflicting mode evidence returns InvalidData when the successor is active.
2. A completed successor or predecessor returns Historical with blank `free_float`; a
   completed successor takes precedence. This is a remaining-work metric, not historical
   endpoint slack. Contradictory completed dates do not change the Historical result.
3. There is a validation gap for an active FS/FF predecessor with an unstarted successor.
   Blank, malformed, after-data-date, or after-remaining-finish actual starts still produce
   numeric values. For example, a predecessor actual start on 10 January 2026 with remaining
   finish on 5 January 2026 returns FS = 0 days and FF = 1 day. The same actual-start problem
   on an active successor is rejected. Missing/malformed actual start does not alter endpoint
   subtraction by itself; an actual start after remaining finish is specifically contradictory
   activity data that still receives Calculated. The actual-start validation is inside the
   `if (activeSuccessor)` block in `XerTransformer.RelationshipAssessment.cs`.
4. Negative or malformed remaining duration is retained as evidence but does not gate
   numeric endpoint calculations. FS/FF active endpoints still calculate. Because the metric
   uses exported endpoint dates instead of duration, this does not prove the arithmetic is
   wrong; it does mean Calculated does not imply internally consistent activity data.
5. Task-dependent, resource-dependent, and milestone types enter the supported activity-type
   branch. LOE, WBS summary, and unknown unfinished activity types are Unsupported. Completed
   activity states become Historical before activity-type validation. These are engine-policy
   observations, not validation of resource-calendar or milestone semantics in a P6 reschedule.
6. Nineteen numeric matrix cases are one .NET tick below whole-hour arithmetic because of
   deliberate feasible-boundary selection. An example export is
   `1.9999999999965277777777777778` days instead of `2`. This is negligible scheduling error
   but a potential presentation/exact-comparison issue. An independent tick projection verifies
   every one of these returned movements is feasible and the next working tick is infeasible.

This harness verifies implementation behavior and elementary calendar arithmetic. It does not
constitute an Oracle P6 rescheduling oracle or validate every external-project/calendar combination.
