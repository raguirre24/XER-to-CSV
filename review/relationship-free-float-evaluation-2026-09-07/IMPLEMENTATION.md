# Relationship allowance implementation — 7 September 2026

Table `06_XER_PREDECESSOR` now describes whether each relationship has a finite remaining
predecessor delay allowance, why a number is absent, and which calculation basis produced it.
The active-predecessor actual-date validation gap is closed; additional supported cases and
explicit estimates preserve the distinction between a remaining-work allowance and historical
endpoint slack. Synthetic, production-test and export-integration evidence is recorded below.

## Contract and behavior

Immediately after `free_float`, Standard Enhanced, Programme Review and Tender Review export
`free_float_status`, `free_float_basis`, `free_float_reason`. Only `Finite` and `Estimated`
contain a numeric float. Negative/subday values and exact zero retain their meanings.
`NoFiniteBound`, `Historical`, `FixedEvent`, `RequiresContext`, `MissingData` and `InvalidData`
remain blank; no infinity or large sentinel is emitted.

- Retained Logic supports an unstarted SF predecessor's remaining start against an active
  successor's remaining finish, with reason `CalculatedRetainedStartToFinish`.
- An active SF predecessor start is `FixedEvent`. Active SS is `FixedEvent` only with explicit
  ActualStart lag basis under Retained Logic/Progress Override. Other progressed SS cases
  remain `RequiresContext`; fixed actual events are never moved.
- Every active endpoint requires a valid actual start and unambiguous owning-project Data
  Date. An actual start after an available remaining start/finish is invalid, on both sides.
- Numeric relationships involving either resource-dependent endpoint are `Estimated` using
  its TASK-calendar endpoint model. Resource assignment rollups and leveling are not solved.
- Closed valid predecessor suspension excludes its civil dates from movement availability,
  preserving the separately configured lag calendar. Open/invalid bounds remain unresolved.
- Direct WBS-summary relationships are ignored / `NoFiniteBound`; LOE requires network context.

Numeric bases are `TaskCalendarRemainingEndpoint`, `TaskCalendarSuspensionAdjusted`,
`TaskCalendarResourceEstimate`, and `TaskCalendarResourceEstimateSuspensionAdjusted`.
Nonnumeric bases are `HistoricalActualEvent`, `FixedActualStart`, `IgnoredRelationship`,
`UnresolvedContext`, and `None` for missing/invalid evidence.

The review schema versions are **Programme 4.0** and **Tender 2.0**. The optional relationship
audit is **1.1**, with 46 columns; it retains legacy `classification` and `reason_code` and adds
`free_float_status` / `free_float_basis`. Existing file counts, manifest columns and data-quality
companion 1.2 remain unchanged. Old exact-version/header consumers must migrate before refresh.
No external Power BI report, SharePoint loader, LongestPathVisual code or deployment is changed.

## Executed synthetic evaluation

The standalone harness executed **1,216 assertions-bearing scenarios**, all passing:

| Surface | Cases |
| --- | ---: |
| 4 relationship types × 9 status pairs × 5 scheduling-option states × 3 signed lags | 540 |
| Activity types at each endpoint across all relationship/status combinations | 504 |
| Malformed, missing, contradictory and normalized task-state evidence | 120 |
| Explicit SS/SF fixed ActualStart cases including absent/malformed lag | 40 |
| Suspension-adjusted finite/resource-estimate cases with independent hour expectations | 12 |

Each case asserts expected classification, public status, basis, reason, numeric availability
and exact agreement with table 06's float metadata. The matrix additionally checks independent
simple-calendar arithmetic. All 21 one-tick boundary differences pass independent forward
feasibility and next-working-tick maximality checks; raw decimal values remain in the CSVs.

The prior contradictory-date reproduction now returns `InvalidData` /
`ActualStartAfterRemainingEndpoint` with blank float. Negative/malformed remaining duration
alone still permits a supported endpoint calculation because duration is not a solver input;
these cases now explicitly assert that documented policy rather than observing it informally.

Results, exact Core DLL SHA-256 and commands are in [status/README.md](status/README.md) and
[status/summary.json](status/summary.json). Source is `Program.cs.txt`, explicitly compiled into
hidden `.build` outputs so the desktop application's recursive C# glob cannot include audit code.
Pre-change evidence is preserved under [before/](before/).

The pre-change Core DLL is retained locally at
`artifacts/relationship-coverage-before/XerToCsvConverter.Core.dll` (SHA-256
`7D7BFDD759CC13482053E88DB003D2390768C4A67789FDD3CDF3E4C0D1B45798`). That directory
also retains `pre-existing-work.patch`, a startup working-tree snapshot captured after parallel
test edits had begun, plus original relationship rows and unrelated-table hashes. The patch is
not a pure pre-implementation/release snapshot. These artifacts support scoped comparison while preserving unrelated resource work;
they are not a published release package. The original XER sample remains unchanged in `XER Sample/`.

## Real sample: every row compared before and after

The unchanged August XER contains **16,761 relationships**. All source row ordinals,
relationship/endpoint identities, float hours and float days matched the baseline. Every new
status/basis/reason field agreed with the shared assessment and actual table 06 export.

| New allowance status | Rows |
| --- | ---: |
| Finite | 2,401 |
| Historical | 14,145 |
| RequiresContext | 213 |
| FixedEvent | 1 |
| NoFiniteBound | 1 |

There are no numeric resource estimates in this sample. Of the finite rows, 945 are positive,
1,456 are zero and none are negative. All **2,401 existing numeric values are unchanged**.

Exactly two prior reason/classification outcomes change, both retaining blank float:

| Source row / relationship ID | Before | After | Explanation |
| --- | --- | --- | --- |
| 4081 / 3086749 | UnsupportedProgressedStartEndpoint | FixedActualPredecessorStart / FixedEvent | Active predecessor SF uses a fixed actual start. |
| 7961 / 3091923 | UnsupportedActivityType | IgnoredSummaryRelationship / NoFiniteBound | Direct WBS-summary relationship is ignored. |

All other 16,759 rows retain their existing assessment fields, values and reasons; they acquire
the new metadata. There are **zero unexplained transitions**. The comparison retains every row,
not just a count or changed subset, in [real-sample-before-after.csv](real-sample-before-after.csv).
Machine-readable counts and input/before/after DLL hashes are in
[implementation-comparison-summary.json](implementation-comparison-summary.json).

Reproduce from the repository root after building current Core:

```powershell
dotnet run --project review\relationship-free-float-evaluation-2026-09-07\status\StatusEvaluation.csproj -- review\relationship-free-float-evaluation-2026-09-07\status
& .\review\relationship-free-float-evaluation-2026-09-07\Evaluate-RealSample.ps1
& .\review\relationship-free-float-evaluation-2026-09-07\Compare-Implementation.ps1
```

These checks establish implementation behavior, finite-bound arithmetic and source/export
reconciliation. They do not certify native P6 rescheduling parity or live Power BI refresh.

## Final production validation

Final Core DLL SHA-256 is
`411A28B16040D83FE8786EC395008C58875AD02F1FACF26856950EBE6E191444`, matching the standalone
harness, real-sample evaluation and export integration runs.

| Check | Result |
| --- | --- |
| Core tests | 1,268 passed; 0 failed; 0 skipped |
| Relationship Audit CLI tests | 36 passed; 0 failed; 2 platform symlink cases skipped |
| Tender Review surface tests | 51 passed; 0 failed; 0 skipped |
| Windows desktop, Web, Programme CLI, Tender CLI builds | All passed; 0 warnings and 0 errors |
| Standalone scenario harness | 1,216 passed; 0 assertion failures |
| Desktop recursive Compile-item isolation | 6 production source items; 0 evaluation files |

The production test artifacts are `coverage-core.trx`, `coverage-audit-cli.trx` and
`coverage-tender-surface.trx` under `artifacts/relationship-coverage-validation/`, alongside
`build-summary.json` and the individual build logs. The relevant project commands are:

```powershell
dotnet test XerToCsvConverter.Core.Tests\XerToCsvConverter.Core.Tests.csproj --no-restore
dotnet test XerToCsvConverter.RelationshipAudit.Cli\Tests\XerToCsvConverter.RelationshipAudit.Cli.Tests.csproj --no-restore
dotnet test XerToCsvConverter.TenderReview.Cli\Tests\XerToCsvConverter.TenderReview.Surface.Tests.csproj --no-restore
dotnet build "XER to CSV.csproj" --no-restore
dotnet build XerToCsvConverter.Web\XerToCsvConverter.Web.csproj --no-restore
dotnet build XerToCsvConverter.ProgrammeReview.Cli\XerToCsvConverter.ProgrammeReview.Cli.csproj --no-restore
dotnet build XerToCsvConverter.TenderReview.Cli\XerToCsvConverter.TenderReview.Cli.csproj --no-restore
dotnet msbuild "XER to CSV.csproj" -getItem:Compile
git diff --check
```

Export integration independently reconciled all 16,761 sample rows against the saved old DLL,
verified all 13 other numbered Standard outputs and all pre-existing table 06 fields except
the already separately compared `free_float` were unchanged, and confirmed file/stream byte
parity. Programme and Tender Retained Logic fixtures each produced 12 files with disk/memory
byte parity, expected float `0`, and matching status/basis/reason. Both original source hashes
remained unchanged. Exact integration commands and output summaries are retained in
`artifacts/relationship-coverage-validation/integration/final-commands.md`.

These local checks do not perform release, deployment, loader migration or live P6/Power BI verification.
