---
name: p6-numbered-xer-reporting
description: Interpret and use the numbered XER-to-CSV Power BI tables in reports, including legacy Enhanced, Programme Review, and Tender Review exports. Use for table grain, joins, snapshot identity, calendar-aware float, resource spreads, and report reconciliation; not as a general P6 scheduling engine.
---

# Numbered XER tables for reporting

Help an agent build, review, or explain reports using this parser's numbered CSVs without confusing raw P6 fields, calculated columns, or export profiles. This portable reference describes the corrected working-tree implementation on 2026-09-06; it is not an Oracle schema specification, a release identifier, or proof of native P6 scheduling parity.

## Establish the actual contract

Inspect the supplied headers, manifest, parser/schema version, and source scope before choosing joins or calculations. Do not assume that identical table numbers imply identical schemas. If files are unavailable, state the profile/version assumption and request only information needed for the task.

- Read [profile-contracts.md](references/profile-contracts.md) for profile detection, exact review headers, namespaces, snapshot dates, and manifest checks.
- Read [table-dictionary.md](references/table-dictionary.md) for the relevant numbered tables, their grain, derived values, and join/aggregation constraints.
- For calendar, relationship float, longest-path/trace, resource spread, or reconciliation work, also read [calculation-rules.md](references/calculation-rules.md).
- For ingestion/export guarantees, Web handoff, older-build differences and genuine unsupported cases, consult [known-limitations.md](references/known-limitations.md).

## Essential decisions

1. Keep snapshot/stage identity in every relationship. Native IDs, activity codes, filenames, paths and hashes are not globally unique. Standard and Tender accept repeated ordered inputs; internal occurrence tokens are distinct from public reporting keys and filename provenance. Programme preserves its stricter snapshot uniqueness contract.
2. Do not join a history/stage fact to a dimension on native ID alone or deduplicate repeated assignments to manufacture uniqueness. Establish the dimension grain first.
3. Distinguish `01` activity free float from `06.free_float`, which is a calculated relationship-specific predecessor delay allowance. Blank means unresolved/unsupported, not zero or driving. Preserve signed values and the user's intentional least-float path policy.
4. Working shifts and dated exceptions determine available hours. Hours-per-day is a conversion factor, not a workweek. `11` contains rules and overrides, not one row for every calendar date.
5. `15.monthly_quantity` is distributed assignment units, not cost, resource availability, or necessarily actual timesheets. Aggregate only compatible units and selected snapshots. Actual and remaining portions are separate.
6. Treat all CSV keys as text, even when native IDs look numeric. Preserve null/blank separately from numeric zero. Check the profile's boolean, date, decimal, and case conventions before transformations.
7. Require a complete current export set. Valid empty Standard tables are header-only; an explicitly requested unavailable/failed Standard table is an error. Invalid actual-date periods are instead reportable warnings: read `XER_DATA_QUALITY.csv` and reconcile allocated plus unallocated actuals. The companion accompanies table 15 even when empty. Fixed review profiles retain their numbered-table schemas and optional-table rules. Never treat an omitted or stale file as evidence of zero data.

Explain which conclusions come from the parser contract, observed source data, and report-specific business policy. Do not silently repair upstream defects in DAX/Power Query or change the parser, visual, report, or deployment unless the user's task authorizes that change.

## Refreshing this reference

When parser source is available, start with `XerToCsvConverter.Core/CoreLogic.cs`, `XerSourceIdentity.cs`, `XerSourceSchema.cs`, `StandardExportSchema.cs`, `StandardExportPublication.cs`, `XerTextEncoding.cs`, the `ProgrammeReview` and `TenderReview` contract/transformer/metadata files, `Calendars/`, `Resources/RemainingResourceProfile.cs`, and the `XerTransformer.*.cs` partials. `CALENDAR_CALCULATIONS.md` explains calculation intent; tests provide examples, not universal P6 parity. Runtime CSV headers and manifests remain necessary when the installed parser differs from this reference.
