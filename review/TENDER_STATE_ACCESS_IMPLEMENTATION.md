# Tender manual State and independent CSV access

Implemented 2026-09-10 following approval of [the revised plan](TENDER_CSV_STATE_AND_ACCESS_PLAN.md). Parser and Tender PBIP source are updated; deployment and live Power BI acceptance have not been performed. Existing unrelated working-tree changes were preserved.

## Result

- Tender datalake schedule admission remains QAC-only. Selected manual CSV bundles accept any explicit nonblank reporting identity, including non-QAC codes absent from `dbo_project`.
- No Tender C/J aliases remain in the reviewed identity, metadata, routing or permission paths. `C5001`, `J5001`, `5001`, `CIVIL` and `JIVIL` remain distinct. Native-to-reporting mapping is explicit and diagnostic, not suffix inference.
- Optional manual State is available in Core, Windows, Web and the Tender CLI. It applies to every ordered stage. Outer whitespace is trimmed, invariant uppercase is applied and eight recognised Australian full names map to abbreviations; custom labels and internal spaces remain valid.
- State is never inferred from XER fields, filenames, codes or permissions. Unknown State is allowed and warned about; only the State-grant branch becomes inapplicable.
- Tender is now schema **3.0**, with required nullable `project_state` after `project_name` in the **18-column manifest**. Every row agrees. Table `02.state` mirrors the same value; the loader validates the mirror before assigning model State from the manifest. The bundle remains ten numbered files plus one manifest.
- State enters the structured bundle-identity hash. State-only changes leave public stage/task keys and the nine non-02 numbered tables unchanged. Ordered inputs retain stable internal tokens; repeated filenames, paths and content remain separate occurrences.
- Report admission is independent of viewer access. The role grants all-project access OR matching nonblank State access OR exact-project access. Catalogue membership and project-specific permission rows are not additional gates. CSV metadata remains authoritative; Athena State uses actual catalogue/source metadata, not permission-row inference.
- Exact whole-project CSV ownership applies across all ten tables. Similar codes do not suppress one another or broaden Athena admission.
- Legitimate optional header-only tables are accepted. The project security route is `Project_Dimension -> 02 -> 01`, with the existing activity/WBS relationship filtering in both normal and security directions. The old direct project/WBS route was removed. Manifest and audit tables remain role-protected.

Programme stays **4.0**. Standard, calendar, relationship free-float and resource-distribution calculations were not changed by this work. No LongestPathVisual, permission-workbook, report-layout, cache or source-XER edits were made.

## Automated validation

Logs below are relative to `../artifacts/tender-state-access-validation/`. They are retained local artifacts, not published outputs.

### Nine .NET builds

Every final target passed with zero warnings and zero errors using the repository's .NET 8.0.421 SDK.

| Target | Log |
| --- | --- |
| Core | `build-Core.log` |
| Windows (`XER to CSV.csproj`) | `02-windows-build-retry.log` |
| Core.Tests | `build-CoreTests-final.log` |
| Web | `04-web-build.log` |
| Programme CLI | `build-Programme.Cli.log` |
| Tender CLI | `06-tender-cli-build.log` |
| Tender Surface.Tests | `07-surface-tests-build.log` |
| Relationship Audit CLI | `build-Audit.Cli.log` |
| Relationship Audit.Tests | `build-Audit.Tests-retry.log` |

### Tests and contract checks

| Check | Result | Evidence |
| --- | --- | --- |
| Full Core suite | 1,343 passed | `test-Core-final.log`, `test-results/core-final.trx` |
| Tender Surface suite | 79 passed | `11-surface-tests.log` |
| Audit suite | 36 passed, 2 skipped: Windows symbolic-link capability | `test-Audit.log`, `test-results/audit.trx` |
| Web download JavaScript | 5 passed | `13-web-download-javascript-approved.log` |
| Windows `--ui-smoke-test` | Exit 0, including State/no-fallback/frozen-request cases | `15-windows-ui-smoke.log` |
| Real Tender CLI mapping | Passed; native `QAC000623-01-02` to explicit `QAC000623` | `tender-mapping-cli.log` |
| Core exports versus literal loader contracts | 4,164 assertions; 13 code/name cases per profile | `final-review-loader-contracts.json` |
| Programme and Tender model deserialization | TOM passed; 109 M expressions parsed | `final-review-loader-models.json` |
| State/access contract validator | 93 checks; 18 State vectors in two cultures, 13 translated access cases, 12 graph assertions | `final-tender-state-access.json` |
| Project filename SHA parity | 16 vectors across three helpers | `final-project-sha256.log` |
| Reporting-skill contract validator | 20 numbered contracts, Tender manifest, 35-column diagnostics, 12 links and whitespace | `reporting-skill-final.log` |
| Generic skill validation | Passed for parser, installed and Tender repository copies | `reporting-skill-generic.log`, `reporting-skill-installed.log`, `reporting-skill-tender-repository.log` |
| `git diff --check` | Both repositories passed | `git-diff-check-parser.log`, `git-diff-check-tender.log` |

State/access and SHA checks include literal-code inspection and independently translated policy/arithmetic. They do **not** execute M or DAX, prove live RLS, or verify downloaded CSV byte hashes inside Power Query.

### July/August real-XER reconciliation

The supplied `2607-EBA_PAA_8.0.xer` and `2608-EBA_PAA_8.0.xer` were read locally. Their source hashes remained unchanged.

The pre-change Debug Core assembly was backed up before implementation, SHA-256 `6F34CD5C1DC1A30D808E8F022254D8C7426F67E96F1B4529F44CBECB692EECD0`. Baseline and current comparisons used frozen export metadata.

- Standard: all 14 numbered outputs byte-identical to the pre-change build; complete disk/Web-stream parity and disk publication passed (`current-standard.json`). Its diagnostic companion remains present.
- Programme: all eleven files, including the manifest, byte-identical for each input. Tender: the nine non-02/non-manifest files byte-identical for each input. The intended Tender differences are 02 manual State and the 3.0 manifest/bundle identity. Both profiles passed actual disk/Web-byte parity; 184 assertions (`current-profiles.json`).
- Existing local all-table reconciliation passed: 33,100 relationships, 31,334 detailed-calendar rows, 12,324 distribution rows and 6,849 resource groups. It checked 2,236,209 retained source cells, 391,414 finite derived numeric cells and 909,978 independently derived values (`local-xer-reconciliation-july-august.log`).
- Source-quality warnings remain visible: 449 total, comprising 447 general warnings and two actual-allocation warnings. There were no remaining-allocation or unknown-quantity warnings in this combined run. This is not a claim that those XERs are free of scheduling defects.

Per-input relationship integration also passed. July reconciled 16,245 assessments and 6,383 resource-month groups; August reconciled 16,855 assessments and 5,936 resource-month groups. Both review profiles retained eleven files, and every table-06 assessment/audit field agreed with normal exports. Standard disk/stream and Programme/Tender disk/memory outputs were byte-identical (`relationship-integration-july.log`, `relationship-integration-august.log`). These runs left State blank, correctly adding one Tender unknown-State warning per stage; that is distinct from the explicit-QLD baseline comparison above.

### Retries and environment constraints

The first Windows build included backup `.cs` files beneath `artifacts`; the root project now excludes `artifacts\**` from compilation. No backups were deleted. The first full Core test run exposed three stale Tender 2.0 expectations producing ten failures; only those version expectations were changed, and the full rerun passed. The first Audit.Tests build returned an uninformative MSBuild failure; its single-node retry passed. Web JavaScript required an approved retry after sandbox `spawn EPERM`. Generic skill validation used an isolated validation-only PyYAML dependency, without altering global Python packages.

## Reproduction entry points

Run each assembly-loading validation in a fresh PowerShell 7 process. Build with `./local-dotnet-sdk/dotnet.exe build <project.csproj> --no-restore -m:1`; tests use `test <test.csproj> --no-build --no-restore`. Build the nine projects listed above individually. The Windows smoke entry point is `./local-dotnet-sdk/dotnet.exe './bin/Debug/net8.0-windows/XER to CSV.dll' --ui-smoke-test`.

- `review/Validate-ReviewLoaderContracts.ps1`
- `review/Validate-ReviewLoaderModels.ps1`
- `review/Validate-TenderStateAccess.ps1`
- `review/Validate-ReportingSkill.ps1`
- `review/Validate-TenderProjectMapping.ps1`
- `review/Validate-TenderStateExports.ps1 -Paths <ordered-XER-paths>`; pass `-BaselineSummary <baseline-json>` for protected-output comparison.
- `review/Validate-ResourceExportIntegration.ps1 -Paths <ordered-XER-paths>`; use its `-BaselineHashes` option for Standard comparison.
- `review/Validate-LocalXerExport.ps1 -Paths <ordered-XER-paths>`; do not exclude resource distribution.
- `review/Validate-RelationshipExportIntegration.ps1 -Paths <ordered-XER-paths> -ProfileFixture <one-XER-path>`; run with each project's fixture.
- `git diff --check` in both repositories, using their normal line-ending configuration.

## Migration and remaining manual acceptance

Upgrade parser and Tender report together, then regenerate Tender **3.0** bundles. Do not edit version cells or mix old/new files. Retain the previous matched parser/report/bundle set for rollback. Programme 4.0 imports remain separate and unchanged.

Before deployment, use Desktop refresh and actual Viewer/View-as tests to verify:

1. QAC and arbitrary non-QAC CSV projects absent from the catalogue; exact-code Athena/CSV replacement while unrelated QAC schedules remain.
2. Independent all-project, State and exact-project grants, wrong/no grants, blank State, custom State and distinct C/J/numeric codes.
3. Missing/orphan WBS activities, ancestor-only WBS rows and flattened hierarchy labels. The new reverse activity-to-WBS security route is statically valid, but ancestor rows without directly related activities require runtime verification.
4. Project/State/stage slicers, tables 01/02/03/06/15, logic view and hidden diagnostic protection.
5. Actual SharePoint/Athena connector refresh, credentials and trusted-publisher access to the selected import location. Manual State is security-relevant classification, not a permission grant.

The existing Tender loader checks hash syntax and manifest consistency, **not** SHA-256 of downloaded CSV bytes. Same-shape numeric tampering is not detected by a refresh-time checksum; actual file hashes require separate verification before trusted publication. Flexible admission also does not make duplicate primary keys or ambiguous model identities valid.

Reporting-skill backups are under `../artifacts/tender-state-access-validation/before-20260910T094933651Z/`; scoped Tender model backups and promotion hashes are under `../artifacts/tender-state-pbip-20260910/`. The parser repository, installed and Tender repository numbered-XER references are synchronised for the changed contract files. Calculation-reference content was not changed.

No upload, publication, permission grant, commit or push was performed.
