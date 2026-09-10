# Tender manual CSV import: PBIP compatibility review

Date: 2026-09-10. Read-only investigation of the existing Tender PBIP, following the exporter reporting-project mapping fix. No Tender report/model, permissions, source XER, SharePoint files or deployment were changed.

## Outcome

The exporter now separates the native P6 short name from the chosen reporting project code. The current Tender CSV loader also uses declared reporting metadata rather than extracting the project from the original XER filename. The reported `QAC000623-01-02` source can therefore be exported as `QAC000623` without a matching native-name requirement in the PBIP.

However, the PBIP is not yet generally compatible with sporadic manual projects independent of datalake registration. Its downstream dimension, refresh audit and identity rules still impose datalake assumptions. A successful exporter build is not proof that such a bundle refreshes and appears correctly in this report.

Working assumption: flexibility means arbitrary source filenames, explicit reporting codes/names, selected Tender stage dates and absent optional scheduling data. It does not mean accepting old schema versions, mixing bundles, inventing missing calculation values or granting report access from CSV contents. The existing current-only Tender 2.0 contract remains appropriate.

## Existing work protected

The Tender repository already has uncommitted changes in `expressions.tmdl`, `tables/06 XER_PREDECESSOR.tmdl`, `TENDER_CSV_SETUP.md` and `XER_TO_CSV_TENDER_PROFILE_SPEC.md`. The parser repository contains the preceding mapping fix. All were preserved.

## Findings and proposed changes

Paths below are relative to the Tender repository. Table and role paths are under `Project Review - Tender Programme (datalake).SemanticModel/definition/`.

| Finding | Evidence | Proposed change |
| --- | --- | --- |
| Non-QAC CSV projects lack project dimension membership | `tables/Project_Dimension.tmdl:80-91` applies `LEFT(ProjectKey, 3) = "QAC"` to all project rows | Keep the QAC rule on the Athena branch, but admit explicitly loaded CSV projects using `IsCsvSource`. |
| A project missing from datalake registration aborts refresh | `tables/Tender CSV Refresh Audit.tmdl:103-115` raises `ERROR()` for missing `dbo_project` membership | Report missing enrichment as a diagnostic for CSV-owned projects; retain fatal source-ownership overlap and structural-integrity checks. |
| Supplied CSV project names are ignored in the project dimension | `tables/Project_Dimension.tmdl:114-140` looks up only datalake and permission metadata | Use the selected manifest's project name for CSV-owned projects, including when task data is empty. Do not require a datalake project name. |
| Arbitrary names could acquire an unintended alias | `tables/Project_Dimension.tmdl:94-107` and `roles/Project Access.tmdl:99-105` alias any leading C/J | Match Core/routing's numeric-suffix-only C/J compatibility. All other codes are exact identities. Test metadata and access together before admitting arbitrary codes. |
| Distinct names with repeated spaces can be merged | `tables/Project_Dimension.tmdl:84-91`, `tables/03 XER_PROJWBS.tmdl:122`, refresh-audit identity expressions use DAX `TRIM` | Preserve the exporter-normalised CSV identity, including internal whitespace; keep existing Athena normalisation source-specific. Reconcile dimension, WBS, audit and access identities consistently. |
| Loader rejects valid warning-bearing header-only outputs | `expressions.tmdl:919-965` requires positive rows for 01/02/03/10 | Allow the contract's empty source-dependent tables, with diagnostics. Require valid project/stage metadata and exact manifest counts. Empty WBS/calendar tables must not fabricate WBS nodes, calendars or zero float. |
| Loader assumes all exported row keys remain usable | `fnTenderCsvConvertValue`, `fnTenderCsvReadTable` required-key and uniqueness checks | Reconcile row-warning exports against model relationship requirements. Preserve unresolved relationships as nullable; do not silently deduplicate ambiguous dimension rows. Define a visible rejected/ambiguous-row diagnostic path where a model relationship requires uniqueness. This needs dedicated acceptance tests, not blanket error suppression. |
| Setup instructions still require datalake registration and a full-history workflow | `TENDER_CSV_SETUP.md` prerequisites and stage-publication guidance | Describe the chosen bundle as the complete selected report scope, which may contain just one sporadic stage. Explain that newest-bundle replacement does not append missing older stages. Document reporting/native identity mapping and current permission source. |

## Access and connection boundaries

`UserPermission` currently comes from the separate SharePoint permissions workbook through `UserPermission_Source`, not from the Tender CSV manifest and not from the old documented `dbo_userpermission` source. The `Project Access` role implements all-access, state-access and project-specific access.

Importing a new code must not grant permission automatically. A missing project-specific permission entry can be diagnosed independently of loading schedule data, but actual visibility must follow the explicitly agreed existing access rules. Retain hidden audit/manifest RLS restrictions. Validate positive and negative identities using View as before release; static validation does not establish RLS correctness.

Independent CSV project identity is not the same as a fully offline report. `dbo_project` still queries Athena and permissions still use SharePoint. Removing all external connections is a separate change, not assumed here. Preserve existing Athena filename filtering and source behaviour for projects that remain Athena-owned.

The selected reporting code must be used consistently in the exporter, `TenderCsvProjectCodes`, relevant finite `SelectedProjects` configuration and encoded SharePoint project folder. For the reported case that code is `QAC000623`, not necessarily the original `QAC000623-01-02`. Keep parser-generated table files and manifest together; never strip suffixes to infer that unrelated projects are equivalent.

## Verification performed

- `review/Validate-ReviewLoaderModels.ps1`: both existing report models deserialised through TOM; all 108 M expressions parsed with the Microsoft parser; CSV helper references passed.
- `review/Validate-ReviewLoaderContracts.ps1`: 2,318 assertions passed; ten contracts per profile, seven flexible-name cases per profile, Programme 4.0 and Tender 2.0. Real Core exports are compared with literal M contracts; the M loader itself is not executed.
- `node review/test-review-project-sha256.cjs`: 16 translated SHA test vectors across three helper copies passed. This does not execute Power Query's implementation.
- Additional real Core in-memory export: complete fixture, absent PROJWBS, absent CALENDAR, and both absent all export eleven files and ten manifest rows with two activities and one project. Missing sources produce header-only 03/10, respectively. Each missing-source variant contradicts the consumer's literal positive-row-count predicate.
- `git diff --check`: passed in parser and Tender repositories for their existing changes.
- No usable local Power Query evaluation harness was found. No live Athena/SharePoint queries, Power BI refresh, visual interaction or View-as tests were performed.

## Acceptance for implementation

1. Exact reported mapping imports under `QAC000623` while preserving the original filename as provenance.
2. Manual codes not beginning QAC, hyphens, punctuation, Unicode and long encoded/hashed codes appear correctly in project/stage slicers.
3. `A B` and `A  B` remain distinct; `CIVIL` and `JIVIL` do not become aliases or share project-specific access. Existing numeric C/J policy remains tested.
4. A selected CSV project absent from `dbo_project` receives its declared name without a governance refresh error or fabricated company/project IDs.
5. Duplicate source names/paths/content across distinct dates remain separate stages. A one-stage bundle contains exactly the chosen stage; replacement never implies an incremental append.
6. Missing WBS/calendar data and unresolved derived values remain visible/diagnosed without silently dropping activities or treating blank float as zero. Genuine duplicate keys cannot be hidden to force one-to-many relationships.
7. Current schemas, headers, manifest/file agreement and source ownership remain validated; malformed or mixed bundles are not labelled successful.
8. Full Desktop refresh, ten-table audit, slicers/joins and View-as positive/negative cases pass for CSV-only schedule selection and mixed Athena/CSV selection.

Implementation requires approval to edit the sibling Tender repository, previously excluded from changes. Any implementation must preserve its existing dirty edits and leave publishing, permission-workbook changes and commits/pushes out of scope unless separately requested.
