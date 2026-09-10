# Tender Review: QAC datalake plus independent manual CSV bundles

Status: approved and implemented in parser and Tender PBIP source, 2026-09-10. See [implementation and validation evidence](TENDER_STATE_ACCESS_IMPLEMENTATION.md) for executed checks and the remaining Desktop/View-as acceptance gates. The proposal below is retained as the approved design record; its investigation references describe the pre-implementation state. It supersedes the earlier C/J alias and Use XER State proposals in this file and the related manual-import review.

## 1. Corrected business requirements

| Source | Project admission | Project identity | State authority |
| --- | --- | --- | --- |
| Tender datalake schedules | QAC projects only | Exact QAC reporting code | Existing datalake metadata |
| Explicitly selected Tender CSV bundle | Any nonblank reporting code, QAC or non-QAC | Reporting code entered for the bundle | New manually entered bundle State; blank allowed |
| Normal Programme Review | Existing rules, unchanged | Existing Programme rules, including its C/J handling | Existing Programme behaviour |

There is no C/J aliasing anywhere in Tender. `C5001`, `J5001` and `5001` are three different manual Tender identities. No prefix, suffix or similar-name inference makes two reporting codes equivalent.

The report must admit sporadic one-stage or multi-stage manual bundles without requiring their projects to exist in `dbo_project` or to have a project-specific UserPermission row. Authorisation still applies independently to every viewer.

Tender stage selection remains `status_date`; the P6 Data Date remains separate. This work introduces no Programme baseline/update semantics, rescheduling or changes to relationship/calendar/resource calculations.

## 2. Verified defects and evidence limits

PBIP references below are relative to `C:/Users/ricar/Documents/Code/Tender-Review/Project Review - Tender Programme (datalake).SemanticModel/definition/`.

- `tables/dbo_project.tmdl:68-70` selects catalogue metadata without a QAC WHERE predicate. This says nothing about whether QAC11111 exists in the live catalogue; live sources were not queried.
- `expressions.tmdl:97` restricts Athena schedules to QAC. Keep this source-specific rule.
- `tables/Project_Dimension.tmdl:80-91` also applies QAC to the combined project set. This incorrectly excludes non-QAC manual CSV projects.
- Tender routing, task metadata, project dimension, refresh audit and role contain C/J transformations. These must all be removed from Tender, including the numeric-only aliases currently retained by Core.
- `tables/Tender CSV Refresh Audit.tmdl:104-113` blocks projects absent from the catalogue or without a project-code UserPermission entry. This conflicts with the role's independent all-project/state/project grants at `roles/Project Access.tmdl:72-125`.
- Current exporter source reserves a nullable `02.state` slot, but has no manual State input. A synthetic local output is not evidence that the user's existing CSV has State. The user's actual table 02 and manifest were not supplied for this review. Do not assume either contains usable State.

The new CSV State must therefore be supplied explicitly as new bundle metadata, not discovered in the old CSV, original XER, filenames, project codes or permissions.

## 3. Parser and new bundle contract

### Manual metadata

Add one optional State field beside Reporting project code and Project name in the Windows and Web Tender forms, and the same field in the Core request and Tender CLI configuration. It applies to every ordered stage in that reporting project. There is no Use XER State mode and no source-data fallback.

- Omitted, null, empty or whitespace-only State means unknown. It must not prevent export or refresh.
- Normalise explicit State consistently across exporter, loader and permission comparisons: trim outer whitespace, uppercase, and map recognised full state names to their existing abbreviations. Preserve custom labels; do not introduce a restrictive state whitelist.
- Show a warning when State is blank: state-based access will not match, but all-project/exact-project access remains possible.
- Freeze the State and ordered sources into the export request before asynchronous work. Do not change or write back to the XER.

### Authoritative transport and migration

Recommend a coordinated **Tender schema 3.0** release because this introduces a new manifest field. Programme remains schema 4.0; Standard is unchanged.

1. Add `project_state` to `XER_CSV_MANIFEST.csv`. The column is required by the new schema; its value is nullable. It is the authoritative manual State for the whole bundle and must agree across all manifest rows.
2. Retain the current exporter's reserved table 02 header to avoid an unnecessary numbered-header change. Populate its `state` slot exclusively from the same resolved manual value. This is a newly populated output, not an assumption about old exports or a source `PROJECT.state` field.
3. The updated loader reads State from the manifest and supplies the model-side project State. Validate any required mirrored table 02 values against the manifest; never use them as a fallback authority.
4. Keep ten numbered CSVs plus one manifest, stable source/stage keys, row counts, hashes, manifest-last publication and rollback safeguards. State changes affect bundle identity, manifest and the mirrored 02 content, not task keys or schedule calculations. Include canonical State in deterministic bundle identity with unambiguous encoding.
5. Accept only the new Tender version and exact contract after coordinated migration. Regenerate old Tender bundles with the updated parser; do not just edit their version cells or fabricate missing columns. Check an actual user's table 02 header and manifest when preparing migration, rather than identifying a build by table number alone.

### Flexible identity

Keep trim/invariant uppercase normalisation of reporting codes, preserving internal spaces, punctuation and Unicode. Reuse safe filename/key encoding without restricting the business code to the datalake naming convention. Consumers must use emitted metadata/keys instead of reconstructing identity from filenames.

The entered reporting code may explicitly map native `QAC000623-01-02` to `QAC000623`, or to any other chosen code. Preserve native evidence and the existing mapping warning. A C-to-J mapping is allowed only as an explicit mapping, never an implicit Tender alias. Keep one PROJECT per source and existing governed stage validation.

Maintain ordered input occurrences and stable internal source tokens. Repeated filenames, paths, hashes and native IDs remain independent. Do not expose source tokens or replace collections with filename-keyed dictionaries.

Parser implementation files include `TenderReviewMetadata.cs`, `TenderReviewContract.cs`, `TenderReviewTransformer.cs`, the manifest writer, `TenderReviewExportDialog.cs`, `MainForm.TenderReview.cs`, Web `Pages/Index.razor` and `TenderReviewCliContract.cs`, with their Core/surface tests and documentation. Remove only Tender business aliases; generic CSV column `SourceAliases` and Programme logic are unrelated.

## 4. Tender PBIP: separate source branches, then combine

1. Keep Athena schedule admission QAC-only. A non-QAC CSV selection must never expand Athena's scope.
2. Build the effective project list from admitted Athena QAC projects plus explicitly selected CSV projects, not from a QAC filter over their union. Carry explicit source ownership through metadata, all ten tables, audits and project filtering.
3. CSV project code/name/State come from the selected bundle. Missing catalogue company/project IDs remain blank. `dbo_project` can provide optional enrichment or conflict diagnostics; it must not override manual identity/State or become a registration gate. Do not derive State from UserPermission or alphabetical MAXX across stages.
4. Remove every Tender C/J identity conversion from routing, display names, metadata lookups, collision handling, audit and RLS. Use one agreed normalised exact identity everywhere. Avoid DAX TRIM for CSV code identity because it collapses internal spaces.
5. Remove fatal catalogue-membership and per-project-permission-membership checks. Distinguish successful import from viewer authorisation. Missing enrichment or blank State is a diagnostic, not a refresh failure or an automatic access grant.
6. Preserve existing report object names, IDs, layout and stage slicer behaviour wherever possible. Surface entered project names/State through the existing project dimension. Do not edit LongestPathVisual, report caches or binary artefacts.

### Same-code source ownership

Recommended default: retain the existing explicit whole-project CSV override, narrowed to exact reporting identity across all ten tables. CSV `QAC000623` replaces the same-code Athena schedule; CSV `QAC000623-01-02` does not suppress `QAC000623`. Unrelated Athena QAC projects remain loaded. C/J codes never suppress one another.

A selected bundle supplies its complete chosen stage set, not an incremental append to older bundles. Do not silently combine CSV and Athena copies of the same project/stage. Side-by-side same-code source comparison would need a separate scenario identity and is outside this plan.

Primary PBIP files: `expressions.tmdl`; tables `01 XER_TASK`, `02 XER_PROJECT`, `03 XER_PROJWBS`, `Project_Dimension`, `Tender CSV Manifest`, `Tender CSV Refresh Audit`; `roles/Project Access.tmdl`; `relationships.tmdl`; and Tender setup/profile documentation. Verify downstream calculated-table, measure and visual references before changing alias helper columns.

## 5. Permissions: independent alternatives

| Existing UserPermission grant | Access to the imported CSV project |
| --- | --- |
| AllAccess 1 | Allowed without a project-specific row, State or catalogue registration |
| AllAccess 2 with matching nonblank State | Allowed without a project-specific row or catalogue registration |
| AllAccess 3 with exact reporting code | Allowed without State or catalogue registration; no C/J aliases |
| Multiple grants | Any applicable grant is sufficient |
| No applicable grant | Denied; successful import does not create permission |

Blank manual State disables only the State-match branch. Never match blank to blank as a State grant. A matching all-project or exact-project grant still applies. Apply the same State normalisation to published metadata and permission values, and regression-test existing Athena outcomes.

Manual State is security-relevant classification: changing it changes the audience of existing state grants. The recommended policy is that an authorised bundle publisher declares this classification. Show that consequence beside the field and in publication instructions. Verify write access to the Active import location before deployment. If contributors are not trusted to classify project visibility, require trusted approval or a separate trusted classification mapping before using their State for RLS. Do not modify permission workbooks or create grants as part of importing a project.

Keep manifest/audit tables role-protected, including new State diagnostics. Test ordinary viewers, not just administrators: Power BI RLS does not apply to workspace Admin, Member or Contributor roles. [Microsoft RLS documentation](https://learn.microsoft.com/en-us/fabric/security/service-admin-row-level-security)

## 6. Warning-bearing data and filtering integrity

Retain all ten fixed output files. Align loader availability rules with legitimate header-only exports, particularly missing WBS/calendar sources. Missing optional metadata, State, WBS or calendars must not be mistaken for a corrupt bundle. Leave unknown `06.free_float` null; do not change any numbered calculation.

Project filtering/security cannot depend on usable WBS. Validate a project-identity route such as `Project_Dimension -> 02_XER_PROJECT -> 01_XER_TASK`, reconciling competing paths and deliberately preserving WBS filtering/security. Do not simply add a second active route. Test empty/orphan WBS and downstream 06/15 visibility; preserve activities and show missing hierarchy context.

Retain genuine structural checks: exact current profile/version, required files/headers, consistent metadata, namespaces, row counts/hashes and model-supportable identities. Do not silently deduplicate or drop ambiguous rows to force refresh. Flexible project admission does not certify malformed data or remove the need for a separately agreed quarantine design if primary keys cannot support model relationships.

## 7. Implementation sequence and acceptance

Split the approved work into coordinated, reviewable patches:

1. Contract and Core: new manual State, manifest version, exact Tender identities and deterministic metadata tests.
2. Export surfaces: Windows, Web and Tender CLI with equivalent fields/messages. No Programme/Standard behaviour changes.
3. PBIP: source-aware metadata/ownership, exact identities, current-contract loader, corrected admission audit and independently validated RLS paths.
4. Documentation: synchronise Tender setup/spec, CLI instructions and Tender sections of repository/installed numbered-XER reporting skills after required backups. Do not rewrite Programme policy.
5. Regression and manual acceptance before any separately authorised deployment. Upgrade parser/report together and regenerate selected bundles; preserve a recoverable old-version set for rollback without mixing contracts.

Automated validation after implementation, reported separately:

- Nine .NET builds: Core, Windows, Core.Tests, Web, Programme CLI, Tender CLI, Tender Surface.Tests, Relationship Audit CLI and Audit.Tests.
- Full Core, Tender Surface and Audit test suites; Web download JavaScript tests; Windows UI smoke; real CLI and Core disk/Web-stream export parity.
- Reporting-skill validation; `review/Validate-ReviewLoaderModels.ps1`; `review/Validate-ReviewLoaderContracts.ps1`; SHA/name/state parity tests; `git diff --check` in both repositories.
- Reconcile all ten table outputs, manifest coverage and source order. State-only edits must not change 01/03/06/07/08/09/10/12/15 data or public stage/task keys. Preserve Standard/Programme golden outputs and Programme aliases.

Essential acceptance cases:

| Case | Required outcome |
| --- | --- |
| QAC11111 CSV and arbitrary non-QAC CSV absent from catalogue | Both import and appear for authorised viewers |
| Non-QAC schedule in datalake | Still excluded |
| C5001, J5001, 5001, CIVIL and JIVIL CSV projects | Distinct routing, display, metadata and permissions |
| QAC000623-01-02 explicitly mapped to QAC000623 | Accepted with mapping evidence; no automatic suffix inference |
| Exact QAC CSV/Athena collision | One explicitly owned source across all ten tables |
| State omitted/null/empty/whitespace | Export/import succeeds; State grant does not match |
| State QLD, Queensland or a custom label | Consistent canonical identity and permission comparison |
| All, matching-State, exact-project, wrong-State and no-grant viewers | Independent OR branches with no implicit alias or access grant |
| Multiple stages and repeated filenames/paths/content in both orders | Correct ordered independent sources and deterministic metadata |
| Internal spaces, punctuation, Unicode and long codes | Distinct business identities and safe opaque keys/folder tokens |
| State-only change with frozen export clock | New bundle identity; unchanged schedule calculations/keys |
| Missing WBS/calendar and warning-bearing data | Legitimate outputs load without project/RLS leakage or activity loss |
| Old/mixed/tampered contract | Clear regeneration/integrity error; no version-cell workaround |

Power BI Desktop refresh and View-as tests are mandatory for project/State/stage slicers, tables 01/02/03/06/15, WBS filtering, the logic view and hidden diagnostics. Validate the deployment with an actual Viewer role before declaring access behaviour proven. [Microsoft role-validation guidance](https://learn.microsoft.com/en-us/fabric/security/service-admin-row-level-security)

Earlier review checks passed TOM/M syntax and exporter-to-literal-header assertions; they did not execute the new design or prove live SharePoint/Athena refresh, live catalogue membership or RLS. No new implementation build/test pass is claimed by this plan revision.

Skill sequence: PBIP safety -> numbered-XER contract and Power Query -> TMDL/security -> validation and Git review. The safety/security review keeps implementation and publication gated on approval, and requires runtime View-as evidence rather than accepting static checks as access proof.

Approval prompt: "Implement the revised TENDER_CSV_STATE_AND_ACCESS_PLAN.md in the parser and Tender PBIP, including optional manual State and Tender schema 3.0. Preserve Programme and Standard. Run the stated checks; do not upload, publish, commit or push."

Original review boundary: only this plan was revised before approval. The subsequent user instruction, "proceed with plan", authorised the scoped parser/PBIP implementation and reporting-skill synchronisation. Existing unrelated changes remain preserved. No upload, permission change, deployment, commit or push is authorised by this work.
