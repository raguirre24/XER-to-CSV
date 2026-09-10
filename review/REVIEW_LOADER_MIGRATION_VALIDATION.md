# Current-only review CSV loaders — 10 September 2026

## Outcome

The local Programme Review loader was accepting schemas 1.0–3.0 while the current exporter emits 4.0. The local Tender Review loader was accepting 1.0 while its exporter emits 2.0. Both still expected the older 16-column table 06. Changing only the version allow-list would therefore have exposed a second header failure.

The report repositories now accept only Programme Review **4.0** and Tender Review **2.0** (`bundle_profile=tender_review`). Current manifests and all ten ordered table schemas match the Core exporter. Table 06 includes `free_float_status`, `free_float_basis` and `free_float_reason` immediately after `free_float`; these fields are imported into the model without filling blank float values. Athena rows have null values for the new metadata. Existing numerical calculations, Athena SQL, relationships, visual definitions and RLS are unchanged.

The legacy Programme key-upgrade branches have been removed. Current keys retain their exact values and must reference a snapshot/stage in the selected manifest. Both loaders support the exporter's full business codes, UTF-8 encoded project keys, bounded filename components and JSON project lists for names containing commas. Programme now also enforces its declared per-table unique keys. Ten numbered CSVs plus the manifest remain the complete bundle envelope; no diagnostic CSV belongs inside a review bundle.

## Files and safety

Programme repository: `C:/Users/ricar/Documents/Code/Programme Review/Programme-Review`:

- `Project Review - Programme (datalake).SemanticModel/definition/expressions.tmdl`
- `Project Review - Programme (datalake).SemanticModel/definition/tables/06 XER_PREDECESSOR.tmdl`
- `SHAREPOINT_CSV_SETUP.md`

Tender repository: `C:/Users/ricar/Documents/Code/Tender-Review`:

- `Project Review - Tender Programme (datalake).SemanticModel/definition/expressions.tmdl`
- `Project Review - Tender Programme (datalake).SemanticModel/definition/tables/06 XER_PREDECESSOR.tmdl`
- `TENDER_CSV_SETUP.md`
- `XER_TO_CSV_TENDER_PROFILE_SPEC.md`

Both repositories were clean before this work. These seven files were edited in staged copies, validated, then copied into their exact original locations only after SHA-256 checks confirmed the originals had not changed. Original copies are under `artifacts/review-loader-migration-20260910/programme-original` and `tender-original` in the parser workspace. No files were deleted. Existing uncommitted exporter work was preserved. No commit, push, SharePoint upload or Service deployment was performed.

## Validation performed on the applied files

| Check | Result |
| --- | --- |
| `review/Validate-ReviewLoaderModels.ps1` | Both full models deserialize through Microsoft TOM; 108 M expressions parse through Microsoft's Power Query parser; all CSV-helper references resolve |
| Programme model | 42 tables, 34 relationships, 35 shared M expressions and 22 M partitions |
| Tender model | 39 tables, 27 relationships, 30 shared M expressions and 21 M partitions |
| `review/Validate-ReviewLoaderContracts.ps1` | 2,318 assertions passed; all 20 literal table contracts and both manifest headers match Core; 14 real in-memory exporter bundles cover seven project-name cases per profile |
| Duplicate Tender sources | Fresh bundles retain two stages with identical original filenames and bytes; separate canonical/key identities remain intact |
| `node review/test-review-project-sha256.cjs` | 48 comparisons passed against native SHA-256: 16 vectors each for the reference helper and both applied loaders |
| M-validator negative checks | Malformed M and an undefined CSV-helper reference are rejected |
| `git diff --check` | Passed separately in parser, Programme and Tender repositories |

Contract cases include `QAC000623-01-02`, spaces, Unicode, slash/backslash/colon/pipe/percent, quoted/comma-containing names and long project names. Exported headers, row counts, actual byte hashes, business metadata and key grammar are checked. SHA validation reads actual helper constants/byte-order lists and compares translated arithmetic, including padding boundaries, with native cryptography. It is not execution of the M engine.

The model parser verifies syntax and references, not M runtime behaviour. Manifest-version and header rejection paths were also reviewed to ensure they are dependencies of returned results, not unused lazy bindings. The checks do not compute CSV hashes inside Power Query; parser hash validation remains authoritative, while existing report manifest hash syntax/consistency validation is preserved.

## .NET and JavaScript regression checks

All nine projects built separately with the repository SDK using:

```powershell
local-dotnet-sdk/dotnet.exe build <project> --no-restore --nologo -v:minimal -m:1 -nr:false
```

Each completed with zero warnings and errors:

| Build target | Result |
| --- | --- |
| Core | Passed |
| Windows app | Passed |
| Core.Tests | Passed |
| Web | Passed |
| Programme Review CLI | Passed |
| Tender Review CLI | Passed |
| Tender Review Surface.Tests | Passed |
| Relationship Audit CLI | Passed |
| Relationship Audit CLI.Tests | Passed |

Tests were run separately with `dotnet test <project> --no-build --no-restore --nologo`:

| Test target | Result |
| --- | --- |
| Core.Tests | 1,300 passed |
| Tender Review Surface.Tests | 60 passed |
| Relationship Audit CLI.Tests | 36 passed; 2 host-capability symbolic-link tests skipped |
| Web `node --test XerToCsvConverter.Web/tests/downloads.test.cjs` | 5 passed |
| `review/Validate-ReportingSkill.ps1` | 20 numbered contracts, 35-column diagnostic header, 12 links and whitespace passed |
| Generic skill-creator `quick_validate.py` | Passed using the existing local PyYAML dependency; no installation |

Logs are under `artifacts/review-loader-migration-20260910/validation-logs`.

The optional existing Programme SQL fixture runner could not start because this Python environment lacks `duckdb`; it is not reported as a pass. Web test process creation and access to the previously installed PyYAML dependency initially encountered sandbox restrictions and passed after approved retries. No dependencies were installed for these checks.

## Desktop/Service acceptance still required

Open the updated local PBIP, retaining the intended source parameters and credentials, and refresh with a newly generated matching-profile bundle. A deployed report or already-open Desktop copy does not acquire these on-disk changes automatically.

Verify both current bundles load, the three new table 06 fields match their CSVs, and blank float stays blank. Exercise header-only optional tables and the hidden refresh audit. In a test location, confirm older versions, mixed-version manifests, the wrong profile, missing/extra/reordered CSV headers and old table 06 headers are rejected; do not alter production bundles for these tests. Validate punctuation/Unicode and a long hashed project-folder name in the actual M runtime, then perform the existing governed View-as checks.

Use the exact generated folder/key identities. Regenerate an old bundle rather than editing its `schema_version` cell. Old versions are not fallback candidates; rollback requires another bundle using the current contract. Live SharePoint/M execution, authentication, Service refresh and Power BI visual/RLS acceptance have not been performed by the offline checks above.
