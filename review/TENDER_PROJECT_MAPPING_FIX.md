# Tender reporting-project mapping fix

## Root cause

The reported source name `QAC000623-01-02` is not truncated by the current parser. The Web form keeps a separately entered `_projectCode` value and sends it after trim/invariant uppercase; it does not infer that value from `PROJECT.proj_short_name`. The value persists across Clear/profile switches so an intentional reporting identity is not discarded. Windows prefills the full native name only when its code field is empty.

The failed check was `TenderReviewTransformer.ResolveProjectMetadata`: it required the XER short name to equal the requested reporting code (with only the pre-existing C/J numeric alias exception), and described any mismatch as multiple projects. A fixture with the exact filename and codes reproduced the user's exception before this change.

## Corrected policy

The caller explicitly selects a reporting project code and assigns the ordered input stages to it. `QAC000623-01-02` can therefore be a P6 project/revision name exported under reporting identity `QAC000623`. This also allows renamed native revisions across stages. No suffix stripping, prefix match, automatic project merging or rewriting of the source XER is performed. A shared prefix is not evidence that sources belong to the same business project: the caller must select the correct stages and reporting code.

Each input still requires exactly one `PROJECT` row, a nonblank native `proj_id` and `proj_short_name`, and a valid P6 Data Date. Source-local references and their existing diagnostics remain intact. Invalid task/project references remain diagnosed and preserved according to the existing row-resilience policy; they are not all hard failures. Repeated names, paths, hashes and snapshot-local IDs remain valid ordered occurrences.

A non-alias mapping produces `TENDER_PROJECT_CODE_MAPPED`, including status date, native short name and requested reporting identity. Its result diagnostic retains `table_name=02_XER_PROJECT`, `source_table=PROJECT`, `column_name=proj_short_name`, original `raw_value` and source-row evidence. Existing exact matches and C/J numeric aliases retain their prior numerical/output behaviour without this new mapping warning. A digits-only name is still not an automatic C/J alias; it can be explicitly mapped and receives the warning.

Windows and Web label the input **Reporting project code**, and Windows/Web/both review CLIs display source diagnostics through one shared message formatter. Messages preserve repeated occurrences and do not expose internal correlation tokens. Ordinary display messages are bounded, while mapping notices are retained. The review workflows no longer tell users to find a diagnostic CSV inside the bundle: each review bundle still contains exactly ten numbered CSVs and its manifest. Standard Enhanced continues to emit its diagnostic CSV.

CSV headers, manifest schemas (Programme 4.0/Tender 2.0), calendar calculations, relationship float and resource distributions are unchanged. Output project keys, metadata, canonical names and bundle identity use the explicit reporting code consistently. The added diagnostic is not a resource allocation and contributes no unallocated units.

## Regression coverage

New tests cover the exact reported pair, reverse mapping, punctuation/Unicode and unrelated textual identities as explicit mappings; disk versus byte/stream output parity; original source-byte preservation; diagnostic evidence; and equality of all non-identity numbered values against a same-source bundle using the native code. They also cover distinct revision names, repeated identical source bytes/names and native IDs in both input orders, multiple/duplicate PROJECT rows, blank native IDs/names/Data Dates and malformed Data Dates.

Surface tests check the reporting-code wording and request wiring, CLI mapping, warning presentation, repeated messages, message limits and token redaction. Windows smoke coverage keeps both flexible-name input and the exact `QAC000623-01-02` to `QAC000623` request case.

The original XER was unavailable at its previously supplied OneDrive location and the explicitly checked matching Downloads location during this run. The exact error is reproduced with real disk/byte parser fixtures, not claimed as a run against that unavailable file. Tests establish local implementation consistency, not native P6 equivalence or a deployed Web refresh.

Build/test results are recorded in `artifacts/tender-project-mapping-validation`. No sibling report repository, original XER, SharePoint data, commit or push is changed by this fix. The running Web app must use the rebuilt version before users see the corrected behaviour.

## Validation results (2026-09-10)

All builds used the repository-bundled .NET SDK, sequentially, with `build --no-restore --nologo -v:minimal -m:1 -nr:false`. Each build passed with zero warnings and zero errors.

| Build target | Result |
| --- | --- |
| Core | Pass |
| Windows | Pass |
| Core.Tests | Pass |
| Web | Pass |
| Programme Review CLI | Pass |
| Tender Review CLI | Pass |
| Tender Surface.Tests | Pass |
| Relationship Audit CLI | Pass |
| Relationship Audit.Tests | Pass |

| Validation target | Result |
| --- | --- |
| Core.Tests, full suite (`--no-build --no-restore`) | 1,314 passed |
| Tender Surface.Tests, full suite (`--no-build --no-restore`) | 66 passed |
| Relationship Audit.Tests, full suite (`--no-build --no-restore`) | 36 passed, 2 skipped: host cannot create unprivileged symbolic links |
| Web download JavaScript tests | 5 passed; approved retry after sandbox `spawn EPERM` |
| Numbered reporting-skill validator | Passed: 20 table contracts, 35-column diagnostic header, 12 links and whitespace |
| Windows `--ui-smoke-test` | Exit 0; includes explicit native-to-reporting code request |
| `review/Validate-TenderProjectMapping.ps1` | Passed: actual CLI invocation, mapping diagnostic, eleven files, ten manifest rows, Tender 2.0, canonical/reporting identities and unchanged source hashes |
| `git diff --check` | Passed |

The CLI smoke script creates only new synthetic fixtures and retained artifacts under a unique validation directory. It never rewrites the reference fixture or the user's XER. The Core mapping regression tests independently check disk/Web-byte agreement and unchanged non-identity numbered-table values for all four mapping cases.
