# Programme month/metadata browser smoke

Run after the frozen before package and new after package are published locally,
in the coordinator's browser CPU window. This is a small synthetic smoke, not a
performance measurement or hosted deployment check.

```powershell
node --test review/programme-month-metadata-20260930/Check-WebMetadata.test.cjs
node review/programme-month-metadata-20260930/Check-WebMetadata.cjs --url http://127.0.0.1:8767/XER-to-CSV/ --before-url http://127.0.0.1:8766/XER-to-CSV/ --channel chrome
node review/programme-month-metadata-20260930/Check-WebMetadata.cjs --url http://127.0.0.1:8767/XER-to-CSV/ --before-url http://127.0.0.1:8766/XER-to-CSV/ --channel msedge
```

`--fixture` selects the original single-project synthetic fixture. Its P1/T1/W1
identities and Jan-30 Data Date are deliberately checked; use a matching fixture.
`--out` changes the JSON evidence location. Defaults are ignored files under
`artifacts/programme-month-metadata-20260930/`, including downloaded ZIPs.
Dependencies use the existing portable helper: ordinary Node packages first,
then `PARSER_PERF_NODE_MODULES`, then the user's bundled runtime.

The smoke requires actual Worker metadata/results and checks unknown/governed
filename month prefills, preserved kind/tag, conflict warnings, and retained
metadata after Standard-to-Programme switching. Its second synthetic project
has Feb-25 Data Date, valid P2/W2/T3 membership and a distinct activity code.
Programme must accept both projects, show explicit warnings, retain Jan-30 and
Feb-25 in table 02, and retain the governed Feb-25 task snapshot date in table 01.
One missing project date must leave month/date blank for manual entry. Tender's
single-project export and rejection of a multi-project file are checked.

Every export must contain all eleven numbered tables and its manifest. The
shared verifier checks CRCs, exact CSV column counts, all declared row totals,
source/table hashes and manifest completeness. Headers must match the frozen
before contract exactly. With `--before-url`, equivalent single-project metadata
is submitted to both versions and all extracted CSV bytes/row order are compared;
only validated manifest export timestamps and their derived identity/path are
normalized. Without `--before-url`, `--reference-report` can identify an existing
performance browser report for header comparison only; this does not claim full
before/after byte parity.

Reports retain metadata, headers, row counts and hashes, not full diagnostics or
source content. Browser failures set a nonzero exit status and remain in JSON.

## Completed validation, 30 September 2026

The before reference is the complete current performance implementation at
`55c1dfd`, rather than the older published parser. Its isolated Core source archive
has SHA-256 `CF7C1B4A294CAF4A74047CCEE7642E83D496BBE51C016E15E6C36D77EF1260CE`.
Evidence stays under ignored `artifacts/programme-month-metadata-20260930/`.

| Check | Result | Evidence |
| --- | --- | --- |
| Complete Core suite | 1,495 passed | `tests/core.trx` |
| Web/CLI surface suite | 126 passed | `tests/surface.trx` |
| Relationship CLI suite | 36 passed; two existing Windows symbolic-link skips | `tests/relationship-cli.trx` |
| JavaScript download/upload/worker suites | 28 passed | `web-tests.tap` |
| Browser verifier regressions | Five passed | `smoke-harness-tests.tap` |
| Before/after native compatibility | 32 cases, two iterations each; zero differences | `native-parity.json`, `native-before/report.json`, `native-after/report.json` |
| Chrome 155.0.8059.12 and Edge 154.0.4258.37 | Eight behaviour checks, six exports and two exact before/after comparisons each | `web-metadata-chrome.json`, `web-metadata-msedge.json` |
| Release Web and worker publish | Successful, no build warnings/errors | `publish.log` |

Native comparison covers both real XERs, Programme history, repeated Tender
stages, evolving schemas, malformed values and missing fields. Extracted file
sets, CSV bytes, headers, row order, manifest values/hashes, complete diagnostic
CSV bytes and rejected-input errors match for previously supported inputs.
The new multi-project cases are separately covered by 18 Core regressions and
the actual worker/browser checks; they deliberately replace the former
Programme multi-project rejection with warnings.

Month controls store `yyyy-MM` and submit `yyyy-MM-01` through the existing
`DateOnly` request contract. The exact P6 date remains editable. For a valid
multi-project XER, the latest date suggests the file metadata, each native date
remains in table 02, and the existing snapshot date still controls table 01,
history, canonical names and manifest metadata. Missing/invalid project dates
require a manual file date and retain raw row diagnostics. Shared activity codes
or duplicate native IDs retain the existing ambiguous-history safeguards.

Programme schema 5.0, Tender schema 4.0, eleven numbered tables plus manifest,
snapshot retention, file naming and key formats are unchanged. Tender's existing
single-native-project admission rule is preserved. The Programme report loader
was checked read-only: its table-02 contract validates dates, unique project keys
and row counts without requiring every project date to equal the manifest date.
No Power BI project was edited or refreshed. Evidence is local; this work has
not been published to GitHub Pages and does not claim a new performance result.
