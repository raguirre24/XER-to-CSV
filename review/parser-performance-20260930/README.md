These harnesses compare the frozen working tree at commit `90b98d3` with the candidate using actual streaming ZIP APIs, `CompressionLevel.Fastest`, parser version `2.6.0`, and a fixed export timestamp. Schedule files, downloaded bundles, extracted CSVs, complete diagnostic sidecars, timings, and JSON reports stay in the ignored `artifacts/parser-performance-20260930` directory.

`Invoke-StreamingParity.ps1` builds and runs the frozen Core before the candidate. Its 32 cases cover the two real schedules, all three profiles, raw/selected/Enhanced Legacy exports, programme history, ordered repeated Tender inputs, changing Tender schemas, malformed values, omitted cells, missing headers, and rejected duplicate headers. Comparison includes file sets, UTF-8 byte hashes, CSV row order, manifests, full diagnostic CSVs, and rejection type/message. Native timing is secondary evidence and does not establish browser speed.

```powershell
./review/parser-performance-20260930/Invoke-StreamingParity.ps1
```

For the final report's R3 artifact names, preserve the original `native-baseline/report.json`, build `ParserParity.csproj` with `-p:HarnessVariant=candidate-r3 -p:EnablePhaseTelemetry=true`, then run `bin/candidate-r3/Release/net8.0/ParserStreamingParity.dll` with `--repo . --label candidate-r3 --output artifacts/parser-performance-20260930/native-candidate-r3 --iterations 2`. Run that executable again with `--compare artifacts/parser-performance-20260930/native-baseline/report.json --against artifacts/parser-performance-20260930/native-candidate-r3/report.json --output artifacts/parser-performance-20260930/native-r3-parity.json`. The candidate label matters; renaming an older report is insufficient.

Capture `native-candidate-r3/build-evidence.json` from that build and the actual passing test/comparison reports. Required fields are `CoreAssemblySha256` (the built `XerToCsvConverter.Core.dll` digest), `CoreTestsPassed` (the final Core TRX passed count), `NativeParityCases` and `NativeParityFailures` (the comparison's `Cases` and `Failures`). The report reconciles these with the final publish's Core digest, actual TRX and native comparison. Keep publish logs, version information and asset digests in `release-evidence/` alongside `release-summary.json`; do not substitute counts from an earlier revision.

Optional phase attribution uses a separate clone. Extract `frozen-current.zip` to `artifacts/parser-performance-20260930/baseline`, then copy its `XerToCsvConverter.Core` directory to the sibling `XerToCsvConverter.Core.Telemetry`. Apply the observer overlay and prepare the separately copied frozen Web:

```powershell
./review/parser-performance-20260930/Apply-FrozenPhaseTelemetry.ps1 `
    -CoreRoot './artifacts/parser-performance-20260930/baseline/XerToCsvConverter.Core.Telemetry'
./review/parser-performance-20260930/Prepare-FrozenObserverWeb.ps1
./local-dotnet-sdk/dotnet.exe publish `
    artifacts/parser-performance-20260930/baseline/XerToCsvConverter.Web.Telemetry/XerToCsvConverter.Web.csproj `
    --configuration Release -p:PublishProfile= --output artifacts/parser-performance-20260930/baseline-observer-web
node review/parser-performance-20260930/serve-browser.cjs artifacts/parser-performance-20260930 baseline-observer
```

The observer-only browser is served on port `8768`, independently of the original baseline on `8766` and the candidate on `8767`. Its upload and operation records appear in `window.__frozenParserPhaseRuns`. Every record supplies all ten Core `exclusive.<Phase>` scalars, including explicit zero where that operation has no such Core phase. In particular, original Legacy processing does not calculate a source SHA-256. The original implementation has no instrumentation for browser result serialization, transfer, preparation or handoff: those measurements are unavailable, rather than assumed zero. Review requests use the fixed fixture export timestamp `2026-09-30T00:00:00Z` to make extracted manifest comparison reproducible. This observer package is excluded from primary speed comparisons.

The overlay copies the optional telemetry helper and adds timers at existing phase boundaries. It retains frozen transformations, validators, sorting, merge logic, calendars, CSV writers, JSON serializers, date formatting, and cache behaviour. `frozen-observer-hunks.patch` records the inserted Core/Web hooks; the new telemetry helper is shared directly from Core. The scripts and artifact evidence record the frozen archive/file hashes and resulting observer source/package hashes. The frozen archive SHA-256 is `B356083271EB1A32E65B6C8CF3B81D4B4A925BAB307A1C6602407DE8E3B5C4E8`.

Exclusive phase totals remove nested work. Inclusive values describe the whole scope and must not be added together. Concurrent overlap is explicitly flagged. Inline cell conversion and normalisation stay in transformation; standalone validation measures the separate checks. CSV serialization, digest computation, and ZIP writes/finalization have distinct buffered timers. Operation elapsed time can additionally include unmeasured setup, UI work, and download handoff.

The final observer-only Chrome attribution run measured standalone validation at 165.4 ms / 9,530.3 ms for Legacy (1.7%), 1,203.1 ms / 26,170.8 ms for Programme (4.6%) and 1,213.6 ms / 25,469.1 ms for Tender (4.8%). Transformation occupied 69.5%, 76.7% and 75.8% respectively. Shares divide exclusive phase duration by that observation's processing duration. Inline conversion/data checks remain inside Transformation, so these figures do not isolate all validation-related work. The separate observer package is excluded from the primary matched speed comparison; its outputs have the independent 32-case frozen parity proof.

To build a timed native harness against the observer Core, pass its project with `-BaselineCoreProject`, add `-PhaseTelemetry`, and choose a new `-EvidenceSuffix`. Phase fields do not affect the byte/diagnostic parity comparison. `native-baseline-telemetry-parity.json` confirms all 32 observer-only cases match the original frozen outputs.

**Implementation review and automated evidence, 30 September 2026**

The raw cache key contains the input occurrence token, content SHA-256, and parser version. Identical content and filenames remain independent occurrences. Browser reads verify length and the sealed content digest before parsing or retention; the private WebCrypto bridge hashes the same buffer it copied into managed memory, with managed SHA fallback when unavailable. Public mutable byte/file APIs retain their input mutation checks. Each export rebinds source identity and current request metadata through owned table/row lists; raw field arrays and original evidence stay unchanged. Programme governance selects retained snapshots before parsing, so superseded malformed inputs are still hashed without entering the retained union. Standard derived stores are invalidated on source/profile changes, removal, and clear. Cancellation and failed ZIP publication return no successful result and allow a clean retry.

The cache admission estimate now includes both live and original arrays, excess original cells discarded from the aligned live row, original strings that are not shared with live strings, header text, and unused table row-list slots. Original raw arrays remain private. The private browser parser starts each table with 64 slots and grows as needed; the public parameterless parser keeps the existing configured capacity behaviour. Empty owned source views reserve one slot. The 256 MiB budget bounds this conservative estimate, rather than active or peak runtime/browser memory. Streamed CSV buffers, derived tables, input copies, ZIP buffers, managed intern/date caches, and browser objects contribute additional temporary or active memory.

Review conversion caches store successful context-free conversions only, keyed by type and trimmed input, with limits of 8,192 entries and an estimated 2 MiB. Required/blank checks run before cache lookup, and malformed values still produce their own originating row/column diagnostics. Text retains its existing whitespace behaviour. Programme history columns are normalised after history calculation; previously projected cells are not parsed again. Bound readers preserve primary-header precedence, alias order, case-insensitive lookup, and the distinction between absent headers, omitted cells, and explicit blanks. Tender union merge keeps first-seen header order and every source row, remapping only incoming schemas or genuinely widened earlier schemas while preserving original raw schema and evidence identity.

Sorting materialises published U+001F-joined ordinal keys once per row and lazily materialises full-row tie keys. Final source ordering and original input order resolve complete ties. The allocation-free virtual joined comparator remains a differential oracle. Embedded separators, empty/control characters, Unicode, surrogate code units, separator collisions, and stable complete ties match the published `string.Join`/ordinal behaviour. This intentionally corrects the frozen working tree's tuple-comparator edge case; ordinary fixture/schedule order is byte-identical.

Buffered Legacy CSV retains UTF-8/BOM behaviour, platform newline, original filename provenance, ragged alignment, null handling, whitespace rules, quoting, and doubled quotes. Review CSV retains strict UTF-8 without BOM, CRLF, fixed schemas, and its existing whitespace quoting rules. Writers retain caller stream ownership and check cancellation. Source-generated raw-row, Tender identity, and relationship evidence JSON are byte-checked against their former reflection serializers, including property order, null/empty values, control characters, and Unicode. Browser session parity assertions compare complete diagnostic values and raw-field presence states as well as diagnostic CSV hashes and displayed messages.

The final compact-parser revision compiled with zero warnings/errors. Automated results available in the ignored artifact evidence are:

| Check | Result | Evidence |
| --- | --- | --- |
| Complete Core suite, compact parser/cache revision | 1,469 passed; 0 failed/skipped | `final-tests/final-core-r3.trx` |
| Compact parser, cache accounting, browser session/hash focused suite | 30 passed; 0 failed/skipped | `final-tests/cache-parser-session-r3.trx` |
| Web/CLI surface suite | 100 passed; 0 failed/skipped | `final-tests/final-surface.trx` |
| Relationship CLI suite | 36 passed; 0 failed; 2 symbolic-link cases skipped because this host disallows unprivileged symbolic-link creation | `final-tests/final-relationship-cli.trx` |
| Frozen observer overlay output parity | 32/32 cases; 0 differences | `native-baseline-telemetry-parity.json` |
| Candidate streaming parity before the compact-parser revision | 32/32 cases; 0 differences | `native-final-parity.json` |
| Candidate streaming parity after compact parser, cache accounting, first-source adoption, and private relationship changes | 32/32 cases; 0 differences; both iterations agree | `native-r3-parity.json` |

Relationship/operation-sharing differential checks additionally passed 562 cases, including the frozen full-evidence oracle and private exported free-float/status/basis/reason columns. These checks establish automated contracts and parity; browser speed, responsiveness, cancellation lifecycle, and memory behaviour are measured separately against the published packages.

For native cache estimates, build the candidate harness with `-p:EnableCacheMeasurements=true`, then run:

```powershell
./local-dotnet-sdk/dotnet.exe build review/parser-performance-20260930/ParserParity.csproj `
    --configuration Release -p:HarnessVariant=candidate-r3 -p:EnablePhaseTelemetry=true -p:EnableCacheMeasurements=true
./local-dotnet-sdk/dotnet.exe review/parser-performance-20260930/bin/candidate-r3/Release/net8.0/ParserStreamingParity.dll `
    --repo . --cache-estimates true --output artifacts/parser-performance-20260930/native-cache-estimates-r3
```

The two real sources were parsed through the private 64-slot parser. August's 13,424,712 source bytes produced a retained estimate of 144,029,890 bytes; July's 10,964,955 source bytes produced 102,204,578 bytes. The combined 246,234,468 bytes fit the 268,435,456-byte budget, and both occurrences remained retained. Native estimate scans took 120.8 ms and 74.9 ms in this run. These values are reproducible formula estimates and native timings, not actual heap measurements or WASM latency predictions. Complete row/capacity/source-hash details are in `native-cache-estimates-r3/report.json`.

The verified R3 native Core assembly SHA-256 is `5E680B3482F6B5850CA090E200F4DAFA0C71FF6B094BC57CDDA44915C2D18A59`. Streaming parity includes complete diagnostic CSVs and extracted CSV hashes/row order, file sets, manifest reconciliation, repeated-stage identity, changing schemas, and malformed-input rejection type/message. ZIP container timestamps are excluded from this native comparison; deterministic Tender ZIP timestamps and repeated ZIP bytes have separate streaming unit coverage.

Native performance is secondary. `native-exclusive-phases.csv` records the frozen observer and earlier candidate second-run phase timings from separate batches. For the frozen observer, standalone checks accounted for 84.1 ms of 3,119.3 ms in Legacy Enhanced, 268.5 ms of 4,753.9 ms in Programme single, and 260.5 ms of 4,705.6 ms in Tender single. Inline source-cell conversion remains inside transformation. Separate batches showed substantial timing variation, so these native figures are not a matched speed comparison and do not establish a 25% browser improvement.

**Final browser reproduction**

Run from the repository root, with installed Chrome and Edge and Node `22.14.0`. The dependency helper first tries ordinary module resolution, then `PARSER_PERF_NODE_MODULES`, then the bundled Codex runtime directory. For a portable setup independent of that runtime, install the exact measured library versions into the ignored artifact tree:

```powershell
$taskBrowserDependencies = Join-Path (Resolve-Path .).Path 'artifacts/parser-performance-20260930/browser-deps'
$env:PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = '1'
npm install --prefix $taskBrowserDependencies --no-audit --no-fund playwright@1.62.1 jszip@3.10.1
$env:PARSER_PERF_NODE_MODULES = Join-Path $taskBrowserDependencies 'node_modules'
```

The harness launches the installed `chrome` and `msedge` channels, so it does not require a separate Playwright browser download. Match SDK `8.0.421`, WebAssembly runtime `8.0.27`, Release interpreter builds with no AOT, and `CompressionLevel.Fastest`. On a fresh SDK installation, install its `wasm-tools` workload before publishing. The machine configuration captured in `machine.json` is Windows build 26200, Snapdragon X1E80100 with 12 cores at 3.40 GHz, 15.6 GiB physical memory and Node 22.14.0; report browser versions come from each installed channel at measurement time.

Build packages before measuring. Extract the original frozen archive to the baseline directory, and publish its Web project independently from the candidate:

```powershell
./local-dotnet-sdk/dotnet.exe publish `
    artifacts/parser-performance-20260930/baseline/XerToCsvConverter.Web/XerToCsvConverter.Web.csproj `
    --configuration Release -p:PublishProfile= --output artifacts/parser-performance-20260930/baseline-web
./local-dotnet-sdk/dotnet.exe publish XerToCsvConverter.Web/XerToCsvConverter.Web.csproj `
    --configuration Release -p:PublishProfile= --output artifacts/parser-performance-20260930/candidate-web
```

Preserve the publish log, asset SHA-256 inventory, SDK/runtime versions, original frozen archive hash, native Core hash and release evidence. The packaged candidate includes the dedicated worker. The local server replaces the version placeholder consistently and serves all packages under `/XER-to-CSV/`, exercising the GitHub Pages base path. After the optional observer package has also been published, start one server in a separate terminal:

```powershell
node review/parser-performance-20260930/serve-browser.cjs artifacts/parser-performance-20260930
```

This starts ports `8766`, `8767` and `8768`; do not start another server on the same ports. Run browser workloads sequentially with no builds, native benchmarks or other browser experiments competing for CPU. Primary workloads use the real August source and the July history source at the paths in `benchmark-browser.cjs`. Change those fixture paths to locally available copies with matching source hashes when reproducing elsewhere.

```powershell
$env:BROWSERS = 'chrome,msedge'
$env:MODES = 'legacy,programme,tender'
$env:VARIANTS = 'baseline,candidate'
$env:PAIRS = '5'
$env:HISTORY = '1'
Remove-Item Env:APPEND,Env:BASELINE_URL,Env:REPORT_FILE,Env:ZIP_PREFIX -ErrorAction SilentlyContinue
node review/parser-performance-20260930/benchmark-browser.cjs
```

There is one warm-up per implementation/mode followed by five alternating matched pairs and a separate second-source/history pair. Each primary iteration removes previous sources and registers fresh occurrence tokens. The candidate's normal 256 MiB cache remains enabled, and the harness requires every new source to be parsed with zero hits. Processing time starts at Parse/Validate and ends at browser download handoff; file admission/metadata and UI startup are recorded separately.

Collect the observer-only frozen phase run separately. Its timer overhead is excluded from primary speed comparisons:

```powershell
$env:BROWSERS = 'chrome'
$env:VARIANTS = 'baseline'
$env:PAIRS = '1'
$env:BASELINE_URL = 'http://127.0.0.1:8768/XER-to-CSV/'
$env:REPORT_FILE = 'browser-observer-phases.json'
$env:ZIP_PREFIX = 'observer'
Remove-Item Env:HISTORY,Env:APPEND -ErrorAction SilentlyContinue
node review/parser-performance-20260930/benchmark-browser.cjs
Remove-Item Env:BASELINE_URL,Env:REPORT_FILE,Env:ZIP_PREFIX -ErrorAction SilentlyContinue
```

Run the full cancellation/download/lifecycle and incremental cache scenarios in both channels. The real-source parameters prevent a small synthetic schedule from being mistaken for the real memory/update test:

```powershell
$taskAugustSource = (Resolve-Path 'XER Sample/2608-EBA_PAA_8.0 (draft).xer').Path
$taskJulySource = 'C:/Users/ricar/Downloads/2607 C5064-C-2607.xer'
foreach ($taskChannel in @('chrome', 'msedge')) {
    node review/parser-performance-20260930/BrowserLifecycle.cjs `
        --channel $taskChannel --bulk-rows 30000 --memory-fixture $taskAugustSource --cycles 10
    if ($LASTEXITCODE -ne 0) { throw "$taskChannel lifecycle failed" }
    node review/parser-performance-20260930/BrowserIncremental.cjs `
        --channel $taskChannel --pairs 5 --real-primary $taskAugustSource --real-prior $taskJulySource --real-pairs 1
    if ($LASTEXITCODE -ne 0) { throw "$taskChannel incremental checks failed" }
}
```

Lifecycle evidence keeps ten complete clear/restart cycles separate from hot update/export/remove cycles. The top-level hot `.cycles` preserves the real 13.4 MB baseline, replaces a small synthetic update, reuses one cached source and exports selected Enhanced tables `01_XER_TASK`, `06_XER_PREDECESSOR` and `15_XER_RESOURCE_DISTRIBUTION`. This covers expensive calculations and the persistent Standard view. Nested `.reviewProfiles.programme` and `.reviewProfiles.tender` each add ten cycles using the small J5001 synthetic baseline plus successive February–November updates, complete review bundles and changing project-name metadata. Each profile retains one worker throughout its ten cycles, discards the prepared outputs, then clears cache/source counts to zero. These scopes cover logical retention and full review lifetimes; they do not establish real-history review peaks or all-fourteen-table Enhanced peaks.

Per-cycle retained cache estimates, source counts, cache hits, worker counts, managed heap samples, collected page heap, process working/private samples and allocation counters are retained. First/last/range summaries describe the observed samples; they do not establish total-memory peaks or a forced plateau. Collection of the main-page heap excludes worker/native Blob storage. Per-cycle and cumulative allocations count metadata/parse/export jobs for Enhanced and metadata/full-review jobs for each review profile, resetting cumulative work separately for each profile. Allocation counters remain separate from retained-memory measurements.

Collected main-page heap also includes the harness's accumulated event/progress and Blob-URL/worker descriptor histories. Those histories retain descriptors rather than Blob/Worker objects, but their instrumentation overhead means the small observed page-heap increases are not a clean application-only leak metric. The `--bulk-rows 30000` flag is required for both real cancellation checks and the complete nine-check lifecycle result.

Managed runtime allocation counters are coarse and can return zero for small review jobs. A reported zero does not prove that the operation allocated nothing. Cumulative allocation represents the reported managed job-counter deltas, rather than complete process allocation or retained memory.

Incremental reports contain `summaries` for dataset/mode/scenario, matched pair count and before/after median milliseconds, plus byte/manifest/order comparisons and actual worker job results. Five synthetic pairs cover repeated export, adding an update, eviction/reread and review metadata edits; one real pair covers repeated export and adding the July/August source. Real-source scenarios remain separate from synthetic timings.

Frozen Legacy releases `_dataStore` after every export. Its repeated-export measurement therefore includes the required Parse click before Export, whereas the candidate retains its parsed raw/Standard view and exports directly. This compares the actual user actions required by the two packages and includes the intended streaming/session retention change. Frozen review exports already reparse their inputs internally. Subsecond synthetic timings may regress or vary through timing noise; they remain separate from the single real-source incremental observations and the primary five-pair performance target.

After all browser runs finish, compare every measured primary/history and incremental ZIP. Only validated export timestamps and their matching bundle-directory bytes may differ; other CSV cells, row order, file sets, source identities and complete diagnostic digests are preserved:

```powershell
node review/parser-performance-20260930/Compare-BrowserOutputs.cjs `
    --report 'artifacts/parser-performance-20260930/browser-benchmark.json,artifacts/parser-performance-20260930/browser-incremental-chrome.json,artifacts/parser-performance-20260930/browser-incremental-msedge.json' `
    --out artifacts/parser-performance-20260930/browser-output-parity.json
node --test review/parser-performance-20260930/Compare-BrowserOutputs.test.cjs `
    review/parser-performance-20260930/PerformanceReportEvidence.test.cjs
node --test XerToCsvConverter.Web/tests/downloads.test.cjs `
    XerToCsvConverter.Web/tests/fileInput.test.cjs XerToCsvConverter.Web/tests/workerEngine.test.cjs `
    > artifacts/parser-performance-20260930/release-evidence/final-web-node-tests.tap
```

Keep the complete final TAP output and record `WebNodeTestsPassed` plus `WebNodeTestsEvidence = "release-evidence/final-web-node-tests.tap"` in `release-summary.json`. Refresh the release evidence only after checking the command exit code. The generator parses and reconciles the actual TAP counts; a scalar count alone is insufficient.

Generate the report only when all final evidence is complete:

```powershell
node review/parser-performance-20260930/Create-PerformanceReport.cjs
git diff --check
```

The generator writes tracked `before-after-report.html` and ignored `final-summary.json`. It requires `native-r3-parity.json`, the R3 native build/Core SHA proof, `final-core-r3.trx` (1,469 passing tests including all eight `ParserCompactCapacityTests` cases), Surface/relationship CLI TRX, full final Node TAP, both latest lifecycle and incremental reports, observer phase evidence, machine context, and output parity over the current ZIPs. Browser reports must postdate the final publish log, and lifecycle/incremental worker hashes must match the release asset inventory. Missing, incomplete, failed or stale final artifacts stop generation; older filenames are never substituted. The compact-parser tests belong to the parser/cache stage E fixup.

Independent commit-stage verification runs after primary browser timings finish:

```powershell
./review/parser-performance-20260930/Invoke-StageIsolation.ps1 -Run -NativeParity
```

Without `-Run`, the script only resolves commits and checks tracked dependencies. Execution creates separate Git archives for stages A, C, B and E beneath `artifacts/parser-performance-20260930/stage-validation/<UTC-GUID>/`, supplies the same SDK pin, and runs strict Core/relationship CLI suites plus two iterations of all 32 streaming parity cases for each archived Core. Fixture locator evidence verifies fixed-relative and ancestor lookups stay inside each archive. Historical source-text surface failures are explicitly excluded from the archived stages; the final D behaviour suite is verified separately. Record the resulting relative `stage-validation/<UTC-GUID>/stage-validation.json` path as `StageIsolationEvidence` in `release-summary.json` only after every stage passes. The report requires this successful evidence, its TRX counters, both fixture locator checks and all four 32-case parity proofs.

The final aggregate reused the exact passed A/C commit archives after correcting a B/E commit boundary, then built the corrected B/E archives. Reuse is recorded through `ReusedFromEvidence`; the verifier rereads the original TRX, native output iterations and fixture locator proof rather than assuming the earlier results. The report verifies those reused paths still point to the same passing stage/commit/archive and checks the original counters and parity files.

Primary tables report both reduction of the before/after medians and the median/range of individually matched pair reductions. The target uses `1 − median(after) / median(before)`; the paired statistic separately exposes runtime tiering and variability. Native timings remain secondary and are not credited towards the browser target. Missing phase/process measurements render as `unavailable`, while explicit measured zero remains zero.

TRX skipped counts use `max(notExecuted, total − executed)`. The relationship CLI records 38 discovered tests, 36 executed/passed and `notExecuted=0`; its two Windows platform skips remain visible through the discovered/executed difference. Final and isolated-stage summaries use the same interpretation.
