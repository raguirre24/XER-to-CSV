# Pre-commit review: XER parser and reporting exports

Review date: 2026-09-05. Workspace: `C:\Users\ricar\Documents\Code\XER to CSV`.

## Current status

The user subsequently approved remediation. All 16 findings below are now resolved
in the working tree; see [Remediation results](REMEDIATION_RESULTS.md) for the
corrected contracts, final validation and remaining manual acceptance boundaries.
The original verdict and findings are retained below as historical review evidence,
not as descriptions of the remediated implementation. Nothing has been committed
or pushed.

## Original review verdict (superseded by remediation)

**NOT READY TO COMMIT OR PUSH as a completed, correctness-approved change.**

The working tree contains useful, extensively tested calendar, relationship-delay,
resource-distribution and Tender Review changes. Nevertheless, the integrated review
found unresolved data-loss, source-identity, export-completeness and consistency
issues. Some are longstanding Standard limitations; others are newly consequential
interactions with the stricter calculations. Passing the existing calculation tests
does not establish that an entire multi-input reporting export is correct.

This audit does not authorize or implement the fixes. Production files were not
changed for the review. Review artifacts and diagnostic characterization tests were
added separately. No commit, push, SharePoint publication, report/visual change, or
edit to the sibling Tender-Review repository is part of this review.

All seven requested incremental builds and both test targets passed. The browser
workflows reached their logged export-completion states, but actual downloaded-file
delivery and integrity were not verified. These results do not resolve the findings.

## Scope and evidence boundaries

- Reviewed all 14 implemented numbered Enhanced tables: 01, 02, 03, 04 and 06-15.
  Number 05 is intentionally absent from the implemented table inventory; it is not
  a missing implementation discovered by this audit.
- Reviewed shared parsing/merge, calendar models, derived values, row grain, key
  generation, null handling, raw/enhanced CSV writing and both review profiles.
- Reviewed Web upload staging, inferred metadata and download/cancellation handoff;
  browser workflow observations are recorded separately from code-proven risks.
- Reviewed the current source, existing calculation documentation, P6 skill and its
  XER table reference. Relevant P6 meanings were checked against Oracle sources.
- Reviewed Standard, Programme Review 3.0 and Tender Review 1.0 as distinct contracts.
  A deliberate difference is not automatically a bug.
- Source references below were checked against the current working tree. Line numbers
  can move when fixes are subsequently made.
- "Pre-existing" means the problematic logic was also observed in `HEAD`, not that
  the current user has approved retaining its consequences indefinitely.
- Diagnostic tests describe present behavior, including defects. They are not golden
  correctness expectations and must not be presented as proof that those behaviors
  are desirable. Change their assertions when a corrective contract is authorized.

## Prioritized findings

### F01 - P1: mixed-schema merging silently removes curve and other source fields

**Classification:** pre-existing merge defect; newly consequential resource-curve
interaction. Affects Standard and any caller using the shared legacy merge;
Programme file/byte processing also relies on that shared processing path. Tender's
separate union-schema merge avoids this specific defect.

[CoreLogic.cs:575](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:575>)
allocates incoming rows using the existing target header length, and lines 603-605
copy only fields already present in the target. Incoming-only columns are discarded.
Desktop parsing collects stores in a `ConcurrentBag` at line 3280 and merges its
enumeration at lines 3348-3350, so the schema selected as the target is not governed
by the ordered input list.

Counterexample: one TASKRSRC schema has no `remain_crv` or `curv_id`; a later input
contains `remain_crv = 80:8;20:8`. The latter assignment independently produces
January/February quantities **80/20**. After legacy merging its curve fields are
absent, so the same assignment silently produces **50/50**. The same mechanism can
discard remaining endpoint dates, scheduling options or newer raw attributes.

**Required direction:** union headers deterministically while preserving every
ordered input occurrence and every source-qualified field value. Do not replace
this with filename-keyed dictionaries. Verify identical results under input order,
schema order and execution-order permutations. Tender's implementation at
[TenderReviewBundleService.cs:479](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/TenderReview/TenderReviewBundleService.cs:479>)
provides an existing union-schema pattern, but changes to Standard/Programme need
their own compatibility review.

### F02 - P1: Standard inputs still use filenames as identity

**Classification:** pre-existing Standard limitation; not a defect in Tender's
ordered token implementation.

[CoreLogic.cs:926](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:926>)
reduces a filesystem path to its basename. The stream entry point passes the supplied
filename into parsing at lines 3523-3525. `CreateKey` at lines 1479-1487 concatenates
that value and the native ID.

Counterexample: two ordered inputs named `2601.xer`, both with PROJECT ID `1`, produce
two rows with `proj_id_key = 2601.xer.1`. Old lookup dictionaries can select the wrong
calendar/project/task; new strict 06/11/15 lookups can instead reject the merged
identities. Neither outcome fulfills repeated-input source isolation.

**Required direction:** separate internal occurrence identity from display filename
throughout the Standard batch, while explicitly deciding how to preserve or version
its public keys and filename-derived metadata. Original filename, path and hash are
lineage attributes, not occurrence identity. Programme currently rejects ambiguous
filenames/content by design; Tender accepts repeated names, paths and hashes using
stable tokens. Do not accidentally erase those intended profile distinctions.

### F03 - P1: displayed activity/relationship dates disagree with effective calculation dates

**Classification:** pre-existing legacy date selection, newly exposed by corrected
free-float and Tender date derivation.

[CoreLogic.cs:2065](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:2065>)
selects `early_start_date` for an unstarted activity, ignoring `restart_date`.
`CalculateFinishDate` at line 2107 selects `early_end_date`, then `late_end_date`,
ignoring `reend_date`. Table 06's displayed successor/predecessor dates at
[CoreLogic.cs:2908](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:2908>)
also use the old early endpoints.

Counterexample: an active activity has early finish January 5 and remaining finish
February 3. Standard 01 reports January 5; remaining resource allocation uses its
assignment remaining period. Corrected 06 free float uses remaining task endpoints.
Tender 01 already prefers `restart_date`/`reend_date`
([TenderReviewTransformer.cs:516](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/TenderReview/TenderReviewTransformer.cs:516>)),
but its 06 endpoint display columns still project the legacy early dates. A profile
can therefore publish different dates for the same endpoint across 01 and 06 even
when the free-float value itself is correctly calculated from remaining endpoints.

**Required direction:** define display-event semantics explicitly, reconcile 01/06
and the calculation inputs, distinguish absent from malformed preferred dates and
avoid substituting late dates for unknown early forecasts without an explicit policy.
Preserve actual Start/Finish semantics for progressed/completed activities. Do not
replace the already-correct Tender remaining-date preference with the legacy behavior.

### F04 - P1: Standard can publish incomplete exports and retain stale empty tables

**Classification:** pre-existing Standard export-completeness problem.

[CoreLogic.cs:3455](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:3455>)
and the in-memory path at line 3605 export only nonempty tables. Failed-generation
validation at lines 3629-3633 covers 06/11/15, not every requested table.

Counterexamples:

- A regenerated 15 has no quantities and is therefore empty. The method skips it;
  an existing `15_XER_RESOURCE_DISTRIBUTION.csv` in the selected folder is not
  replaced with a header-only file and can continue feeding obsolete quantities.
- TASK and PROJECT are present but CALENDAR is absent. A request for 01 and 02
  returns an export containing only 02, with no explicit failure for missing 01.

**Required direction:** distinguish legitimately empty, unsupported, missing-source
and failed-calculation outcomes. Emit the agreed header-only schema for empty
requested tables, reject failed required outputs before publication, and define a
complete export-set replacement policy. Prefer staging plus an atomic publication
boundary where possible. Never delete unrelated files from a user's output folder.

### F05 - P1: raw table names are not contained within the Standard output folder

**Classification:** pre-existing path-safety issue; fixed review-profile filenames
do not share this particular path construction.

The parser accepts arbitrary `%T` names
([CoreLogic.cs:803](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:803>)).
Standard disk export directly uses `Path.Combine(outputDirectory, tableName + ".csv")`
at line 3457; the writer opens the result using `FileMode.Create` at line 1247.

A selected raw table named `..\outside` can resolve outside the chosen output folder
and overwrite an existing CSV there. This finding follows the verified input-to-path
data flow; no overwrite/exploit was performed during the audit.

**Required direction:** validate export table filenames, reject rooted/separator/
traversal names and verify resolved destination containment before opening any file.
Apply equivalent entry-name safety where raw table names are used in downloadable
archives. Keep governed profile filenames fixed.

### F06 - P2: table 06 total float invents an eight-hour day

**Classification:** pre-existing legacy/Programme calculation defect. Tender's
independent total-float conversion correctly requires valid HPD.

[CoreLogic.cs:2965](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:2965>)
defaults unresolved successor HPD to eight. Lines 2994-2998 then convert successor
total float using that invented factor.

Counterexample: 80 hours of successor float and missing calendar HPD produce
**10.0 days** in 06, whereas 01 correctly leaves total float blank. Corrected
relationship `free_float` is also blank. This is inconsistent reliability within
the exported relationship record.

**Required direction:** preserve unknown conversions as blank or fail according to
the selected profile contract; do not invent HPD. Keep relationship free float,
successor activity total float and display lag as separate quantities with separately
documented calendars. Tender's strict conversion is at
[TenderReviewTransformer.cs:591](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/TenderReview/TenderReviewTransformer.cs:591>).

### F07 - P2: Standard table 01 Units % Complete excludes nonlabor

**Classification:** pre-existing calculation defect in the Standard `%` column.
The fixed review-profile 01 schemas do not export this legacy `%` column.

[CoreLogic.cs:2207](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:2207>)
uses only `act_work_qty` and `remain_work_qty`. An active activity with zero labor,
60 actual nonlabor units and 40 remaining nonlabor units is reported as **0.00%**
instead of **60%**. Oracle defines activity Units % Complete over labor and nonlabor
units, while TASK maps `act_equip_qty`/`remain_equip_qty` to the nonlabor amounts.
[Oracle data dictionary](https://docs.oracle.com/cd/G18294_01/English/User_Guides/p6_eppm_data_dictionary/46503.htm),
[Oracle TASK mapping](https://docs.oracle.com/cd/F51303_01/English/Mapping_and_Schema/xer_import_export_data_map_project/97906.htm).

**Required direction:** calculate the selected percent-complete type from its full
documented inputs, retain explicit zero-denominator/unknown-input policies and test
labor-only, nonlabor-only and mixed assignments. Do not introduce material quantities
of incompatible units into the labor/nonlabor time-unit formula.

### F08 - P2: malformed percent-complete values can become nonfinite CSV numbers

**Classification:** pre-existing input-validation defect.

[CoreLogic.cs:2195](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:2195>)
uses `double.TryParse` and clamps without checking finiteness. Active CP_Phys input
`phys_complete_pct = NaN` is exported as `% = NaN`. Other percent-complete paths also
need explicit finite-input checks and should not conflate unknown metadata with a
proven zero percentage.

**Required direction:** reject or preserve unknown malformed percentages according
to contract, rather than emitting nonfinite numbers. No separate physical-percent
scaling defect was established by this review.

### F09 - P2: discarded Programme snapshots can block retained parsed-data exports

**Classification:** pre-existing filtering order with a newly consequential strict
curve-validation interaction. The parsed-data entry points differ from file/byte
entry points in this respect.

[ProgrammeReviewBundleService.cs:215](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/ProgrammeReview/ProgrammeReviewBundleService.cs:215>)
resolves the retained snapshot metadata but passes the whole caller-supplied parsed
store onward at line 220. The transformer calculates shared 15 before projecting
retained sources
([ProgrammeReviewTransformer.cs:38](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/ProgrammeReview/ProgrammeReviewTransformer.cs:38>)).

Counterexample: BL02 is selected; discarded BL01 contains an unsupported or missing
resource curve. The parsed-data export fails because of BL01, although the file/byte
paths parse only retained snapshots and would not evaluate that discarded curve.

**Required direction:** filter a source-preserving parsed-store view to resolved
retained snapshots before shared calculations. Test parity among file, byte and
parsed-data entry points, including discarded malformed calendars/curves and unused
definitions. Do not mutate the caller's original parsed data.

### F10 - P2: mismatched external relationship metadata can become a false local edge

**Classification:** pre-existing Core key derivation plus a new Tender validation
gap. Requires inconsistent/colliding external metadata; ordinary P6 database IDs
are normally unique, so this is a defensive-integrity scenario, not a claim that all
external relationships are wrong.

Core derives endpoint keys from source/native task ID, without checking
`pred_proj_id` for those exported keys
([CoreLogic.cs:2856](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:2856>)).
Tender's raw relationship record drops endpoint project fields at
[TenderReviewTransformer.cs:249](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/TenderReview/TenderReviewTransformer.cs:249>),
and lines 684-697 resolve only source/task ID.

Counterexample: `pred_proj_id=P2`, `pred_task_id=T1`, but only local P1/T1 exists.
The corrected free-float lookup correctly leaves the value blank, yet both review
profiles can publish the edge pointing to local P1/T1. Referential integrity of the
emitted key alone does not establish the relationship's actual source identity.

**Required direction:** carry and validate predecessor/successor project context
through key derivation and profile validation. Reject unsupported external endpoints
or represent them explicitly; never turn an unresolved external edge into a local one.

### F11 - P2: remaining legacy lookups still select ambiguous identities by row order

**Classification:** pre-existing lookup-integrity problem, separate from F02's
repeated-filename scenario.

Calendar conversion and project-data-date lookup assignments overwrite previous
values at
[CoreLogic.cs:1999](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:1999>)
and line 2047. Display task lookup does the same at line 3148.

Counterexample: duplicate source-local C1 rows specify 8 and 10 HPD. Table 01 converts
80 hours into 8 days if 10 HPD is last; reversing the order changes the answer. Table
11 rejects the same ambiguous calendar, and the new relationship engine does not
trust it. Project data dates and relationship display metadata have similar old
last-row-wins exposure.

**Required direction:** use source-qualified identities and explicit ambiguity
handling consistently for calculated/display values. Detect duplicates rather than
selecting an arbitrary row; preserve the separate grain of legitimate task/resource
assignments and activities in different source occurrences.

### F12 - P2: WBS parent membership validation is not hierarchy validation

**Classification:** pre-existing malformed-input/hierarchy integrity gap.

[CoreLogic.cs:2524](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:2524>)
only checks whether a parent key exists anywhere in the generated WBS-key set.
W1 -> W2 -> W1 survives because both parents exist. Duplicate identities,
project-inconsistent ancestry and cycles are not established as safe by membership.

**Required direction:** add source/project-aware ancestry validation with cycle and
duplicate detection. Define root/orphan behavior explicitly. The current clearing
of missing parents is intentional legacy behavior; do not silently replace it with
a different hierarchy policy without agreement. No flattening into WBS levels is
performed by table 03 itself.

### F13 - P2: Programme filename inference mistakes project numbers for update dates

**Classification:** pre-existing Web metadata inference defect; reproduced in the
browser, not merely inferred from a regular expression.

[Index.razor:1002](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Web/Pages/Index.razor:1002>)
accepts any isolated four-digit sequence whose last two digits are a valid month.
For `J5001_C_BL01_2026-01-31.xer`, it selects `5001` from the project code and
`MonthStartFromTag` at lines 1023-1024 returns **2050-01-01**. Initial staging sets
`SnapshotKind=Baseline` and `SnapshotTag=BL01`, but still applies this update-derived
MonthUpdate at lines 445-447. The root agent observed this exact incorrect default
in the Programme form and corrected it through the UI before continuing export.

**Required direction:** use the governed filename grammar and detected metadata,
avoid applying update inference to baseline rows, and leave genuinely ambiguous
values for explicit user entry. Test project codes containing plausible YYMM digits
and filenames containing full ISO dates; never silently change the source Data Date.

### F14 - P2: asynchronous Web uploads can survive Clear All and reuse source tokens

**Classification:** pre-existing upload-lifecycle race; newly consequential Tender
ordinal-reset interaction. The interleaving is code-proven; a rendered-browser race
reproduction was not completed.

[Index.razor:387](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Web/Pages/Index.razor:387>)
does not establish an upload-busy state or operation generation. It captures profile
limits and the existing byte total, then awaits file reads at lines 430-434. It
allocates tokens at line 441 and commits a pending batch at lines 459-462 after
checking only the current `_isProcessing` flag. `ClearFiles` at lines 492-497 clears
the visible list and resets `_nextTenderSourceOrdinal` without invalidating uploads.

Counterexample: a multi-file batch has already staged token 0 and is awaiting its
next file. Clear All resets the ordinal; a new upload or the remaining old batch can
allocate token 0 again. The old batch can subsequently reappear and append duplicate
tokens. Concurrent batches can also validate aggregate sizes against stale totals,
and changing profile during reads leaves a batch using the old profile's limits.

**Required direction:** serialize or generation-scope uploads; invalidate pending
work on Clear All/profile changes; reserve occurrence tokens without reuse while
work is in flight; revalidate the combined staged state at commit. Preserve Tender's
intentional repeated filename/path/hash support. Add rendered async-interleaving tests,
not only source-string assertions or core token tests.

### F15 - P2: BOM autodetection can silently replace invalid source bytes

**Classification:** pre-existing malformed-input data-fidelity gap, not a regression
for valid UTF-8 files.

[CoreLogic.cs:757](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:757>)
enables StreamReader BOM autodetection. On a detected UTF-8 BOM, the substituted
decoder can replace invalid bytes instead of triggering the intended strict UTF-8 /
Windows-1252 fallback. Related readers use the same autodetection pattern.

A read-only compiled-Core reproduction parsed a BOM-prefixed RSRC row containing
Windows-1252 byte `E9` in `Café` as **Caf�**. The same byte sequence without the BOM
correctly triggered the ordinary Windows-1252 fallback and preserved **Café**. This
fixture deliberately has inconsistent encoding metadata; silent replacement can
still corrupt names, identifiers or curve data without a clear input warning.

**Required direction:** retain strict decoding after BOM detection and explicitly
reject or diagnose inconsistent BOM/content according to an agreed fallback policy.
Test valid UTF-8 with/without BOM and invalid-BOM data across stream, file and metadata
entry points; do not weaken correct decoding for valid files.

### F16 - P2: cancellation messaging overpromises after browser download handoff

**Classification:** pre-existing download helper behavior reused by new Tender UI.
This is a handoff/cancellation contract issue, not a confirmed failed-download bug.

[DownloadService.cs:21](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Web/Services/DownloadService.cs:21>)
invokes JavaScript with a cancellation token and then checks the token again. The
JavaScript helper at
[index.html:30](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Web/wwwroot/index.html:30>)
awaits the stream's array buffer and unconditionally clicks the download link at
line 38; it receives no abort/generation handshake. A .NET cancellation after handoff
does not establish that the browser never initiated delivery. Nevertheless,
Programme/Tender cancellation messages at Index.razor lines 619/694 state that
"no download was created."

**Required direction:** distinguish cancellation before generation, before handoff
and after browser handoff. Add an abortable handoff protocol where practical, and
avoid promising non-delivery once the browser controls the download. The root's
download inspection was inconclusive, so this review neither certifies delivery nor
labels the browser's unobserved download event as a product failure.

## Additional policy limitations, not newly introduced arithmetic defects

1. **04 is an earliest-input snapshot, not a selected P6 baseline.**
   [CoreLogic.cs:2299](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:2299>)
   finds the minimum filename-derived MonthUpdate across the whole table. It then
   retains every matching row. With project A's earliest input in January and
   project B's in February, B receives no baseline rows. Multiple inputs in the same
   earliest month all qualify. This is the implemented legacy policy; deciding to
   support one governed baseline per project/snapshot requires an explicit change.
   Neither Programme nor Tender exports table 04.
2. **MonthUpdate is not universally the Data Date.** Standard derives it from an
   initial YYMM filename token, assuming 20YY. Review profiles use governed metadata
   and canonical identities. Missing or nonmatching filename prefixes leave Standard
   MonthUpdate blank; files named as BLxx do not automatically become Standard 04.
3. **15 does not export assignment ID.** Its Standard internal grain is assignment x
   actual/remaining x month. Several assignments can produce identical visible
   task/resource/month keys. Programme preserves those rows; Tender deliberately
   aggregates them after assignment-level spreading. Sum `monthly_quantity`, not
   repeated total-period diagnostics. Current nonmaterial `unit/time` labels are
   legacy labels; the allocated measure is assignment units, not a rate calculation.
4. **P6 parity remains bounded.** Corrected 06 is the documented maximum signed
   predecessor-working-time delay allowance, not a universal promise of equality
   with every P6 displayed relationship float. Actual resource rows remain a
   calendar-weighted estimate, not timesheet history. Unsupported progressed
   nonlinear curves, special 0% curve behavior and opaque-only curve definitions
   fail closed as documented; a failed unsupported case is not a hidden uniform
   fallback. Native P6 golden exports are required before claiming wider parity.
5. **Browser cancellation and memory are bounded.** The shared in-memory export
   method at CoreLogic.cs lines 3555-3617 performs synchronous enhancement/CSV work
   without yielding; Tender's transformer also builds its enhanced inputs
   synchronously. On WebAssembly, UI cancellation cannot be processed throughout
   those CPU-bound phases. The 256 MB uploaded-input limit is not a peak-memory cap:
   raw bytes, parsed objects, CSV arrays, ZIP data, JavaScript ArrayBuffer and Blob
   can coexist. The review did not measure worst-case browser memory or latency.

## Coverage matrix: all 14 implemented numbered tables

For Standard, `raw + ...` means every retained source header/value in its existing
order, followed by the named derived fields. The common legacy merge can still
remove later-input-only headers (F01). All Standard CSVs append `FileName` in the
writer. They are not deduplicated merely because the intended grain has an ID.

| Number / actual table | Standard grain and derivation | Output shape / calendar use | Programme / Tender coverage | Audit result |
| --- | --- | --- | --- | --- |
| 01 `01_XER_TASK` | One raw TASK row; readable status, display dates, ID/name, durations, floats, %, project Data Date and keys | Fixed 39 columns before FileName; duration/activity float hours divided by activity HPD; does not run CPM | 47 / 27 fixed columns; Tender corrects raw remaining-date and strict hour conversion paths | F02, F03, F07, F08, F11; missing-output F04 |
| 02 `02_XER_PROJECT` | One PROJECT row | Raw + `proj_id_key`, `MonthUpdate`; no new scheduling calculation | 4 / 9 columns; Tender adds governed project/status attributes | F01/F02/F04 shared; no separate arithmetic defect found |
| 03 `03_XER_PROJWBS` | One PROJWBS row, adjacency list | Raw + `wbs_id_key`, `parent_wbs_id_key`, `MonthUpdate`; missing parents cleared | 4 / 4 columns | F12; no WBS flattening or baseline calculation |
| 04 `04_XER_BASELINE` | Filtered copy of 01 at globally earliest valid MonthUpdate | Same 39 columns as 01; no independent baseline date math | Absent / absent | Explicit legacy policy limitation; inherits 01 values |
| 06 `06_XER_PREDECESSOR` | One TASKPRED relationship row | Raw + 17 fields; signed predecessor-delay `free_float`, predecessor-HPD lag, successor total float | 16 / 16 columns; profile relationship key added; Tender strict conversions | F03, F06, F10, F11; corrected free-float engine reviewed separately |
| 07 `07_XER_ACTVTYPE` | One ACTVTYPE dictionary row | Raw + `actv_code_type_id_key`, `MonthUpdate`; no duration calculation | 2 / 2 columns | Shared identity/schema/export risks; no separate arithmetic defect |
| 08 `08_XER_ACTVCODE` | One ACTVCODE dictionary row | Raw + `actv_code_id_key`, `actv_code_type_id_key`, `MonthUpdate` | 3 / 3 columns | Shared identity/schema/export risks; preserve dictionary scope |
| 09 `09_XER_TASKACTV` | One task/activity-code assignment | Raw + `actv_code_id_key`, `task_id_key`, `MonthUpdate` | 2 / 2 columns | Shared risks; join through 08 to 07, do not treat as task grain |
| 10 `10_XER_CALENDAR` | One source CALENDAR row | Raw + `clndr_id_key`, `MonthUpdate`; preserves raw work definition and period conversion factors | 2 / 2 columns, identity/name only | Preservation correct; do not infer shifts from HPD; F01/F02/F11 affect consumers |
| 11 `11_XER_CALENDAR_DETAILED` | Seven weekday rules plus resolved dated exceptions per source/calendar | Fixed 12 columns before FileName; normalized actual interval hours, not HPD conversion | Absent / absent | Shared model consistent; expand standard rules and replace by exceptions, never sum both |
| 12 `12_XER_RSRC` | One RSRC dictionary row | Raw + `rsrc_id_key`, `clndr_id_key`, `unit_id_key`, `MonthUpdate` | 2 / 2 columns; default-rate field is text | No independent allocation; `def_qty_per_hr` is not monthly assignment quantity |
| 13 `13_XER_TASKRSRC` | One raw resource assignment | Raw + `rsrc_id_key`, `task_id_key`, `MonthUpdate`; raw quantities, dates and curves preserved if merge retains fields | Absent / absent | F01 particularly consequential; not the monthly fact table |
| 14 `14_XER_UMEASURE` | One unit-of-measure dictionary row | Raw + `unit_id_key`, `MonthUpdate` | Absent / absent | Descriptive units only; no date or scheduling calculation |
| 15 `15_XER_RESOURCE_DISTRIBUTION` | Assignment x actual/remaining x monthly slice; Tender later aggregates | Fixed 26 columns before FileName; exact calendar working time, cumulative F4 quantity allocation, named/manual remaining profiles | 9 / 9 columns; Programme keeps assignment rows, Tender has task/resource/actual/month key | F01, F02, F04, F09; actual estimate and supported-curve boundaries explicit |

Implementation anchors: table-name inventory
[CoreLogic.cs:225](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:225>),
simple table definitions
[CoreLogic.cs:3159](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/CoreLogic.cs:3159>),
11
[XerTransformer.CalendarCalculations.cs:19](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/XerTransformer.CalendarCalculations.cs:19>),
15
[XerTransformer.ResourceDistribution.cs:22](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/XerTransformer.ResourceDistribution.cs:22>).

## Calculation and reporting contracts checked

### Calendar tables and arithmetic

- Table 10's `day_hr_cnt`, `week_hr_cnt`, `month_hr_cnt`, `year_hr_cnt` are period
  conversion settings, not computed averages of calendar availability.
- The shared model parses explicit weekday shifts, split shifts, overnight spill,
  dated exceptions and base-calendar exception inheritance within source identity.
- Table 11 has weekday-rule rows with blank dates and resolved dated replacement
  rows. Its weekday-number field uses Monday=1 through Sunday=7; raw P6 weekday
  encoding uses Sunday=1. These are different documented representations.
- 06 working-date arithmetic and 15 allocation reuse the resolved interval model.
  15 uses the resource calendar for resource-dependent activities and the task
  calendar for task-dependent work; day diagnostics do not drive quantity weights.
- Missing or ambiguous availability is not safely replaced by a generic 8h/24h
  schedule. Remaining legacy conversions/display lookups still need F06/F11 fixes.

### Relationship free float

- The corrected column is relationship-level signed predecessor-delay allowance,
  using FS/SS/FF/SF event mapping, configured lag calendar, predecessor movement
  calendar and predecessor HPD at final conversion.
- Zero movement preserves the original timestamp; moved starts/finishes use their
  documented work-event boundaries. Feasibility/maximality are explicitly checked.
- Blank is not zero: completed/unsupported, ambiguous, unresolved or unsupported
  progressed cases remain unknown. Relationship float is not the predecessor's
  stored activity free float, successor total float, or a summable path quantity.
- The visual's intentional least-float selection was not changed by this work.
  A parser-derived value does not independently certify P6 driving status.

### Resource distribution and curves

- Actual quantity is regular plus overtime actual units; actual periods use
  assignment actual dates, with the assignment's own project Data Date only when
  an active assignment lacks an actual finish. This remains a uniform estimate.
- Remaining uses assignment `restart_date`/`reend_date`. A valid explicit
  `remain_crv` profile takes precedence over a named curve, including manual ID 9.
- Manual profiles use quantity/working-hour bands, validate total duration and
  quantity, preserve zero-quantity gaps and support active assignments. Optional
  trailing separators do not legitimize internal empty bands.
- Named RSRCCURVDATA uses 21 percentage fields: index 0 has special actual semantics;
  indices 1-20 allocate quantities over successive 5% working-duration bands. They
  are not cumulative percentages or curve heights. Supported totals permit only
  the documented small serialization tolerance; arbitrary sums are not normalized.
- Unassigned curves preserve uniform spreading. Unsupported or malformed referenced
  profiles fail with context, rather than silently reverting to uniform allocation.
- Month slices are adjacent half-open intervals. Monthly quantities are differences
  between rounded cumulative allocations; they reconcile at four decimals and do
  not send rounding residue into a zero-work/zero-usage trailing month.
- Changes are assignment-level before Tender's aggregation. Applying a curve to
  already-summed task/resource quantities would be incorrect when assignments have
  different calendars, periods or shapes.
- The independent numerical review found no additional blocker within the supported
  subset: unstarted named curves with zero at index 0, linear curves on active work,
  and explicit manual remaining profiles. This is not native P6 golden validation.

## Intended profile differences to preserve

| Concern | Standard Enhanced | Programme Review 3.0 | Tender Review 1.0 |
| --- | --- | --- | --- |
| Purpose | Legacy broad raw/enhanced export | One project/programme retained baseline/update history | Ordered Tender stage inputs with governed status identity |
| Input identity | Legacy filename-derived (F02) | Governed snapshots; ambiguous filenames/duplicate content rejected | Stable internal occurrence token; repeated filename/path/hash accepted |
| Tables | 14 numbered tables available; selected outputs | 10 fixed CSVs + manifest | Same 10 fixed CSV filenames + independent manifest |
| Required source tables | Legacy per-transform dependencies | 01/02/03/10 required; other fixed files may be header-only | 01/02/03/10 required; other fixed files may be header-only |
| Table 01 | Fixed legacy 39 columns + writer FileName | 47 columns including history/variance fields | 27 columns, includes status_date, no Programme history fields |
| Table 02 | Raw project fields plus key/month | 4 columns | 9 columns including add_date/state/region/tender status/governed status date |
| 04/11/13/14 | Available | Not included | Not included |
| Table 10 | Full raw calendar fields | Key/name only | Key/name only |
| Table 15 | 26 columns plus FileName, diagnostics/method retained | 9 columns; assignment monthly rows retained, no declared primary key | 9 columns; aggregated task/resource/actual/month grain |
| Dates / booleans | Legacy timestamp/raw field conventions; 15 actual flag 1/0 | ISO date-only; lowercase true/false | ISO date-only; lowercase true/false |
| Keys | Filename/native-ID display keys | Governed project/programme/snapshot namespace | Independent Tender namespace including governed stage identity |
| Publication | Legacy files, subject to F04/F05 | Validated bundle, deterministic CSV/hash/manifest, staged publication | Separate validated bundle, deterministic CSV/hash/manifest, staged publication |

Programme's history variances intentionally follow its existing Athena Monday-Friday
endpoint-count convention, not actual P6 calendar shifts or holidays.
`Driven_DataDate` is a history/reporting heuristic, not CPM relationship-driving
status. PreviousDataDate is selected from project history and can differ from the
previous observed task row supplying previous remaining days. Those concepts must be
named precisely in the reporting skill; this audit does not silently convert them
to the newly corrected scheduling-calendar model.

Fixed schemas are defined by
[ProgrammeReviewContract.cs:36](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/ProgrammeReview/ProgrammeReviewContract.cs:36>)
and
[TenderReviewContract.cs:32](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core/TenderReview/TenderReviewContract.cs:32>).
In both profiles, optional missing numeric floats remain blank, not zero; 01 status
labels are readable while relationship status columns retain raw TK_* values.

## Recommended remediation sequence, subject to approval

1. Resolve source occurrence identity and deterministic union-schema merging first;
   otherwise later calculation validation cannot trust its inputs.
2. Make requested export outcomes complete, empty-table behavior explicit and output
   paths contained. Preserve user files outside the export's exact governed scope.
3. Reconcile activity/relationship display dates, HPD conversion and endpoint-project
   identity without broadening supported free-float semantics by assumption.
4. Correct percent-complete inputs and finite-number handling; add consistent
   duplicate/hierarchy validation and an explicit Standard baseline-scope decision.
5. Filter Programme parsed data to retained snapshots before shared calculations.
6. Correct Web filename inference, upload operation/token lifecycle and cancellation
   handoff claims; make malformed-BOM decoding explicitly strict or diagnostic.
7. Convert diagnostic characterizations to desired-contract regressions, add batch
   permutation/repeated-input and stale-export tests, then rerun all required
   validation targets. Keep Standard/Programme behavior changes explicit rather
   than hiding them inside Tender-only code.
8. Golden-compare representative P6 schedules/resource usage and complete the UI
   acceptance checks before claiming end-to-end reporting parity.

## Validation results

The validation agent ran the following targets serially; the root agent confirmed
the results for this review. Commands used the workspace SDK executable
`.\local-dotnet-sdk\dotnet.exe`, shown below as `dotnet` for compactness. All builds
were incremental with `--no-restore`, not clean rebuilds. Each build reported zero
warnings and zero errors. Earlier counts in calculation documentation are historical
and do not replace these results.

| Required target | Final result |
| --- | --- |
| `dotnet build XerToCsvConverter.Core/XerToCsvConverter.Core.csproj --no-restore` | Passed, exit 0 |
| `dotnet build "XER to CSV.csproj" --no-restore` | Passed, exit 0 |
| `dotnet build XerToCsvConverter.Core.Tests/XerToCsvConverter.Core.Tests.csproj --no-restore` | Passed, exit 0 |
| `dotnet build XerToCsvConverter.Web/XerToCsvConverter.Web.csproj --no-restore` | Passed, exit 0 |
| `dotnet build XerToCsvConverter.ProgrammeReview.Cli/XerToCsvConverter.ProgrammeReview.Cli.csproj --no-restore` | Passed, exit 0 |
| `dotnet build XerToCsvConverter.TenderReview.Cli/XerToCsvConverter.TenderReview.Cli.csproj --no-restore` | Passed, exit 0 |
| `dotnet build XerToCsvConverter.TenderReview.Cli/Tests/XerToCsvConverter.TenderReview.Surface.Tests.csproj --no-restore` | Passed, exit 0 |
| `dotnet test XerToCsvConverter.Core.Tests/XerToCsvConverter.Core.Tests.csproj --no-restore --no-build` | 526 passed, 0 failed, 0 skipped |
| `dotnet test XerToCsvConverter.TenderReview.Cli/Tests/XerToCsvConverter.TenderReview.Surface.Tests.csproj --no-restore --no-build` | 13 passed, 0 failed, 0 skipped |
| `git diff --check` | Passed, exit 0; nine tracked-file LF-to-CRLF normalization notices, no whitespace errors |
| Untracked source/test/review/skill whitespace | Passed; no trailing whitespace in checked text files (XER fixture delimiters excluded) |
| Official Skill Creator `quick_validate.py` | Passed: Skill is valid |
| `review/Validate-ReportingSkill.ps1` | Passed: all 20 review table headers match the built Core contracts exactly, all 5 relative reference links resolve, skill whitespace clean |
| Independent skill forward test | Passed realistic cross-profile reporting exercise; two clarity improvements incorporated |

Review characterizations are in
[LegacyTableReviewTests.cs](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core.Tests/LegacyTableReviewTests.cs>)
and
[ReviewProfileAuditTests.cs](</C:/Users/ricar/Documents/Code/XER to CSV/XerToCsvConverter.Core.Tests/ReviewProfileAuditTests.cs>).
Their passing status confirms reproduction of current defects/limits, **not
correctness of those defects**. Twelve legacy cases and six profile audit cases
characterize the current implementation. Independent semantic checks used read-only
compiled-Core reproductions for schema loss and malformed-BOM decoding; they added
no production files or additional audit tests.

## Browser and manual acceptance

The root agent exercised the Codex in-app browser at `http://127.0.0.1:54301` using
`review/fixtures/J5001_C_BL01_2026-01-31.xer`. The following are observed UI/log
results, not downloaded-file integrity checks. All three profile runs had empty
JavaScript error/warning logs.

| Acceptance surface | Final result |
| --- | --- |
| Standard browser workflow | Parsed 13 raw tables successfully. Selecting all 14 Enhanced tables produced a completion log for 13 CSVs: 04 was absent because this filename has no valid Standard MonthUpdate prefix (F04 and the legacy 04 policy). |
| Programme Review browser workflow | Baseline BL01 initially received erroneous MonthUpdate 2050-01-01 (F13). After correction to 2026-01-31 through the UI, the workflow logged completion for 10 CSVs plus manifest. |
| Tender Review browser workflow | The same file was uploaded twice as two occurrences. Duplicate default status date 2026-09-05 was rejected. Editing the second date through real keyboard input to 2026-09-07 produced a logged two-stage completion for 10 CSVs plus manifest. |
| Download delivery and archive/CSV integrity | **Not verified.** UI reported downloaded; waiting for a download event timed out and no file was located. This is an unresolved browser/tool observation boundary, not a confirmed product bug. |
| Upload concurrency / Clear All interleaving | Not runtime-reproduced; F14 is supported by the source lifecycle and a concrete interleaving. |
| Windows interactive UX / cancellation / file publication | Not run. |
| Native P6 resource-usage / relationship-value golden comparison | Not run; no parity claim from synthetic tests alone. |
| Power BI consumer refresh / visual behavior | Not run; no report or visual changes authorized by this audit. |

Tender surface tests primarily exercise the public profile contract and Razor
source-string assertions; they are not rendered upload-concurrency or actual browser
download tests. Positive code checks include ordered source/content-token matching,
union-schema merging, frozen export inputs, token-qualified progress, once-per-upload
local status date capture, strict governed dates and ZIP validation before handoff.

The temporary review tab and local server were stopped after these checks. No
user schedules or third-party uploads were used; the fixture contains synthetic data.

## Reusable AI reporting skill

Created the portable, versioned
[p6-numbered-xer-reporting skill](</C:/Users/ricar/Documents/Code/XER to CSV/skills/p6-numbered-xer-reporting/SKILL.md>)
and installed an identical hash-verified copy at
`C:\Users\ricar\.codex\skills\p6-numbered-xer-reporting` with approval. It can be
invoked as `$p6-numbered-xer-reporting`; the Markdown skill and references are also
portable to other agents that support the SKILL.md format.

The entry point routes to exact profile contracts, a dictionary of all 14 numbered
tables, calendar/float/curve and reporting-reconciliation rules, and dated known
limitations. It distinguishes verified parser behavior from native P6 parity and
report-specific policy. It does not instruct agents to silently repair parser defects
in Power Query/DAX, collapse repeated stages/assignments, or alter the user's intended
least-float chain selection.

An independent agent used only the skill and a realistic Programme/Tender modeling
request. It correctly rejected native-ID joins, filename/hash deduplication, dropping
Programme assignment contributions, summing repeated working-hour diagnostics as
capacity, and recomputing relationship float from date-only review endpoints and the
key/name-only calendar table. Clarifications for multi-bundle comparison models and
ranking edges with different predecessor HPD factors were incorporated afterwards.

The official skill validator required PyYAML, which was installed with approval in a
temporary validation-only directory, not the user's Python installation. The skill
itself has no Python or other runtime dependency. Its schema/reference check is
reproducible with
[Validate-ReportingSkill.ps1](</C:/Users/ricar/Documents/Code/XER to CSV/review/Validate-ReportingSkill.ps1>)
after building Core. Structural/schema checks and the forward exercise do not replace
the native P6 and report acceptance checks listed above.

## References

- Local specification/skill inputs were read without editing the Tender-Review
  repository. Existing parser semantics and historical validation are documented in
  [CALENDAR_CALCULATIONS.md](</C:/Users/ricar/Documents/Code/XER to CSV/CALENDAR_CALCULATIONS.md>)
  and
  [PROGRAMME_REVIEW_EXPORT.md](</C:/Users/ricar/Documents/Code/XER to CSV/PROGRAMME_REVIEW_EXPORT.md>).
- [Oracle TASK field mapping](https://docs.oracle.com/cd/F51303_01/English/Mapping_and_Schema/xer_import_export_data_map_project/97906.htm)
  distinguishes remaining early dates, raw activity float and labor/nonlabor fields.
- [Oracle Units % Complete definition](https://docs.oracle.com/cd/G18294_01/English/User_Guides/p6_eppm_data_dictionary/46503.htm).
- [Oracle resource-curve behavior](https://docs.oracle.com/cd/G48902_01/English/User_Guides/p6_pro_user/resource_curves.htm),
  [curve-band / 0% semantics](https://docs.oracle.com/cd/G18296_01/client_help/en_US/modify_resource_curves_dialog_box.htm),
  [future-period planning](https://docs.oracle.com/cd/G18294_01/p6help/en/future_period_bucket_planning.htm).

The independent profile and numerical/Web semantics cross-checks are complete and
integrated above. This report's explicit non-ready verdict remains until the
identified issues are resolved or their retention is explicitly accepted with a
clearly narrowed release claim. Passing automated tests alone is not that acceptance.
