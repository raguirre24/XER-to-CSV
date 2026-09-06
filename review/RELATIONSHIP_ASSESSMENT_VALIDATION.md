# Relationship assessment validation

The shared evaluator explains every TASKPRED occurrence and supplies the existing
`06.free_float` value. The optional [audit CLI](../XerToCsvConverter.RelationshipAudit.Cli/README.md)
publishes one explicitly requested audit CSV. It does not add files to normal
Standard, Programme Review or Tender Review exports. Calculation and progress
semantics are documented in [CALENDAR_CALCULATIONS.md](../CALENDAR_CALCULATIONS.md)
and the portable [reporting reference](../skills/p6-numbered-xer-reporting/references/relationship-audit.md).

## Reusable local checks

Build Core and run each script in a fresh PowerShell process. The new integration
scripts load the Core assembly from bytes so they do not lock a concurrent build's
DLL. They read original sources locally, preserve ordered occurrences and verify
source hashes afterward. Review-profile outputs are generated in fresh ignored
artifact directories, never in the original input folder.

```powershell
./review/Validate-LocalXerExport.ps1 -Paths @('<July.xer>', '<August.xer>')
./review/Validate-RelationshipExportIntegration.ps1 -Paths @('<July.xer>', '<August.xer>') -BaselineHashes ./artifacts/relationship-before-standard-hashes.json
./review/Validate-RelationshipMetadataClones.ps1 -Paths @('<July.xer>', '<August.xer>')
./review/Validate-RelationshipExportIntegration.ps1 -Paths @('./review/fixtures/J5001_C_BL01_Retained_2026-01-31.xer') -ProfileFixture ./review/fixtures/J5001_C_BL01_Retained_2026-01-31.xer -ExpectedFixtureReason CalculatedRetainedRemainingRelationship -ExpectedFixtureFloat '0'
./review/Compare-RelationshipBaseline.ps1 -Mode Capture -Paths @('<July.xer>', '<August.xer>') -CoreAssemblyPath '<prior-Core.dll>' -BaselineHashes ./artifacts/relationship-before-standard-hashes.json -BaselineRows '<new-diagnostic-baseline.json>'
./review/Compare-RelationshipBaseline.ps1 -Mode Compare -Paths @('<July.xer>', '<August.xer>') -CoreAssemblyPath '<current-Core.dll>' -BaselineHashes ./artifacts/relationship-before-standard-hashes.json -BaselineRows '<diagnostic-baseline.json>' -DeltaOutput '<new-diagnostic-deltas.json>'
./review/Validate-ReportingSkill.ps1
```

The baseline hash argument is optional. When supplied, it must describe the same
ordered source SHA-256 values, the prior Standard output hashes and the canonical
table 06 hash with only `free_float` omitted. Capture it before rebuilding the
old assembly. This detects changes to every non-06 file and every other table 06
column; ordinary whole-file comparison would conflate the intended float changes
with unintended changes. A hash baseline supplements source reconciliation; it
does not establish that the earlier values were correct.

`Compare-RelationshipBaseline.ps1` closes the separate float-delta check. Its
Capture mode runs with a prior assembly and accepts the recovered rows only if
the complete table 06 CSV hash matches the independently captured old hash. Its
Compare mode checks every relationship's identity/order and non-float content,
records every changed allowance with its current assessment reason, and fails if
any previously numeric allowance changes. Both modes byte-load Core in separate
fresh processes and write only new diagnostic JSON files, not normal exports.

`Validate-LocalXerExport.ps1` independently checks retained raw values, fixed and
dynamic headers, keys/references, baseline selection, calendar rule identities,
01 arithmetic/dates and 06 display metadata. It reconciles actual and remaining
resource units, including the existing unallocated-actual companion. Its optional
table 15 exclusion is not used for the complete checks below.

`Validate-RelationshipExportIntegration.ps1` checks all 14 Standard tables plus
the existing companion for exact file/stream equality, then checks audit equality,
source relationship row identity/order and every exported free-float value against
its assessment. It rejects numbers on noncalculated assessments. Both review
profiles use the explicit `-ProfileFixture` (defaulting to the checked-in J5001
fixture): all twelve files must match between disk and memory publication, all ten
numbered headers must match the public contract, and relationship values and
monthly resource groups must reconcile to the shared Standard results. It reports
which original sources and which profile fixture were actually tested.

`-ProfileProjectCode` supplies explicit review-request metadata without changing
the raw XER. `-RecordProfileValidationFailures` records a governed-profile
validation exception and continues the other profile; its summary expressly
reports the failure. It does not convert a rejected profile into a passing check
or suppress publication/I/O failures.

## Unmodified July/August sources

The pre-change Core assembly SHA-256 was
`AF06EBF97A6F0C8D94C56971725016E3AACDDD7EB0526D67B7F233E40259A4FA`.
The full old source validator passed before rebuilding: 2,236,209 retained source
cells, 909,978 independently derived checks and 6,849 resource groups. Its combined
table 11 contained 31,334 rows and table 15 contained 12,324 rows, with two existing
actual-date warnings and 128 valid actual fallback rows.

The relationship integration check on the new working tree passed for all 33,100
original relationships. File and stream paths produced byte-identical normal
exports and identical audit text. Every audit value matched table 06. All thirteen
other numbered CSVs and `XER_DATA_QUALITY.csv` remained byte-identical to the
pre-change baseline; every non-float field in table 06 remained unchanged.

The retained self-contained build's Core assembly
`74CFED1FF996BDEE9183FA53C1AA5DAC2A9E5725F11FBF1A7AE684F82F9C4FD8`
recovered the old table 06 byte-for-byte: full CSV SHA-256
`833B09910EB5E73E35020BB93FC337D44FE4BA7A9C21D3A5D9CB38AE17106496`,
matching the pre-build capture despite the different Debug/Release assembly hash.
Row-by-row comparison against final Core
`13530175C61AF199B2E7DD2EC8C139E193650504781807B57C06976DC9FF83DE`
verified all 4,665 previously numeric allowances unchanged. Exactly 118 rows
changed from blank to numeric, all `CalculatedRetainedRemainingRelationship`.
Every changed row is recorded locally in
`artifacts/relationship-baseline-deltas-sanitized.json`; the recovered old row values are in
`artifacts/relationship-recovered-baseline-rows.json`. Current full table 06 hash:
`D4C6EC14A71B861731C4CFBDD1CB57751B8D78497F81E95C25C71357C1767EB2`.

| Assessment | Combined original rows |
| --- | ---: |
| Calculated | 4,783 |
| Historical | 27,873 |
| Unsupported | 444 |
| Total | 33,100 |

There were no ignored, missing-data or invalid-data rows in this particular pair
of schedules. That does not remove those classifications from the contract; the
controlled progress/settings tests exercise other cases. Resource warning count
remained two. The checked-in J5001 fixture also passed both twelve-file review
profile checks, twenty exact numbered headers, relationship projection and
resource-group reconciliation.

The separate retained-logic fixture changes its two activity statuses/actual
starts and supplies explicit scheduling flags. It retains the original fixture's
calendar, remaining dates, relationship, resource curve and quantities. The
harness explicitly requires `CalculatedRetainedRemainingRelationship` and zero
float, then verifies that same numeric zero in Standard, Programme and Tender.
Both twelve-file review outputs match between disk and memory, with unchanged
fixed headers and reconciled resource quantities. This specifically tests an
active-successor FS edge across all three export profiles.
The existing independent source/derived validator also passed that controlled
fixture: 89 retained source cells, 44 finite numeric cells, 53 derived checks,
one resource group and no warnings.

## Governed profile boundary and metadata clones

The original PROJECT.proj_short_name is `PAA-8.0_0226`. Tender requires an
alphanumeric governed project identity and validates the raw project code before
matching the requested identity. Its request/source records have no project-code
override that bypasses that validation. A real original review export must not be
claimed successful by silently relabeling this source or relaxing the contract.

Programme accepts an explicit request project code separately from the raw source
label. Both unmodified originals passed Programme checks with declared
`project_code=J5001` test metadata; this is not a source edit or a claim that J5001
is their production business identity. July's 16,245 and August's 16,855
relationships matched the shared assessment, their 6,383 and 5,936 monthly
resource groups reconciled, and all twelve files matched between disk and memory.
Tender continues to validate the raw label and rejects that same request. One
local July publication attempt hit a transient staging-directory access error;
the isolated retry passed without a production-code or source-data change.

`Validate-RelationshipMetadataClones.ps1` first verifies the original input's
actual Tender rejection. It then creates a separate, explicitly diagnostic XER
copy changing only PROJECT.proj_short_name to `J5001`. Every other byte, including
tasks, calendars, relationships, scheduling settings, assignment quantities and
source dates, is retained. It runs the three-profile integration checks against
that local copy and verifies the original source hash afterward. The reported
clone hash/path and changed field distinguish this evidence from an unchanged
original-source export. Generated clone bundles are test artifacts, not governed
production bundles.

Both metadata-only clones passed Programme's twelve-file disk/memory comparison,
shared relationship projection and resource quantity reconciliation. Tender then
rejected both with the same unresolved required `PROJWBS.parent_wbs_id` value
`3689`. That source hierarchy was preserved. No WBS row was repaired or deleted
to obtain a passing review export.

The portable skill passed twenty exact numbered review headers, the resource
diagnostic header, twelve relative links and whitespace checks. Skill Creator's
`quick_validate.py` passed using PyYAML installed solely in the ignored local
validation-artifact directory; no global Python package installation was required.
After wording QA, both portable and installed skill copies passed that validator;
the five installed changed files were hash-matched to the portable sources.

## Final automated and local checks

All nine Debug build targets below passed with zero warnings and zero errors.
Each used `--no-restore --disable-build-servers -m:1 -p:UseSharedCompilation=false`.
These are builds of the working tree, not a new release publication.

| Build target | Result |
| --- | --- |
| `XerToCsvConverter.Core/XerToCsvConverter.Core.csproj` | Passed, 0 warnings / 0 errors |
| `XerToCsvConverter.Core.Tests/XerToCsvConverter.Core.Tests.csproj` | Passed, 0 warnings / 0 errors |
| `XER to CSV.csproj` | Passed, 0 warnings / 0 errors |
| `XerToCsvConverter.Web/XerToCsvConverter.Web.csproj` | Passed, 0 warnings / 0 errors |
| `XerToCsvConverter.ProgrammeReview.Cli/XerToCsvConverter.ProgrammeReview.Cli.csproj` | Passed, 0 warnings / 0 errors |
| `XerToCsvConverter.TenderReview.Cli/XerToCsvConverter.TenderReview.Cli.csproj` | Passed, 0 warnings / 0 errors |
| `XerToCsvConverter.TenderReview.Cli/Tests/XerToCsvConverter.TenderReview.Surface.Tests.csproj` | Passed, 0 warnings / 0 errors |
| `XerToCsvConverter.RelationshipAudit.Cli/XerToCsvConverter.RelationshipAudit.Cli.csproj` | Passed, 0 warnings / 0 errors |
| `XerToCsvConverter.RelationshipAudit.Cli/Tests/XerToCsvConverter.RelationshipAudit.Cli.Tests.csproj` | Passed, 0 warnings / 0 errors |

Core tests passed 999/999; existing surface tests passed 50/50; the audit CLI
passed 28 tests, with two filesystem-link tests explicitly skipped because this
Windows environment cannot create unprivileged symbolic links. The skip is not
evidence that those two live link scenarios passed. Node checks passed 5/5.
The latest Debug Windows `--ui-smoke-test` exited zero; it is a limited local UI
smoke check, not comprehensive manual UI or published-package acceptance.

The final Core build was also used to repeat the combined 33,100-relationship
integration: all fifteen Standard files matched disk/stream paths, all non-float
baseline hashes remained equal, and the retained-logic fixture produced zero in
both twelve-file Programme/Tender profiles. Reporting-skill validation passed
twenty exact numbered headers and twelve relative links. The independent
skill-reader QA correctly distinguished fixed historical links, remaining versus
display endpoints, optional audit files and original versus caller-edited input
evidence; its wording findings were corrected before installation.

The original-source validator also passed with table 15 included: 2,236,209
retained source cells, 391,414 finite derived numeric cells, 909,978 independent
derived checks, 6,849 reconciled resource groups and 128 actual fallback rows.
The two existing actual-date warnings remain visible. Tables 10, 11 and 15
retain their pre-change values; the final integration verified those file hashes.

July relationship `8639741` (predecessor `4806388` to successor `4447632`)
now has `free_float=0`: predecessor remaining finish 2026-08-04 17:00 to
successor remaining restart 2026-08-05 08:00 on calendar 2392. Their actual
display starts remain 2026-07-20. This is the newly supported Retained Logic FS
allowance, not a moved actual start or a native P6 driving assertion.

`git diff --check` passed. No source XER, sibling repository, normal numbered
schema, manifest version or visual policy was modified; no commit, push or
SharePoint upload was performed.

## Evidence limits

These checks establish the supported implementation and the tested source/profile
contracts. They are not a native P6 scheduling run, Resource Usage parity test,
browser-saved ZIP verification, deployed UI check, SharePoint refresh or Power BI
report acceptance. No original XER was edited or uploaded. The optional audit does
not change the visual's least-float selection policy, activity float, table 11
hours, resource quantity semantics or fixed profile schemas.
