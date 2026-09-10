# Flexible review project names — 2026-09-10

The reported `QAC000623-01-02` rejection came from the ASCII-only project-code validators inherited from the original report contract. Both Programme Review and Tender Review now accept nonblank project codes with hyphens, spaces, punctuation and Unicode. Core normalization trims and applies invariant uppercase. Numbered `ProjectCode` and manifest `project_code` retain all other characters.

`ReviewProjectIdentity` separates the business code from filenames and key components. UTF-8 percent encoding protects separators, literal percent signs, dots and Windows filename characters; ordinary letters, digits, underscores, hyphens and spaces remain readable. Long generated file/folder components use `~` plus a full SHA-256; keys and metadata retain the full normalized identity. Tender no longer collapses spaces to underscores. Regenerate affected old space-named stages together and use the same entered project code as the XER (existing C/J numeric aliases remain supported).

Windows dialogs and Web request construction share Core normalization. Programme filename suggestions read the governed suffix before project text, avoiding project digits or `BL` text being mistaken for the snapshot. Both CLIs use the same Core path. Standard calculation code, review headers/schema versions, eleven-file review envelopes and ordered Tender occurrence tokens are unchanged. The reporting skill's profile reference documents the naming/migration rules; its remaining outdated twelve-file statement was corrected to match existing commit `b6490e2`.

## Builds

Each target passed using `local-dotnet-sdk/dotnet.exe build <project> --no-restore --nologo -v:minimal`, with zero warnings and errors:

| Target | Result |
| --- | --- |
| Core | Passed |
| Windows (`XER to CSV.csproj`) | Passed |
| Core.Tests | Passed |
| Web | Passed |
| Programme Review CLI | Passed |
| Tender Review CLI | Passed |
| Tender Review Surface.Tests | Passed |
| Relationship Audit CLI | Passed |
| Relationship Audit CLI.Tests | Passed with `-m:1 -nr:false` |

The audit-test build initially exited unsuccessfully without diagnostic errors under parallel MSBuild; the single-process retry passed.

## Tests and reconciliation

| Check | Result |
| --- | --- |
| Core.Tests, full suite (`dotnet test ... --no-build --no-restore`) | 1,300 passed |
| Tender Surface.Tests, full suite | 60 passed |
| Relationship Audit CLI.Tests, full suite | 36 passed; 2 symbolic-link tests skipped by the host capability gate |
| Web download JavaScript (`node --test XerToCsvConverter.Web/tests/downloads.test.cjs`) | 5 passed |
| Windows app `--ui-smoke-test` | Passed; both dialogs accept hyphenated/Unicode/slash project code |
| `review/Validate-ReportingSkill.ps1` | Passed: 20 numbered contracts, diagnostic header, links and whitespace |
| Skill creator `quick_validate.py skills/p6-numbered-xer-reporting` | Passed with PyYAML in an isolated ignored validation directory |
| `git diff --check` | Passed |

Naming tests cover the exact reported code, preserved whitespace, Unicode, slash/backslash/colon/pipe/percent, dots, Windows device-like names, quotes/commas, long names, distinct identities and blank rejection. New integration tests export both profiles through real disk and byte/stream paths and compare every output byte, metadata and joins. Tender tests retain duplicate filenames, paths and hashes in both input orders. The focused suite also passed (103 tests, included in the full suite above).

Local reconciliation used the available original `C:/Users/ricar/Downloads/2607 C5064-C-2607.xer`. `Validate-LocalXerExport.ps1` passed all 14 numbered tables, 812,519 retained source cells, 382,462 independently checked derived values and 4,658 resource allocation groups, including tables 10/11/15. `Validate-RelationshipExportIntegration.ps1` passed disk/stream byte parity, all 14,138 relationship assessments, and both eleven-file review fixture bundles. Original source hashes remained unchanged.

Two stale validation expectations were updated before rerunning: the local validator lacked the previously added table 06 metadata columns; the relationship integration validator still expected a diagnostic CSV inside review bundles. The latter now checks the exact eleven-file envelope, reads diagnostics from `DataQualityTable`, and uses encoded project components when checking review keys. All existing reconciliation checks remain active.

The previously supplied `2609-NE Part B Backup74 LIVE.xer` was not available at its former OneDrive path during this run. Its reported project code is reproduced in the disk/stream regression fixtures. Validation establishes local parser/export consistency; it does not establish a deployed Web or live Power BI refresh. No sibling repository, SharePoint upload, commit or push was performed.
