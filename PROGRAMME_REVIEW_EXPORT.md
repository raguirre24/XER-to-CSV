# Programme Review CSV export profile

The Programme Review profile is a separate, versioned export path available in the Windows application, web application, CLI, and shared Core API. The existing raw and **Enhanced Power BI** exports remain available and keep their original behaviour.

It creates one full-history local bundle for one project and programme type. The bundle is validated and written to a staging directory first; the final directory only appears after all ten numbered CSV files are complete, their hashes have been calculated, and `XER_CSV_MANIFEST.csv` has been written last: eleven files in total (`XER_DATA_QUALITY.csv` is not emitted in Review bundles, preserving the strict 11-file bundle contract).

The diagnostic companion `XER_DATA_QUALITY.csv` is reserved for Standard Enhanced exports and is not emitted in Programme Review or Tender Review bundles (ensuring compatibility with Power BI bundle loaders that strictly enforce the eleven-file contract). Diagnostic warnings are exposed on the result object (`DataQualityTable`, `WarningCount`) and logged. Invalid actual date periods do not block otherwise valid allocations: valid remaining work still distributes. Windows, Web and CLI report completion with warnings; CLI exit code remains zero. Manifest `complete` means publication completed, not that the schedule is free of data-quality issues. The manifest contains ten rows per retained source (one per contract table). See [the warning and reconciliation contract](review/NONBLOCKING_ACTUAL_WARNINGS.md).

## Use the Windows application

1. Add the baseline and applicable update XER files and select the output folder. **Create Programme Review bundle** becomes available immediately; the legacy **Parse and load tables** step is not required for this profile.
2. Choose **Create Programme Review bundle**. The Windows app scans each selected file directly and prefills the editable Data date from the single unambiguous `PROJECT[last_recalc_date]` row. This streaming scan does not load the full history into memory. If the value is missing, invalid, or ambiguous, the affected cell stays blank for manual `yyyy-MM-dd` entry.
3. Enter the governed project code/name and choose `C - Contract` or `T - Target`. The governed project name and baseline `month_update` are intentionally blank: neither is inferred from the P6 project short name or data date. Review each file's baseline/update kind, tag, explicit month update, and detected XER data date.
4. Choose **Validate and export**. The application parses and hashes the selected source files through the atomic bundle service. Closing the window cancels the operation and waits for cleanup. If legacy preview data was already loaded, it is released before bundle creation and must be parsed again before a Standard/Enhanced export.

The legacy **Export enhanced Power BI** action remains separate and continues to create the existing Enhanced tables. The resizable Windows layout keeps Programme Review, Power BI, Export all, Export selected, Cancel, and Browse visible at startup, including high-DPI displays.

## Use the web application

1. Add all history XER files and select **Programme Review** under **Export Profile**.
2. Enter the governed project identity and review each file's snapshot metadata. The editable data date is prefilled from the single `PROJECT[last_recalc_date]` row when it is present and unambiguous. Recognised `BLnn`/`BLnn-A` and `YYMM` tags are suggested from filenames, but an update is never silently relabelled as a baseline.
3. Choose **Create Programme Review Bundle**. The browser hashes and parses only the retained history, validates the same contract as the Windows/CLI paths, and downloads `<bundle_id>.zip` containing one `<bundle_id>/` folder and exactly twelve files.

The browser path does not write temporary bundles to the client file system. Large downloads use a streamed browser Blob rather than a base64 data URL. It accepts at most 24 files, 128 MB per file, and 256 MB in total. Uploads are read directly into their retained byte arrays and the core browser entrypoint does not clone the full input history. Histories above 64 MB display a high-memory warning; use a 64-bit desktop browser, keep the tab open, and close other memory-heavy tabs. The Windows app remains the reliable option above the browser ceiling or when browser memory is constrained. Parsing yields cooperatively for progress and cancellation. Some report transformations remain CPU-bound and can briefly pause the tab; cancellation is applied at the next safe checkpoint.

## Run the CLI

Create a JSON configuration such as:

```json
{
  "project_code": "J1234",
  "project_name": "Example Project",
  "programme_type": "C",
  "parser_version": "2.6.0",
  "snapshots": [
    {
      "original_xer_filename": "Original approved baseline.xer",
      "xer_file_path": "XER/Original approved baseline.xer",
      "snapshot_kind": "baseline",
      "snapshot_tag": "BL01-A",
      "month_update": "2026-01-31",
      "data_date": "2026-01-31"
    },
    {
      "original_xer_filename": "2602 update from P6.xer",
      "xer_file_path": "XER/2602 update from P6.xer",
      "snapshot_kind": "update",
      "snapshot_tag": "2602",
      "month_update": "2026-02-01",
      "update_date": "2026-02-28",
      "data_date": "2026-02-27"
    }
  ]
}
```

Paths in `xer_file_path` are resolved relative to the configuration file. Existing XER files do not need to be renamed; `original_xer_filename` must exactly match each path's filename. Run:

```powershell
dotnet run --project .\XerToCsvConverter.ProgrammeReview.Cli\XerToCsvConverter.ProgrammeReview.Cli.csproj -- --config .\bundle.json --output-root .\ProgrammeReviewBundles
```

The command prints the completed bundle path. A non-zero exit leaves no published partial bundle.

The same workflow is available to application callers through:

- `ProgrammeReviewBundleService.BuildFromXerFilesAsync(...)`
- `ProgrammeReviewBundleService.BuildFromParsedDataAsync(...)` for a merged `XerDataStore`; each parsed snapshot must include its precomputed `source_sha256`.
- `ProgrammeReviewBundleService.BuildFromXerBytesAsync(...)` for browser/in-memory inputs; it returns all twelve files without using the file system.
- `ProgrammeReviewBundleService.BuildFromParsedDataToMemoryAsync(...)` for callers that already own a parsed data store and need in-memory output.

## Metadata and selection rules

- `project_code` is uppercase A-Z/0-9/underscore. `programme_type` is `C` for Contract or `T` for Target.
- Update tags use `YYMM`. Their `month_update` must be in the same month and `update_date`, when supplied, must be that month-end.
- Baselines use `BLnn` or `BLnn-A`. If several candidates are supplied, the profile mirrors the Athena scope: highest baseline number, then highest letter revision, newest `month_update`, then canonical filename. Only that baseline and updates after its `month_update` anchor are included. A baseline `update_date` should be omitted; if supplied, it must equal the `month_update` month-end. The effective baseline update month is derived from `month_update` and shifted backwards around retained update collisions, matching Athena.
- The canonical identity is `<PROJECT>-<C|T>-<TAG>_<data-date-as-YYYYMMDD>.xer`. The manifest remains authoritative.
- `exported_at_utc` is optional. Supply it when byte-for-byte deterministic bundle naming is required; otherwise the current UTC second is used.
- The profile rejects ambiguous filenames/tags, duplicate XER content (even under different filenames), missing metadata, multiple task projects inside one XER, invalid types, duplicate primary keys, and broken task/WBS/calendar/code/resource relationships.

## Output contract

Each bundle contains exactly:

```text
01_XER_TASK.csv
02_XER_PROJECT.csv
03_XER_PROJWBS.csv
06_XER_PREDECESSOR.csv
07_XER_ACTVTYPE.csv
08_XER_ACTVCODE.csv
09_XER_TASKACTV.csv
10_XER_CALENDAR.csv
12_XER_RSRC.csv
15_XER_RESOURCE_DISTRIBUTION.csv
XER_CSV_MANIFEST.csv
```

CSV files are UTF-8 without a BOM, comma-delimited, RFC-style quoted, and use CRLF records. Dates are `yyyy-MM-dd`; numeric and integer tokens use invariant culture; booleans are lowercase `true`/`false`. Optional source tables are emitted header-only. The exact ordered schemas are exposed by `ProgrammeReviewContract.Tables` as schema version `3.0`.

All relationship keys use the compact form `CSV::<project_code>::<programme_type>::<snapshot_tag>::<native_id>`. The `::` delimiter is valid in DAX hierarchy identifiers and avoids the vertical pipe reserved by `PATH()`. Keys are stable when a project bundle is regenerated, while the project/programme/snapshot namespace prevents collisions within the whole-project replacement. Power BI resolves each project/programme to the newest completed active bundle. Historical task matching uses `project + task_code`, never P6 `task_id`.

## SharePoint publication

Keep each project's bundles beneath its own exact uppercase project-code folder. For a Windows export, upload the parser-generated bundle folder. For a web export, extract `<bundle_id>.zip` first and upload its inner `<bundle_id>` folder; do not upload the ZIP itself.

The destination is:

```text
<XerCsvLibrary>/P6/XER CSV/Active/<PROJECT_CODE>/<bundle_id>/
```

The current report reuses its existing `SharePointSite`, whose library is `Documents`; `XerCsvLibrary` may be changed if an override site exposes a different exact library name. The `<PROJECT_CODE>` folder, `XerCsvProjectCodes` value, and manifest `project_code` must use the same uppercase code. Upload the ten table CSVs first and `XER_CSV_MANIFEST.csv` last. Do not copy the staging directory or a partially written bundle. Manifest-free staging folders are ignored. Power BI orders contract-valid manifest-bearing folders by the timestamp in `bundle_id`, opens only the newest selected manifest, and fails refresh if that selected bundle is invalid rather than silently reverting to older data. After the report has accepted the new bundle, move superseded bundles to `Archive/<PROJECT_CODE>`. To roll back, move the latest bundle to Archive and restore the required preceding completed bundle to `Active/<PROJECT_CODE>` before refreshing.

Direct SharePoint authentication/upload is intentionally out of scope. Use a durable organisational/service account for the report's SharePoint connection and keep RLS authority in `dbo_project`/`dbo_userpermission`; manifest project metadata never grants access.

## Parity boundary

The profile reproduces the current Athena task-history logic from the available Enhanced tables: retained baseline/update ordering, previous task/project snapshots, adjusted baseline, Monday-Friday variances, `Driven_DataDate`, and last-period flags. It also reuses the parser's existing calendar-aware predecessor and resource-distribution transformers. These calculations must still be golden-compared against an Athena project with a baseline and at least two updates before production activation. Power BI Desktop/Service refresh, credentials, privacy levels, RLS, and visual behaviour remain manual acceptance checks.
