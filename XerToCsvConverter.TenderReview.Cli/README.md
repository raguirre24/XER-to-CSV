# Tender Review CLI

This separate command creates a validated local `tender_review` schema **3.0** bundle. It does not publish or upload files. Use the matching Tender 3.0 report loader and regenerate older bundles; do not edit their version cells. Programme Review remains a separate profile.

```powershell
dotnet run --project XerToCsvConverter.TenderReview.Cli -- --config tender.json --output-root .\output
```

The configuration uses an ordered `sources` array. Each array position receives a stable internal source token for that request, so repeated paths, original filenames and file hashes remain distinct Tender stages. `status_date` is explicit and editable; it must use ISO `yyyy-MM-dd` and be unique within the build.

`project_code` is your explicit reporting identity for all selected stages. It controls numbered-table `ProjectCode`, compact keys, filenames and the manifest; it does not have to equal P6 `PROJECT.proj_short_name`. For example, report code `QAC000623` can contain a stage whose P6 short name is `QAC000623-01-02`. The parser does not infer this mapping by stripping suffixes: selecting the sources under that code makes the assignment explicit. Each XER must still contain exactly one `PROJECT` row. Different native names in selected revisions are retained in the Core result's `TENDER_PROJECT_CODE_MAPPED` diagnostic; source XER names are not rewritten.

Tender has no C/J aliases: `C5001`, `J5001` and `5001` are distinct reporting identities. An intentionally configured source-to-report mapping remains explicit and produces its mapping diagnostic.

`state` is optional manual metadata for the whole reporting project and every selected stage. It is never read from the XER, project code, filename or permissions. Core trims outer whitespace, applies uppercase and maps recognised full state names to abbreviations (`Queensland` becomes `QLD`); custom labels are preserved in uppercase. Omitted, null, empty and whitespace-only values mean unknown and do not block export. `TENDER_PROJECT_STATE_UNKNOWN` warns that state-based access cannot match; an existing all-project or exact-project grant may still apply. No permission is created by the import.

State changes the audience of existing state grants. Only an authorised bundle publisher should classify project visibility; use trusted approval before publishing if contributors are not authorised to do so. The manifest's nullable `project_state` is authoritative and table 02 mirrors the same manual value. A State-only change creates a new bundle identity without changing stage/task keys or scheduling calculations. The bundle remains exactly ten numbered CSVs plus one manifest; diagnostics are printed on stderr, not added as another CSV.

```json
{
  "project_code": "C5001",
  "project_name": "Example Tender",
  "state": "Queensland",
  "sources": [
    {
      "xer_file_path": "stages\\tender.xer",
      "original_xer_filename": "tender.xer",
      "status_date": "2026-09-01"
    },
    {
      "xer_file_path": "stages\\tender.xer",
      "original_xer_filename": "tender.xer",
      "status_date": "2026-09-05"
    }
  ]
}
```

Relative XER paths resolve from the configuration file's directory. Supplying `exported_at_utc` freezes the bundle clock for deterministic repeat builds; otherwise Core captures one UTC build instant.
