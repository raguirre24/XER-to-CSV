# Tender Review CLI

This separate command creates a validated local `tender_review` bundle. It does not publish or upload files.

```powershell
dotnet run --project XerToCsvConverter.TenderReview.Cli -- --config tender.json --output-root .\output
```

The configuration uses an ordered `sources` array. Each array position receives a stable internal source token for that request, so repeated paths, original filenames and file hashes remain distinct Tender stages. `status_date` is explicit and editable; it must use ISO `yyyy-MM-dd` and be unique within the build.

```json
{
  "project_code": "C5001",
  "project_name": "Example Tender",
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
