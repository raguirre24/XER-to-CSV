using System.IO.Compression;
using System.Text;
using System.Text.Json;
using Microsoft.VisualBasic.FileIO;
using XerToCsvConverter.ProgrammeReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ProgrammeReviewMultiProjectTests
{
    [Fact]
    public async Task Multiple_project_dates_preserve_rows_history_snapshot_metadata_and_raw_warning_evidence()
    {
        ProgrammeReviewSnapshot baseline = Baseline();
        ProgrammeReviewSnapshot update = ProgrammeReviewNamingTests.Snapshot(
            "update source.xer", ProgrammeReviewSnapshotKind.Update, "2602", "2026-02-01", "2026-02-27");
        XerDataStore store = CreateStore(
            new Source(baseline, [new("P1", "2026-01-30 08:15"), new("P2", "2026-01-27 10:45")],
                [new("T1", "P1", "A100", "2026-02-02", "2026-02-06"), new("T2", "P2", "B200", "2026-02-09", "2026-02-13")]),
            new Source(update, [new("P1", "2026-02-26 08:15"), new("P2", "2026-02-27 10:45")],
                [new("T1", "P1", "A100", "2026-02-04", "2026-02-10"), new("T2", "P2", "B200", "2026-02-11", "2026-02-17")]));
        string[][] originalFields = store.TableNames.SelectMany(name => store.GetTable(name)!.Rows)
            .Select(row => row.Fields.ToArray()).ToArray();
        ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request([baseline, update]);

        ProgrammeReviewInMemoryBundleResult result = await new ProgrammeReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, request);

        IReadOnlyList<Dictionary<string, string>> projects = ReadCsv(result.Files["02_XER_PROJECT.csv"]);
        Assert.Equal(4, projects.Count);
        Assert.Equal("2026-01-30", Project(projects, "BL01", "P1")["last_recalc_date"]);
        Assert.Equal("2026-01-27", Project(projects, "BL01", "P2")["last_recalc_date"]);
        Assert.Equal("2026-02-26", Project(projects, "2602", "P1")["last_recalc_date"]);
        Assert.Equal("2026-02-27", Project(projects, "2602", "P2")["last_recalc_date"]);
        Assert.All(projects, row => Assert.Equal("J123", row["ProjectCode"]));

        IReadOnlyList<Dictionary<string, string>> tasks = ReadCsv(result.Files["01_XER_TASK.csv"]);
        Assert.Equal(4, tasks.Count);
        foreach (Dictionary<string, string> row in tasks.Where(row => row["filename"].Contains("-2602_", StringComparison.Ordinal)))
        {
            Assert.Equal("2026-02-27", row["data_date"]);
            Assert.Equal("2026-01-30", row["PreviousDataDate"]);
            Assert.Equal(row["task_code"] == "A100" ? "2026-02-06" : "2026-02-13", row["Baseline Finish"]);
            Assert.Equal(row["Baseline Finish"], row["Previous Month Finish"]);
            Assert.StartsWith("CSV::J123::C::2602::", row["task_id_key"], StringComparison.Ordinal);
        }
        Assert.Equal(12, result.Files.Count);
        Assert.Equal(22, result.ManifestRows.Count);
        Assert.All(result.ManifestRows, row =>
        {
            bool isBaseline = row.OriginalXerFilename == baseline.OriginalXerFilename;
            Assert.Equal(isBaseline ? baseline.DataDate : update.DataDate, row.DataDate);
            Assert.Equal(isBaseline ? "J123-C-BL01_20260130.xer" : "J123-C-2602_20260227.xer", row.CanonicalXerFilename);
        });
        Assert.All(result.ManifestRows.Where(row => row.TableName is "01_XER_TASK" or "02_XER_PROJECT"),
            row => Assert.Equal(2, row.RowCount));

        IReadOnlyList<Dictionary<string, string>> warnings = QualityRows(result);
        Assert.Equal(4, warnings.Count(row => row["issue_code"] == "REVIEW_MULTIPLE_PROJECTS"));
        Assert.DoesNotContain(warnings, row => row["issue_code"] is "REVIEW_PROJECT_REFERENCE_INVALID" or "REVIEW_HISTORY_AMBIGUOUS");
        Dictionary<string, string> mismatch = Assert.Single(warnings, row =>
            row["issue_code"] == "REVIEW_PROJECT_DATA_DATE_MISMATCH" && row["FileName"] == baseline.OriginalXerFilename);
        Assert.Equal("PROJECT", mismatch["source_table"]);
        Assert.Equal("02_XER_PROJECT", mismatch["table_name"]);
        Assert.Equal("2", mismatch["source_row_number"]);
        Assert.Equal("CSV::J123::C::BL01", mismatch["source_namespace"]);
        Assert.Equal("last_recalc_date", mismatch["column_name"]);
        Assert.Equal("2026-01-27 10:45", mismatch["raw_value"]);
        using (JsonDocument json = JsonDocument.Parse(mismatch["raw_row_json"]))
        {
            JsonElement[] fields = json.RootElement.EnumerateArray().ToArray();
            Assert.Equal("proj_id", fields[0].GetProperty("column").GetString());
            Assert.Equal("P2", fields[0].GetProperty("value").GetString());
            Assert.Equal("2026-01-27 10:45", fields[1].GetProperty("value").GetString());
            Assert.Equal("Present", fields[1].GetProperty("presence").GetString());
        }
        Assert.DoesNotContain("private-occurrence-", Encoding.UTF8.GetString(XerDataQuality.WriteToBytes(result.DataQualityTable!)), StringComparison.Ordinal);
        string[][] fieldsAfter = store.TableNames.SelectMany(name => store.GetTable(name)!.Rows)
            .Select(row => row.Fields).ToArray();
        Assert.Equal(originalFields.Length, fieldsAfter.Length);
        for (int index = 0; index < originalFields.Length; index++) Assert.Equal(originalFields[index], fieldsAfter[index]);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task Reused_activity_codes_or_native_task_ids_are_retained_with_blank_history(bool sameCode, bool sameId)
    {
        ProgrammeReviewSnapshot baseline = Baseline();
        XerDataStore store = CreateStore(new Source(baseline,
            [new("P1", "2026-01-30"), new("P2", "2026-01-29")],
            [new("T1", "P1", "A100"), new(sameId ? "T1" : "T2", "P2", sameCode ? "A100" : "B200")]));

        ProgrammeReviewInMemoryBundleResult result = await Build(store, baseline);

        IReadOnlyList<Dictionary<string, string>> rows = ReadCsv(result.Files["01_XER_TASK.csv"]);
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, rows.Select(row => row["proj_id_key"]).Distinct(StringComparer.Ordinal).Count());
        Assert.All(rows, row =>
        {
            Assert.Equal("", row["Baseline Finish"]);
            Assert.Equal("", row["PreviousDataDate"]);
            Assert.Equal("", row["Finish_Variance_Previous_Month"]);
            Assert.Equal("", row["Driven_DataDate"]);
            Assert.NotEmpty(row["task_id_key"]);
        });
        Assert.Equal(2, QualityRows(result).Count(row => row["issue_code"] == "REVIEW_HISTORY_AMBIGUOUS"));
        if (sameId) Assert.Contains(QualityRows(result), row => row["issue_code"] == "REVIEW_KEY_AMBIGUOUS");
    }

    [Fact]
    public async Task Duplicate_project_ids_and_orphan_task_references_remain_visible()
    {
        ProgrammeReviewSnapshot baseline = Baseline();
        XerDataStore store = CreateStore(new Source(baseline,
            [new("P1", "2026-01-30"), new("P2", "2026-01-29"), new("P2", "2026-01-28")],
            [new("T1", "P2", "B200"), new("T2", "ORPHAN", "C300")]));
        ProgrammeReviewInMemoryBundleResult result = await Build(store, baseline);
        Assert.Equal(3, ReadCsv(result.Files["02_XER_PROJECT.csv"]).Count);
        Assert.Equal(2, ReadCsv(result.Files["01_XER_TASK.csv"]).Count);
        IReadOnlyList<Dictionary<string, string>> warnings = QualityRows(result);
        Assert.Contains(warnings, row => row["issue_code"] == "REVIEW_KEY_AMBIGUOUS" && row["table_name"] == "02_XER_PROJECT");
        Dictionary<string, string> orphan = Assert.Single(warnings, row => row["issue_code"] == "REVIEW_PROJECT_REFERENCE_INVALID");
        Assert.Equal("2", orphan["source_row_number"]);
        Assert.Contains("ORPHAN", orphan["raw_row_json"], StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("", false, "Blank", "REVIEW_PROJECT_DATA_DATE_MISSING")]
    [InlineData(" ", false, "Blank", "REVIEW_PROJECT_DATA_DATE_MISSING")]
    [InlineData("", true, "OmittedCell", "REVIEW_PROJECT_DATA_DATE_MISSING")]
    [InlineData("invalid", false, "Present", "REVIEW_VALUE_INVALID")]
    [InlineData("2026-02-30", false, "Present", "REVIEW_VALUE_INVALID")]
    public async Task Multi_project_invalid_or_missing_dates_keep_raw_evidence_and_blank_cells(
        string rawDate, bool omitted, string presence, string issueCode)
    {
        ProgrammeReviewSnapshot baseline = Baseline();
        XerDataStore store = CreateStore(new Source(baseline,
            [new("P1", "2026-01-30"), new("P2", rawDate, omitted)], [new("T1", "P1", "A100")]));
        ProgrammeReviewInMemoryBundleResult result = await Build(store, baseline);
        Assert.Equal("", Project(ReadCsv(result.Files["02_XER_PROJECT.csv"]), "BL01", "P2")["last_recalc_date"]);
        Dictionary<string, string> warning = Assert.Single(QualityRows(result), row =>
            row["issue_code"] == issueCode && row["column_name"] == "last_recalc_date");
        Assert.Equal("2", warning["source_row_number"]);
        Assert.Equal(rawDate, warning["raw_value"]);
        using JsonDocument json = JsonDocument.Parse(warning["raw_row_json"]);
        Assert.Equal(presence, json.RootElement[1].GetProperty("presence").GetString());
        Assert.Equal(omitted ? "" : rawDate, json.RootElement[1].GetProperty("value").GetString());
    }

    [Theory]
    [InlineData("")]
    [InlineData("invalid")]
    [InlineData("2026-01-29")]
    public async Task Single_project_missing_invalid_or_mismatched_date_remains_fatal(string rawDate)
    {
        ProgrammeReviewSnapshot baseline = Baseline();
        XerDataStore store = CreateStore(new Source(baseline, [new("P1", rawDate)], [new("T1", "P1", "A100")]));
        ProgrammeReviewValidationException error = await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() => Build(store, baseline));
        Assert.Contains("02_XER_PROJECT/baseline source.xer", error.Message, StringComparison.Ordinal);
        Assert.Contains("last_recalc_date", error.Message, StringComparison.Ordinal);
        if (rawDate == "2026-01-29")
            Assert.Equal("02_XER_PROJECT/baseline source.xer: manifest data_date 2026-01-30 does not match XER last_recalc_date 2026-01-29.", error.Message);
    }

    [Fact]
    public async Task Multi_project_missing_date_header_preserves_absent_header_evidence()
    {
        ProgrammeReviewSnapshot baseline = Baseline();
        XerDataStore store = CreateStore(new Source(baseline,
            [new("P1", "2026-01-30"), new("P2", "2026-01-29")], [new("T1", "P1", "A100")]));
        XerTable original = store.GetTable("PROJECT")!;
        XerTable missingHeader = NewTable("PROJECT", ["proj_id"]);
        foreach (DataRow row in original.Rows)
            missingHeader.AddRow(new DataRow([row.Fields[0]], row.SourceFilename, row.SourceToken));
        store.AddTable(missingHeader);
        ProgrammeReviewInMemoryBundleResult result = await Build(store, baseline);
        IReadOnlyList<Dictionary<string, string>> projects = ReadCsv(result.Files["02_XER_PROJECT.csv"]);
        Assert.Equal(2, projects.Count);
        Assert.All(projects, row => Assert.Equal("", row["last_recalc_date"]));
        Dictionary<string, string> warning = Assert.Single(QualityRows(result), row =>
            row["issue_code"] == "REVIEW_PROJECT_DATA_DATE_MISSING" && row["source_row_number"] == "2");
        Assert.Equal("last_recalc_date", warning["column_name"]);
        Assert.DoesNotContain("last_recalc_date", warning["raw_row_json"], StringComparison.Ordinal);
        DataRow sourceRow = missingHeader.Rows[1];
        Assert.Equal(XerRawFieldState.AbsentHeader, sourceRow.GetRawField("last_recalc_date").State);
    }

    [Fact]
    public async Task Single_project_date_projection_keeps_exact_existing_csv_bytes()
    {
        ProgrammeReviewSnapshot baseline = Baseline();
        XerDataStore store = CreateStore(new Source(baseline,
            [new("P1", "2026-01-30 08:15")], [new("T1", "P1", "A100")]));
        ProgrammeReviewInMemoryBundleResult result = await Build(store, baseline);
        string expected = "last_recalc_date,proj_id_key,monthupdate,ProjectCode\r\n"
            + "2026-01-30,CSV::J123::C::BL01::P1,2026-01-31,J123\r\n";
        Assert.Equal(Encoding.UTF8.GetBytes(expected), result.Files["02_XER_PROJECT.csv"]);
        Dictionary<string, string> task = Assert.Single(ReadCsv(result.Files["01_XER_TASK.csv"]));
        Assert.Equal("2026-01-30", task["data_date"]);
        Assert.Equal("J123-C-BL01_20260130.xer", task["filename"]);
        Assert.DoesNotContain(QualityRows(result), row => row["issue_code"] is
            "REVIEW_MULTIPLE_PROJECTS" or "REVIEW_PROJECT_DATA_DATE_MISMATCH" or "REVIEW_PROJECT_DATA_DATE_MISSING");
    }

    [Fact]
    public async Task Repeated_single_native_project_identity_retains_strict_date_mismatch()
    {
        ProgrammeReviewSnapshot baseline = Baseline();
        XerDataStore store = CreateStore(new Source(baseline,
            [new("P1", "2026-01-30"), new("p1", "2026-01-29")], [new("T1", "P1", "A100")]));
        await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() => Build(store, baseline));
    }

    [Fact]
    public async Task Zero_governed_project_ids_remain_fatal()
    {
        ProgrammeReviewSnapshot baseline = Baseline();
        XerDataStore store = CreateStore(new Source(baseline, [new("", "2026-01-30")], [new("T1", "P1", "A100")]));
        ProgrammeReviewValidationException error = await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() => Build(store, baseline));
        Assert.Contains("found 0", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Multi_project_memory_and_streaming_bundles_match_every_csv_and_diagnostic_byte()
    {
        ProgrammeReviewSnapshot baseline = Baseline();
        XerDataStore store = CreateStore(new Source(baseline,
            [new("P1", "2026-01-30"), new("P2", "2026-01-29")], [new("T1", "P1", "A100"), new("T2", "P2", "B200")]));
        ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request([baseline]);
        var service = new ProgrammeReviewBundleService();
        ProgrammeReviewInMemoryBundleResult memory = await service.BuildFromParsedDataToMemoryAsync(store, request);
        using var output = new MemoryStream();
        ProgrammeReviewZipStreamBundleResult streaming = await service.BuildFromParsedDataToZipStreamAsync(store, request, output);
        output.Position = 0;
        using var zip = new ZipArchive(output, ZipArchiveMode.Read);
        Assert.Equal(12, zip.Entries.Count);
        foreach (ZipArchiveEntry entry in zip.Entries)
        {
            using Stream source = entry.Open();
            using var bytes = new MemoryStream();
            source.CopyTo(bytes);
            Assert.Equal(memory.Files[entry.Name], bytes.ToArray());
        }
        Assert.Equal(XerDataQuality.WriteToBytes(memory.DataQualityTable!), XerDataQuality.WriteToBytes(streaming.DataQualityTable!));
    }

    private static ProgrammeReviewSnapshot Baseline() => ProgrammeReviewNamingTests.Snapshot(
        "baseline source.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");

    private static Task<ProgrammeReviewInMemoryBundleResult> Build(XerDataStore store, params ProgrammeReviewSnapshot[] snapshots) =>
        new ProgrammeReviewBundleService().BuildFromParsedDataToMemoryAsync(store, ProgrammeReviewNamingTests.Request(snapshots));

    private static Dictionary<string, string> Project(IReadOnlyList<Dictionary<string, string>> rows, string tag, string id) =>
        Assert.Single(rows, row => row["proj_id_key"] == $"CSV::J123::C::{tag}::{id}");

    private static IReadOnlyList<Dictionary<string, string>> QualityRows(ProgrammeReviewInMemoryBundleResult result) =>
        ReadCsv(XerDataQuality.WriteToBytes(result.DataQualityTable!));

    private static IReadOnlyList<Dictionary<string, string>> ReadCsv(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var parser = new TextFieldParser(stream, Encoding.UTF8, detectEncoding: false)
        {
            TextFieldType = FieldType.Delimited,
            HasFieldsEnclosedInQuotes = true,
            TrimWhiteSpace = false
        };
        parser.SetDelimiters(",");
        string[] headers = parser.ReadFields()!;
        var rows = new List<Dictionary<string, string>>();
        while (!parser.EndOfData)
            rows.Add(headers.Zip(parser.ReadFields()!).ToDictionary(pair => pair.First, pair => pair.Second, StringComparer.Ordinal));
        return rows;
    }

    private static XerDataStore CreateStore(params Source[] sources)
    {
        var store = new XerDataStore();
        XerTable projects = NewTable("PROJECT", ["proj_id", "last_recalc_date"]);
        XerTable tasks = NewTable("TASK", ["task_id", "proj_id", "wbs_id", "clndr_id", "task_type", "status_code",
            "task_code", "task_name", "rsrc_id", "act_start_date", "act_end_date", "early_start_date", "early_end_date",
            "late_start_date", "late_end_date", "target_start_date", "target_end_date", "cstr_type", "cstr_date", "priority_type",
            "float_path", "float_path_order", "driving_path_flag", "remain_drtn_hr_cnt", "target_drtn_hr_cnt", "total_float_hr_cnt",
            "free_float_hr_cnt", "complete_pct_type", "phys_complete_pct", "act_work_qty", "remain_work_qty"]);
        XerTable wbs = NewTable("PROJWBS", ["wbs_id", "parent_wbs_id", "proj_id", "wbs_name"]);
        XerTable calendars = NewTable("CALENDAR", ["clndr_id", "clndr_name", "day_hr_cnt", "clndr_type", "clndr_data"]);
        foreach (Source source in sources)
        {
            string filename = source.Snapshot.OriginalXerFilename;
            string token = "private-occurrence-" + filename;
            foreach (ProjectInput project in source.Projects)
            {
                projects.AddRow(new DataRow(project.OmittedDate ? [project.Id] : [project.Id, project.Date], filename, token));
                wbs.AddRow(new DataRow(["W" + project.Id, "", project.Id, "Root " + project.Id], filename, token));
            }
            foreach (TaskInput task in source.Tasks)
                tasks.AddRow(new DataRow([task.Id, task.Project, "W" + task.Project, "C1", "TT_Task", "TK_NotStart",
                    task.Code, "Activity " + task.Code, "", "", "", task.Start, task.Finish, task.Start, task.Finish,
                    task.Start, task.Finish, "", "", "", "", "", "Y", "24", "40", "16", "8", "CP_Phys", "0", "0", "0"], filename, token));
            calendars.AddRow(new DataRow(["C1", "Standard 8h", "8", "CA_Project", P6TestCalendars.WorkWeek()], filename, token));
        }
        store.AddTable(tasks);
        store.AddTable(projects);
        store.AddTable(wbs);
        store.AddTable(calendars);
        return store;
    }

    private static XerTable NewTable(string name, string[] headers)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        return table;
    }

    private sealed record Source(ProgrammeReviewSnapshot Snapshot, ProjectInput[] Projects, TaskInput[] Tasks);
    private sealed record ProjectInput(string Id, string Date, bool OmittedDate = false);
    private sealed record TaskInput(string Id, string Project, string Code, string Start = "2026-02-02", string Finish = "2026-02-06");
}
