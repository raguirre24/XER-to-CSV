using System.Globalization;
using System.Text;
using Microsoft.VisualBasic.FileIO;

namespace XerToCsvConverter.Core.Tests;

public sealed class DataQualityActualExportTests
{
    private const string SourceNamespace = "2607, \"snapshot\".xer";
    private const string SourceToken = "stable-ordered-occurrence-1";
    private const string OriginalFilename = "2607, \"snapshot\".xer";
    private const string Distribution = EnhancedTableNames.XerResourceDist15;
    private const string Diagnostic = "XER_DATA_QUALITY";
    private static readonly string[] DiagnosticColumns =
    [
        "diagnostic_schema_version", "severity", "issue_code", "table_name", "source_namespace",
        "source_row_number", "proj_id_key", "task_id_key", "rsrc_id_key", "taskrsrc_id_key",
        "taskrsrc_id", "task_code", "rsrc_name", "rsrc_type", "unit", "status_code",
        "act_start_date", "act_end_date", "project_data_date", "act_reg_qty", "act_ot_qty",
        "unallocated_actual_quantity", "message"
    ];

    [Fact]
    public void Invalid_actuals_preserve_valid_remaining_and_other_assignments_and_reconcile_at_export_precision()
    {
        XerDataStore store = Store();
        AddAssignment(store, "A1", "10.00004", "0.00002", "4", "2026-08-03 08:00", "");
        AddAssignment(store, "A2", "3", "0", "6", "2026-08-06 08:00", "");
        AddAssignment(store, "A3", "2", "1", "2", "2026-07-20 08:00", "2026-07-21 17:00");
        var transformer = new XerTransformer(store);

        XerTable distribution = Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution());
        XerTable diagnostics = transformer.CreateDataQualityTable();
        var rows = Records(distribution);
        var issues = Records(diagnostics);

        Assert.Equal(2, issues.Count);
        Assert.Equal(3m, rows.Where(row => row["is_actual"] == "1").Sum(row => Number(row["monthly_quantity"])));
        Assert.Equal(12m, rows.Where(row => row["is_actual"] == "0").Sum(row => Number(row["monthly_quantity"])));
        Assert.Equal(16.0001m,
            rows.Where(row => row["is_actual"] == "1").Sum(row => Number(row["monthly_quantity"]))
            + issues.Sum(row => Number(row["unallocated_actual_quantity"])));
        Assert.All(rows, row => Assert.False(string.IsNullOrWhiteSpace(row["distribution_month"])));
        Assert.All(issues, issue =>
        {
            Assert.Equal("1.0", issue["diagnostic_schema_version"]);
            Assert.Equal("Warning", issue["severity"]);
            Assert.Equal("ACTUAL_FINISH_BEFORE_START", issue["issue_code"]);
            Assert.Equal(Distribution, issue["table_name"]);
            Assert.Equal("2026-07-25 08:00", issue["project_data_date"]);
            Assert.Equal("", issue["act_end_date"]);
            Assert.NotEmpty(issue["message"]);
        });
        Assert.Equal(new[] { "A1", "A2" }, issues.Select(row => row["taskrsrc_id"]));
        Assert.Equal(new[] { "1", "2" }, issues.Select(row => row["source_row_number"]));
        Assert.Null(transformer.GetGenerationFailure(Distribution));
    }

    [Theory]
    [InlineData("act_start_date", "", "TK_Active")]
    [InlineData("act_start_date", " not-a-date, \"raw\" ", "TK_Active")]
    [InlineData("act_end_date", " not-a-date, \"raw\" ", "TK_Active")]
    [InlineData("act_end_date", "", "TK_Complete")]
    [InlineData("act_end_date", "invalid", "TK_Complete")]
    public void Unresolvable_actual_dates_are_preserved_verbatim_and_do_not_create_a_guessed_month(
        string field, string raw, string status)
    {
        XerDataStore store = Store(status: status);
        AddAssignment(store, "A1", " 1.250000 ", "0.125", "0", "2026-07-20 08:00", "2026-07-21 17:00");
        Set(store, "TASKRSRC", field, raw);
        string[][] before = store.GetTable("TASKRSRC")!.Rows.Select(row => row.Fields.ToArray()).ToArray();
        var transformer = new XerTransformer(store);

        XerTable distribution = Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution());
        XerTable diagnostics = transformer.CreateDataQualityTable();
        Dictionary<string, string> issue = Assert.Single(Records(diagnostics));

        Assert.Empty(distribution.Rows);
        Assert.Equal(DiagnosticColumns, diagnostics.Headers);
        Assert.Equal("ACTUAL_PERIOD_INVALID", issue["issue_code"]);
        Assert.Equal(raw, issue[field]);
        Assert.Equal(" 1.250000 ", issue["act_reg_qty"]);
        Assert.Equal("0.125", issue["act_ot_qty"]);
        Assert.Equal("1.3750", issue["unallocated_actual_quantity"]);
        Assert.Equal(SourceNamespace, issue["source_namespace"]);
        Assert.Equal(SourceNamespace + ".A1", issue["taskrsrc_id_key"]);
        Assert.Equal(SourceNamespace + ".P1", issue["proj_id_key"]);
        Assert.Equal(SourceNamespace + ".T1", issue["task_id_key"]);
        Assert.Equal(SourceNamespace + ".R1", issue["rsrc_id_key"]);
        Assert.Equal("A100", issue["task_code"]);
        Assert.Equal("Crew, \"one\"", issue["rsrc_name"]);
        Assert.Equal("RT_Labor", issue["rsrc_type"]);
        Assert.Equal("unit/time", issue["unit"]);
        Assert.Equal(status, issue["status_code"]);
        Assert.Equal(before[0], store.GetTable("TASKRSRC")!.Rows[0].Fields);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" invalid project data date ")]
    public void Missing_or_invalid_data_date_only_warns_for_actuals_that_need_it(string dataDate)
    {
        XerDataStore store = Store(dataDate: dataDate);
        AddAssignment(store, "A1", "7", "0", "0", "2026-07-20 08:00", "");
        AddAssignment(store, "A2", "5", "0", "0", "2026-07-20 08:00", "2026-07-21 17:00");
        var transformer = new XerTransformer(store);

        var rows = Records(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()));
        var issue = Assert.Single(Records(transformer.CreateDataQualityTable()));

        Assert.Equal(5m, rows.Sum(row => Number(row["monthly_quantity"])));
        Assert.Equal("7.0000", issue["unallocated_actual_quantity"]);
        Assert.Equal("A1", issue["taskrsrc_id"]);
        Assert.Equal(dataDate, issue["project_data_date"]);
        Assert.Equal("ACTUAL_PERIOD_INVALID", issue["issue_code"]);
    }

    [Fact]
    public void Explicit_reversed_finish_is_not_replaced_by_a_valid_data_date()
    {
        XerDataStore store = Store();
        AddAssignment(store, "A1", "7", "0", "0", "2026-07-20 08:00", "2026-07-19 17:00");
        var transformer = new XerTransformer(store);

        Assert.Empty(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()).Rows);
        var issue = Assert.Single(Records(transformer.CreateDataQualityTable()));
        Assert.Equal("ACTUAL_FINISH_BEFORE_START", issue["issue_code"]);
        Assert.Equal("2026-07-19 17:00", issue["act_end_date"]);
        Assert.Equal("7.0000", issue["unallocated_actual_quantity"]);
    }

    [Fact]
    public void Explicit_actual_period_after_data_date_keeps_its_recorded_dates_without_rescheduling()
    {
        XerDataStore store = Store();
        AddAssignment(store, "A1", "7", "0", "0", "2026-08-03 08:00", "2026-08-04 17:00");
        var transformer = new XerTransformer(store);

        var row = Assert.Single(Records(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution())));

        Assert.Equal("2026-08-01", row["distribution_month"]);
        Assert.Equal("2026-08-03 08:00:00", row["Start"]);
        Assert.Equal("2026-08-04 17:00:00", row["Finish"]);
        Assert.Equal("7.0000", row["monthly_quantity"]);
        Assert.Empty(transformer.CreateDataQualityTable().Rows);
    }

    [Fact]
    public void Repeated_generation_replaces_warnings_and_a_corrected_retry_clears_them()
    {
        XerDataStore store = Store();
        AddAssignment(store, "A1", "7", "0", "0", "2026-08-03 08:00", "");
        var transformer = new XerTransformer(store);
        Assert.Empty(transformer.CreateDataQualityTable().Rows);
        Assert.NotNull(transformer.Create15XerResourceDistribution());
        Assert.Single(transformer.CreateDataQualityTable().Rows);
        Assert.NotNull(transformer.Create15XerResourceDistribution());
        Assert.Single(transformer.CreateDataQualityTable().Rows);

        Set(store, "TASKRSRC", "act_start_date", "2026-07-20 08:00");

        Assert.NotEmpty(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()).Rows);
        Assert.Empty(transformer.CreateDataQualityTable().Rows);
    }

    [Fact]
    public async Task Ordered_repeated_filenames_keep_diagnostics_quantities_and_source_row_numbers_isolated()
    {
        XerDataStore first = Store();
        AddAssignment(first, "ZERO", "0", "0", "0", "invalid", "invalid");
        AddAssignment(first, "A1", "7", "0", "0", "2026-08-03 08:00", "");
        XerDataStore second = Store(dataDate: "2026-08-10 08:00");
        AddAssignment(second, "A1", "9", "0", "0", "2026-08-03 08:00", "");
        using MemoryStream firstStream = XerStream(first);
        using MemoryStream secondStream = XerStream(second);
        var service = new ProcessingService();
        XerDataStore parsed = await service.ParseXerStreamsAsync(
            [(firstStream, "same.xer"), (secondStream, "same.xer")], null, CancellationToken.None);

        Dictionary<string, byte[]> files = await service.ExportTablesToMemoryAsync(
            parsed, [Distribution], null, CancellationToken.None);
        Dictionary<string, string> issue = Assert.Single(CsvRecords(files[Diagnostic]));
        var rows = CsvRecords(files[Distribution]);

        Assert.Equal(2, files.Count);
        Assert.Equal("same.xer#source-000001", issue["source_namespace"]);
        Assert.Equal("same.xer#source-000001.A1", issue["taskrsrc_id_key"]);
        Assert.Equal("same.xer", issue["FileName"]);
        Assert.Equal("2", issue["source_row_number"]);
        Assert.Equal("7.0000", issue["unallocated_actual_quantity"]);
        Assert.Equal("2026-07-25 08:00", issue["project_data_date"]);
        Assert.Equal(9m, rows.Sum(row => Number(row["monthly_quantity"])));
        Assert.All(rows, row => Assert.Equal("same.xer#source-000002.T1", row["task_id_key"]));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Standard_export_includes_a_quoted_lossless_sidecar_and_valid_remaining(bool toDisk)
    {
        using var output = new TemporaryOutput();
        XerDataStore store = Store();
        AddAssignment(store, "A1", " 1.25 ", "0.125", "4", " invalid, \"raw\" ", "");

        Dictionary<string, byte[]> files = await Export(toDisk, store, [Distribution], output.Path);
        var issue = Assert.Single(CsvRecords(files[Diagnostic]));

        Assert.Equal(new[] { Distribution, Diagnostic }, files.Keys.OrderBy(key => key, StringComparer.Ordinal));
        Assert.Equal(DiagnosticColumns.Append("FileName"), ReadCsv(files[Diagnostic])[0]);
        Assert.Equal(" invalid, \"raw\" ", issue["act_start_date"]);
        Assert.Equal(" 1.25 ", issue["act_reg_qty"]);
        Assert.Equal("Crew, \"one\"", issue["rsrc_name"]);
        Assert.Equal(OriginalFilename, issue["FileName"]);
        Assert.Equal("1.3750", issue["unallocated_actual_quantity"]);
        Assert.Equal(4m, CsvRecords(files[Distribution]).Sum(row => Number(row["monthly_quantity"])));
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    [InlineData(true, 0)]
    [InlineData(true, 1)]
    [InlineData(true, 2)]
    public async Task Typed_standard_results_report_actual_warning_counts_for_every_successful_export(
        bool toDisk, int warningCount)
    {
        using var output = new TemporaryOutput();
        XerDataStore store = Store();
        AddAssignment(store, "VALID", "1", "0", "0", "2026-07-20 08:00", "2026-07-21 17:00");
        for (int i = 0; i < warningCount; i++)
            AddAssignment(store, "WARNING" + i, "7", "0", "4", "2026-08-03 08:00", "");
        var service = new ProcessingService();

        if (toDisk)
        {
            StandardDiskExportResult result = await service.ExportTablesWithDiagnosticsAsync(
                store, [Distribution], output.Path, null, CancellationToken.None);
            Assert.Equal(warningCount, result.WarningCount);
            Assert.Equal(2, result.Files.Count);
            Assert.Equal(warningCount, CsvRecords(File.ReadAllBytes(
                Assert.Single(result.Files, path => Path.GetFileName(path) == Diagnostic + ".csv"))).Count);
        }
        else
        {
            StandardMemoryExportResult result = await service.ExportTablesToMemoryWithDiagnosticsAsync(
                store, [Distribution], null, CancellationToken.None);
            Assert.Equal(warningCount, result.WarningCount);
            Assert.Equal(2, result.Files.Count);
            Assert.Equal(warningCount, CsvRecords(result.Files[Diagnostic]).Count);
        }
    }

    [Fact]
    public async Task Header_only_sidecar_replaces_stale_warnings_without_touching_unrelated_files()
    {
        using var output = new TemporaryOutput();
        string sidecar = Path.Combine(output.Path, Diagnostic + ".csv");
        string unrelated = Path.Combine(output.Path, "unrelated.txt");
        File.WriteAllText(sidecar, "old warnings");
        File.WriteAllText(unrelated, "unchanged");
        XerDataStore store = Store();
        AddAssignment(store, "A1", "7", "0", "0", "2026-07-20 08:00", "2026-07-21 17:00");

        var files = await Export(true, store, [Distribution], output.Path);

        Assert.Equal(2, files.Count);
        Assert.Equal(DiagnosticColumns.Append("FileName"), Assert.Single(ReadCsv(files[Diagnostic])));
        Assert.Equal("unchanged", File.ReadAllText(unrelated));
        Assert.Empty(Directory.GetDirectories(output.Path));
    }

    [Fact]
    public async Task Empty_assignment_source_still_exports_header_only_distribution_and_sidecar()
    {
        using var output = new TemporaryOutput();
        var files = await Export(false, Store(), [Distribution], output.Path);
        Assert.Equal(2, files.Count);
        Assert.Single(ReadCsv(files[Distribution]));
        Assert.Equal(DiagnosticColumns.Append("FileName"), Assert.Single(ReadCsv(files[Diagnostic])));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Unrequested_distribution_does_not_add_diagnostics_or_validate_its_bad_actuals(bool toDisk)
    {
        using var output = new TemporaryOutput();
        XerDataStore store = Store();
        AddAssignment(store, "A1", "invalid", "0", "0", "invalid", "invalid");

        var files = await Export(toDisk, store, [EnhancedTableNames.XerProject02], output.Path);

        Assert.Equal(EnhancedTableNames.XerProject02, Assert.Single(files).Key);
    }

    [Theory]
    [InlineData("TASKRSRC", "remain_qty", "invalid")]
    [InlineData("TASKRSRC", "act_reg_qty", "NaN")]
    [InlineData("TASKRSRC", "reend_date", "invalid")]
    [InlineData("CALENDAR", "clndr_data", "malformed")]
    [InlineData("TASK", "clndr_id", "missing")]
    public async Task Actual_date_warning_does_not_hide_required_quantity_remaining_or_calendar_failure(
        string table, string field, string value)
    {
        using var output = new TemporaryOutput();
        string distributionPath = Path.Combine(output.Path, Distribution + ".csv");
        string diagnosticPath = Path.Combine(output.Path, Diagnostic + ".csv");
        File.WriteAllText(distributionPath, "prior distribution");
        File.WriteAllText(diagnosticPath, "prior diagnostics");
        XerDataStore store = Store();
        AddAssignment(store, "A1", "7", "0", "4", "2026-08-03 08:00", "");
        Set(store, table, field, value);

        await Assert.ThrowsAsync<InvalidDataException>(() => new ProcessingService().ExportTablesAsync(
            store, [Distribution], output.Path, null, CancellationToken.None));

        Assert.Equal("prior distribution", File.ReadAllText(distributionPath));
        Assert.Equal("prior diagnostics", File.ReadAllText(diagnosticPath));
        Assert.Equal(2, Directory.GetFiles(output.Path).Length);
        Assert.Empty(Directory.GetDirectories(output.Path));
    }

    [Fact]
    public async Task Sidecar_publication_failure_rolls_back_already_replaced_distribution()
    {
        if (!OperatingSystem.IsWindows()) return;
        using var output = new TemporaryOutput();
        string distributionPath = Path.Combine(output.Path, Distribution + ".csv");
        string diagnosticPath = Path.Combine(output.Path, Diagnostic + ".csv");
        File.WriteAllText(distributionPath, "prior distribution");
        File.WriteAllText(diagnosticPath, "prior diagnostics");
        XerDataStore store = Store();
        AddAssignment(store, "A1", "7", "0", "4", "2026-08-03 08:00", "");

        using (var locked = new FileStream(diagnosticPath, FileMode.Open, FileAccess.Read, FileShare.Read))
            await Assert.ThrowsAnyAsync<IOException>(() => new ProcessingService().ExportTablesAsync(
                store, [Distribution], output.Path, null, CancellationToken.None));

        Assert.Equal("prior distribution", File.ReadAllText(distributionPath));
        Assert.Equal("prior diagnostics", File.ReadAllText(diagnosticPath));
        Assert.Equal(2, Directory.GetFiles(output.Path).Length);
        Assert.Empty(Directory.GetDirectories(output.Path));
    }

    private static XerDataStore Store(string status = "TK_Active", string dataDate = "2026-07-25 08:00")
    {
        var store = new XerDataStore();
        AddTable(store, "PROJECT", ["proj_id", "clndr_id", "last_recalc_date"], ["P1", "C1", dataDate]);
        AddTable(store, "TASK", ["task_id", "proj_id", "clndr_id", "task_type", "status_code", "task_code"],
            ["T1", "P1", "C1", "TT_Task", status, "A100"]);
        AddTable(store, "RSRC", ["rsrc_id", "clndr_id", "rsrc_type", "rsrc_name", "rsrc_short_name"],
            ["R1", "C1", "RT_Labor", "Crew, \"one\"", "CREW"]);
        AddTable(store, "CALENDAR", ["clndr_id", "clndr_name", "clndr_type", "day_hr_cnt", "clndr_data"],
            ["C1", "Calendar", "CA_Project", "8", P6TestCalendars.WorkWeek()]);
        AddTable(store, "TASKRSRC", ["taskrsrc_id", "task_id", "rsrc_id", "proj_id", "act_reg_qty",
            "act_ot_qty", "remain_qty", "act_start_date", "act_end_date", "restart_date", "reend_date"]);
        return store;
    }

    private static void AddAssignment(XerDataStore store, string id, string actual, string overtime,
        string remaining, string start, string finish) => store.GetTable("TASKRSRC")!.AddRow(new DataRow(
            [id, "T1", "R1", "P1", actual, overtime, remaining, start, finish,
                "2026-07-27 08:00", "2026-07-28 17:00"], SourceNamespace, SourceToken, OriginalFilename));

    private static void AddTable(XerDataStore store, string name, string[] headers, params string[][] rows)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        foreach (string[] row in rows) table.AddRow(new DataRow(row, SourceNamespace, SourceToken, OriginalFilename));
        store.AddTable(table);
    }

    private static void Set(XerDataStore store, string tableName, string field, string value)
    {
        XerTable table = store.GetTable(tableName)!;
        Assert.Single(table.Rows).Fields[table.FieldIndexes[field]] = value;
    }

    private static decimal Number(string value) => decimal.Parse(value, CultureInfo.InvariantCulture);

    private static IReadOnlyList<Dictionary<string, string>> Records(XerTable table) => table.Rows
        .Select(row => table.Headers!.Select((name, i) => (name, row.Fields[i]))
            .ToDictionary(pair => pair.name, pair => pair.Item2, StringComparer.Ordinal)).ToArray();

    private static IReadOnlyList<Dictionary<string, string>> CsvRecords(byte[] bytes)
    {
        IReadOnlyList<string[]> csv = ReadCsv(bytes);
        return csv.Skip(1).Select(row => csv[0].Select((name, i) => (name, row[i]))
            .ToDictionary(pair => pair.name, pair => pair.Item2, StringComparer.Ordinal)).ToArray();
    }

    private static IReadOnlyList<string[]> ReadCsv(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var parser = new TextFieldParser(stream, Encoding.UTF8, detectEncoding: true)
        {
            TextFieldType = FieldType.Delimited, HasFieldsEnclosedInQuotes = true, TrimWhiteSpace = false
        };
        parser.SetDelimiters(",");
        var result = new List<string[]>();
        while (!parser.EndOfData) result.Add(parser.ReadFields()!);
        return result;
    }

    private static MemoryStream XerStream(XerDataStore store)
    {
        var text = new StringBuilder();
        foreach (string name in store.TableNames)
        {
            XerTable table = store.GetTable(name)!;
            text.Append("%T\t").AppendLine(name);
            text.Append("%F\t").AppendLine(string.Join('\t', table.Headers!));
            foreach (DataRow row in table.Rows) text.Append("%R\t").AppendLine(string.Join('\t', row.Fields));
        }
        text.AppendLine("%E");
        return new MemoryStream(Encoding.UTF8.GetBytes(text.ToString()));
    }

    private static async Task<Dictionary<string, byte[]>> Export(bool toDisk, XerDataStore store,
        List<string> selected, string outputPath)
    {
        var service = new ProcessingService();
        if (!toDisk) return await service.ExportTablesToMemoryAsync(store, selected, null, CancellationToken.None);
        List<string> paths = await service.ExportTablesAsync(store, selected, outputPath, null, CancellationToken.None);
        return paths.ToDictionary(path => Path.GetFileNameWithoutExtension(path), File.ReadAllBytes, StringComparer.Ordinal);
    }

    private sealed class TemporaryOutput : IDisposable
    {
        private readonly DirectoryInfo _directory = Directory.CreateTempSubdirectory("xer-actual-warning-");
        public string Path => _directory.FullName;
        public void Dispose() => _directory.Delete(true);
    }
}
