using System.Globalization;
using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed partial class ResourceCurveProfileTests
{
    [Theory]
    [InlineData("standard", false)]
    [InlineData("standard", true)]
    [InlineData("programme", false)]
    [InlineData("programme", true)]
    [InlineData("tender", false)]
    [InlineData("tender", true)]
    public async Task Forecast_and_fallback_export_conserved_units_and_successful_method_diagnostics(string profile, bool disk)
    {
        using var output = new TemporaryOutput();
        ProgrammeReviewSnapshot baseline = Baseline();
        TenderReviewSource tenderSource = TenderReviewNamingTests.Source(0, "curve.xer", "2026-09-05");
        string source = profile switch
        {
            "programme" => baseline.OriginalXerFilename,
            "tender" => tenderSource.SourceToken,
            _ => "standard.xer"
        };
        XerDataStore store = ForecastExportStore(source, profile == "programme" ? "J123" : "J5001");
        IReadOnlyDictionary<string, byte[]> files;
        int warningCount;
        long? manifestWarningCount = null;
        if (profile == "standard")
        {
            var service = new ProcessingService();
            var names = new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerResourceDist15 };
            var memory = await service.ExportTablesToMemoryWithDiagnosticsAsync(store, names, null, CancellationToken.None);
            warningCount = memory.WarningCount;
            files = memory.Files.ToDictionary(pair => pair.Key + ".csv", pair => pair.Value);
            if (disk)
            {
                var result = await service.ExportTablesWithDiagnosticsAsync(store, names, output.Path, null, CancellationToken.None);
                Assert.Equal(warningCount, result.WarningCount);
                foreach (var pair in files)
                    Assert.Equal(pair.Value, await File.ReadAllBytesAsync(Path.Combine(output.Path, pair.Key)));
            }
        }
        else if (profile == "programme")
        {
            var service = new ProgrammeReviewBundleService();
            var request = ProgrammeReviewNamingTests.Request([baseline]);
            var memory = await service.BuildFromParsedDataToMemoryAsync(store, request);
            warningCount = memory.WarningCount;
            manifestWarningCount = memory.ManifestRows.Single(row => row.TableName == XerDataQuality.TableName).RowCount;
            Assert.All(memory.ManifestRows, row => Assert.Equal("4.0", row.SchemaVersion));
            files = memory.Files;
            if (disk)
            {
                var result = await service.BuildFromParsedDataAsync(store, request, output.Path);
                Assert.Equal(warningCount, result.WarningCount);
                foreach (string name in new[] { DistributionFile, XerDataQuality.FileName })
                    Assert.Equal(files[name], await File.ReadAllBytesAsync(Path.Combine(result.BundlePath, name)));
            }
        }
        else
        {
            var service = new TenderReviewBundleService();
            var request = TenderReviewNamingTests.Request([tenderSource]);
            var memory = await service.BuildFromParsedDataToMemoryAsync(store, request);
            warningCount = memory.WarningCount;
            manifestWarningCount = memory.ManifestRows.Single(row => row.TableName == XerDataQuality.TableName).RowCount;
            Assert.All(memory.ManifestRows, row => Assert.Equal("2.0", row.SchemaVersion));
            files = memory.Files;
            if (disk)
            {
                var result = await service.BuildFromParsedDataAsync(store, request, output.Path);
                Assert.Equal(warningCount, result.WarningCount);
                foreach (string name in new[] { DistributionFile, XerDataQuality.FileName })
                    Assert.Equal(files[name], await File.ReadAllBytesAsync(Path.Combine(result.BundlePath, name)));
            }
        }

        Assert.Equal(profile == "standard" ? 3 : 12, files.Count);
        Assert.Equal(2, warningCount);
        if (manifestWarningCount.HasValue) Assert.Equal(2, manifestWarningCount);
        IReadOnlyList<string[]> csv = ReadCsv(files[DistributionFile]);
        Assert.Equal(profile == "standard" ? StandardHeaders : ProfileHeaders, string.Join(',', csv[0]));
        Dictionary<string, string>[] rows = Records(csv);
        Assert.Equal(profile == "tender" ? 2 : 4, rows.Length);
        Assert.Equal(new[] { 175m, 125m }, rows.GroupBy(row => row["distribution_month"]).Select(group => Quantities(group).Sum()));
        Assert.Equal(300m, Quantities(rows).Sum());
        Assert.All(rows, row => Assert.Equal(profile == "standard" ? "0" : "false", row["is_actual"]));
        if (profile == "standard")
        {
            Assert.Equal(new[] { 75m, 25m }, Quantities(rows.Where(row => row["distribution_type"] == "Resource Curve Forecast")));
            Assert.Equal(new[] { 100m, 100m }, Quantities(rows.Where(row => row["distribution_type"] == "Working Hours Fallback")));
        }

        Dictionary<string, string>[] notes = MethodRecords(files[XerDataQuality.FileName]);
        Assert.Equal(new[] { "REMAINING_CURVE_ESTIMATED", "REMAINING_CURVE_UNIFORM_FALLBACK" },
            notes.Select(row => row["issue_code"]).Order());
        string prefix = profile switch
        {
            "programme" => "CSV::J123::C::BL01::",
            "tender" => "CSV::J5001::TENDER::20260905::",
            _ => "standard.xer."
        };
        Assert.All(notes, note =>
        {
            Assert.Equal("1.2", note["diagnostic_schema_version"]);
            Assert.Equal("", note["allocation_portion"]);
            Assert.Equal("", note["unallocated_actual_quantity"]);
            Assert.Equal("", note["unallocated_remaining_quantity"]);
            Assert.Equal(prefix + "T1", note["task_id_key"]);
            Assert.Equal(prefix + note["taskrsrc_id"], note["taskrsrc_id_key"]);
            Assert.Equal("TASKRSRC", note["source_table"]);
            Assert.Contains("curv_id", note["raw_row_json"]);
            Assert.Equal(profile == "tender" ? "curve.xer" : source, note["FileName"]);
            Assert.False(string.IsNullOrWhiteSpace(note["source_row_number"]));
            if (profile == "tender") Assert.DoesNotContain(tenderSource.SourceToken, note["message"]);
        });
    }

    [Fact]
    public async Task Forecasts_with_repeated_filenames_and_native_curve_ids_keep_independent_source_phases()
    {
        XerDataStore first = ForecastExportStore("first", "J5001", includeFallback: false);
        XerDataStore second = ForecastExportStore("second", "J5001", includeFallback: false);
        SetField(second, "TASKRSRC", "act_start_date", "2026-01-28 00:00"); // A=48,R=48 => p=.5.
        using var left = new MemoryStream(XerBytes(first));
        using var right = new MemoryStream(XerBytes(second));
        var service = new ProcessingService();
        XerDataStore parsed = await service.ParseXerStreamsAsync(new List<(Stream, string)>
            { (left, "same.xer"), (right, "same.xer") }, null, CancellationToken.None);
        var export = await service.ExportTablesToMemoryWithDiagnosticsAsync(parsed,
            [EnhancedTableNames.XerResourceDist15], null, CancellationToken.None);
        Dictionary<string, string>[] rows = Records(ReadCsv(export.Files[EnhancedTableNames.XerResourceDist15]));
        var groups = rows.GroupBy(row => row["task_id_key"]).OrderBy(group => group.Key).ToArray();
        Assert.Equal(2, groups.Length);
        Assert.Equal(new[] { 75m, 25m }, Quantities(groups[0]));
        Assert.Equal(new[] { 50m, 50m }, Quantities(groups[1]));
        Dictionary<string, string>[] notes = MethodRecords(export.Files[XerDataQuality.TableName]);
        Assert.Equal(2, notes.Length);
        Assert.Equal(2, notes.Select(row => row["source_namespace"]).Distinct().Count());
        Assert.All(notes, row => Assert.Equal("same.xer", row["FileName"]));
    }

    private static Dictionary<string, string>[] MethodRecords(byte[] bytes)
    {
        IReadOnlyList<string[]> csv = ReadCsv(bytes);
        return csv.Skip(1).Select(values => csv[0].Zip(values).ToDictionary(pair => pair.First, pair => pair.Second)).ToArray();
    }

    private static XerDataStore ForecastExportStore(string source, string projectCode, bool includeFallback = true)
    {
        Assignment[] assignments = includeFallback
            ? [new("A1", "FRONT", "100"), new("A2", "FRONT", "200")]
            : [new("A1", "FRONT", "100")];
        XerDataStore store = Store(source, projectCode, "DT_FixedDrtn", assignments);
        SetField(store, "TASK", "status_code", "TK_Active");
        SetField(store, "TASK", "act_start_date", "2026-01-29 08:00");
        XerTable table = store.GetTable("TASKRSRC")!;
        foreach (DataRow row in table.Rows)
            row.Fields[table.FieldIndexes["act_start_date"]] = row.Fields[table.FieldIndexes["taskrsrc_id"]] == "A1"
                ? "2026-01-29 08:00" : "invalid";
        return store;
    }
}
