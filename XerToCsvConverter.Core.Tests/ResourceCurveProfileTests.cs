using System.Globalization;
using System.Text;
using Microsoft.VisualBasic.FileIO;
using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ResourceCurveProfileTests
{
    private const string DistributionFile = "15_XER_RESOURCE_DISTRIBUTION.csv";
    private const string ProfileHeaders =
        "task_id_key,rsrc_id_key,is_actual,distribution_month,monthly_quantity,rsrc_name,rsrc_type,unit,ProjectCode";
    private const string StandardHeaders =
        "task_id_key,rsrc_id_key,clndr_id_key,proj_id_key,distribution_month,month_start_date,month_end_date,"
        + "monthly_quantity,distribution_type,month_working_hours,total_working_hours,calendar_hours_per_day,"
        + "month_working_days,total_working_days,month_calendar_days,total_calendar_days,Start,Finish,is_actual,"
        + "status_code,Unit,task_code,rsrc_short_name,rsrc_name,rsrc_type,MonthUpdate,FileName";
    private const string Start = "2026-01-31 00:00";
    private const string Finish = "2026-02-02 00:00";

    [Theory]
    [InlineData("FRONT", "DT_FixedDrtn", 80, 20, false)]
    [InlineData("BACK", "DT_FixedDUR2", 20, 80, false)]
    [InlineData("", "DT_FixedQty", 50, 50, false)]
    [InlineData("MISSING", "DT_FixedDrtn", 80, 20, true)]
    public async Task Standard_programme_and_tender_preserve_the_same_assignment_spread_and_profile_headers(
        string curveId, string durationType, int january, int february, bool useManualProfile)
    {
        const string standardSource = "standard-source-occurrence";
        XerDataStore standardStore = ProfileStore(standardSource, "J5001");
        Dictionary<string, byte[]> standard = await new ProcessingService().ExportTablesToMemoryAsync(
            standardStore, new List<string> { EnhancedTableNames.XerResourceDist15 },
            null, CancellationToken.None);

        ProgrammeReviewSnapshot baseline = Baseline();
        ProgrammeReviewInMemoryBundleResult programme = await new ProgrammeReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(
                ProfileStore(baseline.OriginalXerFilename, "J123"),
                ProgrammeReviewNamingTests.Request(new[] { baseline }));

        TenderReviewSource tenderSource = TenderReviewNamingTests.Source(0, "curve.xer", "2026-09-05");
        TenderReviewInMemoryBundleResult tender = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(
                ProfileStore(tenderSource.SourceToken, "J5001"),
                TenderReviewNamingTests.Request(new[] { tenderSource }));

        IReadOnlyList<string[]> standardCsv = ReadCsv(standard[EnhancedTableNames.XerResourceDist15]);
        IReadOnlyList<string[]> programmeCsv = ReadCsv(programme.Files[DistributionFile]);
        IReadOnlyList<string[]> tenderCsv = ReadCsv(tender.Files[DistributionFile]);
        Assert.Equal(ProfileHeaders, string.Join(',', programmeCsv[0]));
        Assert.Equal(ProfileHeaders, string.Join(',', tenderCsv[0]));
        Assert.Equal(StandardHeaders, string.Join(',', standardCsv[0]));

        foreach (IReadOnlyList<string[]> csv in new[] { standardCsv, programmeCsv, tenderCsv })
        {
            Dictionary<string, string>[] rows = Records(csv);
            Assert.Equal(new[] { "2026-01-01", "2026-02-01" },
                rows.Select(row => row["distribution_month"]));
            Assert.Equal(new[] { (decimal)january, february }, Quantities(rows));
            // Standard retains numeric flags; review contracts serialize booleans.
            string expectedActualFlag = ReferenceEquals(csv, standardCsv) ? "0" : "false";
            Assert.All(rows, row => Assert.Equal(expectedActualFlag, row["is_actual"]));
            Assert.Equal(100m, Quantities(rows).Sum());
        }

        // Curve weighting changes quantities, never the underlying calendar measurements.
        Assert.All(Records(standardCsv), row =>
        {
            Assert.Equal(standardSource, row["FileName"]);
            Assert.Equal(useManualProfile ? "Remaining Units Profile"
                : curveId.Length == 0 ? "Working Hours" : "Resource Curve", row["distribution_type"]);
            Assert.Equal("24.00", row["month_working_hours"]);
            Assert.Equal("48.00", row["total_working_hours"]);
            Assert.Equal("1.00", row["month_working_days"]);
            Assert.Equal("2.00", row["total_working_days"]);
        });
        Assert.All(programme.ManifestRows, row => Assert.Equal("3.0", row.SchemaVersion));
        Assert.All(tender.ManifestRows, row => Assert.Equal("1.0", row.SchemaVersion));

        XerDataStore ProfileStore(string source, string projectCode)
        {
            XerDataStore store = Store(source, projectCode, durationType, new Assignment("A1", curveId, "100"));
            if (useManualProfile)
            {
                // An explicit remaining profile overrides even an unavailable named curve.
                XerTable assignments = store.GetTable("TASKRSRC")!;
                AddTable(store, source, "TASKRSRC", assignments.Headers!.Append("remain_crv").ToArray(),
                    assignments.Rows.Select(row => row.Fields.Append("80:24;20:24").ToArray()).ToArray());
            }
            return store;
        }
    }

    [Fact]
    public async Task Tender_aggregates_only_after_each_assignment_uses_its_own_curve()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "curve.xer", "2026-09-05");
        XerDataStore store = Store(source.SourceToken, "J5001", "DT_FixedDrtn",
            new Assignment("A1", "FRONT", "100"),
            new Assignment("A2", "BACK", "200"));
        XerTable standard = Assert.IsType<XerTable>(
            new XerTransformer(store).Create15XerResourceDistribution());
        Assert.Equal(4, standard.Rows.Count);

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, TenderReviewNamingTests.Request(new[] { source }));

        Dictionary<string, string>[] rows = Records(ReadCsv(result.Files[DistributionFile]));
        Assert.Equal(2, rows.Length);
        Assert.Equal(new[] { 120m, 180m }, Quantities(rows));
        Assert.Equal(300m, Quantities(rows).Sum());
        Assert.All(rows, row =>
        {
            Assert.Equal("CSV::J5001::TENDER::20260905::T1", row["task_id_key"]);
            Assert.Equal("CSV::J5001::TENDER::20260905::R1", row["rsrc_id_key"]);
        });
        Assert.Equal(2, result.ManifestRows.Single(
            row => row.TableName == EnhancedTableNames.XerResourceDist15).RowCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Recorded_actual_fallbacks_preserve_exact_units_through_both_fixed_review_projections(
        bool pointActual)
    {
        const string actualStart = "2026-01-31 12:00"; // Saturday.
        string actualFinish = pointActual ? actualStart : "2026-02-01 12:00"; // Sunday.
        var exports = await ExportAllProfiles(ActualStore);
        decimal[] expected = pointActual ? [23.4567m] : [11.7284m, 11.7283m];
        string[] months = pointActual ? ["2026-01-01"] : ["2026-01-01", "2026-02-01"];

        foreach (var pair in exports)
        {
            IReadOnlyList<string[]> csv = ReadCsv(pair.Value);
            Assert.Equal(pair.Key == "standard" ? StandardHeaders : ProfileHeaders, string.Join(',', csv[0]));
            Dictionary<string, string>[] rows = Records(csv);
            Assert.Equal(months, rows.Select(row => row["distribution_month"]));
            Assert.Equal(expected, Quantities(rows));
            Assert.Equal(23.4567m, Quantities(rows).Sum());
            Assert.All(rows, row => Assert.Equal(pair.Key == "standard" ? "1" : "true", row["is_actual"]));
            if (pair.Key == "standard")
            {
                Assert.All(rows, row =>
                {
                    Assert.Equal(pointActual ? "Actual Recorded Date" : "Actual Elapsed Time", row["distribution_type"]);
                    Assert.Equal("0.00", row["month_working_hours"]);
                    Assert.Equal("0.00", row["total_working_hours"]);
                    Assert.Equal("0.00", row["month_working_days"]);
                    Assert.Equal("0.00", row["total_working_days"]);
                });
            }
            else
            {
                Assert.All(rows, row =>
                {
                    Assert.Equal(9, row.Count);
                    Assert.Equal(pair.Key == "programme" ? "J123" : "J5001", row["ProjectCode"]);
                    Assert.StartsWith(pair.Key == "programme" ? "CSV::J123::C::BL01::" : "CSV::J5001::TENDER::20260905::",
                        row["task_id_key"]);
                });
            }
        }

        XerDataStore ActualStore(string source, string projectCode)
        {
            XerDataStore store = Store(source, projectCode, "DT_FixedDrtn", new Assignment("A1", "FRONT", "0"));
            SetField(store, "PROJECT", "last_recalc_date", "2026-02-02");
            SetField(store, "TASK", "status_code", "TK_Complete");
            SetField(store, "TASK", "act_start_date", actualStart);
            SetField(store, "TASK", "act_end_date", actualFinish);
            SetField(store, "TASK", "remain_drtn_hr_cnt", "0");
            SetField(store, "TASK", "act_work_qty", "23.4567");
            SetField(store, "TASK", "remain_work_qty", "0");
            SetField(store, "CALENDAR", "day_hr_cnt", "8");
            SetField(store, "CALENDAR", "clndr_data", P6TestCalendars.WorkWeek());
            SetField(store, "TASKRSRC", "act_reg_qty", "20.0001");
            SetField(store, "TASKRSRC", "act_ot_qty", "3.4566");
            SetField(store, "TASKRSRC", "act_start_date", actualStart);
            SetField(store, "TASKRSRC", "act_end_date", actualFinish);
            return store;
        }
    }

    [Theory]
    [InlineData("FRONT", "2026-02-20 00:00")]
    [InlineData("BACK", "2026-03-01 00:00")]
    public async Task Active_nonlinear_curve_with_one_monthly_bucket_preserves_all_20_units_in_review_profiles(
        string curveId, string finish)
    {
        var exports = await ExportAllProfiles(ActiveStore);

        foreach (var pair in exports)
        {
            IReadOnlyList<string[]> csv = ReadCsv(pair.Value);
            Assert.Equal(pair.Key == "standard" ? StandardHeaders : ProfileHeaders, string.Join(',', csv[0]));
            Dictionary<string, string> row = Assert.Single(Records(csv));
            Assert.Equal("2026-02-01", row["distribution_month"]);
            Assert.Equal(20m, Assert.Single(Quantities([row])));
            Assert.Equal(pair.Key == "standard" ? "0" : "false", row["is_actual"]);
            if (pair.Key == "standard") Assert.Equal("Resource Curve", row["distribution_type"]);
            else Assert.Equal(9, row.Count);
        }

        XerDataStore ActiveStore(string source, string projectCode)
        {
            XerDataStore store = Store(source, projectCode, "DT_FixedDrtn", new Assignment("A1", curveId, "20"));
            SetField(store, "PROJECT", "last_recalc_date", "2026-02-02");
            SetField(store, "TASK", "status_code", "TK_Active");
            SetField(store, "TASK", "act_start_date", "2026-01-31 00:00");
            SetField(store, "TASKRSRC", "act_start_date", "2026-01-31 00:00");
            SetField(store, "TASK", "remain_work_qty", "20");
            foreach (string name in new[] { "TASK", "TASKRSRC" })
            {
                SetField(store, name, "restart_date", "2026-02-02 00:00");
                SetField(store, name, "reend_date", finish);
            }
            return store;
        }
    }

    private static async Task<IReadOnlyDictionary<string, byte[]>> ExportAllProfiles(
        Func<string, string, XerDataStore> createStore)
    {
        Dictionary<string, byte[]> standard = await new ProcessingService().ExportTablesToMemoryAsync(
            createStore("standard-source-occurrence", "J5001"), [EnhancedTableNames.XerResourceDist15],
            null, CancellationToken.None);
        ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
            "resource fallback baseline.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-02-28", "2026-02-02");
        ProgrammeReviewInMemoryBundleResult programme = await new ProgrammeReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(createStore(baseline.OriginalXerFilename, "J123"),
                ProgrammeReviewNamingTests.Request([baseline]));
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "resource fallback.xer", "2026-09-05");
        TenderReviewInMemoryBundleResult tender = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(createStore(source.SourceToken, "J5001"),
                TenderReviewNamingTests.Request([source]));
        Assert.All(programme.ManifestRows, row => Assert.Equal("3.0", row.SchemaVersion));
        Assert.All(tender.ManifestRows, row => Assert.Equal("1.0", row.SchemaVersion));
        return new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["standard"] = standard[EnhancedTableNames.XerResourceDist15],
            ["programme"] = programme.Files[DistributionFile],
            ["tender"] = tender.Files[DistributionFile]
        };
    }

    private static void SetField(XerDataStore store, string tableName, string fieldName, string value)
    {
        XerTable table = store.GetTable(tableName)!;
        Assert.Single(table.Rows).Fields[table.FieldIndexes[fieldName]] = value;
    }

    [Fact]
    public async Task Repeated_curve_and_assignment_ids_resolve_by_source_occurrence_not_filename()
    {
        TenderReviewSource first = TenderReviewNamingTests.Source(0, "same.xer", "2026-09-05");
        TenderReviewSource second = TenderReviewNamingTests.Source(1, "same.xer", "2026-09-06");
        XerDataStore firstStore = Store(first.SourceToken, "J5001", "DT_FixedDrtn",
            new Assignment("A1", "FRONT", "100"));
        XerDataStore secondStore = Store(second.SourceToken, "J5001", "DT_FixedDrtn",
            new Assignment("A1", "FRONT", "100"));
        XerTable secondCurves = secondStore.GetTable("RSRCCURVDATA")!;
        DataRow secondFront = secondCurves.Rows.Single(
            row => row.Fields[secondCurves.FieldIndexes["curv_id"]] == "FRONT");
        for (int point = 1; point <= 20; point++)
            secondFront.Fields[secondCurves.FieldIndexes[$"pct_usage_{point}"]] = point <= 10 ? "2" : "8";
        Merge(firstStore, secondStore);

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(firstStore,
                TenderReviewNamingTests.Request(new[] { first, second }));

        Dictionary<string, string>[] rows = Records(ReadCsv(result.Files[DistributionFile]));
        Assert.Equal(4, rows.Length);
        Assert.Equal(new[] { 80m, 20m }, Quantities(rows.Where(
            row => row["task_id_key"] == "CSV::J5001::TENDER::20260905::T1")));
        Assert.Equal(new[] { 20m, 80m }, Quantities(rows.Where(
            row => row["task_id_key"] == "CSV::J5001::TENDER::20260906::T1")));
    }

    [Fact]
    public async Task Ordered_file_and_byte_inputs_keep_repeated_curve_filenames_paths_and_hashes()
    {
        using var temporary = new TemporaryOutput();
        string xerPath = Path.Combine(temporary.Path, "same.xer");
        byte[] bytes = XerBytes(Store("serialization-source", "J5001", "DT_FixedDrtn",
            new Assignment("A1", "FRONT", "100")));
        await File.WriteAllBytesAsync(xerPath, bytes);
        TenderReviewSource first = TenderReviewNamingTests.Source(0, "same.xer", "2026-09-05") with
        {
            XerFilePath = xerPath,
            SourceSha256 = null
        };
        TenderReviewSource second = TenderReviewNamingTests.Source(1, "same.xer", "2026-09-06") with
        {
            XerFilePath = xerPath,
            SourceSha256 = null
        };
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { first, second });
        var service = new TenderReviewBundleService();

        TenderReviewInMemoryBundleResult memory = await service.BuildFromXerBytesAsync(request, new[]
        {
            new TenderReviewSourceBytes { SourceToken = first.SourceToken, Content = bytes },
            new TenderReviewSourceBytes { SourceToken = second.SourceToken, Content = bytes }
        });
        string outputPath = Path.Combine(temporary.Path, "bundles");
        Directory.CreateDirectory(outputPath);
        TenderReviewBundleResult disk = await service.BuildFromXerFilesAsync(request, outputPath);

        Assert.Equal(memory.Files[DistributionFile],
            await File.ReadAllBytesAsync(Path.Combine(disk.BundlePath, DistributionFile)));
        Assert.Single(memory.ManifestRows.Select(row => row.OriginalXerFilename).Distinct());
        Assert.Single(memory.ManifestRows.Select(row => row.SourceSha256).Distinct());
        Dictionary<string, string>[] rows = Records(ReadCsv(memory.Files[DistributionFile]));
        Assert.Equal(4, rows.Length);
        Assert.Equal(2, rows.Select(row => row["task_id_key"]).Distinct().Count());
        foreach (IGrouping<string, Dictionary<string, string>> sourceRows in rows.GroupBy(row => row["task_id_key"]))
            Assert.Equal(new[] { 80m, 20m }, Quantities(sourceRows));
        Assert.Equal(200m, Quantities(rows).Sum());
    }

    [Theory]
    [InlineData("standard", false, false)]
    [InlineData("standard", true, false)]
    [InlineData("programme", false, false)]
    [InlineData("programme", true, false)]
    [InlineData("tender", false, false)]
    [InlineData("tender", true, false)]
    [InlineData("standard", false, true)]
    [InlineData("standard", true, true)]
    [InlineData("programme", false, true)]
    [InlineData("programme", true, true)]
    [InlineData("tender", false, true)]
    [InlineData("tender", true, true)]
    public async Task Invalid_assigned_curve_warns_while_every_profile_exports_valid_assignments(
        string profile, bool toDisk, bool malformedDefinition)
    {
        using var output = new TemporaryOutput();
        ProgrammeReviewSnapshot baseline = Baseline();
        TenderReviewSource tenderSource = TenderReviewNamingTests.Source(0, "curve.xer", "2026-09-05");
        string source = profile switch
        {
            "programme" => baseline.OriginalXerFilename,
            "tender" => tenderSource.SourceToken,
            _ => "standard-source-occurrence"
        };
        XerDataStore store = Store(source, profile == "programme" ? "J123" : "J5001", "DT_FixedDrtn",
            new Assignment("A1", "FRONT", "100"),
            new Assignment("A2", malformedDefinition ? "BACK" : "MISSING", "200"));
        if (malformedDefinition)
        {
            XerTable curves = store.GetTable("RSRCCURVDATA")!;
            DataRow back = curves.Rows.Single(row => row.Fields[curves.FieldIndexes["curv_id"]] == "BACK");
            back.Fields[curves.FieldIndexes["pct_usage_20"]] = "NaN";
        }

        IReadOnlyDictionary<string, byte[]> files;
        switch (profile)
        {
            case "programme":
                var programme = new ProgrammeReviewBundleService();
                ProgrammeReviewBundleRequest programmeRequest = ProgrammeReviewNamingTests.Request(new[] { baseline });
                if (toDisk)
                {
                    var result = await programme.BuildFromParsedDataAsync(store, programmeRequest, output.Path);
                    Assert.Equal(1, result.WarningCount);
                    files = Directory.EnumerateFiles(result.BundlePath).ToDictionary(path => Path.GetFileName(path), File.ReadAllBytes);
                }
                else
                {
                    var result = await programme.BuildFromParsedDataToMemoryAsync(store, programmeRequest);
                    Assert.Equal(1, result.WarningCount);
                    files = result.Files;
                }
                break;
            case "tender":
                var tender = new TenderReviewBundleService();
                TenderReviewBundleRequest tenderRequest = TenderReviewNamingTests.Request(new[] { tenderSource });
                if (toDisk)
                {
                    var result = await tender.BuildFromParsedDataAsync(store, tenderRequest, output.Path);
                    Assert.Equal(1, result.WarningCount);
                    files = Directory.EnumerateFiles(result.BundlePath).ToDictionary(path => Path.GetFileName(path), File.ReadAllBytes);
                }
                else
                {
                    var result = await tender.BuildFromParsedDataToMemoryAsync(store, tenderRequest);
                    Assert.Equal(1, result.WarningCount);
                    files = result.Files;
                }
                break;
            default:
                var standard = new ProcessingService();
                var tables = new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerResourceDist15 };
                if (toDisk)
                {
                    var result = await standard.ExportTablesWithDiagnosticsAsync(store, tables, output.Path, null, CancellationToken.None);
                    Assert.Equal(1, result.WarningCount);
                    files = result.Files.ToDictionary(path => Path.GetFileName(path), File.ReadAllBytes);
                }
                else
                {
                    var result = await standard.ExportTablesToMemoryWithDiagnosticsAsync(store, tables, null, CancellationToken.None);
                    Assert.Equal(1, result.WarningCount);
                    files = result.Files.ToDictionary(pair => pair.Key + ".csv", pair => pair.Value);
                }
                break;
        }

        Assert.Equal(profile == "standard" ? 3 : 12, files.Count);
        Dictionary<string, string>[] allocations = Records(ReadCsv(files[DistributionFile]));
        Assert.Equal(new[] { 80m, 20m }, Quantities(allocations));
        IReadOnlyList<string[]> diagnosticCsv = ReadCsv(files[XerDataQuality.FileName]);
        string[] diagnosticValues = Assert.Single(diagnosticCsv.Skip(1));
        var warning = diagnosticCsv[0].Zip(diagnosticValues).ToDictionary(pair => pair.First, pair => pair.Second);
        Assert.Equal("REMAINING_PROFILE_INVALID", warning["issue_code"]);
        Assert.Equal("Remaining", warning["allocation_portion"]);
        Assert.Equal("A2", warning["taskrsrc_id"]);
        Assert.Equal(malformedDefinition ? "BACK" : "MISSING", warning["curv_id"]);
        Assert.Equal("200", warning["remain_qty"]);
        Assert.Equal("200.0000", warning["unallocated_remaining_quantity"]);
        Assert.Equal(300m, Quantities(allocations).Sum() + decimal.Parse(warning["unallocated_remaining_quantity"], CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task Standard_raw_export_does_not_require_an_unrequested_curve_spread()
    {
        XerDataStore store = Store("source-occurrence", "J5001", "DT_FixedDrtn",
            new Assignment("A1", "MISSING", "100"));

        Dictionary<string, byte[]> result = await new ProcessingService().ExportTablesToMemoryAsync(
            store, new List<string> { "AUDIT_SOURCE" }, null, CancellationToken.None);

        Assert.Equal("AUDIT_SOURCE", Assert.Single(result).Key);
    }

    private static ProgrammeReviewSnapshot Baseline() => ProgrammeReviewNamingTests.Snapshot(
        "curve baseline.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");

    private static XerDataStore Store(string source, string projectCode, string durationType,
        params Assignment[] assignments)
    {
        var store = new XerDataStore();
        AddTable(store, source, "TASK", new[]
        {
            "task_id", "proj_id", "wbs_id", "clndr_id", "task_type", "status_code", "task_code", "task_name",
            "rsrc_id", "act_start_date", "act_end_date", "early_start_date", "early_end_date", "late_start_date",
            "late_end_date", "restart_date", "reend_date", "target_start_date", "target_end_date", "cstr_type",
            "cstr_date", "priority_type", "float_path", "float_path_order", "driving_path_flag",
            "remain_drtn_hr_cnt", "target_drtn_hr_cnt", "total_float_hr_cnt", "free_float_hr_cnt",
            "complete_pct_type", "phys_complete_pct", "act_work_qty", "remain_work_qty", "duration_type"
        }, new[]
        {
            "T1", "P1", "W1", "C1", "TT_Task", "TK_NotStart", "A100", "Curved assignment",
            "", "", "", Start, Finish, Start, Finish, Start, Finish, Start, Finish, "", "", "", "", "", "Y",
            "48", "48", "0", "0", "CP_Drtn", "0", "0", "100", durationType
        });
        AddTable(store, source, "PROJECT",
            new[] { "proj_id", "last_recalc_date", "proj_short_name", "clndr_id" },
            new[] { "P1", "2026-01-30", projectCode, "C1" });
        AddTable(store, source, "PROJWBS", new[] { "wbs_id", "parent_wbs_id", "proj_id", "wbs_name" },
            new[] { "W1", "", "P1", "Root WBS" });
        AddTable(store, source, "CALENDAR",
            new[] { "clndr_id", "clndr_name", "clndr_type", "day_hr_cnt", "clndr_data" },
            new[] { "C1", "Continuous", "CA_Project", "24", P6TestCalendars.WorkWeek("24") });
        AddTable(store, source, "RSRC",
            new[] { "rsrc_id", "rsrc_short_name", "rsrc_name", "rsrc_type", "clndr_id", "def_qty_per_hr" },
            new[] { "R1", "LAB", "Labour", "RT_Labor", "C1", "1" });
        AddTable(store, source, "TASKRSRC", new[]
        {
            "taskrsrc_id", "task_id", "proj_id", "rsrc_id", "act_reg_qty", "act_ot_qty", "remain_qty",
            "act_start_date", "act_end_date", "restart_date", "reend_date", "target_start_date", "target_end_date",
            "target_qty", "curv_id"
        }, assignments.Select(assignment => new[]
        {
            assignment.Id, "T1", "P1", "R1", "0", "0", assignment.Quantity, "", "", Start, Finish,
            Start, Finish, assignment.Quantity, assignment.CurveId
        }).ToArray());
        AddTable(store, source, "RSRCCURVDATA",
            new[] { "curv_id", "curv_name" }.Concat(Enumerable.Range(0, 21).Select(point => $"pct_usage_{point}")).ToArray(),
            Curve("FRONT", frontLoaded: true), Curve("BACK", frontLoaded: false));
        AddTable(store, source, "AUDIT_SOURCE", new[] { "id" }, new[] { "preserved" });
        return store;
    }

    private static string[] Curve(string id, bool frontLoaded) => new[] { id, id, "0" }
        .Concat(Enumerable.Range(1, 20).Select(point => (point <= 10) == frontLoaded ? "8" : "2")).ToArray();

    private static void AddTable(XerDataStore store, string source, string name, string[] headers,
        params string[][] rows)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        foreach (string[] fields in rows)
        {
            Assert.Equal(headers.Length, fields.Length);
            table.AddRow(new DataRow(fields, source));
        }
        store.AddTable(table);
    }

    private static void Merge(XerDataStore destination, XerDataStore source)
    {
        foreach (string name in source.TableNames)
        {
            XerTable sourceTable = source.GetTable(name)!;
            XerTable destinationTable = destination.GetTable(name)!;
            Assert.Equal(destinationTable.Headers, sourceTable.Headers);
            foreach (DataRow row in sourceTable.Rows)
                destinationTable.AddRow(new DataRow(row.Fields.ToArray(), row.SourceFilename));
        }
    }

    private static byte[] XerBytes(XerDataStore store)
    {
        var builder = new StringBuilder();
        foreach (string name in store.TableNames)
        {
            XerTable table = store.GetTable(name)!;
            builder.Append("%T\t").Append(name).Append("\r\n");
            builder.Append("%F\t").AppendJoin('\t', table.Headers!).Append("\r\n");
            foreach (DataRow row in table.Rows)
                builder.Append("%R\t").AppendJoin('\t', row.Fields).Append("\r\n");
        }
        builder.Append("%E\r\n");
        return Encoding.UTF8.GetBytes(builder.ToString());
    }

    private static IReadOnlyList<string[]> ReadCsv(byte[] content)
    {
        using var stream = new MemoryStream(content, writable: false);
        using var parser = new TextFieldParser(stream, Encoding.UTF8, detectEncoding: true)
        {
            TextFieldType = FieldType.Delimited,
            HasFieldsEnclosedInQuotes = true,
            TrimWhiteSpace = false
        };
        parser.SetDelimiters(",");
        var rows = new List<string[]>();
        while (!parser.EndOfData)
            rows.Add(parser.ReadFields() ?? throw new InvalidDataException("Unreadable resource curve CSV."));
        return rows;
    }

    private static Dictionary<string, string>[] Records(IReadOnlyList<string[]> csv) => csv.Skip(1)
        .Select(values => csv[0].Zip(values).ToDictionary(pair => pair.First, pair => pair.Second, StringComparer.Ordinal))
        .OrderBy(row => row["distribution_month"], StringComparer.Ordinal).ToArray();

    private static decimal[] Quantities(IEnumerable<Dictionary<string, string>> rows) => rows
        .Select(row => decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)).ToArray();

    private readonly record struct Assignment(string Id, string CurveId, string Quantity);

    private sealed class TemporaryOutput : IDisposable
    {
        private readonly DirectoryInfo _directory = Directory.CreateTempSubdirectory("xer-resource-curve-");
        public string Path => _directory.FullName;
        public void Dispose() => _directory.Delete(recursive: true);
    }
}
