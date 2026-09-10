using System.Security.Cryptography;
using System.Text;
using System.Globalization;
using Microsoft.VisualBasic.FileIO;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class TenderReviewBundleServiceTests
{
    [Fact]
    public async Task Build_emits_exact_dates_manifest_counts_hashes_and_header_only_optional_tables()
    {
        string sharedHash = TenderReviewNamingTests.Hash("permitted-identical-content");
        TenderReviewSource first = TenderReviewNamingTests.Source(
            0, "repeated.xer", "2026-09-05", sharedHash);
        TenderReviewSource second = TenderReviewNamingTests.Source(
            1, "repeated.xer", "2026-09-06", sharedHash);
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { first, second });
        XerDataStore store = BuildDataStore(
            new Stage(first, "2026-08-31", "2026-07-01 15:45", "10"),
            new Stage(second, "2026-09-02", null, "8"));

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, request);

        Assert.Equal(11, result.Files.Count);
        Assert.Equal(
            TenderReviewContract.Tables.Select(table => table.FileName)
                .Append(TenderReviewContract.ManifestFileName).Order(StringComparer.Ordinal),
            result.Files.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(20, result.ManifestRows.Count);
        Assert.All(result.ManifestRows, row =>
        {
            Assert.Equal("3.0", row.SchemaVersion);
            Assert.Equal("tender_review", row.BundleProfile);
            Assert.Equal("COMPLETE", row.BundleStatus);
            Assert.Equal(result.BundleId, row.BundleId);
            Assert.Equal(new DateTimeOffset(2026, 9, 5, 1, 2, 3, TimeSpan.Zero), row.ExportedAtUtc);
        });

        IReadOnlyList<string[]> task = ReadCsv(result.Files["01_XER_TASK.csv"]);
        string[] taskHeader = task[0];
        var firstTask = Row(taskHeader, task.Single(row => row.Contains("J5001-TENDER-20260905.xer")));
        Assert.Equal("2026-09-05", firstTask["status_date"]);
        Assert.Equal("2026-09-05", firstTask["UpdateDate"]);
        Assert.Equal("2026-08-31", firstTask["data_date"]);
        Assert.Equal("2026-08-31", firstTask["monthupdate"]);
        Assert.Equal("2", firstTask["remaining_duration"]);
        Assert.Equal("3", firstTask["total_float"]);
        Assert.Equal("1", firstTask["free_float"]);
        Assert.Equal("CSV::J5001::TENDER::20260905::T1", firstTask["task_id_key"]);

        IReadOnlyList<string[]> project = ReadCsv(result.Files["02_XER_PROJECT.csv"]);
        var firstProject = Row(project[0], project.Single(row => row.Contains("2026-07-01")));
        Assert.Equal("2026-08-31", firstProject["last_recalc_date"]);
        Assert.Equal("2026-08-31", firstProject["monthupdate"]);
        Assert.Equal("2026-07-01", firstProject["add_date"]);
        Assert.Equal("2026-09-05", firstProject["udf_datalake_status_date"]);
        Assert.Equal("NSW", firstProject["state"]);
        Assert.Equal(string.Empty, firstProject["region"]);
        Assert.Equal(string.Empty, firstProject["tender_status"]);

        TenderReviewManifestRow firstTaskManifest = result.ManifestRows.Single(row =>
            row.StatusDate == first.StatusDate && row.TableName == "01_XER_TASK");
        Assert.Equal(first.StatusDate, firstTaskManifest.UpdateDate);
        Assert.Equal(new DateOnly(2026, 8, 31), firstTaskManifest.DataDate);
        Assert.Equal(first.OriginalXerFilename, firstTaskManifest.OriginalXerFilename);
        Assert.Equal(sharedHash, firstTaskManifest.SourceSha256);
        Assert.Equal(1, firstTaskManifest.RowCount);

        foreach (TenderReviewTableContract tableContract in TenderReviewContract.Tables)
        {
            byte[] csv = result.Files[tableContract.FileName];
            string actualHash = Convert.ToHexString(SHA256.HashData(csv)).ToLowerInvariant();
            Assert.Equal(result.CsvSha256ByFile[tableContract.FileName], actualHash);
            TenderReviewManifestRow[] rows = result.ManifestRows
                .Where(row => row.TableName == tableContract.TableName).ToArray();
            Assert.Equal(2, rows.Length);
            Assert.All(rows, row => Assert.Equal(actualHash, row.CsvSha256));
            Assert.Equal(ReadCsv(csv).Count - 1, rows.Sum(row => row.RowCount));
        }

        foreach (TenderReviewTableContract optional in TenderReviewContract.Tables.Where(table => !table.SourceRequired))
        {
            Assert.Equal(
                string.Join(',', optional.Columns.Select(column => column.Name)) + "\r\n",
                Encoding.UTF8.GetString(result.Files[optional.FileName]));
        }

        string manifestText = Encoding.UTF8.GetString(result.Files[TenderReviewContract.ManifestFileName]);
        Assert.StartsWith(string.Join(',', TenderReviewContract.ManifestColumns) + "\r\n",
            manifestText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Ordered_byte_and_file_entrypoints_allow_repeated_name_path_and_hash_and_match_contract()
    {
        string temp = NewTempDirectory();
        string output = Path.Combine(temp, "output");
        Directory.CreateDirectory(output);
        string path = Path.Combine(temp, "same-stage.xer");
        byte[] xer = MinimalXerBytes("J5001", "2026-08-31", "2026-06-02 12:00");
        await File.WriteAllBytesAsync(path, xer);
        TenderReviewSource first = TenderReviewNamingTests.Source(
            0, Path.GetFileName(path), "2026-09-05") with
        {
            XerFilePath = path,
            SourceSha256 = null
        };
        TenderReviewSource second = TenderReviewNamingTests.Source(
            1, Path.GetFileName(path), "2026-09-06") with
        {
            XerFilePath = path,
            SourceSha256 = null
        };
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { first, second });
        var progress = new RecordingProgress();

        try
        {
            TenderReviewBundleResult disk = await new TenderReviewBundleService()
                .BuildFromXerFilesAsync(request, output);
            TenderReviewInMemoryBundleResult memory = await new TenderReviewBundleService()
                .BuildFromXerBytesAsync(request, new[]
                {
                    new TenderReviewSourceBytes { SourceToken = first.SourceToken, Content = xer },
                    new TenderReviewSourceBytes { SourceToken = second.SourceToken, Content = xer }
                }, progress);

            Assert.Equal(disk.BundleId, memory.BundleId);
            Assert.Equal(2, disk.ManifestRows.Select(row => row.CanonicalXerFilename).Distinct().Count());
            Assert.Single(disk.ManifestRows.Select(row => row.OriginalXerFilename).Distinct());
            Assert.Single(disk.ManifestRows.Select(row => row.SourceSha256).Distinct());
            Assert.Equal(20, disk.ManifestRows.Count);
            foreach (string fileName in memory.Files.Keys)
                Assert.Equal(memory.Files[fileName], File.ReadAllBytes(Path.Combine(disk.BundlePath, fileName)));

            string[] progressOrder = progress.Items
                .Where(item => item.Message?.StartsWith(
                    "Parsing Tender source:", StringComparison.Ordinal) == true)
                .Select(item => item.FilePath)
                .OfType<string>()
                .ToArray();
            Assert.Equal(new[] { first.SourceToken, second.SourceToken }, progressOrder);
            Assert.All(progress.Items.Where(item => item.FilePath is not null), item =>
                Assert.Contains(item.FilePath!, new[] { first.SourceToken, second.SourceToken }));
        }
        finally
        {
            Directory.Delete(temp, recursive: true);
        }
    }

    [Fact]
    public async Task Build_is_byte_deterministic_for_identical_ordered_inputs_metadata_and_clock()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { source });
        XerDataStore store = BuildDataStore(new Stage(source, "2026-08-31", null, "8"));
        var service = new TenderReviewBundleService();

        TenderReviewInMemoryBundleResult first =
            await service.BuildFromParsedDataToMemoryAsync(store, request);
        TenderReviewInMemoryBundleResult second =
            await service.BuildFromParsedDataToMemoryAsync(store, request);

        Assert.Equal(first.BundleId, second.BundleId);
        Assert.Equal(first.Files.Keys.Order(StringComparer.Ordinal), second.Files.Keys.Order(StringComparer.Ordinal));
        foreach (string file in first.Files.Keys)
            Assert.Equal(first.Files[file], second.Files[file]);
    }

    [Fact]
    public async Task Csv_is_utf8_without_bom_crlf_and_quotes_preserved_text()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(
            new Stage(source, "2026-08-31", null, "8", "  Activity, \"North\"  "));

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(
                store, TenderReviewNamingTests.Request(new[] { source }));
        byte[] bytes = result.Files["01_XER_TASK.csv"];
        Assert.False(bytes.AsSpan().StartsWith(new byte[] { 0xEF, 0xBB, 0xBF }));
        string text = Encoding.UTF8.GetString(bytes);
        Assert.Contains("\r\n", text, StringComparison.Ordinal);
        Assert.DoesNotContain("\n", text.Replace("\r\n", string.Empty, StringComparison.Ordinal),
            StringComparison.Ordinal);
        Assert.Contains("\"  Activity, \"\"North\"\"  \"", text, StringComparison.Ordinal);
        IReadOnlyList<string[]> csv = ReadCsv(bytes);
        Assert.Equal("  Activity, \"North\"  ", Row(csv[0], csv[1])["task_name"]);
    }

    [Fact]
    public async Task Project_add_date_is_blank_when_absent_and_raw_when_present()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { source });
        XerDataStore absent = BuildDataStore(
            new[] { new Stage(source, "2026-08-31", null, "8") }, includeAddDateHeader: false);
        XerDataStore present = BuildDataStore(
            new Stage(source, "2026-08-31", "2025-12-24 23:59", "8"));

        TenderReviewInMemoryBundleResult blank = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(absent, request);
        TenderReviewInMemoryBundleResult preserved = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(present, request);

        IReadOnlyList<string[]> blankCsv = ReadCsv(blank.Files["02_XER_PROJECT.csv"]);
        IReadOnlyList<string[]> preservedCsv = ReadCsv(preserved.Files["02_XER_PROJECT.csv"]);
        Assert.Equal(string.Empty, Row(blankCsv[0], blankCsv[1])["add_date"]);
        Assert.Equal("2025-12-24", Row(preservedCsv[0], preservedCsv[1])["add_date"]);
    }

    [Fact]
    public async Task Ordered_byte_parser_unions_later_optional_project_headers()
    {
        TenderReviewSource first = TenderReviewNamingTests.Source(
            0, "same.xer", "2026-09-05") with { SourceSha256 = null };
        TenderReviewSource second = TenderReviewNamingTests.Source(
            1, "same.xer", "2026-09-06") with { SourceSha256 = null };
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { first, second });

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromXerBytesAsync(request, new[]
            {
                new TenderReviewSourceBytes
                {
                    SourceToken = first.SourceToken,
                    Content = MinimalXerBytes("J5001", "2026-08-31", "", includeAddDateHeader: false)
                },
                new TenderReviewSourceBytes
                {
                    SourceToken = second.SourceToken,
                    Content = MinimalXerBytes("J5001", "2026-09-02", "2026-07-14 13:30")
                }
            });

        IReadOnlyList<string[]> csv = ReadCsv(result.Files["02_XER_PROJECT.csv"]);
        Dictionary<string, string> firstRow = Row(
            csv[0], csv.Single(row => row.Contains("2026-08-31")));
        Dictionary<string, string> secondRow = Row(
            csv[0], csv.Single(row => row.Contains("2026-09-02")));
        Assert.Equal(string.Empty, firstRow["add_date"]);
        Assert.Equal("2026-07-14", secondRow["add_date"]);
    }

    [Fact]
    public async Task C_J_and_digits_only_native_codes_require_explicit_reporting_mapping()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { source });
        XerDataStore alias = BuildDataStore(
            new Stage(source, "2026-08-31", null, "8", RawProjectCode: "C5001"));
        XerDataStore digitsOnly = BuildDataStore(
            new Stage(source, "2026-08-31", null, "8", RawProjectCode: "5001"));

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(alias, request);
        Assert.All(result.ManifestRows, row => Assert.Equal("J5001", row.ProjectCode));

        Assert.Single(result.DataQualityTable!.Rows, row =>
            row.Fields[result.DataQualityTable.FieldIndexes["issue_code"]] == "TENDER_PROJECT_CODE_MAPPED");
        TenderReviewInMemoryBundleResult mapped = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(digitsOnly, request);
        Assert.All(mapped.ManifestRows, row => Assert.Equal("J5001", row.ProjectCode));
        Assert.Single(mapped.DataQualityTable!.Rows, row =>
            row.Fields[mapped.DataQualityTable.FieldIndexes["issue_code"]] == "TENDER_PROJECT_CODE_MAPPED");
        Assert.False(TenderReviewNaming.IsSameProjectIdentity("5001", "J5001"));
    }

    [Fact]
    public async Task Project_code_with_spaces_and_underscores_builds_tender_bundle_successfully()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        TenderReviewBundleRequest request = new()
        {
            ProjectCode = "NE Part B",
            ProjectName = "North East Highway Part B",
            Sources = new[] { source }
        };
        XerDataStore store = BuildDataStore(
            new Stage(source, "2026-08-31", null, "8", RawProjectCode: "NE Part B"));

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, request);

        Assert.All(result.ManifestRows, row => Assert.Equal("NE PART B", row.ProjectCode));
        Assert.StartsWith("NE PART B_TENDER_", result.BundleId);
        Assert.All(result.ManifestRows, row => Assert.Equal("NE PART B-TENDER-20260905.xer", row.CanonicalXerFilename));
    }

    [Fact]
    public async Task Task_start_finish_follow_raw_status_dependent_rules_and_completed_floats_are_blank()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { source });
        XerDataStore store = BuildDataStore(new Stage(source, "2026-08-31", null, "8"));
        XerTable task = store.GetTable("TASK")!;
        task.Rows[0].Fields[task.FieldIndexes["early_start_date"]] = "2026-09-01";
        task.Rows[0].Fields[task.FieldIndexes["early_end_date"]] = "2026-09-20";
        task.Rows[0].Fields[task.FieldIndexes["restart_date"]] = "2026-09-04";
        task.Rows[0].Fields[task.FieldIndexes["reend_date"]] = "2026-09-18";
        task.AddRow(new DataRow(TaskFields(
            "T2", "P1", "W1", "C1", "A200", "Active", "8", "16", "8", "4",
            statusCode: "TK_Active", actualStart: "2026-09-03",
            earlyStart: "2026-09-02", earlyEnd: "2026-09-21",
            restart: "2026-09-05", reend: "2026-09-17"), source.SourceToken));
        task.AddRow(new DataRow(TaskFields(
            "T3", "P1", "W1", "C1", "A300", "Complete", "0", "16", "80", "40",
            statusCode: "TK_Complete", actualStart: "2026-08-30", actualEnd: "2026-09-12",
            earlyStart: "2026-09-02", earlyEnd: "2026-09-21",
            restart: "2026-09-05", reend: "2026-09-17"), source.SourceToken));
        task.AddRow(new DataRow(TaskFields(
            "T4", "P1", "W1", "C1", "A400", "Fallback", "8", "16", "8", "4",
            earlyStart: "2026-09-06", earlyEnd: "2026-09-22",
            restart: "", reend: ""), source.SourceToken));

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, request);
        IReadOnlyList<string[]> csv = ReadCsv(result.Files["01_XER_TASK.csv"]);
        Dictionary<string, string> notStarted = Row(csv[0], csv.Single(row => row.Contains("A100")));
        Dictionary<string, string> active = Row(csv[0], csv.Single(row => row.Contains("A200")));
        Dictionary<string, string> complete = Row(csv[0], csv.Single(row => row.Contains("A300")));
        Dictionary<string, string> fallback = Row(csv[0], csv.Single(row => row.Contains("A400")));

        Assert.Equal("2026-09-04", notStarted["Start"]);
        Assert.Equal("2026-09-18", notStarted["Finish"]);
        Assert.Equal("2026-09-03", active["Start"]);
        Assert.Equal("2026-09-17", active["Finish"]);
        Assert.Equal("2026-08-30", complete["Start"]);
        Assert.Equal("2026-09-12", complete["Finish"]);
        Assert.Equal(string.Empty, complete["total_float"]);
        Assert.Equal(string.Empty, complete["free_float"]);
        Assert.Equal("2026-09-06", fallback["Start"]);
        Assert.Equal("2026-09-22", fallback["Finish"]);
    }

    [Fact]
    public async Task Unknown_task_status_preserves_row_with_blank_dates_and_warnings()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(new Stage(source, "2026-08-31", null, "8"));
        XerTable task = store.GetTable("TASK")!;
        task.Rows[0].Fields[task.FieldIndexes["status_code"]] = "TK_Unknown";

        var error = await BuildWithWarnings(store, source);
        Assert.Contains("not a recognised P6 activity status", error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Activity_and_relationship_hour_conversion_use_own_calendars()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { source });
        XerDataStore store = BuildDataStore(new Stage(source, "2026-08-31", null, "10"));
        AddSecondTaskAndRelationship(
            store, source.SourceToken, successorCalendarHours: "8",
            lagSetting: "rcal_Predecessor");

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, request);
        IReadOnlyList<string[]> tasks = ReadCsv(result.Files["01_XER_TASK.csv"]);
        var firstTask = Row(tasks[0], tasks.Single(row => row.Contains("A100")));
        Assert.Equal("2", firstTask["remaining_duration"]);
        Assert.Equal("3", firstTask["total_float"]);
        IReadOnlyList<string[]> predecessors = ReadCsv(result.Files["06_XER_PREDECESSOR.csv"]);
        var relationship = Row(predecessors[0], predecessors[1]);
        Assert.Equal("2", relationship["lag"]);
        Assert.Equal("2", relationship["total_float"]);
    }

    [Fact]
    public async Task Successor_lag_projection_keeps_predecessor_day_conversion()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(new Stage(source, "2026-08-31", null, "10"));
        AddSecondTaskAndRelationship(
            store, source.SourceToken, successorCalendarHours: "8",
            lagSetting: "rcal_Successor");

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(
                store, TenderReviewNamingTests.Request(new[] { source }));
        IReadOnlyList<string[]> csv = ReadCsv(result.Files["06_XER_PREDECESSOR.csv"]);
        Dictionary<string, string> relationship = Row(csv[0], csv[1]);
        Assert.Equal("2", relationship["lag"]);
        Assert.Equal("2", relationship["total_float"]);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("rcal_Unknown")]
    public async Task Nonzero_lag_requires_explicit_recognised_calendar_setting(string? setting)
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(new Stage(source, "2026-08-31", null, "10"));
        AddSecondTaskAndRelationship(
            store, source.SourceToken, successorCalendarHours: "8", lagSetting: setting);

        var error = await BuildWithWarnings(store, source);
        Assert.Contains("RELATIONSHIP_UnresolvedLagCalendarSetting", error.Message, StringComparison.Ordinal);
        var relationships = ReadCsv(error.Result.Files["06_XER_PREDECESSOR.csv"]);
        Assert.Equal("", Row(relationships[0], Assert.Single(relationships.Skip(1)))["free_float"]);
        Assert.Equal("2", Row(relationships[0], relationships[1])["lag"]);
    }

    [Fact]
    public async Task Zero_lag_does_not_require_an_applicable_lag_calendar_setting()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { source });
        XerDataStore absent = BuildDataStore(new Stage(source, "2026-08-31", null, "10"));
        AddSecondTaskAndRelationship(
            absent, source.SourceToken, successorCalendarHours: "8",
            lagSetting: null, lagHours: "0");
        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(absent, request);
        IReadOnlyList<string[]> csv = ReadCsv(result.Files["06_XER_PREDECESSOR.csv"]);
        Assert.Equal("0", Row(csv[0], csv[1])["lag"]);

        XerDataStore unknown = BuildDataStore(new Stage(source, "2026-08-31", null, "10"));
        AddSecondTaskAndRelationship(
            unknown, source.SourceToken, successorCalendarHours: "8",
            lagSetting: "rcal_Unknown", lagHours: "0");
        var unknownResult = await new TenderReviewBundleService().BuildFromParsedDataToMemoryAsync(unknown, request);
        var unknownCsv = ReadCsv(unknownResult.Files["06_XER_PREDECESSOR.csv"]);
        Assert.Equal("0", Row(unknownCsv[0], unknownCsv[1])["lag"]);
    }

    [Theory]
    [InlineData("rcal_Predecessor", "0", "8")]
    [InlineData("rcal_Successor", "0", "8")]
    public async Task Relationship_lag_day_conversion_warns_for_invalid_predecessor_hours(
        string setting,
        string predecessorHours,
        string successorHours)
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(
            new[] { new Stage(source, "2026-08-31", null, predecessorHours) },
            blankTaskHours: true);
        AddSecondTaskAndRelationship(
            store, source.SourceToken, successorCalendarHours: successorHours,
            lagSetting: setting, blankSuccessorHours: true);

        var error = await BuildWithWarnings(store, source);
        Assert.Contains("finite positive CALENDAR.day_hr_cnt", error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Completed_successor_keeps_task_and_relationship_total_float_blank()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(new Stage(source, "2026-08-31", null, "10"));
        AddSecondTaskAndRelationship(
            store, source.SourceToken, successorCalendarHours: "8",
            lagSetting: "rcal_Predecessor", successorStatusCode: "TK_Complete");

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(
                store, TenderReviewNamingTests.Request(new[] { source }));
        IReadOnlyList<string[]> tasks = ReadCsv(result.Files["01_XER_TASK.csv"]);
        Dictionary<string, string> successor = Row(
            tasks[0], tasks.Single(row => row.Contains("A200")));
        Assert.Equal(string.Empty, successor["total_float"]);
        Assert.Equal(string.Empty, successor["free_float"]);
        IReadOnlyList<string[]> relationships = ReadCsv(result.Files["06_XER_PREDECESSOR.csv"]);
        Assert.Equal(string.Empty, Row(relationships[0], relationships[1])["total_float"]);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-a-number")]
    [InlineData("0")]
    [InlineData("-1")]
    public async Task Nonblank_task_hours_require_finite_positive_calendar_hours(string calendarHours)
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(
            new Stage(source, "2026-08-31", null, calendarHours));

        var error = await BuildWithWarnings(store, source);
        Assert.Contains("finite positive CALENDAR.day_hr_cnt", error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Missing_calendar_hours_header_blanks_nonblank_task_hour_conversion()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(
            new[] { new Stage(source, "2026-08-31", null, "8") },
            includeCalendarHoursHeader: false);

        var error = await BuildWithWarnings(store, source);
        Assert.Contains("finite positive CALENDAR.day_hr_cnt", error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Blank_nullable_task_hours_do_not_require_calendar_hours()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(
            new[] { new Stage(source, "2026-08-31", null, "0") },
            blankTaskHours: true);

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(
                store, TenderReviewNamingTests.Request(new[] { source }));
        IReadOnlyList<string[]> csv = ReadCsv(result.Files["01_XER_TASK.csv"]);
        Dictionary<string, string> row = Row(csv[0], csv[1]);
        Assert.Equal(string.Empty, row["remaining_duration"]);
        Assert.Equal(string.Empty, row["total_float"]);
        Assert.Equal(string.Empty, row["free_float"]);
    }

    [Fact]
    public async Task Resource_distribution_aggregates_exact_output_grain_and_validates_task_calendar()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { source });
        XerDataStore valid = BuildDataStore(new Stage(source, "2026-08-31", null, "8"));
        AddResourceAssignments(valid, source.SourceToken, "2", "3");

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(valid, request);
        IReadOnlyList<string[]> csv = ReadCsv(result.Files["15_XER_RESOURCE_DISTRIBUTION.csv"]);
        Assert.Equal(2, csv.Count);
        Dictionary<string, string> row = Row(csv[0], csv[1]);
        Assert.Equal("5", row["monthly_quantity"]);
        Assert.Equal("hours", row["unit"]);

        XerDataStore invalid = BuildDataStore(
            new[] { new Stage(source, "2026-08-31", null, "0") },
            blankTaskHours: true);
        AddResourceAssignments(invalid, source.SourceToken, "2");
        var withoutDayFactor = await new TenderReviewBundleService().BuildFromParsedDataToMemoryAsync(invalid, request);
        var validUnits = ReadCsv(withoutDayFactor.Files["15_XER_RESOURCE_DISTRIBUTION.csv"]);
        Assert.Equal("2", Row(validUnits[0], validUnits[1])["monthly_quantity"]);
    }

    [Fact]
    public async Task Resource_distribution_conserves_rounded_assignment_totals_through_tender_aggregation()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(new Stage(source, "2026-01-30", null, "24"));
        AddContinuousCalendarResourceAssignments(store, source.SourceToken, "1", "1");

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, TenderReviewNamingTests.Request(new[] { source }));
        IReadOnlyList<string[]> csv = ReadCsv(result.Files["15_XER_RESOURCE_DISTRIBUTION.csv"]);
        Assert.Equal(
            "task_id_key,rsrc_id_key,is_actual,distribution_month,monthly_quantity,rsrc_name,rsrc_type,unit,ProjectCode",
            string.Join(',', csv[0]));
        Dictionary<string, string>[] rows = csv.Skip(1).Select(values => Row(csv[0], values)).ToArray();

        Assert.Equal(new[] { "2026-01-01", "2026-02-01", "2026-03-01" },
            rows.Select(row => row["distribution_month"]));
        Assert.Equal(new[] { 0.0666m, 1.8668m, 0.0666m },
            rows.Select(row => decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)));
        Assert.Equal(2m, rows.Sum(row => decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)));
        Assert.All(rows, row =>
        {
            Assert.Equal("CSV::J5001::TENDER::20260905::T1", row["task_id_key"]);
            Assert.Equal("CSV::J5001::TENDER::20260905::R1", row["rsrc_id_key"]);
        });
        Assert.Equal("clndr_id_key,clndr_name", string.Join(',', ReadCsv(result.Files["10_XER_CALENDAR.csv"])[0]));
        Assert.DoesNotContain("11_XER_CALENDAR_DETAILED.csv", result.Files.Keys);
        Assert.Equal(11, result.Files.Count);
        Assert.DoesNotContain(XerDataQuality.FileName, result.Files.Keys);
        Assert.All(result.ManifestRows, row => Assert.Equal("3.0", row.SchemaVersion));
        Assert.Equal(3, result.ManifestRows.Single(row => row.TableName == "15_XER_RESOURCE_DISTRIBUTION").RowCount);
    }

    [Fact]
    public async Task Resource_aggregate_overflow_preserves_every_assignment_contribution_and_its_source_ordinal()
    {
        const string quantity = "50000000000000000000000000000";
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "overflow.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(new Stage(source, "2026-08-31", null, "8"));
        AddResourceAssignments(store, source.SourceToken, quantity, quantity);
        var result = await new TenderReviewBundleService().BuildFromParsedDataToMemoryAsync(
            store, TenderReviewNamingTests.Request(new[] { source }));
        Assert.Equal(11, result.Files.Count);
        Assert.DoesNotContain(XerDataQuality.FileName, result.Files.Keys);
        var csv = ReadCsv(result.Files["15_XER_RESOURCE_DISTRIBUTION.csv"]);
        Assert.Equal(3, csv.Count);
        Assert.All(csv.Skip(1), row => Assert.Equal(decimal.Parse(quantity, CultureInfo.InvariantCulture),
            decimal.Parse(Row(csv[0], row)["monthly_quantity"], CultureInfo.InvariantCulture)));
        byte[] qualityBytes = XerDataQuality.WriteToBytes(result.DataQualityTable!, CancellationToken.None);
        var diagnostics = ReadCsv(qualityBytes);
        var aggregation = diagnostics.Skip(1).Select(row => Row(diagnostics[0], row))
            .Where(row => row["issue_code"] == "REVIEW_AGGREGATION_UNAVAILABLE").ToArray();
        Assert.Equal(2, aggregation.Length);
        Assert.Equal(new[] { "1", "2" }, aggregation.Select(row => row["source_row_number"]).Order().ToArray());
        Assert.All(aggregation, row =>
        {
            Assert.Equal("TASKRSRC", row["source_table"]);
            Assert.Contains("taskrsrc_id", row["raw_row_json"], StringComparison.Ordinal);
            Assert.Equal("", row["allocation_portion"]);
            Assert.Equal("", row["unallocated_remaining_quantity"]);
        });
        Assert.DoesNotContain(source.SourceToken, Encoding.UTF8.GetString(qualityBytes), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Invalid_positive_resource_assignment_warns_without_dropping_valid_tender_output(bool toDisk)
    {
        string root = NewTempDirectory();
        try
        {
            TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
            XerDataStore store = BuildDataStore(new Stage(source, "2026-01-30", null, "24"));
            AddContinuousCalendarResourceAssignments(store, source.SourceToken, "1", "2");
            XerTable assignments = store.GetTable("TASKRSRC")!;
            assignments.Rows[1].Fields[assignments.FieldIndexes["restart_date"]] = "not-a-date";
            TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { source });
            var service = new TenderReviewBundleService();

            IReadOnlyDictionary<string, byte[]> files;
            XerTable? qualityTable;
            if (toDisk)
            {
                var result = await service.BuildFromParsedDataAsync(store, request, root);
                Assert.Equal(1, result.WarningCount);
                qualityTable = result.DataQualityTable;
                files = Directory.EnumerateFiles(result.BundlePath).ToDictionary(path => Path.GetFileName(path), File.ReadAllBytes);
            }
            else
            {
                var result = await service.BuildFromParsedDataToMemoryAsync(store, request);
                Assert.Equal(1, result.WarningCount);
                qualityTable = result.DataQualityTable;
                files = result.Files;
            }
            Assert.Equal(11, files.Count);
            Assert.DoesNotContain(XerDataQuality.FileName, files.Keys);
            var csv = ReadCsv(files["15_XER_RESOURCE_DISTRIBUTION.csv"]);
            Assert.Equal(1m, csv.Skip(1).Select(values => Row(csv[0], values))
                .Sum(row => decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)));
            var diagnosticCsv = ReadCsv(XerDataQuality.WriteToBytes(qualityTable!, CancellationToken.None));
            var warning = Row(diagnosticCsv[0], Assert.Single(diagnosticCsv.Skip(1)));
            Assert.Equal("REMAINING_PERIOD_INVALID", warning["issue_code"]);
            Assert.Equal("A2", warning["taskrsrc_id"]);
            Assert.Equal("not-a-date", warning["restart_date"]);
            Assert.Equal("2", warning["remain_qty"]);
            Assert.Equal("2.0000", warning["unallocated_remaining_quantity"]);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Theory]
    [InlineData("24", true)]
    [InlineData("0", false)]
    public async Task Resource_dependent_distribution_validates_the_selected_resource_calendar(string resourceHours, bool succeeds)
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(
            new[] { new Stage(source, "2026-08-31", null, "0") }, blankTaskHours: true);
        XerTable task = store.GetTable("TASK")!;
        task.Rows[0].Fields[task.FieldIndexes["task_type"]] = "TT_Rsrc";
        store.GetTable("CALENDAR")!.AddRow(new DataRow(new[]
        {
            "RESOURCE", "Resource calendar", resourceHours, "CA_Resource", P6TestCalendars.WorkWeek("24")
        }, source.SourceToken));
        AddResourceAssignments(store, source.SourceToken, "2");
        XerTable resource = store.GetTable("RSRC")!;
        resource.Rows[0].Fields[resource.FieldIndexes["clndr_id"]] = "RESOURCE";
        var request = TenderReviewNamingTests.Request(new[] { source });

        // Availability can allocate units without a day-conversion factor.
        Assert.Equal(resourceHours == "24", succeeds);
        var result = await new TenderReviewBundleService().BuildFromParsedDataToMemoryAsync(store, request);
        var csv = ReadCsv(result.Files["15_XER_RESOURCE_DISTRIBUTION.csv"]);
        Assert.Equal("2", Row(csv[0], Assert.Single(csv.Skip(1)))["monthly_quantity"]);
    }

    [Fact]
    public async Task Tender_identifiers_and_resource_distribution_month_are_invariant_under_non_gregorian_culture()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(
            0, "stage.xer", "2026-09-05");
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { source });
        XerDataStore store = BuildDataStore(
            new Stage(source, "2026-08-31", null, "8"));
        AddResourceAssignments(store, source.SourceToken, "2");
        CultureInfo previousCulture = CultureInfo.CurrentCulture;
        CultureInfo previousUiCulture = CultureInfo.CurrentUICulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("ar-SA");
            CultureInfo.CurrentUICulture = new CultureInfo("ar-SA");

            TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
                .BuildFromParsedDataToMemoryAsync(store, request);

            Assert.Equal("J5001-TENDER-20260905.xer",
                TenderReviewNaming.CreateCanonicalFilename("J5001", source.StatusDate));
            Assert.Equal("CSV::J5001::TENDER::20260905::T1",
                TenderReviewNaming.NamespaceKey(
                    $"{source.SourceToken}.T1", source.SourceToken,
                    "J5001", source.StatusDate));
            Assert.StartsWith("J5001_TENDER_20260905T010203Z_", result.BundleId,
                StringComparison.Ordinal);
            IReadOnlyList<string[]> csv = ReadCsv(
                result.Files["15_XER_RESOURCE_DISTRIBUTION.csv"]);
            Assert.Equal("2026-09-01", Row(csv[0], csv[1])["distribution_month"]);
        }
        finally
        {
            CultureInfo.CurrentCulture = previousCulture;
            CultureInfo.CurrentUICulture = previousUiCulture;
        }
    }

    [Fact]
    public async Task Raw_task_resource_assignment_must_resolve_task_in_same_source()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(new Stage(source, "2026-08-31", null, "8"));
        AddResourceAssignments(store, source.SourceToken, "2");
        XerTable assignments = store.GetTable("TASKRSRC")!;
        assignments.Rows[0].Fields[assignments.FieldIndexes["task_id"]] = "MISSING";

        var error = await BuildWithWarnings(store, source);
        Assert.Contains("MISSING", error.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ASSIGNMENT_CONTEXT_INVALID", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Unresolved_required_calendar_reference_preserves_the_bundle_with_warnings()
    {
        string root = NewTempDirectory();
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(
            new Stage(source, "2026-08-31", null, "8", CalendarIdInCalendarTable: "OTHER"));
        try
        {
            var result = await new TenderReviewBundleService().BuildFromParsedDataAsync(
                store, TenderReviewNamingTests.Request(new[] { source }), root);
            Assert.True(result.WarningCount > 0);
            Assert.Equal(11, Directory.EnumerateFiles(result.BundlePath).Count());
            Assert.DoesNotContain(XerDataQuality.FileName, Directory.EnumerateFiles(result.BundlePath).Select(Path.GetFileName));
            byte[] qualityBytes = XerDataQuality.WriteToBytes(result.DataQualityTable!, CancellationToken.None);
            Assert.Contains("C1", Encoding.UTF8.GetString(qualityBytes), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Cancellation_removes_atomic_staging_output()
    {
        string root = NewTempDirectory();
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(
            new[] { new Stage(source, "2026-08-31", null, "8") },
            taskCountPerStage: 30_000);
        using var cancellation = new CancellationTokenSource();
        try
        {
            Task<TenderReviewBundleResult> build = Task.Run(() =>
                new TenderReviewBundleService().BuildFromParsedDataAsync(
                    store, TenderReviewNamingTests.Request(new[] { source }), root,
                    cancellation.Token));
            Assert.True(SpinWait.SpinUntil(
                () => Directory.EnumerateDirectories(root).Any(),
                TimeSpan.FromSeconds(5)));
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => build);
            Assert.Empty(Directory.EnumerateFileSystemEntries(root));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Theory]
    [InlineData("rcal_Predecessor", "0")]
    [InlineData("rcal_Project", "0")]
    [InlineData("rcal_ProjDefault", "0")]
    [InlineData("rcal_24Hour", "0.5")]
    [InlineData("rcal_Successor", "0.5")]
    [InlineData(" RCAL_SUCCESSOR ", "0.5")]
    [InlineData("", "0.5")]
    public async Task Relationship_float_uses_shared_calendar_policy_in_tender_export(string setting, string expectedFloat)
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(new Stage(source, "2026-08-31", null, "8"));
        AddSecondTaskAndRelationship(store, source.SourceToken, "24", setting, lagHours: "8");
        XerTable tasks = store.GetTable("TASK")!;
        DataRow predecessor = tasks.Rows.Single(row => row.Fields[tasks.FieldIndexes["task_id"]] == "T1");
        DataRow successor = tasks.Rows.Single(row => row.Fields[tasks.FieldIndexes["task_id"]] == "T2");
        predecessor.Fields[tasks.FieldIndexes["early_start_date"]] = "2026-09-04 08:00";
        predecessor.Fields[tasks.FieldIndexes["restart_date"]] = "2026-09-04 08:00";
        predecessor.Fields[tasks.FieldIndexes["early_end_date"]] = "2026-09-04 12:00";
        predecessor.Fields[tasks.FieldIndexes["reend_date"]] = "2026-09-04 12:00";
        successor.Fields[tasks.FieldIndexes["early_start_date"]] = "2026-09-07 12:00";
        successor.Fields[tasks.FieldIndexes["restart_date"]] = "2026-09-07 12:00";

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, TenderReviewNamingTests.Request(new[] { source }));
        IReadOnlyList<string[]> csv = ReadCsv(result.Files["06_XER_PREDECESSOR.csv"]);
        Dictionary<string, string> relationship = Row(csv[0], csv[1]);
        Assert.Equal("1", relationship["lag"]); // Always predecessor day units.
        Assert.Equal(expectedFloat, relationship["free_float"]);
    }

    [Theory]
    [InlineData("2026-09-07 17:00", "2026-09-08 17:00", "0.125")]
    [InlineData("2026-09-07 09:00", "2026-09-08 09:00", "0.875")]
    [InlineData("2026-09-07 17:00", "2026-09-08 00:00", "-0.125")]
    public async Task Relationship_delay_allowance_and_status_reach_tender_v2_csv(
        string predecessorFinish, string successorStart, string expectedFloat)
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(new Stage(source, "2026-08-31", null, "8"));
        AddSecondTaskAndRelationship(store, source.SourceToken, "24", "rcal_24Hour", lagHours: "8");
        XerTable tasks = store.GetTable("TASK")!;
        SetDates("T1", "2026-09-07 08:00", predecessorFinish);
        SetDates("T2", successorStart, "2026-09-08 18:00");

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, TenderReviewNamingTests.Request(new[] { source }));
        IReadOnlyList<string[]> csv = ReadCsv(result.Files["06_XER_PREDECESSOR.csv"]);
        Assert.Equal(2, csv.Count);
        Assert.Equal(19, csv[0].Length);
        Assert.Equal(TenderReviewContract.Tables.Single(table => table.TableName == "06_XER_PREDECESSOR")
            .Columns.Select(column => column.Name), csv[0]);
        Dictionary<string, string> relationship = Row(csv[0], csv[1]);

        // Independent endpoint limits after removing eight elapsed lag hours:
        // Tue 09:00, Tue 01:00 and Mon 16:00 respectively. On the predecessor's
        // 08-12/13-17 weekday calendar these permit +1, +7 and -1 working hours.
        Assert.Equal(expectedFloat, relationship["free_float"]);
        Assert.Equal("1", relationship["lag"]);
        RelationshipFloatAssessment assessment = Assert.Single(new XerTransformer(store).AssessRelationships());
        Assert.Equal(assessment.AllowanceStatus.ToString(), relationship["free_float_status"]);
        Assert.Equal(assessment.CalculationBasis, relationship["free_float_basis"]);
        Assert.Equal(assessment.ReasonCode, relationship["free_float_reason"]);
        Assert.Equal("PR_FS", relationship["pred_type"]);
        Assert.Equal("CSV::J5001::TENDER::20260905::T1", relationship["pred_task_id_key"]);
        Assert.Equal("CSV::J5001::TENDER::20260905::T2", relationship["task_id_key"]);
        Assert.All(result.ManifestRows, row => Assert.Equal("3.0", row.SchemaVersion));

        void SetDates(string taskId, string start, string finish)
        {
            DataRow task = tasks.Rows.Single(row => row.Fields[tasks.FieldIndexes["task_id"]] == taskId);
            foreach (string field in new[] { "early_start_date", "restart_date" })
                task.Fields[tasks.FieldIndexes[field]] = start;
            foreach (string field in new[] { "early_end_date", "reend_date" })
                task.Fields[tasks.FieldIndexes[field]] = finish;
        }
    }

    [Fact]
    public async Task Nonzero_project_lag_requires_a_resolvable_default_calendar()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "stage.xer", "2026-09-05");
        XerDataStore store = BuildDataStore(new Stage(source, "2026-08-31", null, "8"));
        AddSecondTaskAndRelationship(store, source.SourceToken, "24", "rcal_Project", lagHours: "8");
        XerTable project = store.GetTable("PROJECT")!;
        project.Rows[0].Fields[project.FieldIndexes["clndr_id"]] = "missing";
        var error = await BuildWithWarnings(store, source);
        Assert.Contains("RELATIONSHIP_UnresolvedLagCalendar", error.Message, StringComparison.Ordinal);
        var relationships = ReadCsv(error.Result.Files["06_XER_PREDECESSOR.csv"]);
        Assert.Equal("", Row(relationships[0], Assert.Single(relationships.Skip(1)))["free_float"]);
    }

    private static XerDataStore BuildDataStore(params Stage[] stages) =>
        BuildDataStore(stages, includeAddDateHeader: true);

    private static async Task<(string Message, TenderReviewInMemoryBundleResult Result)> BuildWithWarnings(
        XerDataStore store, TenderReviewSource source)
    {
        var result = await new TenderReviewBundleService().BuildFromParsedDataToMemoryAsync(
            store, TenderReviewNamingTests.Request(new[] { source }));
        Assert.Equal(11, result.Files.Count);
        Assert.DoesNotContain(XerDataQuality.FileName, result.Files.Keys);
        Assert.True(result.WarningCount > 0);
        byte[] qualityBytes = XerDataQuality.WriteToBytes(result.DataQualityTable!, CancellationToken.None);
        return (Encoding.UTF8.GetString(qualityBytes), result);
    }

    private static XerDataStore BuildDataStore(
        IReadOnlyList<Stage> stages,
        bool includeAddDateHeader = true,
        int taskCountPerStage = 1,
        bool includeCalendarHoursHeader = true,
        bool blankTaskHours = false)
    {
        var store = new XerDataStore();
        var task = NewTable("TASK", TaskHeaders);
        string[] projectHeaders = includeAddDateHeader
            ? new[] { "proj_id", "last_recalc_date", "proj_short_name", "add_date", "clndr_id" }
            : new[] { "proj_id", "last_recalc_date", "proj_short_name", "clndr_id" };
        var project = NewTable("PROJECT", projectHeaders);
        var wbs = NewTable("PROJWBS", new[]
        {
            "wbs_id", "parent_wbs_id", "proj_id", "wbs_name"
        });
        string[] calendarHeaders = includeCalendarHoursHeader
            ? new[] { "clndr_id", "clndr_name", "day_hr_cnt", "clndr_type", "clndr_data" }
            : new[] { "clndr_id", "clndr_name", "clndr_type", "clndr_data" };
        var calendar = NewTable("CALENDAR", calendarHeaders);

        foreach (Stage stage in stages)
        {
            for (int taskIndex = 1; taskIndex <= taskCountPerStage; taskIndex++)
            {
                string taskName = taskIndex == 1 ? stage.TaskName : $"Activity {taskIndex}";
                task.AddRow(new DataRow(TaskFields(
                    $"T{taskIndex}", "P1", "W1", "C1", $"A{99 + taskIndex}", taskName,
                    blankTaskHours ? "" : "20",
                    blankTaskHours ? "" : "40",
                    blankTaskHours ? "" : "30",
                    blankTaskHours ? "" : "10"), stage.Source.SourceToken));
            }

            string rawProjectCode = stage.RawProjectCode ?? "J5001";
            string[] projectFields = includeAddDateHeader
                ? new[] { "P1", stage.DataDate, rawProjectCode, stage.AddDate ?? string.Empty, "C1" }
                : new[] { "P1", stage.DataDate, rawProjectCode, "C1" };
            project.AddRow(new DataRow(projectFields, stage.Source.SourceToken));
            wbs.AddRow(new DataRow(new[] { "W1", "", "P1", "Root WBS" },
                stage.Source.SourceToken));
            string[] calendarFields = includeCalendarHoursHeader
                ? new[]
                {
                    stage.CalendarIdInCalendarTable ?? "C1", "Activity calendar",
                    stage.CalendarHours, "CA_Project", P6TestCalendars.WorkWeek(stage.CalendarHours)
                }
                : new[]
                {
                    stage.CalendarIdInCalendarTable ?? "C1", "Activity calendar",
                    "CA_Project", P6TestCalendars.WorkWeek()
                };
            calendar.AddRow(new DataRow(calendarFields, stage.Source.SourceToken));
        }

        store.AddTable(task);
        store.AddTable(project);
        store.AddTable(wbs);
        store.AddTable(calendar);
        return store;
    }

    private static void AddSecondTaskAndRelationship(
        XerDataStore store,
        string sourceToken,
        string successorCalendarHours,
        string? lagSetting,
        string lagHours = "20",
        string successorStatusCode = "TK_NotStart",
        bool blankSuccessorHours = false)
    {
        XerTable task = store.GetTable("TASK")!;
        task.AddRow(new DataRow(TaskFields(
            "T2", "P1", "W1", "C2", "A200", "Successor",
            blankSuccessorHours ? "" : "16",
            blankSuccessorHours ? "" : "24",
            blankSuccessorHours ? "" : "16",
            blankSuccessorHours ? "" : "8",
            statusCode: successorStatusCode,
            actualStart: successorStatusCode == "TK_Complete" ? "2026-09-03" : string.Empty,
            actualEnd: successorStatusCode == "TK_Complete" ? "2026-09-10" : string.Empty), sourceToken));
        XerTable calendar = store.GetTable("CALENDAR")!;
        calendar.AddRow(new DataRow(new[]
        {
            "C2", "Successor calendar", successorCalendarHours, "CA_Project", P6TestCalendars.WorkWeek(successorCalendarHours)
        }, sourceToken));
        var predecessor = NewTable("TASKPRED", new[]
        {
            "task_pred_id", "task_id", "pred_task_id", "pred_type", "lag_hr_cnt", "proj_id"
        });
        predecessor.AddRow(new DataRow(new[]
        {
            "R1", "T2", "T1", "PR_FS", lagHours, "P1"
        }, sourceToken));
        store.AddTable(predecessor);
        if (lagSetting is not null)
        {
            var options = NewTable("SCHEDOPTIONS", new[]
            {
                "proj_id", "sched_calendar_on_relationship_lag"
            });
            options.AddRow(new DataRow(new[] { "P1", lagSetting }, sourceToken));
            store.AddTable(options);
        }
    }

    private static void AddContinuousCalendarResourceAssignments(
        XerDataStore store, string sourceToken, params string[] quantities)
    {
        AddResourceAssignments(store, sourceToken, quantities);
        XerTable task = store.GetTable("TASK")!;
        foreach (string field in new[] { "early_start_date", "late_start_date", "restart_date", "target_start_date" })
            task.Rows[0].Fields[task.FieldIndexes[field]] = "2026-01-31 00:00";
        foreach (string field in new[] { "early_end_date", "late_end_date", "reend_date", "target_end_date" })
            task.Rows[0].Fields[task.FieldIndexes[field]] = "2026-03-02 00:00";

        XerTable original = store.GetTable("TASKRSRC")!;
        var assignments = NewTable("TASKRSRC", original.Headers!);
        for (int index = 0; index < original.Rows.Count; index++)
        {
            string[] fields = original.Rows[index].Fields.ToArray();
            fields[assignments.FieldIndexes["restart_date"]] = "2026-01-31 00:00";
            fields[assignments.FieldIndexes["reend_date"]] = "2026-03-02 00:00";
            assignments.AddRow(new DataRow(fields, sourceToken));
        }
        store.AddTable(assignments);
    }

    private static void AddResourceAssignments(
        XerDataStore store,
        string sourceToken,
        params string[] remainingQuantities)
    {
        var resource = NewTable("RSRC", new[]
        {
            "rsrc_id", "rsrc_short_name", "rsrc_name", "rsrc_type", "unit_id",
            "clndr_id", "def_qty_per_hr"
        });
        resource.AddRow(new DataRow(new[]
        {
            "R1", "LAB", "Labour", "RT_Labor", "", "C1", "1"
        }, sourceToken));
        store.AddTable(resource);

        var assignments = NewTable("TASKRSRC", new[]
        {
            "task_id", "rsrc_id", "act_reg_qty", "act_ot_qty", "remain_qty",
            "act_start_date", "act_end_date", "restart_date", "reend_date", "taskrsrc_id", "proj_id"
        });
        foreach (string quantity in remainingQuantities)
        {
            assignments.AddRow(new DataRow(new[]
            {
                "T1", "R1", "", "", quantity, "", "", "2026-09-07", "2026-09-11", $"A{assignments.Rows.Count + 1}", "P1"
            }, sourceToken));
        }
        store.AddTable(assignments);
    }

    private static readonly string[] TaskHeaders =
    {
        "task_id", "proj_id", "wbs_id", "clndr_id", "task_type", "status_code", "task_code", "task_name",
        "rsrc_id", "act_start_date", "act_end_date", "early_start_date", "early_end_date", "late_start_date",
        "late_end_date", "restart_date", "reend_date", "target_start_date", "target_end_date", "cstr_type", "cstr_date", "priority_type",
        "float_path", "float_path_order", "driving_path_flag", "remain_drtn_hr_cnt", "target_drtn_hr_cnt",
        "total_float_hr_cnt", "free_float_hr_cnt", "complete_pct_type", "phys_complete_pct", "act_work_qty",
        "remain_work_qty"
    };

    private static string[] TaskFields(
        string taskId,
        string projectId,
        string wbsId,
        string calendarId,
        string taskCode,
        string taskName,
        string remainingHours,
        string originalHours,
        string totalFloatHours,
        string freeFloatHours,
        string statusCode = "TK_NotStart",
        string actualStart = "",
        string actualEnd = "",
        string earlyStart = "2026-09-07",
        string earlyEnd = "2026-09-11",
        string restart = "2026-09-07",
        string reend = "2026-09-11") =>
    new[]
    {
        taskId, projectId, wbsId, calendarId, "TT_Task", statusCode, taskCode, taskName,
        "", actualStart, actualEnd, earlyStart, earlyEnd, earlyStart, earlyEnd,
        restart, reend, earlyStart, earlyEnd, "", "", "", "", "", "Y", remainingHours,
        originalHours, totalFloatHours, freeFloatHours, "CP_Phys", "0", "0", "0"
    };

    private static byte[] MinimalXerBytes(
        string projectCode,
        string dataDate,
        string addDate,
        bool includeAddDateHeader = true)
    {
        var builder = new StringBuilder();
        AppendXerTable(builder, "TASK", TaskHeaders, TaskFields(
            "T1", "P1", "W1", "C1", "A100", "Byte activity", "16", "40", "8", "4"));
        if (includeAddDateHeader)
        {
            AppendXerTable(builder, "PROJECT",
                new[] { "proj_id", "last_recalc_date", "proj_short_name", "add_date" },
                new[] { "P1", dataDate, projectCode, addDate });
        }
        else
        {
            AppendXerTable(builder, "PROJECT",
                new[] { "proj_id", "last_recalc_date", "proj_short_name" },
                new[] { "P1", dataDate, projectCode });
        }
        AppendXerTable(builder, "PROJWBS",
            new[] { "wbs_id", "parent_wbs_id", "proj_id", "wbs_name" },
            new[] { "W1", "", "P1", "Root WBS" });
        AppendXerTable(builder, "CALENDAR",
            new[] { "clndr_id", "clndr_name", "day_hr_cnt", "clndr_type", "clndr_data" },
            new[] { "C1", "Standard 8h", "8", "CA_Project", P6TestCalendars.WorkWeek() });
        builder.Append("%E\r\n");
        return Encoding.UTF8.GetBytes(builder.ToString());
    }

    private static void AppendXerTable(
        StringBuilder builder,
        string name,
        string[] headers,
        string[] values)
    {
        builder.Append("%T\t").Append(name).Append("\r\n");
        builder.Append("%F\t").AppendJoin('\t', headers).Append("\r\n");
        builder.Append("%R\t").AppendJoin('\t', values).Append("\r\n");
    }

    private static XerTable NewTable(string name, string[] headers)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        return table;
    }

    private static IReadOnlyList<string[]> ReadCsv(byte[] content)
    {
        using var stream = new MemoryStream(content, writable: false);
        using var parser = new TextFieldParser(stream, Encoding.UTF8, detectEncoding: false)
        {
            TextFieldType = FieldType.Delimited,
            HasFieldsEnclosedInQuotes = true,
            TrimWhiteSpace = false
        };
        parser.SetDelimiters(",");
        var rows = new List<string[]>();
        while (!parser.EndOfData)
            rows.Add(parser.ReadFields() ?? throw new InvalidDataException("Unreadable Tender CSV row."));
        return rows;
    }

    private static Dictionary<string, string> Row(string[] headers, string[] values)
    {
        Assert.Equal(headers.Length, values.Length);
        return headers.Zip(values).ToDictionary(pair => pair.First, pair => pair.Second,
            StringComparer.Ordinal);
    }

    private static string NewTempDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), "XerToCsvConverter.Tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed record Stage(
        TenderReviewSource Source,
        string DataDate,
        string? AddDate,
        string CalendarHours,
        string TaskName = "Tender activity",
        string? RawProjectCode = null,
        string? CalendarIdInCalendarTable = null);

    private sealed class RecordingProgress : IProgress<ProcessingService.DetailedProgress>
    {
        private readonly List<ProcessingService.DetailedProgress> _items = new();

        internal IReadOnlyList<ProcessingService.DetailedProgress> Items
        {
            get
            {
                lock (_items) return _items.ToArray();
            }
        }

        public void Report(ProcessingService.DetailedProgress value)
        {
            lock (_items) _items.Add(value);
        }
    }
}
