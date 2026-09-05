using System.Text;
using Microsoft.VisualBasic.FileIO;
using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

// Correctness regressions for the integrated precommit review findings.
public sealed class ReviewProfileAuditTests
{
    [Theory]
    [InlineData(false, "pred_proj_id")]
    [InlineData(true, "pred_proj_id")]
    [InlineData(false, "proj_id")]
    [InlineData(true, "proj_id")]
    public async Task Relationship_project_mismatch_is_rejected_before_export(bool tender, string projectField)
    {
        string source = Source(tender);
        XerDataStore store = Store(source, tender ? "J5001" : "J123");
        AddRelationship(store, source, "P1");
        Set(store, "TASKPRED", 0, projectField, "P2");

        Exception error = tender
            ? await Assert.ThrowsAsync<TenderReviewValidationException>(() => Export(store, true))
            : await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() => Export(store, false));
        Assert.Contains(projectField, error.Message, StringComparison.Ordinal);
        Assert.Contains("P2", error.Message, StringComparison.Ordinal);
        Assert.Contains("external", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(false, "")]
    [InlineData(true, "")]
    [InlineData(false, "P1")]
    [InlineData(true, "P1")]
    public async Task Relationship_without_conflicting_project_metadata_resolves_unique_local_endpoints(
        bool tender, string declaredProject)
    {
        string source = Source(tender);
        XerDataStore store = Store(source, tender ? "J5001" : "J123");
        AddRelationship(store, source, declaredProject);
        Set(store, "TASKPRED", 0, "proj_id", declaredProject);

        IReadOnlyDictionary<string, byte[]> files = await Export(store, tender);

        Dictionary<string, string> relationship = Assert.Single(Rows(files["06_XER_PREDECESSOR.csv"]));
        string localPredecessorKey = Rows(files["01_XER_TASK.csv"])
            .Single(row => row["task_code"] == "A100")["task_id_key"];
        Assert.Equal(localPredecessorKey, relationship["pred_task_id_key"]);
        Assert.Equal("0", relationship["free_float"]);
    }

    [Theory]
    [InlineData(false, "task_id")]
    [InlineData(true, "task_id")]
    [InlineData(false, "pred_task_id")]
    [InlineData(true, "pred_task_id")]
    public async Task Relationship_missing_endpoint_is_rejected_even_with_blank_lag_and_float(
        bool tender, string taskField)
    {
        string source = Source(tender);
        XerDataStore store = Store(source, tender ? "J5001" : "J123");
        AddRelationship(store, source, "P1");
        Set(store, "TASKPRED", 0, taskField, "MISSING");
        Set(store, "TASKPRED", 0, "lag_hr_cnt", "");
        Set(store, "TASK", 0, "total_float_hr_cnt", "");
        Set(store, "TASK", 1, "total_float_hr_cnt", "");

        Exception error = tender
            ? await Assert.ThrowsAsync<TenderReviewValidationException>(() => Export(store, true))
            : await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() => Export(store, false));
        Assert.Contains(taskField, error.Message, StringComparison.Ordinal);
        Assert.Contains("MISSING", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Programme_relationship_total_float_preserves_missing_hours_per_day_as_blank()
    {
        string source = Source(false);
        XerDataStore store = Store(source, "J123");
        AddRelationship(store, source, "P1");
        Set(store, "CALENDAR", 0, "day_hr_cnt", "");

        IReadOnlyDictionary<string, byte[]> files = await Export(store, false);

        Dictionary<string, string> successor = Rows(files["01_XER_TASK.csv"])
            .Single(row => row["task_code"] == "A200");
        Dictionary<string, string> relationship = Assert.Single(Rows(files["06_XER_PREDECESSOR.csv"]));
        Assert.Equal(string.Empty, successor["total_float"]);
        Assert.Equal(string.Empty, relationship["total_float"]);
        Assert.Equal(string.Empty, relationship["free_float"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Relationship_display_dates_match_effective_task_endpoints(bool tender)
    {
        string source = Source(tender);
        XerDataStore store = Store(source, tender ? "J5001" : "J123");
        AddRelationship(store, source, "P1");
        Set(store, "TASK", 0, "early_end_date", "2026-01-02 17:00");
        Set(store, "TASK", 0, "reend_date", "2026-01-05 17:00");
        Set(store, "TASK", 1, "early_start_date", "2026-01-05 08:00");
        Set(store, "TASK", 1, "restart_date", "2026-01-06 08:00");
        Set(store, "TASK", 1, "early_end_date", "2026-01-06 17:00");
        Set(store, "TASK", 1, "reend_date", "2026-01-07 17:00");

        IReadOnlyDictionary<string, byte[]> files = await Export(store, tender);

        Dictionary<string, string> predecessor = Rows(files["01_XER_TASK.csv"])
            .Single(row => row["task_code"] == "A100");
        Dictionary<string, string> successor = Rows(files["01_XER_TASK.csv"])
            .Single(row => row["task_code"] == "A200");
        Dictionary<string, string> relationship = Assert.Single(Rows(files["06_XER_PREDECESSOR.csv"]));
        Assert.Equal("2026-01-05", predecessor["Finish"]);
        Assert.Equal(predecessor["Finish"], relationship["predecessor_finish"]);
        Assert.Equal("2026-01-06", successor["Start"]);
        Assert.Equal(successor["Start"], relationship["start"]);
        Assert.Equal(successor["Finish"], relationship["finish"]);
        Assert.Equal("0", relationship["free_float"]);
    }

    [Fact]
    public async Task Programme_and_tender_keep_null_task_floats_null()
    {
        foreach (bool tender in new[] { false, true })
        {
            XerDataStore store = Store(Source(tender), tender ? "J5001" : "J123");
            Set(store, "TASK", 0, "total_float_hr_cnt", "");
            Set(store, "TASK", 0, "free_float_hr_cnt", "");
            Set(store, "TASK", 0, "remain_drtn_hr_cnt", "");

            IReadOnlyDictionary<string, byte[]> files = await Export(store, tender);

            Dictionary<string, string> task = Rows(files["01_XER_TASK.csv"])
                .Single(row => row["task_code"] == "A100");
            Assert.Equal(string.Empty, task["total_float"]);
            Assert.Equal(string.Empty, task["free_float"]);
            Assert.Equal(string.Empty, task["remaining_duration"]);
        }
    }

    [Theory]
    [InlineData(false, true)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(true, false)]
    public async Task Optional_partial_raw_schema_is_allowed_only_when_no_rows_need_projection(bool tender, bool empty)
    {
        string source = Source(tender);
        XerDataStore store = Store(source, tender ? "J5001" : "J123");
        Add(store, source, "RSRC", new[] { "rsrc_id" }, empty ? Array.Empty<string[]>() : new[] { new[] { "R1" } });

        if (empty)
        {
            IReadOnlyDictionary<string, byte[]> files = await Export(store, tender);
            Assert.Equal(11, files.Count);
            Assert.Empty(Rows(files["12_XER_RSRC.csv"]));
            Assert.Equal("rsrc_id_key,def_qty_per_hr", Encoding.UTF8.GetString(files["12_XER_RSRC.csv"])
                .TrimStart('\uFEFF').Trim());
        }
        else
        {
            Exception error = tender
                ? await Assert.ThrowsAsync<TenderReviewValidationException>(() => Export(store, true))
                : await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() => Export(store, false));
            Assert.Contains("def_qty_per_hr", error.Message, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Empty_required_table_still_rejects_the_bundle(bool tender)
    {
        string source = Source(tender);
        XerDataStore store = Store(source, tender ? "J5001" : "J123");
        Add(store, source, "PROJWBS", new[] { "wbs_id", "parent_wbs_id", "proj_id", "wbs_name" });

        Exception error = tender
            ? await Assert.ThrowsAsync<TenderReviewValidationException>(() => Export(store, true))
            : await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() => Export(store, false));
        Assert.Contains("03_XER_PROJWBS", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Tender_byte_parser_rebinds_all_tables_to_ordered_governed_tokens()
    {
        string sourceToken = Source(true);
        XerDataStore store = Store(sourceToken, "J5001");
        AddRelationship(store, sourceToken, "P1");
        Set(store, "TASKPRED", 0, "lag_hr_cnt", "8");
        Add(store, sourceToken, "SCHEDOPTIONS",
            new[] { "proj_id", "sched_calendar_on_relationship_lag" },
            new[] { "P1", "rcal_Predecessor" });
        XerTable tasks = store.GetTable("TASK")!;
        Add(store, sourceToken, "TASK", tasks.Headers!.Append("duration_type").ToArray(),
            tasks.Rows.Select(row => row.Fields.Append("DT_FixedDrtn").ToArray()).ToArray());
        Add(store, sourceToken, "RSRC",
            new[] { "rsrc_id", "rsrc_name", "rsrc_short_name", "rsrc_type", "clndr_id", "def_qty_per_hr" },
            new[] { "R1", "Governed resource", "R1", "RT_Labor", "C1", "1" });
        Add(store, sourceToken, "TASKRSRC", new[]
        {
            "taskrsrc_id", "task_id", "proj_id", "rsrc_id", "remain_qty", "act_reg_qty", "act_ot_qty",
            "restart_date", "reend_date", "curv_id"
        }, new[] { "A1", "T1", "P1", "R1", "100", "0", "0", "2026-01-30 08:00", "2026-02-02 17:00", "FRONT" });
        Add(store, sourceToken, "RSRCCURVDATA",
            new[] { "curv_id" }.Concat(Enumerable.Range(0, 21).Select(index => "pct_usage_" + index)).ToArray(),
            new[] { "FRONT" }.Concat(Enumerable.Range(0, 21).Select(index => index == 0 ? "0" : index <= 10 ? "8" : "2")).ToArray());
        byte[] content = SerializeXer(store);
        TenderReviewSource first = TenderReviewNamingTests.Source(0, "repeated.xer", "2026-09-05") with { SourceSha256 = null };
        TenderReviewSource second = TenderReviewNamingTests.Source(1, "repeated.xer", "2026-09-06") with { SourceSha256 = null };
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { first, second });

        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService().BuildFromXerBytesAsync(request,
            new[]
            {
                new TenderReviewSourceBytes { SourceToken = first.SourceToken, Content = content },
                new TenderReviewSourceBytes { SourceToken = second.SourceToken, Content = content }
            });

        Dictionary<string, string>[] relationships = Rows(result.Files["06_XER_PREDECESSOR.csv"]);
        Assert.Equal(2, relationships.Length);
        Assert.All(relationships, row =>
        {
            Assert.Equal("1", row["lag"]);
            Assert.NotEqual("", row["free_float"]);
        });
        foreach (IGrouping<string, Dictionary<string, string>> stage in Rows(result.Files["15_XER_RESOURCE_DISTRIBUTION.csv"])
            .GroupBy(row => row["task_id_key"], StringComparer.Ordinal))
            Assert.Equal(new[] { "80", "20" }, stage.OrderBy(row => row["distribution_month"], StringComparer.Ordinal)
                .Select(row => row["monthly_quantity"]).ToArray());
        Assert.Equal(2, relationships.Select(row => row["task_id_key"]).Distinct(StringComparer.Ordinal).Count());

        var parsed = new XerDataStore();
        var parser = new XerParser();
        foreach (TenderReviewSource source in new[] { first, second })
        {
            using var stream = new MemoryStream(content, writable: false);
            parsed.MergeStore(await parser.ParseXerStreamAsync(stream, source.SourceToken, null, CancellationToken.None));
        }
        DataRow[] callerRows = parsed.TableNames.SelectMany(name => parsed.GetTable(name)!.Rows).ToArray();
        string hash = ProgrammeReviewCsv.ComputeSha256(content);
        TenderReviewBundleRequest parsedRequest = request with
        {
            Sources = new[] { first with { SourceSha256 = hash }, second with { SourceSha256 = hash } }
        };
        TenderReviewInMemoryBundleResult parsedResult = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(parsed, parsedRequest);
        foreach ((string name, byte[] expected) in result.Files) Assert.Equal(expected, parsedResult.Files[name]);

        string tempRoot = Path.Combine(Path.GetTempPath(), "XerToCsvConverter.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);
        try
        {
            string path = Path.Combine(tempRoot, "repeated.xer");
            await File.WriteAllBytesAsync(path, content);
            TenderReviewBundleRequest fileRequest = request with
            {
                Sources = new[] { first with { XerFilePath = path }, second with { XerFilePath = path } }
            };
            var service = new TenderReviewBundleService();
            TenderReviewBundleResult files = await service.BuildFromXerFilesAsync(fileRequest, Path.Combine(tempRoot, "files"));
            TenderReviewBundleResult parsedFiles = await service.BuildFromParsedDataAsync(parsed, parsedRequest,
                Path.Combine(tempRoot, "parsed"));
            foreach ((string name, byte[] expected) in result.Files)
            {
                Assert.Equal(expected, await File.ReadAllBytesAsync(Path.Combine(files.BundlePath, name)));
                Assert.Equal(expected, await File.ReadAllBytesAsync(Path.Combine(parsedFiles.BundlePath, name)));
            }
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
        Assert.Equal(callerRows, parsed.TableNames.SelectMany(name => parsed.GetTable(name)!.Rows));
    }

    [Theory]
    [InlineData("missing_curve")]
    [InlineData("malformed_calendar")]
    [InlineData("missing_calendar")]
    [InlineData("external_relationship")]
    public async Task Discarded_programme_baseline_does_not_affect_any_entrypoint_or_mutate_caller(string defect)
    {
        const string discardedSource = "discarded.xer";
        XerDataStore retained = Store("audit.xer", "J123");
        XerDataStore discarded = Store(discardedSource, "J123");
        Add(retained, "audit.xer", "RSRC",
            new[] { "rsrc_id", "rsrc_name", "rsrc_short_name", "rsrc_type", "clndr_id", "def_qty_per_hr" },
            new[] { "R1", "Retained resource", "R1", "RT_Labor", "C1", "1" });
        Add(retained, "audit.xer", "TASKRSRC", new[]
        {
            "taskrsrc_id", "task_id", "proj_id", "rsrc_id", "remain_qty", "act_reg_qty", "act_ot_qty",
            "restart_date", "reend_date", "remain_crv"
        }, new[] { "A1", "T1", "P1", "R1", "8", "0", "0", "2026-01-02 08:00", "2026-01-02 17:00", "8:8" });
        XerTable discardedTasks = discarded.GetTable("TASK")!;
        Add(discarded, discardedSource, "TASK", discardedTasks.Headers!.Append("duration_type").ToArray(),
            discardedTasks.Rows.Select(row => row.Fields.Append("DT_FixedDrtn").ToArray()).ToArray());
        Add(discarded, discardedSource, "RSRC",
            new[] { "rsrc_id", "rsrc_name", "rsrc_short_name", "rsrc_type", "clndr_id" },
            new[] { "R1", "Resource", "R1", "RT_Labor", "C1" });
        Add(discarded, discardedSource, "TASKRSRC", new[]
        {
            "taskrsrc_id", "task_id", "proj_id", "rsrc_id", "remain_qty", "act_reg_qty", "act_ot_qty",
            "restart_date", "reend_date", "curv_id"
        }, new[] { "A1", "T1", "P1", "R1", "8", "0", "0", "2026-01-02 08:00", "2026-01-02 17:00", "MISSING" });
        if (defect != "missing_curve") Set(discarded, "TASKRSRC", 0, "curv_id", "");
        if (defect == "malformed_calendar") Set(discarded, "CALENDAR", 0, "clndr_data", "malformed");
        if (defect == "missing_calendar") Set(discarded, "TASK", 0, "clndr_id", "MISSING");
        if (defect == "external_relationship") AddRelationship(discarded, discardedSource, "P2");
        // This unused, malformed definition must not be validated merely because
        // the caller previously parsed the discarded input.
        Add(discarded, discardedSource, "RSRCCURVDATA", new[] { "curv_id", "pct_usage_1" },
            new[] { "UNUSED", "NaN" });

        byte[] retainedBytes = SerializeXer(retained);
        byte[] discardedBytes = SerializeXer(discarded);
        foreach (string name in discarded.TableNames)
        {
            XerTable incoming = discarded.GetTable(name)!;
            XerTable? existing = retained.GetTable(name);
            if (existing is null)
            {
                retained.AddTable(incoming);
                continue;
            }
            string[] headers = existing.Headers!.Concat(incoming.Headers!)
                .Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            var union = new XerTable(name);
            union.SetHeaders(headers);
            foreach (XerTable table in new[] { existing, incoming })
                foreach (DataRow row in table.Rows)
                    union.AddRow(row with { Fields = headers.Select(header => table.FieldIndexes.TryGetValue(header, out int index)
                        ? row.Fields[index] : string.Empty).ToArray() });
            retained.AddTable(union);
        }
        ProgrammeReviewSnapshot currentBaseline = ProgrammeReviewNamingTests.Snapshot(
            "audit.xer", ProgrammeReviewSnapshotKind.Baseline, "BL02", "2026-01-31", "2026-01-01") with
            { SourceSha256 = ProgrammeReviewCsv.ComputeSha256(retainedBytes) };
        ProgrammeReviewSnapshot obsoleteBaseline = ProgrammeReviewNamingTests.Snapshot(
            discardedSource, ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-01") with
            { SourceSha256 = ProgrammeReviewCsv.ComputeSha256(discardedBytes) };
        ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request(new[] { currentBaseline, obsoleteBaseline });
        ResolvedProgrammeReviewRequest resolved = ProgrammeReviewNaming.Resolve(request, snapshot => snapshot.SourceSha256!);
        Assert.Equal("audit.xer", Assert.Single(resolved.Snapshots).OriginalXerFilename);

        byte[] originalContent = SerializeXer(retained);
        DataRow[] originalRows = retained.TableNames.SelectMany(name => retained.GetTable(name)!.Rows).ToArray();
        var service = new ProgrammeReviewBundleService();
        ProgrammeReviewInMemoryBundleResult parsed = await service.BuildFromParsedDataToMemoryAsync(retained, request);
        ProgrammeReviewInMemoryBundleResult bytes = await service.BuildFromXerBytesAsync(request,
            new Dictionary<string, byte[]>(StringComparer.Ordinal)
            {
                ["audit.xer"] = retainedBytes,
                [discardedSource] = discardedBytes
            });
        Assert.Equal(11, parsed.Files.Count);
        Dictionary<string, string> allocation = Assert.Single(Rows(parsed.Files["15_XER_RESOURCE_DISTRIBUTION.csv"]));
        Assert.Equal("8", allocation["monthly_quantity"]);
        Assert.Equal("Retained resource", allocation["rsrc_name"]);
        foreach ((string name, byte[] content) in parsed.Files) Assert.Equal(content, bytes.Files[name]);

        string tempRoot = Path.Combine(Path.GetTempPath(), "XerToCsvConverter.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);
        try
        {
            string sourceRoot = Path.Combine(tempRoot, "sources");
            Directory.CreateDirectory(sourceRoot);
            string retainedPath = Path.Combine(sourceRoot, "audit.xer");
            string discardedPath = Path.Combine(sourceRoot, discardedSource);
            await File.WriteAllBytesAsync(retainedPath, retainedBytes);
            await File.WriteAllBytesAsync(discardedPath, discardedBytes);
            ProgrammeReviewBundleRequest fileRequest = request with
            {
                Snapshots = new[]
                {
                    currentBaseline with { XerFilePath = retainedPath },
                    obsoleteBaseline with { XerFilePath = discardedPath }
                }
            };
            ProgrammeReviewBundleResult fromFiles = await service.BuildFromXerFilesAsync(fileRequest,
                Path.Combine(tempRoot, "from-files"));
            ProgrammeReviewBundleResult fromParsed = await service.BuildFromParsedDataAsync(retained, request,
                Path.Combine(tempRoot, "from-parsed"));
            foreach ((string name, byte[] content) in parsed.Files)
            {
                Assert.Equal(content, await File.ReadAllBytesAsync(Path.Combine(fromFiles.BundlePath, name)));
                Assert.Equal(content, await File.ReadAllBytesAsync(Path.Combine(fromParsed.BundlePath, name)));
            }
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
        Assert.Equal(originalContent, SerializeXer(retained));
        Assert.Equal(originalRows, retained.TableNames.SelectMany(name => retained.GetTable(name)!.Rows));
    }

    private static byte[] SerializeXer(XerDataStore store)
    {
        var builder = new StringBuilder();
        foreach (string name in store.TableNames)
        {
            XerTable table = store.GetTable(name)!;
            builder.Append("%T\t").Append(name).Append("\r\n%F\t")
                .AppendJoin('\t', table.Headers!).Append("\r\n");
            foreach (DataRow row in table.Rows)
                builder.Append("%R\t").AppendJoin('\t', row.Fields).Append("\r\n");
        }
        builder.Append("%E\r\n");
        return Encoding.UTF8.GetBytes(builder.ToString());
    }

    private static async Task<IReadOnlyDictionary<string, byte[]>> Export(XerDataStore store, bool tender)
    {
        if (tender)
        {
            TenderReviewSource source = TenderReviewNamingTests.Source(0, "audit.xer", "2026-09-05");
            TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
                .BuildFromParsedDataToMemoryAsync(store, TenderReviewNamingTests.Request(new[] { source }));
            return result.Files;
        }
        ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
            "audit.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-01");
        ProgrammeReviewInMemoryBundleResult programme = await new ProgrammeReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, ProgrammeReviewNamingTests.Request(new[] { baseline }));
        return programme.Files;
    }

    private static string Source(bool tender) => tender ? TenderReviewNaming.CreateSourceToken(0) : "audit.xer";

    private static XerDataStore Store(string source, string projectCode)
    {
        var store = new XerDataStore();
        string[] headers =
        {
            "task_id", "proj_id", "wbs_id", "clndr_id", "task_type", "status_code", "task_code", "task_name",
            "rsrc_id", "act_start_date", "act_end_date", "early_start_date", "early_end_date", "late_start_date",
            "late_end_date", "restart_date", "reend_date", "target_start_date", "target_end_date", "cstr_type",
            "cstr_date", "priority_type", "float_path", "float_path_order", "driving_path_flag",
            "remain_drtn_hr_cnt", "target_drtn_hr_cnt", "total_float_hr_cnt", "free_float_hr_cnt",
            "complete_pct_type", "phys_complete_pct", "act_work_qty", "remain_work_qty"
        };
        Add(store, source, "TASK", headers,
            Task("T1", "A100", "2026-01-02 08:00", "2026-01-02 17:00"),
            Task("T2", "A200", "2026-01-05 08:00", "2026-01-05 17:00"));
        Add(store, source, "PROJECT", new[] { "proj_id", "last_recalc_date", "proj_short_name", "clndr_id" },
            new[] { "P1", "2026-01-01", projectCode, "C1" });
        Add(store, source, "PROJWBS", new[] { "wbs_id", "parent_wbs_id", "proj_id", "wbs_name" },
            new[] { "W1", "", "P1", "Root" });
        Add(store, source, "CALENDAR", new[] { "clndr_id", "clndr_name", "day_hr_cnt", "clndr_type", "clndr_data" },
            new[] { "C1", "Eight hours", "8", "CA_Project", P6TestCalendars.WorkWeek() });
        return store;

        string[] Task(string id, string code, string start, string finish) => new[]
        {
            id, "P1", "W1", "C1", "TT_Task", "TK_NotStart", code, code,
            "", "", "", start, finish, start, finish, start, finish, start, finish,
            "", "", "", "", "", "Y", "8", "8", "8", "0", "CP_Drtn", "0", "0", "0"
        };
    }

    private static void AddRelationship(XerDataStore store, string source, string predecessorProject) =>
        Add(store, source, "TASKPRED",
            new[] { "task_pred_id", "task_id", "pred_task_id", "pred_type", "lag_hr_cnt", "proj_id", "pred_proj_id" },
            new[] { "L1", "T2", "T1", "PR_FS", "0", "P1", predecessorProject });

    private static void Add(XerDataStore store, string source, string name, string[] headers, params string[][] rows)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        foreach (string[] row in rows)
        {
            Assert.Equal(headers.Length, row.Length);
            table.AddRow(new DataRow(row, source));
        }
        store.AddTable(table);
    }

    private static void Set(XerDataStore store, string tableName, int row, string field, string value)
    {
        XerTable table = store.GetTable(tableName)!;
        table.Rows[row].Fields[table.FieldIndexes[field]] = value;
    }

    private static Dictionary<string, string>[] Rows(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes, writable: false);
        using var parser = new TextFieldParser(stream, Encoding.UTF8, detectEncoding: true)
        {
            TextFieldType = FieldType.Delimited,
            HasFieldsEnclosedInQuotes = true,
            TrimWhiteSpace = false
        };
        parser.SetDelimiters(",");
        string[] headers = parser.ReadFields() ?? throw new InvalidDataException("Missing audit CSV headers.");
        var rows = new List<Dictionary<string, string>>();
        while (!parser.EndOfData)
        {
            string[] values = parser.ReadFields() ?? throw new InvalidDataException("Missing audit CSV record.");
            rows.Add(headers.Zip(values).ToDictionary(pair => pair.First, pair => pair.Second, StringComparer.Ordinal));
        }
        return rows.ToArray();
    }
}
