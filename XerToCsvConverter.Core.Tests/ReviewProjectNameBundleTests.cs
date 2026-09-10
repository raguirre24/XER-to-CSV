using System.Text;
using Microsoft.VisualBasic.FileIO;
using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ReviewProjectNameBundleTests
{
    public static IEnumerable<object[]> FlexibleProjectNames()
    {
        string[] names =
        [
            "QAC000623-01-02",
            "  NE Part B  ",
            "Māori 工程 Étage 2",
            "../North\\South:Part|B%25",
            "North, \"Section B\"",
            new string('A', 260) + " Māori/Part B"
        ];
        foreach (bool tender in new[] { false, true })
            foreach (string name in names)
                yield return [tender, name];
    }

    [Theory]
    [MemberData(nameof(FlexibleProjectNames))]
    public async Task Flexible_project_codes_preserve_metadata_safe_paths_and_join_keys_across_disk_and_web(
        bool tender, string projectCode)
    {
        string temp = NewTempDirectory();
        try
        {
            string path = Path.Combine(temp, "source.xer");
            byte[] xer = MinimalXerBytes(projectCode);
            await File.WriteAllBytesAsync(path, xer);
            string output = Path.Combine(temp, "output");
            Directory.CreateDirectory(output);
            IReadOnlyDictionary<string, byte[]> files;
            string bundlePath;
            string bundleId;
            string canonicalName;
            if (tender)
            {
                TenderReviewSource source = TenderReviewNamingTests.Source(0, "source.xer", "2026-09-05")
                    with { XerFilePath = path, SourceSha256 = null };
                TenderReviewBundleRequest request = TenderReviewNamingTests.Request([source])
                    with { ProjectCode = projectCode };
                var service = new TenderReviewBundleService();
                TenderReviewBundleResult disk = await service.BuildFromXerFilesAsync(request, output);
                TenderReviewInMemoryBundleResult memory = await service.BuildFromXerBytesAsync(request,
                    [new TenderReviewSourceBytes { SourceToken = source.SourceToken, Content = xer }]);
                Assert.Equal(disk.BundleId, memory.BundleId);
                Assert.All(memory.ManifestRows, row => Assert.Equal(projectCode.Trim().ToUpperInvariant(), row.ProjectCode));
                files = memory.Files;
                bundlePath = disk.BundlePath;
                bundleId = disk.BundleId;
                canonicalName = Assert.Single(memory.ManifestRows.Select(row => row.CanonicalXerFilename).Distinct());
            }
            else
            {
                ProgrammeReviewSnapshot source = ProgrammeReviewNamingTests.Snapshot(
                    "source.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-08-31", "2026-08-31")
                    with { XerFilePath = path, SourceSha256 = null };
                ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request([source])
                    with { ProjectCode = projectCode };
                var service = new ProgrammeReviewBundleService();
                ProgrammeReviewBundleResult disk = await service.BuildFromXerFilesAsync(request, output);
                ProgrammeReviewInMemoryBundleResult memory = await service.BuildFromXerBytesAsync(request,
                    new Dictionary<string, byte[]> { [source.OriginalXerFilename] = xer });
                Assert.Equal(disk.BundleId, memory.BundleId);
                Assert.All(memory.ManifestRows, row => Assert.Equal(projectCode.Trim().ToUpperInvariant(), row.ProjectCode));
                files = memory.Files;
                bundlePath = disk.BundlePath;
                bundleId = disk.BundleId;
                canonicalName = Assert.Single(memory.ManifestRows.Select(row => row.CanonicalXerFilename).Distinct());
            }

            Assert.Equal(Path.GetFullPath(output), Path.GetDirectoryName(Path.GetFullPath(bundlePath)));
            AssertSafeFilename(bundleId);
            AssertSafeFilename(canonicalName);
            Assert.True(bundleId.Length < 180);
            Assert.True(canonicalName.Length < 150);
            Assert.Equal(11, files.Count);
            foreach ((string name, byte[] content) in files)
            {
                AssertSafeFilename(name);
                Assert.Equal(content, await File.ReadAllBytesAsync(Path.Combine(bundlePath, name)));
            }
            AssertMetadataAndJoins(files, projectCode.Trim().ToUpperInvariant(), canonicalName);
        }
        finally
        {
            Directory.Delete(temp, recursive: true);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Tender_special_project_names_preserve_repeated_filename_path_hash_and_input_order(bool reverse)
    {
        const string projectCode = "NE/Part|B%25";
        string temp = NewTempDirectory();
        try
        {
            string path = Path.Combine(temp, "same.xer");
            byte[] xer = MinimalXerBytes(projectCode);
            await File.WriteAllBytesAsync(path, xer);
            TenderReviewSource[] sources =
            [
                TenderReviewNamingTests.Source(0, "same.xer", "2026-09-05") with { XerFilePath = path, SourceSha256 = null },
                TenderReviewNamingTests.Source(1, "same.xer", "2026-09-06") with { XerFilePath = path, SourceSha256 = null }
            ];
            if (reverse) Array.Reverse(sources);
            TenderReviewBundleRequest request = TenderReviewNamingTests.Request(sources) with { ProjectCode = projectCode };
            var service = new TenderReviewBundleService();
            string output = Path.Combine(temp, "output");
            Directory.CreateDirectory(output);
            TenderReviewBundleResult disk = await service.BuildFromXerFilesAsync(request, output);
            var progress = new RecordingProgress();
            TenderReviewInMemoryBundleResult memory = await service.BuildFromXerBytesAsync(request,
                sources.Select(source => new TenderReviewSourceBytes { SourceToken = source.SourceToken, Content = xer }).ToArray(),
                progress);

            Assert.Equal(disk.BundleId, memory.BundleId);
            Assert.Equal(sources.Select(source => source.SourceToken), progress.Sources);
            Assert.Equal(20, memory.ManifestRows.Count);
            Assert.Single(memory.ManifestRows.Select(row => row.SourceSha256).Distinct());
            Assert.Single(memory.ManifestRows.Select(row => row.OriginalXerFilename).Distinct());
            Assert.Equal(2, memory.ManifestRows.Select(row => row.CanonicalXerFilename).Distinct().Count());
            Assert.All(memory.ManifestRows, row => Assert.Equal(projectCode.ToUpperInvariant(), row.ProjectCode));
            Dictionary<string, string>[] tasks = ReadCsv(memory.Files["01_XER_TASK.csv"]);
            Assert.Equal(4, tasks.Length);
            Assert.Equal(4, tasks.Select(row => row["task_id_key"]).Distinct().Count());
            Assert.Equal(new[] { "2026-09-05", "2026-09-06" }, tasks.Select(row => row["status_date"]).Distinct());
            foreach ((string name, byte[] content) in memory.Files)
                Assert.Equal(content, await File.ReadAllBytesAsync(Path.Combine(disk.BundlePath, name)));
        }
        finally
        {
            Directory.Delete(temp, recursive: true);
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task Empty_project_code_still_fails_both_review_profiles(string projectCode)
    {
        byte[] xer = MinimalXerBytes("QAC000623-01-02");
        TenderReviewSource tenderSource = TenderReviewNamingTests.Source(0, "source.xer", "2026-09-05")
            with { SourceSha256 = null };
        await Assert.ThrowsAsync<TenderReviewValidationException>(() => new TenderReviewBundleService()
            .BuildFromXerBytesAsync(TenderReviewNamingTests.Request([tenderSource]) with { ProjectCode = projectCode },
                [new TenderReviewSourceBytes { SourceToken = tenderSource.SourceToken, Content = xer }]));
        ProgrammeReviewSnapshot programmeSource = ProgrammeReviewNamingTests.Snapshot(
            "source.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-08-31", "2026-08-31")
            with { SourceSha256 = null };
        await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() => new ProgrammeReviewBundleService()
            .BuildFromXerBytesAsync(ProgrammeReviewNamingTests.Request([programmeSource]) with { ProjectCode = projectCode },
                new Dictionary<string, byte[]> { [programmeSource.OriginalXerFilename] = xer }));
    }

    private static void AssertMetadataAndJoins(IReadOnlyDictionary<string, byte[]> files, string projectCode, string canonicalName)
    {
        foreach ((string _, byte[] content) in files)
            foreach (Dictionary<string, string> row in ReadCsv(content))
            {
                if (row.TryGetValue("ProjectCode", out string? code)) Assert.Equal(projectCode, code);
                if (row.TryGetValue("project_code", out string? manifestCode)) Assert.Equal(projectCode, manifestCode);
                foreach ((string field, string value) in row.Where(pair => pair.Key.EndsWith("_key", StringComparison.Ordinal)))
                {
                    if (value.Length == 0) continue;
                    Assert.StartsWith("CSV::", value);
                    Assert.DoesNotContain('|', value);
                    Assert.DoesNotContain('/', value);
                    Assert.DoesNotContain('\\', value);
                    Assert.Equal(5, value.Split("::", StringSplitOptions.None).Length);
                }
            }

        Dictionary<string, string>[] tasks = ReadCsv(files["01_XER_TASK.csv"]);
        Dictionary<string, string> project = Assert.Single(ReadCsv(files["02_XER_PROJECT.csv"]));
        Dictionary<string, string> wbs = Assert.Single(ReadCsv(files["03_XER_PROJWBS.csv"]));
        Dictionary<string, string> calendar = Assert.Single(ReadCsv(files["10_XER_CALENDAR.csv"]));
        Dictionary<string, string> relationship = Assert.Single(ReadCsv(files["06_XER_PREDECESSOR.csv"]));
        Assert.Equal(2, tasks.Length);
        Assert.All(tasks, task =>
        {
            Assert.Equal(project["proj_id_key"], task["proj_id_key"]);
            Assert.Equal(wbs["wbs_id_key"], task["wbs_id_key"]);
            Assert.Equal(calendar["clndr_id_key"], task["calendar_id_key"]);
            Assert.Equal(canonicalName, task["filename"]);
        });
        Assert.Contains(relationship["task_id_key"], tasks.Select(row => row["task_id_key"]));
        Assert.Contains(relationship["pred_task_id_key"], tasks.Select(row => row["task_id_key"]));
        if (projectCode.Contains('"') || projectCode.Contains(','))
            Assert.Contains("\"" + projectCode.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"",
                Encoding.UTF8.GetString(files["01_XER_TASK.csv"]), StringComparison.Ordinal);
        if (projectCode.Length > 200)
            Assert.All(tasks, task => Assert.True(task["task_id_key"].Length > 260));
    }

    private static void AssertSafeFilename(string filename)
    {
        Assert.Equal(filename, Path.GetFileName(filename));
        Assert.DoesNotContain(filename, character => "<>:\"/\\|?*".Contains(character) || char.IsControl(character));
    }

    private static Dictionary<string, string>[] ReadCsv(byte[] content)
    {
        using var stream = new MemoryStream(content, writable: false);
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
        {
            string[] values = parser.ReadFields()!;
            Assert.Equal(headers.Length, values.Length);
            rows.Add(headers.Zip(values).ToDictionary(pair => pair.First, pair => pair.Second, StringComparer.Ordinal));
        }
        return rows.ToArray();
    }

    private static byte[] MinimalXerBytes(string projectCode)
    {
        var builder = new StringBuilder();
        string[] taskHeaders =
        [
            "task_id", "proj_id", "wbs_id", "clndr_id", "task_type", "status_code", "task_code", "task_name",
            "rsrc_id", "act_start_date", "act_end_date", "early_start_date", "early_end_date", "late_start_date",
            "late_end_date", "restart_date", "reend_date", "target_start_date", "target_end_date", "cstr_type", "cstr_date", "priority_type",
            "float_path", "float_path_order", "driving_path_flag", "remain_drtn_hr_cnt", "target_drtn_hr_cnt",
            "total_float_hr_cnt", "free_float_hr_cnt", "complete_pct_type", "phys_complete_pct", "act_work_qty", "remain_work_qty"
        ];
        builder.Append("%T\tTASK\r\n%F\t").AppendJoin('\t', taskHeaders).Append("\r\n");
        for (int id = 1; id <= 2; id++)
        {
            string start = id == 1 ? "2026-09-07 08:00" : "2026-09-09 08:00";
            string finish = id == 1 ? "2026-09-08 17:00" : "2026-09-10 17:00";
            string[] values =
            [
                $"T{id}", "P1", "W1", "C1", "TT_Task", "TK_NotStart", $"A{id}00", $"Activity {id}",
                "", "", "", start, finish, start, finish, start, finish, start, finish, "", "", "", "", "", "Y",
                "16", "40", "8", "4", "CP_Phys", "0", "0", "0"
            ];
            builder.Append("%R\t").AppendJoin('\t', values).Append("\r\n");
        }
        AddTable("PROJECT", ["proj_id", "last_recalc_date", "proj_short_name", "add_date"],
            ["P1", "2026-08-31", projectCode, "2026-06-02"]);
        AddTable("PROJWBS", ["wbs_id", "parent_wbs_id", "proj_id", "wbs_name"], ["W1", "", "P1", "Root WBS"]);
        AddTable("CALENDAR", ["clndr_id", "clndr_name", "day_hr_cnt", "clndr_type", "clndr_data"],
            ["C1", "Standard 8h", "8", "CA_Project", P6TestCalendars.WorkWeek()]);
        AddTable("TASKPRED", ["task_pred_id", "proj_id", "pred_proj_id", "task_id", "pred_task_id", "pred_type", "lag_hr_cnt"],
            ["R1", "P1", "P1", "T2", "T1", "PR_FS", "0"]);
        builder.Append("%E\r\n");
        return Encoding.UTF8.GetBytes(builder.ToString());

        void AddTable(string name, string[] headers, string[] values)
        {
            builder.Append("%T\t").Append(name).Append("\r\n%F\t").AppendJoin('\t', headers)
                .Append("\r\n%R\t").AppendJoin('\t', values).Append("\r\n");
        }
    }

    private static string NewTempDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), "XerToCsvConverter.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed class RecordingProgress : IProgress<ProcessingService.DetailedProgress>
    {
        public List<string?> Sources { get; } = [];

        public void Report(ProcessingService.DetailedProgress value)
        {
            if (value.Message?.StartsWith("Parsing Tender source:", StringComparison.Ordinal) == true)
                Sources.Add(value.FilePath);
        }
    }
}
