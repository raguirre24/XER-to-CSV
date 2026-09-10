using System.Globalization;
using System.Text;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class TenderReviewStateTests
{
    [Fact]
    public void Manifest_state_is_additive_without_breaking_the_existing_public_constructor()
    {
        DateOnly date = new(2026, 9, 5);
        var row = new TenderReviewManifestRow("3.0", "tender_review", "bundle", "COMPLETE", "test",
            "QAC000623", "NE Part B", "stage.xer", "QAC000623-TENDER-20260905.xer", date, date, date,
            new string('a', 64), "02_XER_PROJECT", 1, new string('b', 64), DateTimeOffset.UnixEpoch);
        Assert.Equal("", row.ProjectState);
        Assert.Equal("QLD", (row with { ProjectState = "QLD" }).ProjectState);
    }

    [Theory]
    [InlineData(null, "")]
    [InlineData("", "")]
    [InlineData(" \t\r\n ", "")]
    [InlineData(" new south wales ", "NSW")]
    [InlineData("Queensland", "QLD")]
    [InlineData(" south australia ", "SA")]
    [InlineData("Tasmania", "TAS")]
    [InlineData("Victoria", "VIC")]
    [InlineData("Western Australia", "WA")]
    [InlineData("Australian Capital Territory", "ACT")]
    [InlineData("Northern Territory", "NT")]
    [InlineData(" qld ", "QLD")]
    [InlineData(" Waikato / Tāmaki 工程 ", "WAIKATO / TĀMAKI 工程")]
    [InlineData("North  Region", "NORTH  REGION")]
    [InlineData("State, \"A\"|B\nC", "STATE, \"A\"|B\nC")]
    public void State_is_optional_canonical_and_accepts_custom_labels(string? raw, string expected)
    {
        Assert.Equal(expected, TenderReviewNaming.NormalizeState(raw));
        Assert.Equal(expected, TenderReviewNaming.NormalizeState(expected));
    }

    [Fact]
    public void State_normalisation_does_not_depend_on_current_culture()
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("tr-TR");
            Assert.Equal("VIC", TenderReviewNaming.NormalizeState("victoria"));
            Assert.Equal("CIVIL", TenderReviewNaming.NormalizeState("civil"));
        }
        finally { CultureInfo.CurrentCulture = previous; }
    }

    [Theory]
    [InlineData(null, "")]
    [InlineData("", "")]
    [InlineData(" \t ", "")]
    [InlineData("Queensland", "QLD")]
    [InlineData(" WAIKATO, \"NORTH\" ", "WAIKATO, \"NORTH\"")]
    public async Task Manual_state_is_authoritative_in_manifest_and_02_never_from_xer(string? state, string expected)
    {
        TenderReviewSource source = Source(0, "stage.xer", "2026-09-05");
        TenderReviewBundleRequest request = Request([source]) with { State = state };
        byte[] input = XerWithState("SOURCE STATE MUST NOT WIN");
        byte[] original = input.ToArray();
        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromXerBytesAsync(request, [new() { SourceToken = source.SourceToken, Content = input }]);

        Assert.Equal(original, input);
        Assert.Equal(state, request.State);
        Assert.Equal(11, result.Files.Count);
        Assert.Equal(10, result.ManifestRows.Count);
        Assert.All(result.ManifestRows, row =>
        {
            Assert.Equal("3.0", row.SchemaVersion);
            Assert.Equal(expected, row.ProjectState);
        });
        var manifest = Rows(result.Files[TenderReviewContract.ManifestFileName]);
        Assert.All(manifest, row => Assert.Equal(expected, row["project_state"]));
        Assert.Equal(expected, Assert.Single(Rows(result.Files["02_XER_PROJECT.csv"]))["state"]);
        Assert.DoesNotContain("SOURCE STATE MUST NOT WIN", Encoding.UTF8.GetString(result.Files["02_XER_PROJECT.csv"]));
        XerTable quality = Assert.IsType<XerTable>(result.DataQualityTable);
        var stateWarnings = quality.Rows.Where(row =>
            row.Fields[quality.FieldIndexes["issue_code"]] == "TENDER_PROJECT_STATE_UNKNOWN").ToArray();
        if (expected.Length == 0)
        {
            DataRow warning = Assert.Single(stateWarnings);
            Assert.Equal("02_XER_PROJECT", warning.Fields[quality.FieldIndexes["table_name"]]);
            Assert.Equal("project_state", warning.Fields[quality.FieldIndexes["column_name"]]);
            Assert.Contains("State-based access will not match", warning.Fields[quality.FieldIndexes["message"]]);
            Assert.DoesNotContain(source.SourceToken, warning.Fields[quality.FieldIndexes["message"]]);
        }
        else Assert.Empty(stateWarnings);
    }

    [Fact]
    public async Task Omitted_state_is_unknown_without_preventing_export()
    {
        TenderReviewSource source = Source(0, "stage.xer", "2026-09-05");
        var request = new TenderReviewBundleRequest
        {
            ProjectCode = "QAC000623", ProjectName = "NE Part B", Sources = [source],
            ExportedAtUtc = new DateTimeOffset(2026, 9, 5, 1, 2, 3, TimeSpan.Zero)
        };
        Assert.Null(request.State);
        var result = await new TenderReviewBundleService().BuildFromXerBytesAsync(request,
            [new() { SourceToken = source.SourceToken, Content = XerWithState("QLD") }]);
        Assert.All(result.ManifestRows, row => Assert.Equal("", row.ProjectState));
        Assert.Equal("", Assert.Single(Rows(result.Files["02_XER_PROJECT.csv"]))["state"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task State_only_change_preserves_nine_tables_keys_sources_and_calculations(bool reverse)
    {
        TenderReviewSource[] sources = [Source(0, "same.xer", "2026-09-05"), Source(1, "same.xer", "2026-09-06")];
        if (reverse) Array.Reverse(sources);
        byte[] input = XerWithState("NEVER AUTHORITATIVE");
        TenderReviewSourceBytes[] uploads = sources.Select(source =>
            new TenderReviewSourceBytes { SourceToken = source.SourceToken, Content = input }).ToArray();
        var service = new TenderReviewBundleService();
        TenderReviewBundleRequest request = Request(sources) with { State = "Queensland" };
        var qld = await service.BuildFromXerBytesAsync(request, uploads);
        var nsw = await service.BuildFromXerBytesAsync(request with { State = "NSW" }, uploads);
        var alias = await service.BuildFromXerBytesAsync(request with { State = " qld " }, uploads);
        var blank = await service.BuildFromXerBytesAsync(request with { State = null }, uploads);

        Assert.NotEqual(qld.BundleId, nsw.BundleId);
        Assert.NotEqual(qld.BundleId, blank.BundleId);
        Assert.Equal(qld.BundleId, alias.BundleId);
        Assert.Equal(qld.ManifestRows, alias.ManifestRows);
        foreach (string file in qld.Files.Keys)
        {
            Assert.Equal(qld.Files[file], alias.Files[file]);
            if (file is "02_XER_PROJECT.csv" or TenderReviewContract.ManifestFileName) continue;
            Assert.Equal(qld.Files[file], nsw.Files[file]);
            Assert.Equal(qld.Files[file], blank.Files[file]);
            Assert.Equal(qld.CsvSha256ByFile[file], nsw.CsvSha256ByFile[file]);
        }
        Assert.NotEqual(qld.CsvSha256ByFile["02_XER_PROJECT.csv"], nsw.CsvSha256ByFile["02_XER_PROJECT.csv"]);
        var qldProjects = Rows(qld.Files["02_XER_PROJECT.csv"]);
        var nswProjects = Rows(nsw.Files["02_XER_PROJECT.csv"]);
        Assert.Equal(2, qldProjects.Length);
        for (int index = 0; index < qldProjects.Length; index++)
        {
            Assert.Equal("QLD", qldProjects[index]["state"]);
            Assert.Equal("NSW", nswProjects[index]["state"]);
            foreach (string field in qldProjects[index].Keys.Where(field => field != "state"))
                Assert.Equal(qldProjects[index][field], nswProjects[index][field]);
        }
        Assert.Equal(4, Rows(qld.Files["01_XER_TASK.csv"]).Select(row => row["task_id_key"]).Distinct().Count());
        Assert.Equal(sources.Select(source => source.StatusDate),
            qld.ManifestRows.Where(row => row.TableName == "01_XER_TASK").Select(row => row.StatusDate));
        Assert.Single(qld.ManifestRows.Select(row => row.SourceSha256).Distinct());
        XerTable quality = Assert.IsType<XerTable>(blank.DataQualityTable);
        var warnings = quality.Rows.Where(row => row.Fields[quality.FieldIndexes["issue_code"]] == "TENDER_PROJECT_STATE_UNKNOWN").ToArray();
        Assert.Equal(sources.Select(source => source.SourceToken), warnings.Select(row => row.SourceToken));
        Assert.Equal(2, warnings.Length);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("Queensland")]
    [InlineData("Custom, \"North\"|South\nZone")]
    public async Task State_disk_and_web_bytes_exports_match_for_repeated_paths(string? state)
    {
        string temp = Path.Combine(Path.GetTempPath(), "XerToCsvConverter.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(temp);
        try
        {
            string path = Path.Combine(temp, "same.xer");
            byte[] content = XerWithState("IGNORED SOURCE STATE");
            await File.WriteAllBytesAsync(path, content);
            TenderReviewSource[] sources =
            [
                Source(0, "same.xer", "2026-09-05") with { XerFilePath = path },
                Source(1, "same.xer", "2026-09-06") with { XerFilePath = path }
            ];
            TenderReviewBundleRequest request = Request(sources) with { State = state };
            var service = new TenderReviewBundleService();
            var disk = await service.BuildFromXerFilesAsync(request, Path.Combine(temp, "output"));
            var memory = await service.BuildFromXerBytesAsync(request, sources.Select(source =>
                new TenderReviewSourceBytes { SourceToken = source.SourceToken, Content = content }).ToArray());
            Assert.Equal(disk.BundleId, memory.BundleId);
            Assert.Equal(disk.ManifestRows, memory.ManifestRows);
            Assert.Equal(disk.WarningCount, memory.WarningCount);
            Assert.Equal(11, Directory.EnumerateFiles(disk.BundlePath).Count());
            foreach ((string name, byte[] bytes) in memory.Files)
                Assert.Equal(bytes, await File.ReadAllBytesAsync(Path.Combine(disk.BundlePath, name)));
            Assert.Equal(content, await File.ReadAllBytesAsync(path));
        }
        finally { Directory.Delete(temp, recursive: true); }
    }

    [Fact]
    public void State_identity_encoding_distinguishes_delimiters_and_equates_only_canonical_states()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "same.xer", "2026-09-05");
        TenderReviewBundleRequest request = Request([source]);
        string[] states = ["", "A|B", "A\nB", "A\\nB", "A,B", "A\"B", "A B", "A  B", "A::B"];
        var identities = states.Select(state => TenderReviewNaming.Resolve(request with { State = state }, item => item.SourceSha256!).BundleId).ToArray();
        Assert.Equal(states.Length, identities.Distinct().Count());
        string Id(string? state) => TenderReviewNaming.Resolve(request with { State = state }, item => item.SourceSha256!).BundleId;
        Assert.Equal(Id(null), Id(" \t "));
        Assert.Equal(Id("Queensland"), Id(" qld "));
    }

    private static TenderReviewSource Source(int ordinal, string filename, string date) =>
        TenderReviewNamingTests.Source(ordinal, filename, date) with { SourceSha256 = null };

    private static TenderReviewBundleRequest Request(IReadOnlyList<TenderReviewSource> sources) =>
        TenderReviewNamingTests.Request(sources) with { ProjectCode = "QAC000623" };

    private static Dictionary<string, string>[] Rows(byte[] bytes) => ReviewProjectNameBundleTests.ReadCsv(bytes);

    private static byte[] XerWithState(string state)
    {
        string xer = Encoding.UTF8.GetString(ReviewProjectNameBundleTests.MinimalXerBytes("QAC000623"));
        xer = xer.Replace("%F\tproj_id\tlast_recalc_date\tproj_short_name\tadd_date\r\n",
            "%F\tproj_id\tlast_recalc_date\tproj_short_name\tadd_date\tstate\r\n", StringComparison.Ordinal);
        xer = xer.Replace("%R\tP1\t2026-08-31\tQAC000623\t2026-06-02\r\n",
            $"%R\tP1\t2026-08-31\tQAC000623\t2026-06-02\t{state}\r\n", StringComparison.Ordinal);
        return Encoding.UTF8.GetBytes(xer);
    }
}
