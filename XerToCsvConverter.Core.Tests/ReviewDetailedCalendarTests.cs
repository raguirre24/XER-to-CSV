using System.Globalization;
using System.Text;
using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ReviewDetailedCalendarTests
{
    internal const string Headers = "clndr_name,clndr_type,date,day_of_week,working_day,work_hours,exception_type,clndr_id_key,MonthUpdate,day_of_week_num,working_day_int";
    private const string TableName = "11_XER_CALENDAR_DETAILED";
    private const string FileName = TableName + ".csv";
    private const string Original = "2501-not-the-governed-date.xer";
    private const string ProjectCode = "J5001";

    [Fact]
    public void Contracts_preserve_exact_case_nullable_rules_and_calendar_detail_grain()
    {
        ProgrammeReviewTableContract programme = ProgrammeReviewContract.GetTable(TableName);
        TenderReviewTableContract tender = TenderReviewContract.GetTable(TableName);
        Assert.Equal(Headers, string.Join(',', programme.Columns.Select(column => column.Name)));
        Assert.Equal(Headers, string.Join(',', tender.Columns.Select(column => column.Name)));
        Assert.Empty(programme.KeyColumns);
        Assert.Empty(tender.KeyColumns);
        Assert.Equal(EnhancedTableNames.XerCalendarDetailed11, programme.EnhancedTableName);
        Assert.Equal(EnhancedTableNames.XerCalendarDetailed11, tender.EnhancedTableName);
        foreach (string column in new[] { "date", "clndr_id_key", "work_hours", "working_day", "working_day_int", "day_of_week_num" })
        {
            Assert.True(programme.Columns.Single(c => c.Name == column).Nullable);
            Assert.True(tender.Columns.Single(c => c.Name == column).Nullable);
        }
        Assert.Equal(ProgrammeReviewColumnType.Text, programme.Columns.Single(c => c.Name == "working_day").Type);
        Assert.Equal(TenderReviewColumnType.Integer, tender.Columns.Single(c => c.Name == "working_day_int").Type);
        Assert.Equal(ProgrammeReviewColumnType.Number, programme.Columns.Single(c => c.Name == "work_hours").Type);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Review_projection_reuses_split_shifts_inheritance_exception_hours_and_correct_governed_date(bool tender)
    {
        string parent = WithExceptions(P6TestCalendars.WorkWeek(), Exception(new(2026, 9, 7)));
        string child = WithExceptions(P6TestCalendars.WorkWeek(),
            Exception(new(2026, 9, 6), "08:00", "12:00"), Exception(new(2026, 9, 8), "08:00", "08:01"));
        byte[] input = Xer(new("C0", parent), new("C1", child, "10", "C0"));
        Bundle output = await Build(tender, input);
        await AssertMatchesEnhanced(output, input, tender);

        var rows = Rows(output.Files[FileName]);
        var c1 = rows.Where(row => row["clndr_id_key"].EndsWith("::C1", StringComparison.Ordinal)).ToArray();
        Assert.Equal(10, c1.Length);
        Assert.Equal(7, c1.Count(row => row["date"] == ""));
        Assert.Equal("8", Assert.Single(c1, row => row["date"] == "" && row["day_of_week"] == "Monday")["work_hours"]);
        Assert.Equal("4", Assert.Single(c1, row => row["date"] == "2026-09-06")["work_hours"]);
        Assert.Equal("0", Assert.Single(c1, row => row["date"] == "2026-09-07")["work_hours"]);
        Assert.Equal(1m / 60m, decimal.Parse(Assert.Single(c1, row => row["date"] == "2026-09-08")["work_hours"], CultureInfo.InvariantCulture));
        Assert.All(rows, row => Assert.Contains(row["working_day"], new[] { "Y", "N" }));
        Assert.All(rows, row => Assert.Contains(row["working_day_int"], new[] { "1", "0" }));
        Assert.All(rows, row => Assert.Equal(tender ? "2026-08-31" : "2026-07-01", row["MonthUpdate"]));
        Assert.All(output.Manifest.Where(row => row["table_name"] == TableName), row =>
        {
            Assert.Equal(rows.Length.ToString(CultureInfo.InvariantCulture), row["row_count"]);
            Assert.Equal(ProgrammeReviewCsv.ComputeSha256(output.Files[FileName]), row["csv_sha256"]);
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Missing_calendar_produces_header_only_11_without_aborting_bundle(bool tender)
    {
        Bundle output = await Build(tender, Xer());
        Assert.Equal(12, output.Files.Count);
        Assert.Equal(Headers + "\r\n", Encoding.UTF8.GetString(output.Files[FileName]));
        Assert.Equal("0", Assert.Single(output.Manifest, row => row["table_name"] == TableName)["row_count"]);
        Assert.Contains(output.Quality.Rows, row => Cell(output.Quality, row, "table_name") == TableName);
    }

    [Theory]
    [InlineData(false, "duplicate")]
    [InlineData(true, "duplicate")]
    [InlineData(false, "blank")]
    [InlineData(true, "blank")]
    [InlineData(false, "inheritance")]
    [InlineData(true, "inheritance")]
    [InlineData(false, "invalid-date-shift")]
    [InlineData(true, "invalid-date-shift")]
    [InlineData(false, "invalid-week")]
    [InlineData(true, "invalid-week")]
    public async Task Invalid_calendar_rows_and_unknown_flags_remain_available_with_source_evidence(bool tender, string defect)
    {
        Calendar[] calendars = defect switch
        {
            "duplicate" => [new("C1", P6TestCalendars.WorkWeek()), new("C1", P6TestCalendars.WorkWeek("10"))],
            "blank" => [new("", P6TestCalendars.WorkWeek())],
            "inheritance" => [new("C1", P6TestCalendars.WorkWeek(), Parent: "absent")],
            "invalid-date-shift" => [new("C1", WithExceptions(P6TestCalendars.WorkWeek(), Exception(new(2026, 9, 7), "27:00", "28:00")))],
            _ => [new("C1", "not a valid calendar")]
        };
        byte[] input = Xer(calendars);
        Bundle output = await Build(tender, input);
        await AssertMatchesEnhanced(output, input, tender);
        var rows = Rows(output.Files[FileName]);
        Assert.Equal(12, output.Files.Count);
        Assert.Contains(rows, row => row["work_hours"] == "" && row["working_day"] == "" && row["working_day_int"] == "");
        if (defect is "duplicate" or "blank")
        {
            Assert.Equal(calendars.Length * 7, rows.Length);
            Assert.All(rows, row => Assert.Equal("", row["clndr_id_key"]));
        }
        if (defect == "inheritance")
            Assert.Contains(rows, row => row["date"] == "" && row["day_of_week"] == "" && row["exception_type"] == "Exception - Invalid");
        var issues = output.Quality.Rows.Where(row => Cell(output.Quality, row, "table_name") == TableName).ToArray();
        Assert.NotEmpty(issues);
        Assert.All(issues, row => Assert.Equal("CALENDAR", Cell(output.Quality, row, "source_table")));
        Assert.All(issues, row => Assert.DoesNotContain("tender-source", Cell(output.Quality, row, "raw_row_json"), StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Repeated_Tender_names_native_ids_and_hashes_keep_independent_stage_rules(bool reverse, bool sameContent)
    {
        var sources = new[] { Source(0, "2026-09-05"), Source(1, "2026-09-06") };
        if (reverse) Array.Reverse(sources);
        var uploads = sources.Select(source => new TenderReviewSourceBytes
        {
            SourceToken = source.SourceToken,
            Content = Xer(new Calendar("C1", P6TestCalendars.WorkWeek(sameContent || source.StatusDate.Day == 5 ? "10" : "6")))
        }).ToArray();
        var service = new TenderReviewBundleService();
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(sources);
        var result = await service.BuildFromXerBytesAsync(request, uploads);
        var repeat = await service.BuildFromXerBytesAsync(request, uploads);
        Assert.Equal(result.Files[FileName], repeat.Files[FileName]);
        Assert.Equal(22, result.ManifestRows.Count);
        Assert.Equal(sources.Select(source => source.StatusDate), result.ManifestRows.Where(row => row.TableName == TableName).Select(row => row.StatusDate));
        Assert.All(result.ManifestRows.Where(row => row.TableName == TableName), row => Assert.Equal(7, row.RowCount));
        var rows = Rows(result.Files[FileName]);
        Assert.Equal(14, rows.Length);
        foreach (TenderReviewSource source in sources)
        {
            string key = $"CSV::J5001::TENDER::{source.StatusDate:yyyyMMdd}::C1";
            var monday = Assert.Single(rows, row => row["clndr_id_key"] == key && row["day_of_week"] == "Monday");
            Assert.Equal(sameContent || source.StatusDate.Day == 5 ? "10" : "6", monday["work_hours"]);
            Assert.Equal("2026-08-31", monday["MonthUpdate"]);
            Assert.DoesNotContain(source.SourceToken, Encoding.UTF8.GetString(result.Files[FileName]), StringComparison.Ordinal);
        }
        Assert.Equal(sameContent ? 1 : 2, result.ManifestRows.Select(row => row.SourceSha256).Distinct().Count());
    }

    [Fact]
    public async Task Programme_retains_only_governed_snapshots_before_detailed_calendar_generation()
    {
        ProgrammeReviewSnapshot[] snapshots =
        [
            Snapshot("old.xer", "BL01", "2026-06-01"),
            Snapshot("baseline.xer", "BL02", "2026-07-01"),
            Snapshot("update.xer", "2608", "2026-08-01", ProgrammeReviewSnapshotKind.Update)
        ];
        var bytes = new Dictionary<string, byte[]>
        {
            ["old.xer"] = Xer(new Calendar("C1", "invalid discarded calendar")),
            ["baseline.xer"] = Xer(new Calendar("C1", P6TestCalendars.WorkWeek("10"))),
            ["update.xer"] = Xer(new Calendar("C1", P6TestCalendars.WorkWeek("6")))
        };
        var result = await new ProgrammeReviewBundleService().BuildFromXerBytesAsync(
            ProgrammeReviewNamingTests.Request(snapshots) with { ProjectCode = ProjectCode }, bytes);
        Assert.Equal(22, result.ManifestRows.Count);
        var rows = Rows(result.Files[FileName]);
        Assert.Equal(14, rows.Length);
        Assert.Equal(new[] { "2026-07-01", "2026-08-01" }, rows.Select(row => row["MonthUpdate"]).Distinct().Order(StringComparer.Ordinal));
        Assert.DoesNotContain(Assert.IsType<XerTable>(result.DataQualityTable).Rows, row => row.OriginalSourceFilename == "old.xer");
        Assert.Equal("10", Assert.Single(rows, row => row["clndr_id_key"] == "CSV::J5001::C::BL02::C1" && row["day_of_week"] == "Monday")["work_hours"]);
        Assert.Equal("6", Assert.Single(rows, row => row["clndr_id_key"] == "CSV::J5001::C::2608::C1" && row["day_of_week"] == "Monday")["work_hours"]);
    }

    private static async Task AssertMatchesEnhanced(Bundle output, byte[] input, bool tender)
    {
        using var stream = new MemoryStream(input, writable: false);
        XerDataStore store = await new ProcessingService().ParseXerStreamsAsync(
            new[] { ((Stream)stream, Original) }, null, CancellationToken.None);
        XerTable detailed = Assert.IsType<XerTable>(new XerTransformer(store).Create11XerCalendarDetailed());
        string prefix = tender ? "CSV::J5001::TENDER::20260905::" : "CSV::J5001::C::BL01::";
        string Canonical(IEnumerable<string> fields) => string.Join('\u001f', fields);
        string[] expected = detailed.Rows.Select(row => Canonical(Headers.Split(',').Select(column => column switch
        {
            "MonthUpdate" => tender ? "2026-08-31" : "2026-07-01",
            "clndr_id_key" => Cell(detailed, row, column) is "" ? "" : prefix + Cell(detailed, row, "clndr_id"),
            _ => Cell(detailed, row, column)
        }))).Order(StringComparer.Ordinal).ToArray();
        string[] actual = Rows(output.Files[FileName]).Select(row => Canonical(Headers.Split(',').Select(column => row[column])))
            .Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(expected, actual);
    }

    private static async Task<Bundle> Build(bool tender, byte[] input)
    {
        if (tender)
        {
            var source = Source(0, "2026-09-05");
            var result = await new TenderReviewBundleService().BuildFromXerBytesAsync(
                TenderReviewNamingTests.Request([source]), [new() { SourceToken = source.SourceToken, Content = input }]);
            return new(result.Files, Rows(result.Files[TenderReviewContract.ManifestFileName]), Assert.IsType<XerTable>(result.DataQualityTable));
        }
        var snapshot = Snapshot(Original, "BL01", "2026-07-01");
        var programme = await new ProgrammeReviewBundleService().BuildFromXerBytesAsync(
            ProgrammeReviewNamingTests.Request([snapshot]) with { ProjectCode = ProjectCode }, new Dictionary<string, byte[]> { [Original] = input });
        return new(programme.Files, Rows(programme.Files[ProgrammeReviewContract.ManifestFileName]), Assert.IsType<XerTable>(programme.DataQualityTable));
    }

    private static TenderReviewSource Source(int index, string statusDate) =>
        TenderReviewNamingTests.Source(index, Original, statusDate) with { SourceSha256 = null };
    private static ProgrammeReviewSnapshot Snapshot(string file, string tag, string month,
        ProgrammeReviewSnapshotKind kind = ProgrammeReviewSnapshotKind.Baseline) =>
        ProgrammeReviewNamingTests.Snapshot(file, kind, tag, month, "2026-08-31") with { SourceSha256 = null };
    private static Dictionary<string, string>[] Rows(byte[] bytes) => ReviewProjectNameBundleTests.ReadCsv(bytes);
    private static string Cell(XerTable table, DataRow row, string column) => row.Fields[table.FieldIndexes[column]];
    private static byte[] Xer(params Calendar[] calendars)
    {
        string original = Encoding.UTF8.GetString(ReviewProjectNameBundleTests.MinimalXerBytes(ProjectCode));
        int start = original.IndexOf("%T\tCALENDAR\r\n", StringComparison.Ordinal);
        int end = original.IndexOf("%T\tTASKPRED\r\n", start, StringComparison.Ordinal);
        string replacement = calendars.Length == 0 ? "" : "%T\tCALENDAR\r\n%F\tclndr_id\tclndr_name\tday_hr_cnt\tclndr_type\tclndr_data\tbase_clndr_id\r\n"
            + string.Concat(calendars.Select(calendar => $"%R\t{calendar.Id}\tCalendar {calendar.Id}\t{calendar.Hours}\tCA_Project\t{calendar.Data}\t{calendar.Parent}\r\n"));
        return Encoding.UTF8.GetBytes(original[..start] + replacement + original[end..]);
    }
    private static string WithExceptions(string week, params string[] exceptions) =>
        week[..^2] + "(0||Exceptions()(" + string.Concat(exceptions) + "))))";
    private static string Exception(DateTime date, string? start = null, string? finish = null) =>
        "(0||0(d|" + (date - new DateTime(1899, 12, 30)).Days.ToString(CultureInfo.InvariantCulture) + ")(" +
        (start is null ? "" : "(0||0(s|" + start + "|f|" + finish + ")())") + "))";
    private sealed record Calendar(string Id, string Data, string Hours = "8", string Parent = "");
    private sealed record Bundle(IReadOnlyDictionary<string, byte[]> Files, Dictionary<string, string>[] Manifest, XerTable Quality);
}
