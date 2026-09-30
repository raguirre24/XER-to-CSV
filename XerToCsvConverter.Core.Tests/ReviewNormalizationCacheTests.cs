using System.Globalization;
using System.Text.Json;
using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ReviewNormalizationCacheTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Successful_conversions_are_shared_across_columns_but_separated_by_type(bool tender)
    {
        var cache = new ReviewValueNormalizationCache();
        string first = Normalize(tender, " 001.2300 ", ProgrammeReviewColumnType.Number, "quantity", false, "row1", cache);
        string second = Normalize(tender, "001.2300", ProgrammeReviewColumnType.Number, "duration", false, "row2", cache);
        Assert.Equal("1.23", first);
        Assert.Same(first, second);
        Assert.Equal(1, cache.Count);
        Assert.True(cache.TryGet((int)ProgrammeReviewColumnType.Number, "001.2300", out string saved));
        Assert.Same(first, saved);
        Assert.Equal("1", Normalize(tender, "1", ProgrammeReviewColumnType.Integer, "count", true, "row3", cache));
        Assert.Equal("1", Normalize(tender, "1", ProgrammeReviewColumnType.Number, "number", true, "row3", cache));
        Assert.Equal("true", Normalize(tender, "1", ProgrammeReviewColumnType.Boolean, "flag", true, "row3", cache));
        Assert.Equal(4, cache.Count);
        Assert.Equal("  untouched  ", Normalize(tender, "  untouched  ", ProgrammeReviewColumnType.Text, "text", true, "row4", cache));
        Assert.Equal(4, cache.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Blank_missing_and_required_checks_run_before_cache_hits(bool tender)
    {
        var cache = new ReviewValueNormalizationCache();
        cache.Add((int)ProgrammeReviewColumnType.Number, "", "sentinel");
        cache.Add((int)ProgrammeReviewColumnType.Text, "", "sentinel");
        foreach (string? raw in new string?[] { null, "", " \t\r\n" })
        foreach (ProgrammeReviewColumnType type in new[] { ProgrammeReviewColumnType.Number, ProgrammeReviewColumnType.Text })
        {
            Assert.Equal(type == ProgrammeReviewColumnType.Text ? raw ?? "" : "",
                Normalize(tender, raw, type, "optional", true, "optional-row", cache));
            Exception error = Assert.ThrowsAny<Exception>(() => Normalize(tender, raw, type, "required", false, "required-row", cache));
            Assert.Contains("required-row", error.Message, StringComparison.Ordinal);
            Assert.Contains("required column 'required' is blank", error.Message, StringComparison.Ordinal);
        }
        Assert.Equal(2, cache.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Failed_overflow_and_type_mismatches_keep_each_rows_context(bool tender)
    {
        var cache = new ReviewValueNormalizationCache();
        Assert.Equal("1", Normalize(tender, "01", ProgrammeReviewColumnType.Number, "valid", true, "first", cache));
        foreach ((ProgrammeReviewColumnType type, string raw) in new[]
        {
            (ProgrammeReviewColumnType.Number, "1e309"),
            (ProgrammeReviewColumnType.Number, "NaN"),
            (ProgrammeReviewColumnType.Integer, "79228162514264337593543950336"),
            (ProgrammeReviewColumnType.Boolean, "01"),
            (ProgrammeReviewColumnType.Date, "2026-02-30")
        })
        {
            foreach (string context in new[] { "sourceA/row1", "sourceB/row2" })
            {
                Exception expected = Assert.ThrowsAny<Exception>(() => Normalize(tender, raw, type, "field-" + context, true, context, null));
                Exception actual = Assert.ThrowsAny<Exception>(() => Normalize(tender, raw, type, "field-" + context, true, context, cache));
                Assert.Equal(expected.GetType(), actual.GetType());
                Assert.Equal(expected.Message, actual.Message);
                Assert.Contains(context, actual.Message, StringComparison.Ordinal);
            }
            Assert.False(cache.TryGet((int)type, raw, out _));
        }
        Assert.Equal(1, cache.Count);
        Assert.Equal(Normalize(tender, "1e308", ProgrammeReviewColumnType.Number, "large", true, "uncached", null),
            Normalize(tender, "1e308", ProgrammeReviewColumnType.Number, "large", true, "cached", cache));
        Assert.Equal(2, cache.Count); // Finite doubles retain the established fallback beyond decimal range.
    }

    [Theory]
    [InlineData(false, ProgrammeReviewColumnType.Number, "bad-number", "1")]
    [InlineData(true, ProgrammeReviewColumnType.Number, "bad-number", "1")]
    [InlineData(false, ProgrammeReviewColumnType.Date, "bad-date", "2026-09-30")]
    [InlineData(true, ProgrammeReviewColumnType.Date, "bad-date", "2026-09-30")]
    public void Memoisation_preserves_full_multiline_diagnostics_and_raw_presence(bool tender,
        ProgrammeReviewColumnType type, string invalid, string valid)
    {
        ReviewDataQualityCollector expected = Collect(cache: null);
        var cache = new ReviewValueNormalizationCache();
        ReviewDataQualityCollector actual = Collect(cache);
        Assert.Equal(expected.Rows.Count, actual.Rows.Count);
        Assert.Equal(5, actual.Rows.Count);
        for (int index = 0; index < actual.Rows.Count; index++)
            Assert.Equal(expected.Rows[index].Fields, actual.Rows[index].Fields);
        Assert.Equal(1, cache.Count);
        int jsonIndex = Array.IndexOf(XerDataQuality.Columns.ToArray(), "raw_row_json");
        using var missingJson = JsonDocument.Parse(actual.Rows[2].Fields[jsonIndex]);
        using var blankJson = JsonDocument.Parse(actual.Rows[3].Fields[jsonIndex]);
        Assert.NotEqual(ValuePresence(missingJson), ValuePresence(blankJson));

        ReviewDataQualityCollector Collect(ReviewValueNormalizationCache? cache)
        {
            var source = new XerTable("TASK");
            source.SetHeaders(["task_id", "value"]);
            source.AddRow(new DataRow(["invalid1", invalid], "source.xer", "internal-only", "source.xer"));
            source.AddRow(new DataRow(["invalid2", invalid], "source.xer", "internal-only", "source.xer"));
            source.AddRow(new DataRow(["missing"], "source.xer", "internal-only", "source.xer"));
            source.AddRow(new DataRow(["blank", ""], "source.xer", "internal-only", "source.xer"));
            source.AddRow(new DataRow(["overflow", valid], "source.xer", "internal-only", "source.xer"));
            source.AddRow(new DataRow(["valid1", valid], "source.xer", "internal-only", "source.xer"));
            source.AddRow(new DataRow(["valid2", valid], "source.xer", "internal-only", "source.xer"));
            var store = new XerDataStore(); store.AddTable(source);
            var collector = new ReviewDataQualityCollector(store);
            for (int rowIndex = 0; rowIndex < source.Rows.Count; rowIndex++)
            {
                DataRow row = source.Rows[rowIndex];
                string context = $"source.xer/row{rowIndex + 1}\r\ncontext";
                string raw = row.GetRawField("value").RawValue;
                _ = collector.Evaluate(collector.ForRow(source, row), "value", raw, () =>
                    row.Fields[0] == "overflow" ? throw new OverflowException(context + ": arithmetic overflow") :
                        Normalize(tender, raw, type, "value", false, context, cache));
            }
            return collector;
        }
        static string ValuePresence(JsonDocument json) => json.RootElement.EnumerateArray()
            .Single(field => field.GetProperty("column").GetString() == "value").GetProperty("presence").GetString()!;
    }

    [Fact]
    public void Operation_memo_stops_admitting_values_at_entry_and_byte_limits()
    {
        var entries = new ReviewValueNormalizationCache();
        for (int index = 0; index < 9000; index++) entries.Add(2, index.ToString(CultureInfo.InvariantCulture), "value");
        Assert.Equal(8192, entries.Count);
        Assert.True(entries.TryGet(2, "0", out _));
        Assert.False(entries.TryGet(2, "8999", out _));

        var bytes = new ReviewValueNormalizationCache();
        string veryLarge = new('x', 1_100_000);
        bytes.Add(2, veryLarge, "value");
        Assert.Equal(0, bytes.Count);
        string large = new('y', 500_000);
        bytes.Add(2, "a" + large, "value"); bytes.Add(2, "b" + large, "value"); bytes.Add(2, "c" + large, "value");
        Assert.Equal(2, bytes.Count);
        Assert.False(bytes.TryGet(2, "c" + large, out _));
        var nextOperation = new ReviewValueNormalizationCache();
        Assert.Equal(0, nextOperation.Count);
    }

    [Theory]
    [InlineData(DateTimeKind.Unspecified)]
    [InlineData(DateTimeKind.Utc)]
    [InlineData(DateTimeKind.Local)]
    public void Formatted_dates_preserve_null_min_kind_seconds_and_culture_after_clear(DateTimeKind kind)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("th-TH");
            DateParser.ClearCache();
            Assert.Equal("", DateParser.Format((DateTime?)null));
            Assert.Equal("", DateParser.Format(DateTime.SpecifyKind(DateTime.MinValue, kind)));
            foreach (DateTime raw in new[] { DateTime.MinValue.AddTicks(1), new DateTime(2026, 9, 30, 13, 24, 59).AddTicks(1234567), DateTime.MaxValue })
            {
                DateTime date = DateTime.SpecifyKind(raw, kind);
                string expected = date.ToString(DateParser.OutputFormat, CultureInfo.InvariantCulture);
                Assert.Equal(expected, DateParser.Format(date));
                Assert.Equal(expected, DateParser.Format((DateTime?)date));
                Assert.Equal(expected, DateParser.Format(DateTime.SpecifyKind(date, DateTimeKind.Utc)));
                DateParser.ClearCache();
                Assert.Equal(expected, DateParser.Format(date));
            }
        }
        finally { DateParser.ClearCache(); CultureInfo.CurrentCulture = previous; }
    }

    private static string Normalize(bool tender, string? raw, ProgrammeReviewColumnType type,
        string column, bool nullable, string context, ReviewValueNormalizationCache? cache) => tender
        ? TenderReviewCsv.Normalize(raw, new TenderReviewColumn(column, (TenderReviewColumnType)type, nullable), context, cache)
        : ProgrammeReviewCsv.Normalize(raw, new ProgrammeReviewColumn(column, type, nullable), context, cache);
}
