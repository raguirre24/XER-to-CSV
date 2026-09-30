using XerToCsvConverter.BrowserProcessing;

namespace XerToCsvConverter.Core.Tests;

public sealed class ParsedSourceCacheAccountingTests
{
    [Fact]
    public void Excess_original_cells_are_charged_after_live_alignment_and_cannot_enter_a_small_cache()
    {
        string identity = "T1";
        string large = new('x', 8000);
        XerDataStore normal = Store(["id"], [identity], 1);
        XerDataStore excess = Store(["id"], [identity, large, "extra"], 1);
        DataRow row = excess.GetTable("RAW")!.Rows.Single();
        Assert.Equal(new[] { identity }, row.Fields);
        Assert.Equal(normal.GetTable("RAW")!.Rows[0].EstimatedRetainedBytes
            + 2 * 8L + 24 + large.Length * 2L + 24 + "extra".Length * 2L, row.EstimatedRetainedBytes);
        long budget = ParsedSourceCache.EstimateBytes(normal) + 500;
        var cache = new ParsedSourceCache(budget);
        cache.Retain("normal", "normal-hash", normal, default);
        cache.Retain("excess", "excess-hash", excess, default);
        Assert.Single(row.Fields);
        Assert.Equal(1, cache.Count);
        Assert.True(cache.TryGet("normal", "normal-hash", out _));
        Assert.False(cache.TryGet("excess", "excess-hash", out _));
        Assert.InRange(cache.RetainedBytes, 0, budget);
    }

    [Fact]
    public void Original_and_live_array_strings_are_charged_once_only_when_the_same_reference_is_shared()
    {
        string original = new('x', 100);
        var source = new DataRow([original], "source.xer");
        DataRow sameReference = source.WithFields([original]);
        DataRow sameText = source.WithFields([new string(original.ToCharArray())]);
        Assert.Equal(source.EstimatedRetainedBytes, sameReference.EstimatedRetainedBytes);
        Assert.Equal(source.EstimatedRetainedBytes + 24 + original.Length * 2L, sameText.EstimatedRetainedBytes);
        DataRow extended = source.WithFields([original, "live extension"]);
        Assert.Equal(source.EstimatedRetainedBytes + 8 + 24 + "live extension".Length * 2L,
            extended.EstimatedRetainedBytes);
        Assert.Equal(new[] { original }, source.Fields);
        Assert.True(default(DataRow).EstimatedRetainedBytes > 0);
    }

    [Fact]
    public void Long_header_strings_are_charged_and_oversized_schema_is_not_retained()
    {
        XerDataStore normal = Store(["a"], [], 1);
        string longHeader = new('h', 12000);
        XerDataStore longSchema = Store([longHeader], [], 1);
        long normalSize = ParsedSourceCache.EstimateBytes(normal);
        Assert.Equal(normalSize + (longHeader.Length - 1) * 2L, ParsedSourceCache.EstimateBytes(longSchema));
        var cache = new ParsedSourceCache(normalSize + 100);
        cache.Retain("long", "hash", longSchema, default);
        Assert.Equal(0, cache.Count);
        Assert.Equal(0, cache.RetainedBytes);
        Assert.Equal(longHeader, longSchema.GetTable("RAW")!.Headers!.Single());
    }

    [Fact]
    public void Empty_table_row_capacity_is_charged_and_budget_reduction_evicts_it()
    {
        XerDataStore small = Store([], null, 1);
        XerDataStore spacious = Store([], null, 1000);
        Assert.Empty(spacious.GetTable("RAW")!.Rows);
        Assert.Equal(1000, spacious.GetTable("RAW")!.Capacity);
        long smallSize = ParsedSourceCache.EstimateBytes(small);
        long spaciousSize = ParsedSourceCache.EstimateBytes(spacious);
        Assert.Equal(smallSize + 999 * 64L, spaciousSize);
        var cache = new ParsedSourceCache(spaciousSize);
        cache.Retain("empty", "hash", spacious, default);
        Assert.Equal(1, cache.Count);
        cache.SetBudget(spaciousSize - 1);
        Assert.Equal(0, cache.Count);
        cache.Retain("empty", "hash", spacious, default);
        Assert.Equal(0, cache.Count);
        Assert.Equal(0, cache.RetainedBytes);
    }

    private static XerDataStore Store(string[] headers, string[]? values, int capacity)
    {
        var table = new XerTable("RAW", capacity);
        table.SetHeaders(headers);
        if (values is not null) table.AddRow(new DataRow(values, "source.xer"));
        var store = new XerDataStore();
        store.AddTable(table);
        return store;
    }
}
