using System.Diagnostics.CodeAnalysis;

namespace XerToCsvConverter.Core.Tests;

// Evaluation oracle frozen from 90b98d3. Calendar clocks use the separately
// differential-tested public calendar implementation; this oracle isolates the
// relationship field/options snapshot optimisation and retains full evidence.
internal sealed partial class FrozenRelationshipAssessmentOracle(XerDataStore store)
{
    private readonly XerDataStore _dataStore = store;
    private static bool IsTableValid([NotNullWhen(true)] XerTable? table) => table is { IsEmpty: false, Headers: not null };
    private static string CreateKey(string filename, string value) => string.IsNullOrEmpty(filename) || string.IsNullOrEmpty(value)
        ? string.Empty : StringInternPool.Intern($"{filename.Trim()}.{value.Trim()}");
    private static string GetFieldValue(string[] row, IReadOnlyDictionary<string, int> indexes, string field) =>
        indexes.TryGetValue(field, out int index) && index >= 0 && index < row.Length ? row[index] ?? "" : "";
    private static readonly WorkingDayCalculator ContinuousRelationshipCalendar = new(
        new(), new(), Enumerable.Repeat(24m, 7).ToArray(),
        Enumerable.Range(0, 7).Select(_ => new List<(TimeSpan, TimeSpan)>
            { (TimeSpan.Zero, TimeSpan.FromDays(1)) }).ToArray());
}