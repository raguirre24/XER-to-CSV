using System.Text;
using System.Text.Json;

namespace XerToCsvConverter.Core.Tests;

public sealed class CoreJsonParityTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Raw_row_generated_json_matches_frozen_anonymous_projection(bool truncateCurrentFields)
    {
        var table = new XerTable("TASK");
        table.SetHeaders(["id", "Unicode-é-中", "controls\u001f", "null_cell", "omitted"]);
        table.AddRow(new DataRow(["T1", "😀 / \\ \"", "\r\n\t\0", null!], "source.xer"));
        DataRow row = table.Rows[0];
        if (truncateCurrentFields) row = row.WithFields(row.Fields.Take(2).ToArray());
        string frozen = JsonSerializer.Serialize((table.Headers ?? []).Select((name, index) => new
        {
            column = name,
            value = index < row.Fields.Length ? row.Fields[index] : null,
            presence = row.GetRawField(name).State.ToString()
        }));
        Assert.Equal(Encoding.UTF8.GetBytes(frozen), Encoding.UTF8.GetBytes(XerDataQuality.RawRowJson(table, row)));
        Assert.Equal("[]", JsonSerializer.Serialize(Array.Empty<RawRowField>(), CoreJsonContext.Default.RawRowFields));
    }

    [Fact]
    public void Tender_identity_generated_json_preserves_frozen_names_order_and_nulls()
    {
        var sources = new[]
        {
            new TenderSourceIdentity(0, "repeated-é\u001f.xer", "CSV-中.xer", "2026-09-01", new string('a', 64)),
            new TenderSourceIdentity(1, "\r\n\"\\.xer", "", null, null)
        };
        var identity = new TenderBundleIdentity("1.2", "TEST 😀", null, sources);
        string frozen = JsonSerializer.Serialize(new
        {
            identity.SchemaVersion,
            identity.ProjectCode,
            identity.ProjectState,
            Sources = sources.Select(source => new
            {
                source.InputIndex,
                source.OriginalXerFilename,
                source.CanonicalXerFilename,
                source.StatusDate,
                source.SourceSha256
            })
        });
        Assert.Equal(Encoding.UTF8.GetBytes(frozen), Encoding.UTF8.GetBytes(
            JsonSerializer.Serialize(identity, CoreJsonContext.Default.TenderBundleIdentity)));
        var empty = identity with { Sources = Array.Empty<TenderSourceIdentity>() };
        Assert.EndsWith("\"Sources\":[]}", JsonSerializer.Serialize(empty, CoreJsonContext.Default.TenderBundleIdentity), StringComparison.Ordinal);
    }

    [Fact]
    public void Relationship_generated_json_matches_frozen_snake_case_and_optional_evaluation_fields()
    {
        var evidence = new Dictionary<string, RelationshipFieldEvidence>(StringComparer.Ordinal)
        {
            ["z.é_中\u001f"] = new("\"\\\r\n\0😀", "Present")
                { EvaluationValue = "", EvaluationState = "Blank" },
            ["a.source"] = new("", "AbsentHeader"),
            ["middle"] = new(null!, null!) { EvaluationValue = "changed", EvaluationState = null }
        };
        var ordered = evidence.OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };
        string frozen = JsonSerializer.Serialize(ordered, options);
        string generated = JsonSerializer.Serialize(ordered, CoreJsonContext.Audit.RelationshipEvidence);
        Assert.Equal(Encoding.UTF8.GetBytes(frozen), Encoding.UTF8.GetBytes(generated));
        Assert.Equal("{}", JsonSerializer.Serialize(new Dictionary<string, RelationshipFieldEvidence>(),
            CoreJsonContext.Audit.RelationshipEvidence));
    }
}
