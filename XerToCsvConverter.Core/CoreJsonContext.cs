using System.Text.Json;
using System.Text.Json.Serialization;

namespace XerToCsvConverter;

// Explicit types keep the historical property names/order and make all three
// export serializers available when browser trimming disables reflection.
internal sealed record RawRowField(
    [property: JsonPropertyName("column"), JsonPropertyOrder(0)] string Column,
    [property: JsonPropertyName("value"), JsonPropertyOrder(1)] string? Value,
    [property: JsonPropertyName("presence"), JsonPropertyOrder(2)] string Presence);

internal sealed record TenderBundleIdentity(
    [property: JsonPropertyOrder(0)] string? SchemaVersion,
    [property: JsonPropertyOrder(1)] string? ProjectCode,
    [property: JsonPropertyOrder(2)] string? ProjectState,
    [property: JsonPropertyOrder(3)] IEnumerable<TenderSourceIdentity> Sources);

internal sealed record TenderSourceIdentity(
    [property: JsonPropertyOrder(0)] int InputIndex,
    [property: JsonPropertyOrder(1)] string? OriginalXerFilename,
    [property: JsonPropertyOrder(2)] string? CanonicalXerFilename,
    [property: JsonPropertyOrder(3)] string? StatusDate,
    [property: JsonPropertyOrder(4)] string? SourceSha256);

[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(IEnumerable<RawRowField>), TypeInfoPropertyName = "RawRowFields")]
[JsonSerializable(typeof(TenderBundleIdentity))]
[JsonSerializable(typeof(Dictionary<string, RelationshipFieldEvidence>), TypeInfoPropertyName = "RelationshipEvidence")]
internal partial class CoreJsonContext : JsonSerializerContext
{
    internal static CoreJsonContext Audit { get; } = new(new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    });
}
