using System.Collections.ObjectModel;
using System.Globalization;
using System.Text.Json.Serialization;

namespace XerToCsvConverter;

public enum RelationshipFloatClassification
{
    Calculated, Ignored, Historical, Unsupported, MissingData, InvalidData
}

/// <summary>Meaning of the relationship allowance, independent of the legacy audit classification.</summary>
public enum RelationshipAllowanceStatus
{
    Finite, Estimated, NoFiniteBound, Historical, FixedEvent, RequiresContext, MissingData, InvalidData
}

/// <summary>Original input evidence; State also distinguishes malformed typed values.</summary>
public sealed record RelationshipFieldEvidence(string RawValue, string State)
{
    // Parsed-data callers can edit existing cells. Keep original evidence distinct
    // from the supplied evaluation value without repeating identical evidence.
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? EvaluationValue { get; init; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? EvaluationState { get; init; }
}

/// <summary>
/// A relationship-only remaining predecessor allowance, evaluated under exported settings.
/// This is neither a reschedule nor a native P6 driving classification. No internal token
/// is exposed; public namespace and the source-local row ordinal identify an occurrence.
/// </summary>
public sealed record RelationshipFloatAssessment
{
    public string FileName { get; init; } = "";
    public string SourceNamespace { get; init; } = "";
    public int SourceRowNumber { get; init; }
    public string RelationshipId { get; init; } = "";
    public string RelationshipIdKey { get; init; } = "";
    public string ProjectIdKey { get; init; } = "";
    public string PredecessorProjectIdKey { get; init; } = "";
    public string SuccessorIdKey { get; init; } = "";
    public string PredecessorIdKey { get; init; } = "";
    public string RelationshipType { get; init; } = "";
    public string PredecessorStatus { get; init; } = "";
    public string SuccessorStatus { get; init; } = "";
    public string SchedulingMode { get; init; } = "Unresolved";
    public string SsLagBasis { get; init; } = "Unresolved";
    public string LagCalendarSetting { get; init; } = "";
    public string LagCalendarKey { get; init; } = "";
    public string PredecessorCalendarKey { get; init; } = "";
    public string RawLag { get; init; } = "";
    public decimal? EffectiveLagHours { get; init; }
    public DateTime? ProjectDataDate { get; init; }
    public DateTime? PredecessorEndpoint { get; init; }
    public DateTime? SuccessorEndpoint { get; init; }
    public string PredecessorEndpointField { get; init; } = "";
    public string SuccessorEndpointField { get; init; } = "";
    public decimal? PredecessorHoursPerDay { get; init; }
    public decimal? FloatHours { get; init; }
    public decimal? FloatDays { get; init; }
    public RelationshipFloatClassification Classification { get; init; }
    public RelationshipAllowanceStatus AllowanceStatus { get; init; } = RelationshipAllowanceStatus.RequiresContext;
    public string CalculationBasis { get; init; } = "None";
    public string ReasonCode { get; init; } = "";
    public string Message { get; init; } = "";
    public IReadOnlyDictionary<string, RelationshipFieldEvidence> InputEvidence { get; init; } =
        new ReadOnlyDictionary<string, RelationshipFieldEvidence>(new Dictionary<string, RelationshipFieldEvidence>());

    public string FormattedDays => Classification == RelationshipFloatClassification.Calculated
        && AllowanceStatus is RelationshipAllowanceStatus.Finite or RelationshipAllowanceStatus.Estimated
        ? FloatDays?.ToString("G29", CultureInfo.InvariantCulture) ?? "" : "";
}
