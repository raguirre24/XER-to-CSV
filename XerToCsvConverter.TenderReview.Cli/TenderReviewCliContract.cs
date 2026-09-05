using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.TenderReview.Cli;

/// <summary>
/// User-editable Tender Review CLI configuration. Source order is significant.
/// </summary>
public sealed record TenderReviewCliConfiguration
{
    public required string ProjectCode { get; init; }
    public required string ProjectName { get; init; }
    public required IReadOnlyList<TenderReviewCliSource> Sources { get; init; }
    public string? ParserVersion { get; init; }
    public DateTimeOffset? ExportedAtUtc { get; init; }
}

/// <summary>
/// One ordered Tender-stage input. StatusDate must be explicitly supplied.
/// </summary>
public sealed record TenderReviewCliSource
{
    public required string XerFilePath { get; init; }
    public required DateOnly StatusDate { get; init; }
    public string? OriginalXerFilename { get; init; }
}

/// <summary>
/// Maps editable CLI configuration to the Core token-based Tender contract.
/// </summary>
public static class TenderReviewCliRequestMapper
{
    public static TenderReviewBundleRequest CreateRequest(
        TenderReviewCliConfiguration configuration,
        string configurationDirectory)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(configurationDirectory);
        ArgumentNullException.ThrowIfNull(configuration.Sources);

        string baseDirectory = Path.GetFullPath(configurationDirectory);
        var sources = new TenderReviewSource[configuration.Sources.Count];
        for (int index = 0; index < configuration.Sources.Count; index++)
        {
            TenderReviewCliSource source = configuration.Sources[index]
                ?? throw new ArgumentException($"Tender source {index + 1} is null.", nameof(configuration));
            if (string.IsNullOrWhiteSpace(source.XerFilePath))
                throw new ArgumentException($"Tender source {index + 1}: xer_file_path is required.", nameof(configuration));

            string fullPath = Path.GetFullPath(source.XerFilePath, baseDirectory);
            string originalFilename = string.IsNullOrWhiteSpace(source.OriginalXerFilename)
                ? Path.GetFileName(fullPath)
                : source.OriginalXerFilename.Trim();
            sources[index] = new TenderReviewSource
            {
                SourceToken = TenderReviewNaming.CreateSourceToken(index),
                OriginalXerFilename = originalFilename,
                XerFilePath = fullPath,
                StatusDate = source.StatusDate
            };
        }

        return new TenderReviewBundleRequest
        {
            ProjectCode = configuration.ProjectCode,
            ProjectName = configuration.ProjectName,
            ParserVersion = configuration.ParserVersion,
            ExportedAtUtc = configuration.ExportedAtUtc,
            Sources = sources
        };
    }
}
