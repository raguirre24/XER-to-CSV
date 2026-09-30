namespace XerToCsvConverter;

/// <summary>Metadata for a completed caller-owned Standard ZIP stream.</summary>
public sealed record StandardZipStreamExportResult(int WarningCount, int FileCount)
{
    internal XerTable? DataQualityTable { get; init; }
}

internal static class ZipStreamExport
{
    internal static void ValidateDestination(Stream output)
    {
        ArgumentNullException.ThrowIfNull(output);
        if (!output.CanWrite) throw new ArgumentException("The ZIP output stream must be writable.", nameof(output));
    }

    internal static IProgress<ProcessingService.DetailedProgress>? MapParseProgress(
        IProgress<ProcessingService.DetailedProgress>? progress, int maximumPercent) =>
        progress is null ? null : new InlineProgress<ProcessingService.DetailedProgress>(value =>
        {
            value.Percent = Math.Clamp(value.Percent, 0, 100) * maximumPercent / 100;
            progress.Report(value);
        });

    private sealed class InlineProgress<T>(Action<T> report) : IProgress<T>
    {
        public void Report(T value) => report(value);
    }
}
