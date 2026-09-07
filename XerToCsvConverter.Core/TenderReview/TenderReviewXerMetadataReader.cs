using System.Text;

namespace XerToCsvConverter.TenderReview;

public sealed record TenderReviewProjectIdentity(string? ProjectCode, string? ProjectName);

/// <summary>
/// Reads project identity metadata (P6 Project ID and root WBS name) from an XER file
/// without loading the complete file into memory.
/// </summary>
public static class TenderReviewXerMetadataReader
{
    private const int CooperativeYieldIntervalLines = 2048;

    public static async ValueTask<TenderReviewProjectIdentity?> ReadProjectIdentityAsync(
        string xerFilePath,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(xerFilePath);
        if (!File.Exists(xerFilePath)) return null;

        await using FileStream stream = new(
            xerFilePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 4096,
            FileOptions.Asynchronous | FileOptions.SequentialScan);

        return await ReadProjectIdentityAsync(stream, cancellationToken).ConfigureAwait(false);
    }

    public static async ValueTask<TenderReviewProjectIdentity?> ReadProjectIdentityAsync(
        Stream xerStream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(xerStream);
        if (!xerStream.CanRead || !xerStream.CanSeek) return null;
        if (xerStream.Position >= xerStream.Length) return null;

        XerTextEncoding.Selection selection = XerTextEncoding.Detect(xerStream);
        try
        {
            return await ReadWithEncodingAsync(xerStream, selection.Encoding, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (DecoderFallbackException) when (!selection.HasBom)
        {
            xerStream.Position = selection.ContentPosition;
            return await ReadWithEncodingAsync(xerStream, XerTextEncoding.Windows1252, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static async ValueTask<TenderReviewProjectIdentity?> ReadWithEncodingAsync(
        Stream xerStream,
        Encoding encoding,
        CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(
            xerStream,
            encoding,
            detectEncodingFromByteOrderMarks: false,
            bufferSize: 4096,
            leaveOpen: true);

        string? currentTable = null;
        string? projectCode = null;
        string? projectName = null;
        int projShortNameIndex = -1;
        int wbsNameIndex = -1;
        int parentWbsIdIndex = -1;
        int lineCount = 0;

        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            cancellationToken.ThrowIfCancellationRequested();
            lineCount++;
            if (lineCount % CooperativeYieldIntervalLines == 0)
                await Task.Delay(1, cancellationToken).ConfigureAwait(false);

            if (line.StartsWith("%T", StringComparison.Ordinal))
            {
                currentTable = line[2..].Trim();
                projShortNameIndex = -1;
                wbsNameIndex = -1;
                parentWbsIdIndex = -1;

                // Stop once we have passed both PROJECT and PROJWBS if we got identity
                if (projectCode is not null && currentTable is not ("PROJECT" or "PROJWBS"))
                    break;

                continue;
            }

            if (string.Equals(currentTable, "PROJECT", StringComparison.OrdinalIgnoreCase))
            {
                if (line.StartsWith("%F", StringComparison.Ordinal))
                {
                    string[] fields = line.Split('\t');
                    projShortNameIndex = Array.FindIndex(
                        fields,
                        f => string.Equals(f.Trim(), "proj_short_name", StringComparison.OrdinalIgnoreCase));
                }
                else if (line.StartsWith("%R", StringComparison.Ordinal) && projShortNameIndex >= 0)
                {
                    string[] values = line.Split('\t');
                    if (projShortNameIndex < values.Length)
                    {
                        string raw = values[projShortNameIndex].Trim();
                        if (raw.Length > 0)
                            projectCode = raw;
                    }
                }
            }
            else if (string.Equals(currentTable, "PROJWBS", StringComparison.OrdinalIgnoreCase))
            {
                if (line.StartsWith("%F", StringComparison.Ordinal))
                {
                    string[] fields = line.Split('\t');
                    wbsNameIndex = Array.FindIndex(
                        fields,
                        f => string.Equals(f.Trim(), "wbs_name", StringComparison.OrdinalIgnoreCase));
                    parentWbsIdIndex = Array.FindIndex(
                        fields,
                        f => string.Equals(f.Trim(), "parent_wbs_id", StringComparison.OrdinalIgnoreCase));
                }
                else if (line.StartsWith("%R", StringComparison.Ordinal) && wbsNameIndex >= 0 && projectName is null)
                {
                    string[] values = line.Split('\t');
                    bool isRoot = parentWbsIdIndex < 0
                        || parentWbsIdIndex >= values.Length
                        || string.IsNullOrWhiteSpace(values[parentWbsIdIndex]);
                    if (isRoot && wbsNameIndex < values.Length)
                    {
                        string raw = values[wbsNameIndex].Trim();
                        if (raw.Length > 0)
                            projectName = raw;
                    }
                }
            }
        }

        return projectCode is not null ? new TenderReviewProjectIdentity(projectCode, projectName) : null;
    }
}
