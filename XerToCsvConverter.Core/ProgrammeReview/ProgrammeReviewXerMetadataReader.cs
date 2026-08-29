using System.Globalization;
using System.Text;

namespace XerToCsvConverter.ProgrammeReview;

/// <summary>
/// Reads the small amount of authoritative project metadata needed by the browser editor without
/// constructing a complete XER data store. A value is returned only for one unambiguous PROJECT row.
/// </summary>
public static class ProgrammeReviewXerMetadataReader
{
    private const int CooperativeYieldIntervalLines = 2048;
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);
    private static readonly Encoding Windows1252 = CreateWindows1252Encoding();

    public static async ValueTask<DateOnly?> ReadSingleProjectDataDateAsync(
        byte[] xerContent,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(xerContent);
        if (xerContent.Length == 0) return null;

        try
        {
            return await ReadWithEncodingAsync(xerContent, StrictUtf8, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (DecoderFallbackException)
        {
            return await ReadWithEncodingAsync(xerContent, Windows1252, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static async ValueTask<DateOnly?> ReadWithEncodingAsync(
        byte[] xerContent,
        Encoding encoding,
        CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream(xerContent, writable: false);
        using var reader = new StreamReader(
            stream,
            encoding,
            detectEncodingFromByteOrderMarks: true,
            bufferSize: 4096,
            leaveOpen: false);

        bool inProjectTable = false;
        int dataDateIndex = -1;
        int projectRowCount = 0;
        DateOnly? dataDate = null;
        int lineCount = 0;

        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            cancellationToken.ThrowIfCancellationRequested();
            lineCount++;
            if (lineCount % CooperativeYieldIntervalLines == 0)
                await Task.Delay(1, cancellationToken).ConfigureAwait(false);

            if (line.StartsWith("%T", StringComparison.Ordinal))
            {
                if (inProjectTable) break;
                inProjectTable = string.Equals(
                    line[2..].Trim(),
                    TableNames.Project,
                    StringComparison.OrdinalIgnoreCase);
                dataDateIndex = -1;
                continue;
            }

            if (!inProjectTable) continue;

            if (line.StartsWith("%F", StringComparison.Ordinal))
            {
                string[] fields = line.Split('\t');
                dataDateIndex = Array.FindIndex(
                    fields,
                    field => string.Equals(field.Trim(), FieldNames.LastRecalcDate, StringComparison.OrdinalIgnoreCase));
                continue;
            }

            if (!line.StartsWith("%R", StringComparison.Ordinal)) continue;

            projectRowCount++;
            if (projectRowCount > 1 || dataDateIndex < 1)
                return null;

            string[] values = line.Split('\t');
            if (dataDateIndex >= values.Length)
                return null;

            DateTime? parsed = DateParser.TryParse(values[dataDateIndex].Trim());
            if (parsed is null)
                return null;

            dataDate = DateOnly.FromDateTime(parsed.Value);
        }

        return projectRowCount == 1 ? dataDate : null;
    }

    private static Encoding CreateWindows1252Encoding()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        return Encoding.GetEncoding(
            1252,
            EncoderFallback.ExceptionFallback,
            DecoderFallback.ExceptionFallback);
    }
}
