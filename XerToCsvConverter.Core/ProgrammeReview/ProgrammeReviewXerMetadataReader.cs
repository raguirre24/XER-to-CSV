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

    public static async ValueTask<DateOnly?> ReadSingleProjectDataDateAsync(
        byte[] xerContent,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(xerContent);
        if (xerContent.Length == 0) return null;

        using var stream = new MemoryStream(xerContent, writable: false);
        return await ReadSingleProjectDataDateAsync(stream, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads the data date directly from an XER file without loading the complete history into memory.
    /// The file is reopened for the Windows-1252 fallback so very large desktop histories remain
    /// bounded to the stream reader buffer.
    /// </summary>
    public static async ValueTask<DateOnly?> ReadSingleProjectDataDateAsync(
        string xerFilePath,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(xerFilePath);

        await using FileStream stream = OpenXerFile(xerFilePath);
        return await ReadSingleProjectDataDateAsync(stream, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Reads an XER metadata date from a readable, seekable stream without taking ownership.</summary>
    public static async ValueTask<DateOnly?> ReadSingleProjectDataDateAsync(
        Stream xerStream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(xerStream);
        if (!xerStream.CanRead)
            throw new ArgumentException("The XER stream must be readable.", nameof(xerStream));
        if (!xerStream.CanSeek)
            throw new ArgumentException("The XER stream must be seekable for encoding fallback.", nameof(xerStream));
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

    private static async ValueTask<DateOnly?> ReadWithEncodingAsync(
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

    private static FileStream OpenXerFile(string xerFilePath) => new(
        xerFilePath,
        FileMode.Open,
        FileAccess.Read,
        FileShare.Read,
        bufferSize: 4096,
        FileOptions.Asynchronous | FileOptions.SequentialScan);

}
