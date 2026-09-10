using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace XerToCsvConverter.TenderReview;

internal sealed record TenderReviewOutputRow(
    ResolvedTenderReviewSource Source,
    IReadOnlyDictionary<string, string> Values);

internal sealed record TenderReviewOutputTable(
    TenderReviewTableContract Contract,
    IReadOnlyList<TenderReviewOutputRow> Rows);

internal static class TenderReviewCsv
{
    private static readonly UTF8Encoding Utf8WithoutBom = new(false, true);

    internal static string WriteTable(
        string path,
        TenderReviewOutputTable table,
        CancellationToken cancellationToken)
    {
        using (var stream = new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
            BufferSize = PerformanceConfig.CsvWriteBufferSize,
            Options = FileOptions.SequentialScan | FileOptions.WriteThrough
        }))
        {
            WriteTable(stream, table, cancellationToken);
        }

        return ComputeSha256(path);
    }

    internal static byte[] WriteTableToBytes(
        TenderReviewOutputTable table,
        CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream();
        WriteTable(stream, table, cancellationToken);
        return stream.ToArray();
    }

    internal static void WriteManifest(
        string path,
        IReadOnlyList<TenderReviewManifestRow> rows,
        CancellationToken cancellationToken)
    {
        using var stream = new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
            BufferSize = PerformanceConfig.CsvWriteBufferSize,
            Options = FileOptions.SequentialScan | FileOptions.WriteThrough
        });
        WriteManifest(stream, rows, cancellationToken);
    }

    internal static byte[] WriteManifestToBytes(
        IReadOnlyList<TenderReviewManifestRow> rows,
        CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream();
        WriteManifest(stream, rows, cancellationToken);
        return stream.ToArray();
    }

    internal static string ComputeSha256(ReadOnlySpan<byte> content) =>
        Convert.ToHexString(SHA256.HashData(content)).ToLowerInvariant();

    internal static string ComputeSha256(string path)
    {
        using FileStream stream = File.OpenRead(path);
        return Convert.ToHexString(SHA256.HashData(stream)).ToLowerInvariant();
    }

    private static void WriteTable(
        Stream stream,
        TenderReviewOutputTable table,
        CancellationToken cancellationToken)
    {
        using var writer = CreateWriter(stream);
        WriteCsvRow(writer, table.Contract.Columns.Select(column => column.Name));
        foreach (TenderReviewOutputRow row in table.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            WriteCsvRow(writer, table.Contract.Columns.Select(column => row.Values[column.Name]));
        }
        writer.Flush();
    }

    private static void WriteManifest(
        Stream stream,
        IReadOnlyList<TenderReviewManifestRow> rows,
        CancellationToken cancellationToken)
    {
        using var writer = CreateWriter(stream);
        WriteCsvRow(writer, TenderReviewContract.ManifestColumns);
        foreach (TenderReviewManifestRow row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            WriteCsvRow(writer, new[]
            {
                row.SchemaVersion,
                row.BundleProfile,
                row.BundleId,
                row.BundleStatus,
                row.ParserVersion,
                row.ProjectCode,
                row.ProjectName,
                row.ProjectState,
                row.OriginalXerFilename,
                row.CanonicalXerFilename,
                Iso(row.StatusDate),
                Iso(row.UpdateDate),
                Iso(row.DataDate),
                row.SourceSha256,
                row.TableName,
                row.RowCount.ToString(CultureInfo.InvariantCulture),
                row.CsvSha256,
                row.ExportedAtUtc.ToUniversalTime().ToString(
                    "yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture)
            });
        }
        writer.Flush();
    }

    internal static string Normalize(string? value, TenderReviewColumn column, string context)
    {
        string original = value ?? string.Empty;
        if (column.Type == TenderReviewColumnType.Text)
        {
            if (!column.Nullable && string.IsNullOrWhiteSpace(original))
                throw new TenderReviewValidationException(
                    $"{context}: required column '{column.Name}' is blank.");
            return original;
        }

        string raw = original.Trim();
        if (raw.Length == 0)
        {
            if (!column.Nullable)
                throw new TenderReviewValidationException(
                    $"{context}: required column '{column.Name}' is blank.");
            return string.Empty;
        }

        return column.Type switch
        {
            TenderReviewColumnType.Text => raw,
            TenderReviewColumnType.Date => NormalizeDate(raw, context, column.Name),
            TenderReviewColumnType.Number => NormalizeNumber(raw, context, column.Name),
            TenderReviewColumnType.Integer => NormalizeInteger(raw, context, column.Name),
            TenderReviewColumnType.Boolean => NormalizeBoolean(raw, context, column.Name),
            _ => throw new ArgumentOutOfRangeException(nameof(column))
        };
    }

    internal static string NormalizeDate(string raw, string context, string columnName)
    {
        if (DateOnly.TryParseExact(raw, "yyyy-MM-dd", CultureInfo.InvariantCulture,
            DateTimeStyles.None, out DateOnly exact))
            return Iso(exact);
        if (DateTimeOffset.TryParse(raw, CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces, out DateTimeOffset dto))
            return Iso(DateOnly.FromDateTime(dto.Date));
        if (DateTime.TryParse(raw, CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces, out DateTime dateTime))
            return Iso(DateOnly.FromDateTime(dateTime));
        throw new TenderReviewValidationException(
            $"{context}: '{raw}' is not a valid date for '{columnName}'.");
    }

    private static string NormalizeNumber(string raw, string context, string columnName)
    {
        if (decimal.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal number))
            return number.ToString("G29", CultureInfo.InvariantCulture);
        if (double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out double dbl)
            && double.IsFinite(dbl))
            return dbl.ToString("R", CultureInfo.InvariantCulture);
        throw new TenderReviewValidationException(
            $"{context}: '{raw}' is not an invariant number for '{columnName}'.");
    }

    private static string NormalizeInteger(string raw, string context, string columnName)
    {
        if (long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out long integer))
            return integer.ToString(CultureInfo.InvariantCulture);
        if (decimal.TryParse(raw, NumberStyles.Number, CultureInfo.InvariantCulture, out decimal number)
            && number == decimal.Truncate(number))
            return number.ToString("0", CultureInfo.InvariantCulture);
        throw new TenderReviewValidationException(
            $"{context}: '{raw}' is not an integer for '{columnName}'.");
    }

    private static string NormalizeBoolean(string raw, string context, string columnName) =>
        raw.ToLowerInvariant() switch
        {
            "true" or "1" or "yes" => "true",
            "false" or "0" or "no" => "false",
            _ => throw new TenderReviewValidationException(
                $"{context}: '{raw}' is not a boolean for '{columnName}'.")
        };

    private static StreamWriter CreateWriter(Stream stream) => new(
        stream,
        Utf8WithoutBom,
        PerformanceConfig.CsvWriteBufferSize,
        leaveOpen: true)
    {
        NewLine = "\r\n"
    };

    private static void WriteCsvRow(TextWriter writer, IEnumerable<string?> values)
    {
        bool first = true;
        foreach (string? value in values)
        {
            if (!first) writer.Write(',');
            WriteCsvField(writer, value ?? string.Empty);
            first = false;
        }
        writer.WriteLine();
    }

    private static void WriteCsvField(TextWriter writer, string value)
    {
        bool quote = value.IndexOfAny(new[] { ',', '"', '\r', '\n' }) >= 0
            || (value.Length > 0 && (char.IsWhiteSpace(value[0]) || char.IsWhiteSpace(value[^1])));
        if (!quote)
        {
            writer.Write(value);
            return;
        }

        writer.Write('"');
        writer.Write(value.Replace("\"", "\"\"", StringComparison.Ordinal));
        writer.Write('"');
    }

    private static string Iso(DateOnly value) =>
        value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
}
