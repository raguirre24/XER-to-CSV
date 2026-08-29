using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace XerToCsvConverter.ProgrammeReview;

internal sealed record ProgrammeReviewOutputRow(
    ResolvedProgrammeReviewSnapshot Snapshot,
    IReadOnlyDictionary<string, string> Values);

internal sealed record ProgrammeReviewOutputTable(
    ProgrammeReviewTableContract Contract,
    IReadOnlyList<ProgrammeReviewOutputRow> Rows);

internal static class ProgrammeReviewCsv
{
    private static readonly UTF8Encoding Utf8WithoutBom = new(false, true);

    internal static string WriteTable(
        string path,
        ProgrammeReviewOutputTable table,
        CancellationToken cancellationToken)
    {
        {
            using var stream = new FileStream(path, new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.Write,
                Share = FileShare.None,
                BufferSize = PerformanceConfig.CsvWriteBufferSize,
                Options = FileOptions.SequentialScan
            });
            WriteTable(stream, table, cancellationToken);
        }
        return ComputeSha256(path);
    }

    internal static byte[] WriteTableToBytes(
        ProgrammeReviewOutputTable table,
        CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream();
        WriteTable(stream, table, cancellationToken);
        return stream.ToArray();
    }

    internal static void WriteManifest(
        string path,
        IReadOnlyList<ProgrammeReviewManifestRow> rows,
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
        IReadOnlyList<ProgrammeReviewManifestRow> rows,
        CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream();
        WriteManifest(stream, rows, cancellationToken);
        return stream.ToArray();
    }

    internal static string ComputeSha256(ReadOnlySpan<byte> content) =>
        Convert.ToHexString(SHA256.HashData(content)).ToLowerInvariant();

    private static void WriteTable(
        Stream stream,
        ProgrammeReviewOutputTable table,
        CancellationToken cancellationToken)
    {
        using var writer = new StreamWriter(stream, Utf8WithoutBom, PerformanceConfig.CsvWriteBufferSize, leaveOpen: true)
        {
            NewLine = "\r\n"
        };

        WriteCsvRow(writer, table.Contract.Columns.Select(c => c.Name));
        foreach (ProgrammeReviewOutputRow row in table.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            WriteCsvRow(writer, table.Contract.Columns.Select(column => row.Values[column.Name]));
        }
        writer.Flush();
    }

    private static void WriteManifest(
        Stream stream,
        IReadOnlyList<ProgrammeReviewManifestRow> rows,
        CancellationToken cancellationToken)
    {
        using var writer = new StreamWriter(stream, Utf8WithoutBom, PerformanceConfig.CsvWriteBufferSize, leaveOpen: true)
        {
            NewLine = "\r\n"
        };

        WriteCsvRow(writer, new[]
        {
            "schema_version", "bundle_id", "bundle_status", "parser_version", "project_code",
            "project_name", "programme_type", "original_xer_filename", "canonical_xer_filename",
            "snapshot_kind", "snapshot_tag", "monthupdate", "update_date", "data_date",
            "source_sha256", "table_name", "row_count", "csv_sha256", "exported_at_utc"
        });

        foreach (ProgrammeReviewManifestRow row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            WriteCsvRow(writer, new[]
            {
                row.SchemaVersion,
                row.BundleId,
                row.BundleStatus,
                row.ParserVersion,
                row.ProjectCode,
                row.ProjectName,
                row.ProgrammeType,
                row.OriginalXerFilename,
                row.CanonicalXerFilename,
                row.SnapshotKind.ToLowerInvariant(),
                row.SnapshotTag,
                row.MonthUpdate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                row.UpdateDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                row.DataDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                row.SourceSha256,
                row.TableName,
                row.RowCount.ToString(CultureInfo.InvariantCulture),
                row.CsvSha256,
                row.ExportedAtUtc.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture)
            });
        }
        writer.Flush();
    }

    internal static string Normalize(string? value, ProgrammeReviewColumn column, string context)
    {
        string original = value ?? string.Empty;
        if (column.Type == ProgrammeReviewColumnType.Text)
        {
            if (!column.Nullable && string.IsNullOrWhiteSpace(original))
                throw new ProgrammeReviewValidationException($"{context}: required column '{column.Name}' is blank.");
            return original;
        }

        string raw = original.Trim();
        if (raw.Length == 0)
        {
            if (!column.Nullable)
                throw new ProgrammeReviewValidationException($"{context}: required column '{column.Name}' is blank.");
            return string.Empty;
        }

        return column.Type switch
        {
            ProgrammeReviewColumnType.Text => raw,
            ProgrammeReviewColumnType.Date => NormalizeDate(raw, context, column.Name),
            ProgrammeReviewColumnType.Number => NormalizeNumber(raw, context, column.Name),
            ProgrammeReviewColumnType.Integer => NormalizeInteger(raw, context, column.Name),
            ProgrammeReviewColumnType.Boolean => NormalizeBoolean(raw, context, column.Name),
            _ => throw new ArgumentOutOfRangeException(nameof(column))
        };
    }

    internal static string ComputeSha256(string path)
    {
        using FileStream stream = File.OpenRead(path);
        return Convert.ToHexString(SHA256.HashData(stream)).ToLowerInvariant();
    }

    private static string NormalizeDate(string raw, string context, string columnName)
    {
        if (DateOnly.TryParseExact(raw, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateOnly exact))
            return exact.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        if (DateTimeOffset.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out DateTimeOffset dto))
            return DateOnly.FromDateTime(dto.Date).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        if (DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out DateTime dt))
            return DateOnly.FromDateTime(dt).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        throw new ProgrammeReviewValidationException($"{context}: '{raw}' is not a valid date for '{columnName}'.");
    }

    private static string NormalizeNumber(string raw, string context, string columnName)
    {
        if (decimal.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal number))
            return number.ToString("G29", CultureInfo.InvariantCulture);
        if (double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out double dbl) && double.IsFinite(dbl))
            return dbl.ToString("R", CultureInfo.InvariantCulture);
        throw new ProgrammeReviewValidationException($"{context}: '{raw}' is not an invariant number for '{columnName}'.");
    }

    private static string NormalizeInteger(string raw, string context, string columnName)
    {
        if (long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out long integer))
            return integer.ToString(CultureInfo.InvariantCulture);
        if (decimal.TryParse(raw, NumberStyles.Number, CultureInfo.InvariantCulture, out decimal number)
            && number == decimal.Truncate(number))
            return number.ToString("0", CultureInfo.InvariantCulture);
        throw new ProgrammeReviewValidationException($"{context}: '{raw}' is not an integer for '{columnName}'.");
    }

    private static string NormalizeBoolean(string raw, string context, string columnName) => raw.ToLowerInvariant() switch
    {
        "true" or "1" or "yes" => "true",
        "false" or "0" or "no" => "false",
        _ => throw new ProgrammeReviewValidationException($"{context}: '{raw}' is not a boolean for '{columnName}'.")
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
}
