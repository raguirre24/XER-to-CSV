using System.Globalization;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Microsoft.Win32.SafeHandles;

namespace XerToCsvConverter;

/// <summary>
/// Explicit relationship-grain audit, never added to a normal numbered-table export.
/// Publication uses a same-directory rename; an interrupted preparation cannot publish
/// a partially written CSV. Input occurrence order is preserved without deduplication.
/// </summary>
public static class RelationshipAuditCsv
{
    public const string SchemaVersion = "1.0";
    private static readonly string[] OptionFields =
    [
        "sched_retained_logic", "sched_progress_override", "sched_lag_early_start_flag",
        "sched_use_expect_end_flag", "sched_outer_depend_type", "sched_calendar_on_relationship_lag"
    ];

    public static IReadOnlyList<string> Columns { get; } = Array.AsReadOnly(new[]
    {
        "audit_schema_version", "FileName", "source_namespace", "source_row_number",
        "task_pred_id", "task_pred_id_key", "proj_id_key", "pred_proj_id_key",
        "task_id_key", "pred_task_id_key", "pred_type", "predecessor_status_code", "status_code",
        "scheduling_mode", "ss_lag_basis", "lag_calendar_setting", "lag_calendar_key",
        "predecessor_calendar_key", "raw_lag_hr_cnt", "effective_lag_hours", "project_data_date",
        "predecessor_endpoint", "successor_endpoint", "predecessor_endpoint_field", "successor_endpoint_field",
        "predecessor_hours_per_day", "free_float_hours", "free_float", "classification", "reason_code", "message"
    }.Concat(OptionFields.SelectMany(field => new[] { field, field + "_state" }))
        .Append("input_evidence").ToArray());

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    public static void Write(IEnumerable<RelationshipFloatAssessment> assessments, TextWriter writer,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(assessments);
        ArgumentNullException.ThrowIfNull(writer);
        cancellationToken.ThrowIfCancellationRequested();
        WriteRow(writer, Columns);
        foreach (RelationshipFloatAssessment assessment in assessments)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ArgumentNullException.ThrowIfNull(assessment);
            WriteRow(writer, Values(assessment));
        }
        cancellationToken.ThrowIfCancellationRequested();
    }

    public static string Publish(IEnumerable<RelationshipFloatAssessment> assessments, string outputPath,
        IReadOnlyList<string> inputPaths, bool overwrite = false, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(assessments);
        cancellationToken.ThrowIfCancellationRequested();
        string destination = ValidateDestination(outputPath, inputPaths, overwrite);
        string temporary = Path.Combine(Path.GetDirectoryName(destination)!,
            ".xer-relationship-audit-" + Guid.NewGuid().ToString("N") + ".tmp");
        bool created = false;
        try
        {
            using (var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                created = true;
                using (var writer = new StreamWriter(stream, new UTF8Encoding(false, true), 65536, leaveOpen: true))
                {
                    Write(assessments, writer, cancellationToken);
                    writer.Flush();
                }
                stream.Flush(flushToDisk: true);
            }
            // Recheck before the irreversible rename, including newly appeared outputs/links.
            ValidateDestination(destination, inputPaths, overwrite);
            cancellationToken.ThrowIfCancellationRequested();
            File.Move(temporary, destination, overwrite);
            created = false;
            // A cancellation arriving after this commit cannot undo a completed publication.
            return destination;
        }
        finally
        {
            if (created) File.Delete(temporary);
        }
    }

    public static string ValidateDestination(string outputPath, IReadOnlyList<string> inputPaths, bool overwrite = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(outputPath);
        ArgumentNullException.ThrowIfNull(inputPaths);
        string destination = Path.GetFullPath(outputPath);
        string filename = Path.GetFileName(destination);
        if (!filename.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)
            || filename.Any(c => char.IsControl(c) || "<>:\"|?*".Contains(c))
            || filename.EndsWith(' ') || filename.EndsWith('.')
            || IsReservedDeviceName(filename))
            throw new ArgumentException("The relationship audit output must have a safe .csv filename.", nameof(outputPath));
        var comparer = OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
        RejectSymbolicLinks(destination);
        string? existingDestination = File.Exists(destination) ? CanonicalExistingFile(destination) : null;
        foreach (string input in inputPaths)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(input);
            string fullInput = Path.GetFullPath(input);
            if (comparer.Equals(destination, fullInput))
                throw new IOException("An audit destination must not replace an input file, even with --overwrite.");
            RejectSymbolicLinks(fullInput);
            if (!File.Exists(fullInput)) throw new FileNotFoundException("A requested XER input does not exist.", fullInput);
            if (existingDestination is not null && comparer.Equals(existingDestination, CanonicalExistingFile(fullInput)))
                throw new IOException("An audit destination must not replace an input through a path alias, even with --overwrite.");
        }
        string parent = Path.GetDirectoryName(destination)!;
        if (!Directory.Exists(parent))
            throw new DirectoryNotFoundException($"The audit output folder does not exist: '{parent}'.");
        if (Directory.Exists(destination)) throw new IOException("The audit destination is a directory.");
        if (!overwrite && File.Exists(destination))
            throw new IOException($"Audit output already exists: '{destination}'. Use --overwrite to replace it explicitly.");
        return destination;
    }

    private static string CanonicalExistingFile(string path)
    {
        if (!OperatingSystem.IsWindows()) return path;
        // Path.GetFullPath does not expand Windows 8.3 aliases. A read-only handle
        // identifies the normalized existing name before an overwrite is considered.
        using SafeFileHandle handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete);
        var buffer = new StringBuilder(32768);
        uint length = GetFinalPathNameByHandle(handle, buffer, (uint)buffer.Capacity, 0);
        if (length == 0) throw new IOException("Cannot verify the audit input/output path identity.",
            new Win32Exception(Marshal.GetLastWin32Error()));
        if (length >= buffer.Capacity) throw new IOException("The audit input/output canonical path is too long.");
        return buffer.ToString();
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern uint GetFinalPathNameByHandle(SafeFileHandle handle, StringBuilder path,
        uint pathLength, uint flags);

    private static bool IsReservedDeviceName(string filename)
    {
        string stem = filename.Split('.')[0].TrimEnd(' ').ToUpperInvariant();
        return stem is "CON" or "PRN" or "AUX" or "NUL"
            || (stem.Length == 4 && (stem.StartsWith("COM", StringComparison.Ordinal)
                || stem.StartsWith("LPT", StringComparison.Ordinal)) && stem[3] is >= '1' and <= '9');
    }

    private static void RejectSymbolicLinks(string path)
    {
        for (string? current = path; current is not null; current = Path.GetDirectoryName(current))
        {
            // LinkTarget distinguishes path redirection (symlinks/junctions) from cloud
            // placeholder reparse points, which may legitimately hold the user's XERs.
            FileSystemInfo info = Directory.Exists(current) ? new DirectoryInfo(current) : new FileInfo(current);
            if (info.LinkTarget is not null)
                throw new IOException($"Audit input/output paths must not traverse a filesystem link: '{current}'.");
            string? parent = Path.GetDirectoryName(current);
            if (parent == current) break;
        }
    }

    private static IEnumerable<string> Values(RelationshipFloatAssessment value)
    {
        string[] fields =
        [
            SchemaVersion, value.FileName, value.SourceNamespace, value.SourceRowNumber.ToString(CultureInfo.InvariantCulture),
            value.RelationshipId, value.RelationshipIdKey, value.ProjectIdKey, value.PredecessorProjectIdKey,
            value.SuccessorIdKey, value.PredecessorIdKey, value.RelationshipType, value.PredecessorStatus, value.SuccessorStatus,
            value.SchedulingMode, value.SsLagBasis, value.LagCalendarSetting, value.LagCalendarKey,
            value.PredecessorCalendarKey, value.RawLag, Number(value.EffectiveLagHours), Date(value.ProjectDataDate),
            Date(value.PredecessorEndpoint), Date(value.SuccessorEndpoint), value.PredecessorEndpointField, value.SuccessorEndpointField,
            Number(value.PredecessorHoursPerDay), Number(value.FloatHours), value.FormattedDays,
            value.Classification.ToString(), value.ReasonCode, value.Message
        ];
        foreach (string field in fields) yield return field;
        foreach (string field in OptionFields)
        {
            if (value.InputEvidence.TryGetValue("successor_options." + field, out RelationshipFieldEvidence? evidence))
            {
                yield return evidence.RawValue;
                yield return evidence.State;
            }
            else
            {
                yield return "";
                yield return "Unavailable";
            }
        }
        // Sorting gives stable bytes even when evidence was collected through different
        // lookup construction paths. These are raw field names, not input identities.
        var orderedEvidence = value.InputEvidence.OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
        yield return JsonSerializer.Serialize(orderedEvidence, JsonOptions);
    }

    private static string Number(decimal? value) => value?.ToString("G29", CultureInfo.InvariantCulture) ?? "";
    private static string Date(DateTime? value) => value?.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture) ?? "";

    private static void WriteRow(TextWriter writer, IEnumerable<string> values)
    {
        bool first = true;
        foreach (string value in values)
        {
            if (!first) writer.Write(',');
            first = false;
            if (value.IndexOfAny([',', '"', '\r', '\n']) < 0) writer.Write(value);
            else
            {
                writer.Write('"');
                writer.Write(value.Replace("\"", "\"\"", StringComparison.Ordinal));
                writer.Write('"');
            }
        }
        writer.Write("\r\n");
    }
}
