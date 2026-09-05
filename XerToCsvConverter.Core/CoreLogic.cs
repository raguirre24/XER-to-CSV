using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace XerToCsvConverter;
    // Configuration class for performance tuning

    public static class PerformanceConfig
    {
        // Parameters (Consider exposing these in a Settings UI for advanced users)

        public static int FileReadBufferSize { get; set; } = 131072; // 128KB

        public static int CsvWriteBufferSize { get; set; } = 131072; // 128KB

        public static int ProgressReportIntervalLines { get; set; } = 5000;

        // Removed BatchProcessingSize as explicit batching was removed from CsvExporter

        public static int MaxParallelFiles { get; set; } = Environment.ProcessorCount;

        public static int MaxParallelTransformations { get; set; } = Environment.ProcessorCount;

        public static int StringBuilderInitialCapacity { get; set; } = 8192; // 8KB

        public static int DefaultTableCapacity { get; set; } = 50000;

        public static bool EnableGlobalStringInterning { get; set; } = true;

        public static int MaxStringInternLength { get; set; } = 512;



        static PerformanceConfig()
        {
            // Optimize Garbage Collection for large data processing
            // Guard for environments where GCSettings may not be available (e.g., Blazor WASM)
            try
            {
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            }
            catch (PlatformNotSupportedException) { }
            // LatencyMode is managed by the MainForm: Batch during processing, Interactive when idle.
        }
    }

    public sealed class UserSettings
    {
        public string LastOutputPath { get; set; } = string.Empty;
        public bool GeneratePowerBI { get; set; }
    }

    // .NET 8: JSON Source Generator for AOT-friendly, reflection-free serialization
    [JsonSerializable(typeof(UserSettings))]
    [JsonSourceGenerationOptions(WriteIndented = true)]
    public partial class UserSettingsJsonContext : JsonSerializerContext { }

    public static class UserSettingsStore
    {
        private static readonly string SettingsPath = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "XER to CSV",
            "settings.json");

        public static UserSettings Load()
        {
            try
            {
                if (!File.Exists(SettingsPath))
                {
                    return new UserSettings();
                }

                string json = File.ReadAllText(SettingsPath);
                // .NET 8: Use source-generated JSON context for faster, AOT-compatible deserialization
                return JsonSerializer.Deserialize(json, UserSettingsJsonContext.Default.UserSettings) ?? new UserSettings();
            }
            catch
            {
                return new UserSettings();
            }
        }

        public static void Save(UserSettings settings)
        {
            if (settings is null) throw new ArgumentNullException(nameof(settings));
            try
            {
                string? directory = Path.GetDirectoryName(SettingsPath);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                // .NET 8: Use source-generated JSON context for faster, AOT-compatible serialization
                string json = JsonSerializer.Serialize(settings, UserSettingsJsonContext.Default.UserSettings);
                File.WriteAllText(SettingsPath, json);
            }
            catch
            {
                // Ignore persistence failures to preserve existing behavior.
            }
        }
    }


    public static class TableNames { public const string Task = "TASK"; public const string Calendar = "CALENDAR"; public const string Project = "PROJECT"; public const string ProjWbs = "PROJWBS"; public const string TaskActv = "TASKACTV"; public const string ActvCode = "ACTVCODE"; public const string ActvType = "ACTVTYPE"; public const string TaskPred = "TASKPRED"; public const string Rsrc = "RSRC"; public const string TaskRsrc = "TASKRSRC"; public const string Umeasure = "UMEASURE"; }



    public static class FieldNames

    {

        // Common Fields

        public const string TaskId = "task_id"; public const string ProjectId = "proj_id"; public const string WbsId = "wbs_id"; public const string CalendarId = "clndr_id"; public const string TaskType = "task_type"; public const string StatusCode = "status_code"; public const string TaskCode = "task_code"; public const string TaskName = "task_name"; public const string RsrcId = "rsrc_id"; public const string ActStartDate = "act_start_date"; public const string ActEndDate = "act_end_date"; public const string EarlyStartDate = "early_start_date"; public const string EarlyEndDate = "early_end_date"; public const string LateEndDate = "late_end_date"; public const string LateStartDate = "late_start_date"; public const string TargetStartDate = "target_start_date"; public const string TargetEndDate = "target_end_date"; public const string CstrType = "cstr_type"; public const string CstrDate = "cstr_date"; public const string PriorityType = "priority_type"; public const string FloatPath = "float_path"; public const string FloatPathOrder = "float_path_order"; public const string DrivingPathFlag = "driving_path_flag"; public const string RemainDurationHrCnt = "remain_drtn_hr_cnt"; public const string TotalFloatHrCnt = "total_float_hr_cnt"; public const string FreeFloatHrCnt = "free_float_hr_cnt"; public const string CompletePctType = "complete_pct_type"; public const string PhysCompletePct = "phys_complete_pct"; public const string ActWorkQty = "act_work_qty"; public const string RemainWorkQty = "remain_work_qty"; public const string TargetDurationHrCnt = "target_drtn_hr_cnt"; public const string ClndrId = "clndr_id"; public const string DayHourCount = "day_hr_cnt"; public const string LastRecalcDate = "last_recalc_date";

        public const string ParentWbsId = "parent_wbs_id";

        public const string ActvCodeTypeId = "actv_code_type_id"; public const string ActvCodeId = "actv_code_id"; public const string FileName = "FileName"; public const string Start = "Start"; public const string Finish = "Finish"; public const string IdName = "ID_Name"; public const string RemainingDuration = "Remaining Duration"; public const string OriginalDuration = "Original Duration"; public const string TotalFloat = "total_float"; public const string FreeFloat = "Free Float"; public const string PercentComplete = "%"; public const string DataDate = "Data Date"; public const string WbsIdKey = "wbs_id_key"; public const string TaskIdKey = "task_id_key"; public const string ParentWbsIdKey = "parent_wbs_id_key"; public const string CalendarIdKey = "calendar_id_key"; public const string ProjIdKey = "proj_id_key"; public const string ActvCodeIdKey = "actv_code_id_key"; public const string ActvCodeTypeIdKey = "actv_code_type_id_key"; public const string ClndrIdKey = "clndr_id_key"; public const string PredTaskId = "pred_task_id"; public const string PredTaskIdKey = "pred_task_id_key"; public const string CalendarName = "clndr_name"; public const string CalendarData = "clndr_data"; public const string CalendarType = "clndr_type";

        public const string Date = "date"; public const string DayOfWeek = "day_of_week"; public const string WorkingDay = "working_day"; public const string WorkHours = "work_hours"; public const string ExceptionType = "exception_type"; public const string RsrcIdKey = "rsrc_id_key";

        public const string MonthUpdate = "MonthUpdate";

        public const string UnitId = "unit_id"; public const string UnitIdKey = "unit_id_key";



        // Predecessor Fields

        public const string PredecessorClndrIdKey = "predecessor_clndr_id_key"; public const string PredecessorStatusCode = "predecessor_status_code"; public const string Lag = "lag"; public const string TimePeriodHoursPerDay = "time_period_hours_per_day"; public const string PredecessorStart = "predecessor_start"; public const string PredecessorFinish = "predecessor_finish"; public const string PredecessorTaskType = "predecessor_task_type"; public const string PredecessorFreeFloat = "free_float"; public const string PredType = "pred_type"; public const string LagHrCnt = "lag_hr_cnt";



        // Calendar Detailed Fields

        public const string DayOfWeekNum = "day_of_week_num"; public const string WorkingDayInt = "working_day_int";



        // ** RESOURCE DISTRIBUTION FIELDS (Native & Calculated) **

        public const string ActRegQty = "act_reg_qty";

        public const string ActOtQty = "act_ot_qty";

        public const string RemainQty = "remain_qty";

        public const string RestartDate = "restart_date";

        public const string ReendDate = "reend_date";



        public const string RsrcShortName = "rsrc_short_name";

        public const string RsrcName = "rsrc_name";

        public const string RsrcType = "rsrc_type";

        public const string UnitName = "unit_name";

        public const string UnitAbbr = "unit_abbrev";



        public const string DistributionMonth = "distribution_month";

        public const string MonthStartDate = "month_start_date";

        public const string MonthEndDate = "month_end_date";

        public const string MonthlyQuantity = "monthly_quantity";



        public const string MonthWorkingDays = "month_working_days";

        public const string MonthCalendarDays = "month_calendar_days";

        public const string TotalWorkingDays = "total_working_days";

        public const string TotalCalendarDays = "total_calendar_days";



        public const string MonthWorkingHours = "month_working_hours";

        public const string TotalWorkingHours = "total_working_hours";

        public const string CalendarHoursPerDay = "calendar_hours_per_day";



        public const string DistributionType = "distribution_type";

        public const string IsActual = "is_actual";

        public const string ProjectResource = "project_resource";

        public const string Unit = "Unit";

    }

    public static class EnhancedTableNames

    {

        public const string XerTask01 = "01_XER_TASK";

        public const string XerProject02 = "02_XER_PROJECT";

        public const string XerProjWbs03 = "03_XER_PROJWBS";

        public const string XerBaseline04 = "04_XER_BASELINE";

        public const string XerPredecessor06 = "06_XER_PREDECESSOR";

        public const string XerActvType07 = "07_XER_ACTVTYPE";

        public const string XerActvCode08 = "08_XER_ACTVCODE";

        public const string XerTaskActv09 = "09_XER_TASKACTV";

        public const string XerCalendar10 = "10_XER_CALENDAR";

        public const string XerCalendarDetailed11 = "11_XER_CALENDAR_DETAILED";

        public const string XerRsrc12 = "12_XER_RSRC";

        public const string XerTaskRsrc13 = "13_XER_TASKRSRC";

        public const string XerUmeasure14 = "14_XER_UMEASURE";

        public const string XerResourceDist15 = "15_XER_RESOURCE_DISTRIBUTION"; // ** NEW **

    }

    public static class StringInternPool
    {
        // Initialize pool with estimated capacity
        private static readonly ConcurrentDictionary<string, string> Pool = new ConcurrentDictionary<string, string>(
            Environment.ProcessorCount * 2, PerformanceConfig.DefaultTableCapacity * 20);

        [return: NotNullIfNotNull(nameof(str))]
        public static string? Intern(string? str)
        {
            if (!PerformanceConfig.EnableGlobalStringInterning) return str;
            if (str == null) return null;
            if (string.IsNullOrEmpty(str)) return string.Empty;
            // Avoid interning very long strings
            if (str.Length > PerformanceConfig.MaxStringInternLength) return str;

            return Pool.GetOrAdd(str, str);
        }

        public static string Intern(ReadOnlySpan<char> value)
        {
            if (value.IsEmpty) return string.Empty;
            string created = new string(value);
            return Intern(created)!;
        }

        public static void Clear() => Pool.Clear();
    }


    // Efficient Date Parsing with caching

    public static class DateParser

    {

        private static readonly ConcurrentDictionary<string, DateTime?> DateCache = new ConcurrentDictionary<string, DateTime?>();

        // Common P6 formats for optimized TryParseExact

        private static readonly string[] P6Formats = {

            "d/M/yyyy", "dd/MM/yyyy", "M/d/yyyy", "MM/dd/yyyy",

            "yyyy-MM-dd", "dd-MMM-yy", "dd-MMM-yyyy",

            // Added common formats with time

            "d/M/yyyy H:mm:ss", "dd/MM/yyyy HH:mm:ss", "M/d/yyyy h:mm:ss tt",

            "yyyy-MM-dd HH:mm:ss"

        };

        public const string OutputFormat = "yyyy-MM-dd HH:mm:ss";



        public static DateTime? TryParse(string? dateStr)
        {
            if (string.IsNullOrWhiteSpace(dateStr)) return null;


            return DateCache.GetOrAdd(dateStr, str =>

            {

                // Try optimized exact parsing first

                if (DateTime.TryParseExact(str, P6Formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime result))

                {

                    // Basic validation for reasonable dates

                    if (result.Year > 1900 && result.Year < 2200) return result;

                }

                // Fallback to general parsing

                if (DateTime.TryParse(str, CultureInfo.InvariantCulture, DateTimeStyles.None, out result))

                {

                    if (result.Year > 1900 && result.Year < 2200) return result;

                }

                return null;

            });

        }



        public static string Format(DateTime? date) => date.HasValue && date.Value != DateTime.MinValue ? date.Value.ToString(OutputFormat, CultureInfo.InvariantCulture) : "";

        public static string Format(DateTime date) => date != DateTime.MinValue ? date.ToString(OutputFormat, CultureInfo.InvariantCulture) : "";

        public static void ClearCache() => DateCache.Clear();

    }



    // Working Day Calculator - Handles calendar-aware date calculations

    // Working Day Calculator - Handles calendar-aware date calculations




    // Lightweight struct for data rows
    public readonly record struct DataRow
    {
        public DataRow(string[] Fields, string SourceFilename, string? sourceToken = null,
            string? originalSourceFilename = null)
        {
            this.Fields = Fields;
            this.SourceFilename = SourceFilename;
            SourceToken = sourceToken ?? SourceFilename;
            OriginalSourceFilename = originalSourceFilename ?? SourceFilename;
        }

        public string[] Fields { get; init; }
        // Compatibility/public-key namespace, distinct from immutable occurrence identity.
        public string SourceFilename { get; init; }
        public string SourceToken { get; init; }
        public string OriginalSourceFilename { get; init; }
        public DataRow WithFields(string[] fields) => this with { Fields = fields };
        public void Deconstruct(out string[] fields, out string sourceFilename)
        {
            fields = Fields;
            sourceFilename = SourceFilename;
        }
    }


    // Represents a table extracted from the XER file

    public class XerTable
    {
        public string Name { get; }
        public string[]? Headers { get; private set; }
        private readonly List<DataRow> _rows;
        private FrozenDictionary<string, int> _fieldIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            .ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);


        public XerTable(string name, int initialCapacity = 0)

        {

            Name = StringInternPool.Intern(name);

            _rows = new List<DataRow>(initialCapacity > 0 ? initialCapacity : PerformanceConfig.DefaultTableCapacity);

        }



        public void SetHeaders(string[] headers)

        {

            Headers = headers;

            BuildFieldIndexes();

        }



        public void AddRow(DataRow row)
        {
            if (Headers is null) throw new InvalidOperationException($"Headers must be set for table {Name} before adding rows.");

            // Align row fields if lengths mismatch (handles variations in XER exports)
            if (row.Fields.Length != Headers.Length)
            {
                row = AlignRowFields(row);
            }

            _rows.Add(row);

        }



        public void AddRows(IEnumerable<DataRow> rows) => _rows.AddRange(rows);



        private DataRow AlignRowFields(DataRow row)
        {
            var values = row.Fields;
            string[] headers = Headers ?? throw new InvalidOperationException($"Headers must be set for table {Name} before aligning rows.");
            var newValues = new string[headers.Length];
            int copyLength = Math.Min(values.Length, headers.Length);
            Array.Copy(values, newValues, copyLength);

            // Fill remaining fields with empty strings
            for (int i = copyLength; i < headers.Length; i++)
            {
                newValues[i] = string.Empty;
            }
            return row.WithFields(newValues);
        }


        public IReadOnlyList<DataRow> Rows => _rows;

        public int RowCount => _rows.Count;

        public bool IsEmpty => _rows.Count == 0;



        // Builds a dictionary for fast field index lookup (Case-insensitive)

        private void BuildFieldIndexes()
        {
            if (Headers == null || Headers.Length == 0)
            {
                _fieldIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
                    .ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
                return;
            }

            var dict = new Dictionary<string, int>(Headers.Length, StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < Headers.Length; i++)
            {
                // Handle potential duplicate or empty headers
                string header = Headers[i];
                if (string.IsNullOrEmpty(header))
                {
                    continue;
                }

                ref int value = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, header, out bool exists);
                if (!exists)
                {
                    value = i;
                }
            }
            _fieldIndexes = dict.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
        }

        public IReadOnlyDictionary<string, int> FieldIndexes => _fieldIndexes;


        public static string GetFieldValueSafe(DataRow row, int index)

        {

            if (row.Fields != null && index >= 0 && index < row.Fields.Length)

            {

                return row.Fields[index] ?? "";

            }

            return "";

        }

    }



    // Manages the collection of all parsed tables

    public class XerDataStore
    {
        private readonly Dictionary<string, XerTable> _tables = new Dictionary<string, XerTable>(StringComparer.OrdinalIgnoreCase);


        // Ordered, schema-preserving merge. Neither the incoming headers nor rows are mutated.
        public void MergeStore(XerDataStore otherStore)
        {
            ArgumentNullException.ThrowIfNull(otherStore);
            if (ReferenceEquals(this, otherStore))
                throw new ArgumentException("A data store cannot be merged into itself.", nameof(otherStore));

            // Preflight both complete stores before mutating either schema or row set.
            // Otherwise a later source's duplicate columns can collapse during union and
            // evade the final export-schema checks with one overwritten value remaining.
            foreach (XerTable table in _tables.Values.Concat(otherStore._tables.Values))
                XerSourceSchema.ValidateHeaders(table.Name, table.Headers);

            foreach (var pair in otherStore._tables)
            {
                XerTable incoming = pair.Value;
                if (!_tables.TryGetValue(pair.Key, out XerTable? existing))
                {
                    var copy = new XerTable(incoming.Name, incoming.RowCount);
                    if (incoming.Headers is not null) copy.SetHeaders(incoming.Headers.ToArray());
                    copy.AddRows(incoming.Rows);
                    _tables.Add(pair.Key, copy);
                    continue;
                }

                string[] currentHeaders = existing.Headers ?? Array.Empty<string>();
                string[] incomingHeaders = incoming.Headers ?? Array.Empty<string>();
                var headers = currentHeaders.ToList();
                var seen = currentHeaders.ToHashSet(StringComparer.OrdinalIgnoreCase);
                foreach (string header in incomingHeaders)
                    if (seen.Add(header)) headers.Add(header);

                if (headers.Count != currentHeaders.Length)
                {
                    var widened = new XerTable(existing.Name, existing.RowCount + incoming.RowCount);
                    widened.SetHeaders(headers.ToArray());
                    foreach (DataRow row in existing.Rows)
                        widened.AddRow(row); // Extends with blanks and preserves source metadata.
                    existing = widened;
                    _tables[pair.Key] = existing;
                }
                else if (existing.Headers is null)
                {
                    existing.SetHeaders(headers.ToArray());
                }

                if (headers.SequenceEqual(incomingHeaders, StringComparer.OrdinalIgnoreCase))
                {
                    existing.AddRows(incoming.Rows);
                    continue;
                }

                foreach (DataRow row in incoming.Rows)
                {
                    var fields = new string[headers.Count];
                    Array.Fill(fields, string.Empty);
                    for (int i = 0; i < Math.Min(incomingHeaders.Length, row.Fields.Length); i++)
                        if (existing.FieldIndexes.TryGetValue(incomingHeaders[i], out int destination))
                            fields[destination] = row.Fields[i] ?? string.Empty;
                    existing.AddRow(row.WithFields(fields));
                }
            }
        }



        public void AddTable(XerTable table) => _tables[table.Name] = table;



        public XerTable? GetTable(string tableName)
        {
            _tables.TryGetValue(tableName, out var table);
            return table;
        }


        public bool ContainsTable(string tableName) => _tables.ContainsKey(tableName);

        public IEnumerable<string> TableNames => _tables.Keys.OrderBy(k => k);

        public int TableCount => _tables.Count;

    }



    // Handles the parsing of XER files

    public class XerParser

    {

        private const char Delimiter = '\t';

        // Parses XER content from a Stream (for in-memory / Blazor scenarios)
        public XerDataStore ParseXerStream(Stream stream, string fileName, Action<int, string>? reportProgressAction, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ArgumentException.ThrowIfNullOrWhiteSpace(fileName);
            if (!stream.CanRead) throw new ArgumentException("The XER stream must be readable.", nameof(stream));

            Stream readableStream = stream;
            MemoryStream? ownedCopy = null;
            if (!stream.CanSeek)
            {
                ownedCopy = new MemoryStream();
                stream.CopyTo(ownedCopy);
                ownedCopy.Position = 0;
                readableStream = ownedCopy;
            }

            XerTextEncoding.Selection selection = XerTextEncoding.Detect(readableStream);
            string sourceToken = "parser-source-" + Guid.NewGuid().ToString("N");
            try
            {
                try
                {
                    return ParseXerStreamWithEncoding(
                        readableStream, fileName, sourceToken, selection.Encoding, reportProgressAction, cancellationToken);
                }
                catch (DecoderFallbackException) when (!selection.HasBom)
                {
                    readableStream.Position = selection.ContentPosition;
                    return ParseXerStreamWithEncoding(
                        readableStream, fileName, sourceToken, XerTextEncoding.Windows1252, reportProgressAction, cancellationToken);
                }
            }
            finally
            {
                ownedCopy?.Dispose();
            }
        }

        // Cooperative asynchronous stream parser for single-threaded browser runtimes. It yields at
        // the configured progress interval so rendering, progress callbacks and cancellation events run.
        public async Task<XerDataStore> ParseXerStreamAsync(
            Stream stream,
            string fileName,
            Action<int, string>? reportProgressAction,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ArgumentException.ThrowIfNullOrWhiteSpace(fileName);
            if (!stream.CanRead) throw new ArgumentException("The XER stream must be readable.", nameof(stream));

            Stream readableStream = stream;
            MemoryStream? ownedCopy = null;
            if (!stream.CanSeek)
            {
                ownedCopy = new MemoryStream();
                await stream.CopyToAsync(ownedCopy, cancellationToken);
                ownedCopy.Position = 0;
                readableStream = ownedCopy;
            }

            XerTextEncoding.Selection selection = XerTextEncoding.Detect(readableStream);
            string sourceToken = "parser-source-" + Guid.NewGuid().ToString("N");
            try
            {
                try
                {
                    return await ParseXerStreamWithEncodingAsync(
                        readableStream, fileName, sourceToken, selection.Encoding, reportProgressAction, cancellationToken);
                }
                catch (DecoderFallbackException) when (!selection.HasBom)
                {
                    readableStream.Position = selection.ContentPosition;
                    return await ParseXerStreamWithEncodingAsync(
                        readableStream, fileName, sourceToken, XerTextEncoding.Windows1252, reportProgressAction, cancellationToken);
                }
            }
            finally
            {
                ownedCopy?.Dispose();
            }
        }

        private static async Task<XerDataStore> ParseXerStreamWithEncodingAsync(
            Stream stream,
            string fileName,
            string sourceToken,
            Encoding encoding,
            Action<int, string>? reportProgressAction,
            CancellationToken cancellationToken)
        {
            var fileStore = new XerDataStore();
            string filename = StringInternPool.Intern(fileName);
            int progressReportInterval = Math.Max(1, PerformanceConfig.ProgressReportIntervalLines);
            var localTables = new Dictionary<string, XerTable>(StringComparer.OrdinalIgnoreCase);
            XerTable? currentTable = null;
            int lineCount = 0;
            long fileSize = stream.CanSeek ? stream.Length : 0;
            long bytesRead = 0;
            int lastReportedProgress = 0;

            using var reader = new StreamReader(stream, encoding, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
            while (await reader.ReadLineAsync(cancellationToken) is { } line)
            {
                cancellationToken.ThrowIfCancellationRequested();
                bytesRead += line.Length + Environment.NewLine.Length;
                lineCount++;

                if (lineCount % progressReportInterval == 0)
                {
                    if (fileSize > 0)
                    {
                        int progress = Math.Min(100, (int)((double)bytesRead * 100 / fileSize));
                        if (progress > lastReportedProgress)
                        {
                            reportProgressAction?.Invoke(progress, $"Parsing {filename}: {progress}%");
                            lastReportedProgress = progress;
                        }
                    }

                    // Task.Delay is deliberate: Task.Yield alone can repeatedly queue microtasks and starve
                    // browser input events on WebAssembly.
                    await Task.Delay(1, cancellationToken);
                }

                ParseXerContentLine(line, filename, sourceToken, localTables, ref currentTable);
            }

            foreach ((string _, XerTable table) in localTables)
                if (table.Headers is not null) fileStore.AddTable(table);

            reportProgressAction?.Invoke(100, $"Finished parsing {filename}");
            return fileStore;
        }

        private static void ParseXerContentLine(
            string line,
            string filename,
            string sourceToken,
            Dictionary<string, XerTable> localTables,
            ref XerTable? currentTable)
        {
            if (string.IsNullOrWhiteSpace(line)) return;
            ReadOnlySpan<char> lineSpan = line.AsSpan();
            if (lineSpan.Length < 2 || lineSpan[0] != '%') return;

            switch (lineSpan[1])
            {
                case 'T':
                    ReadOnlySpan<char> tableNameSpan = lineSpan[2..].Trim();
                    if (!tableNameSpan.IsEmpty)
                    {
                        string currentTableName = StringInternPool.Intern(tableNameSpan);
                        StandardExportPublication.ValidateTableName(currentTableName);
                        currentTable = new XerTable(currentTableName);
                        localTables[currentTable.Name] = currentTable;
                    }
                    break;
                case 'F':
                    if (currentTable != null)
                    {
                        ReadOnlySpan<char> fieldsLine = lineSpan[2..];
                        if (!fieldsLine.IsEmpty && fieldsLine[0] == Delimiter) fieldsLine = fieldsLine[1..];
                        string[] headers = FastSplitAndIntern(fieldsLine, Delimiter, trim: true);
                        XerSourceSchema.ValidateHeaders(currentTable.Name, headers);
                        currentTable.SetHeaders(headers);
                    }
                    break;
                case 'R':
                    if (currentTable?.Headers is not null)
                    {
                        ReadOnlySpan<char> dataLine = lineSpan[2..];
                        if (!dataLine.IsEmpty && dataLine[0] == Delimiter) dataLine = dataLine[1..];
                        currentTable.AddRow(new DataRow(
                            FastSplitAndIntern(dataLine, Delimiter, trim: false), filename, sourceToken));
                    }
                    break;
            }
        }

        private XerDataStore ParseXerStreamWithEncoding(
            Stream stream,
            string fileName,
            string sourceToken,
            Encoding encoding,
            Action<int, string>? reportProgressAction,
            CancellationToken cancellationToken)
        {
            var fileStore = new XerDataStore();
            string filename = StringInternPool.Intern(fileName);
            int progressReportInterval = Math.Max(1, PerformanceConfig.ProgressReportIntervalLines);

            var localTables = new Dictionary<string, XerTable>(StringComparer.OrdinalIgnoreCase);
            XerTable? currentTable = null;
            int lineCount = 0;

            long fileSize = stream.CanSeek ? stream.Length : 0;
            long bytesRead = 0;
            int lastReportedProgress = 0;

            using var reader = new StreamReader(stream, encoding, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
            string? line;

            while ((line = reader.ReadLine()) != null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                bytesRead += line.Length + Environment.NewLine.Length;
                lineCount++;

                if (lineCount % progressReportInterval == 0 && fileSize > 0)
                {
                    int progress = Math.Min(100, (int)((double)bytesRead * 100 / fileSize));
                    if (progress > lastReportedProgress)
                    {
                        reportProgressAction?.Invoke(progress, $"Parsing {filename}: {progress}%");
                        lastReportedProgress = progress;
                    }
                }

                if (string.IsNullOrWhiteSpace(line)) continue;
                ReadOnlySpan<char> lineSpan = line.AsSpan();
                if (lineSpan.Length < 2 || lineSpan[0] != '%') continue;

                char typeChar = lineSpan[1];
                switch (typeChar)
                {
                    case 'T':
                        ReadOnlySpan<char> tableNameSpan = lineSpan[2..].Trim();
                        if (!tableNameSpan.IsEmpty)
                        {
                            string currentTableName = StringInternPool.Intern(tableNameSpan);
                            StandardExportPublication.ValidateTableName(currentTableName);
                            currentTable = new XerTable(currentTableName);
                            localTables[currentTable.Name] = currentTable;
                        }
                        break;
                    case 'F':
                        if (currentTable != null)
                        {
                            ReadOnlySpan<char> fieldsLine = lineSpan[2..];
                            if (!fieldsLine.IsEmpty && fieldsLine[0] == Delimiter) fieldsLine = fieldsLine[1..];
                            string[] headers = FastSplitAndIntern(fieldsLine, Delimiter, trim: true);
                            XerSourceSchema.ValidateHeaders(currentTable.Name, headers);
                            currentTable.SetHeaders(headers);
                        }
                        break;
                    case 'R':
                        if (currentTable != null && currentTable.Headers is not null)
                        {
                            ReadOnlySpan<char> dataLine = lineSpan[2..];
                            if (!dataLine.IsEmpty && dataLine[0] == Delimiter) dataLine = dataLine[1..];
                            string[] values = FastSplitAndIntern(dataLine, Delimiter, trim: false);
                            currentTable.AddRow(new DataRow(values, filename, sourceToken));
                        }
                        break;
                }
            }

            foreach (var kvp in localTables)
            {
                if (kvp.Value.Headers is not null)
                {
                    fileStore.AddTable(kvp.Value);
                }
            }

            reportProgressAction?.Invoke(100, $"Finished parsing {filename}");
            return fileStore;
        }

        // File, synchronous stream and cooperative browser parsing share the same strict decoder.
        public XerDataStore ParseXerFile(string xerFilePath, Action<int, string>? reportProgressAction, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(xerFilePath);
            using var stream = new FileStream(xerFilePath, new FileStreamOptions
            {
                Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.Read,
                BufferSize = PerformanceConfig.FileReadBufferSize, Options = FileOptions.SequentialScan
            });
            return ParseXerStream(stream, Path.GetFileName(xerFilePath), reportProgressAction, cancellationToken);
        }



        // Optimized string splitting and interning implementation using Span<T> ranges.
        private static int FastSplit(ReadOnlySpan<char> text, char delimiter, Span<Range> destination)
        {
            int count = 0;
            int start = 0;

            for (int i = 0; i < text.Length; i++)
            {
                if (text[i] == delimiter)
                {
                    destination[count++] = new Range(start, i);
                    start = i + 1;
                }
            }

            destination[count++] = new Range(start, text.Length);
            return count;
        }

        private static string[] FastSplitAndIntern(ReadOnlySpan<char> text, char delimiter, bool trim)
        {
            if (text.IsEmpty) return Array.Empty<string>();

            int segmentCount = 1;
            for (int i = 0; i < text.Length; i++)
            {
                if (text[i] == delimiter) segmentCount++;
            }

            Range[]? rented = null;
            Span<Range> ranges = segmentCount <= 128
                ? stackalloc Range[segmentCount]
                : (rented = ArrayPool<Range>.Shared.Rent(segmentCount)).AsSpan(0, segmentCount);

            try
            {
                int actualCount = FastSplit(text, delimiter, ranges);
                string[] result = new string[actualCount];

                for (int i = 0; i < actualCount; i++)
                {
                    ReadOnlySpan<char> segment = text[ranges[i]];
                    if (trim) segment = segment.Trim();
                    result[i] = StringInternPool.Intern(segment);
                }

                return result;
            }
            finally
            {
                if (rented != null)
                {
                    ArrayPool<Range>.Shared.Return(rented);
                }
            }
        }
    }



    // Handles exporting data to CSV format
    public class CsvExporter
    {
        // .NET 8: SearchValues<char> uses SIMD vectorization for fast character searching
        private static readonly SearchValues<char> CsvSpecialChars = SearchValues.Create(",\r\n\"");

        // PERFORMANCE OPTIMIZATION: Removed explicit batching (List<string>), relying solely on StreamWriter buffering
        public void WriteTableToCsv(XerTable? table, string csvFilePath)
        {
            if (table == null || table.Headers == null) return;
            if (table.IsEmpty && table.Headers.Length == 0) return;

            int bufferSize = PerformanceConfig.CsvWriteBufferSize;

            try
            {
                // Use buffered FileStream and StreamWriter
                var fileOptions = new FileStreamOptions
                {
                    Mode = FileMode.Create,
                    Access = FileAccess.Write,
                    Share = FileShare.None,
                    BufferSize = bufferSize,
                    Options = FileOptions.SequentialScan
                };
                using var fileStream = new FileStream(csvFilePath, fileOptions);
                // Use UTF8 encoding for output
                using var writer = new StreamWriter(fileStream, Encoding.UTF8, bufferSize);
                {
                    string[] headerRow = table.Headers;

                    // Write Headers
                    for (int i = 0; i < headerRow.Length; i++)
                    {
                        if (i > 0) writer.Write(',');
                        WriteEscapedField(writer, headerRow[i]);
                    }

                    // Add FileName column
                    if (headerRow.Length > 0) writer.Write(',');
                    WriteEscapedField(writer, FieldNames.FileName);
                    writer.WriteLine();

                    // Write Data Rows
                    foreach (var dataRow in table.Rows)
                    {
                        string[] rowFields = dataRow.Fields;
                        if (rowFields == null) continue;

                        for (int j = 0; j < rowFields.Length; j++)
                        {
                            if (j > 0) writer.Write(',');
                            WriteEscapedField(writer, rowFields[j] ?? string.Empty);
                        }

                        // Add FileName data
                        if (rowFields.Length > 0) writer.Write(',');
                        WriteEscapedField(writer, dataRow.OriginalSourceFilename);
                        writer.WriteLine();
                    }

                    writer.Flush();
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to write CSV file '{Path.GetFileName(csvFilePath)}'. {ex.Message}", ex);
            }
        }

        // Writes table to a Stream (for in-memory / Blazor scenarios)
        public void WriteTableToStream(XerTable? table, Stream outputStream)
        {
            if (table == null || table.Headers == null) return;
            if (table.IsEmpty && table.Headers.Length == 0) return;

            using var writer = new StreamWriter(outputStream, Encoding.UTF8, bufferSize: PerformanceConfig.CsvWriteBufferSize, leaveOpen: true);

            string[] headerRow = table.Headers;
            for (int i = 0; i < headerRow.Length; i++)
            {
                if (i > 0) writer.Write(',');
                WriteEscapedField(writer, headerRow[i]);
            }
            if (headerRow.Length > 0) writer.Write(',');
            WriteEscapedField(writer, FieldNames.FileName);
            writer.WriteLine();

            foreach (var dataRow in table.Rows)
            {
                string[] rowFields = dataRow.Fields;
                if (rowFields == null) continue;
                for (int j = 0; j < rowFields.Length; j++)
                {
                    if (j > 0) writer.Write(',');
                    WriteEscapedField(writer, rowFields[j] ?? string.Empty);
                }
                if (rowFields.Length > 0) writer.Write(',');
                WriteEscapedField(writer, dataRow.OriginalSourceFilename);
                writer.WriteLine();
            }
            writer.Flush();
        }

        // Efficient CSV field escaping writing directly to the stream to avoid allocations
        private static void WriteEscapedField(TextWriter writer, string field)
        {
            if (string.IsNullOrEmpty(field)) return;

            // .NET 8: Use SearchValues with SIMD vectorization for fast special character detection
            ReadOnlySpan<char> fieldSpan = field.AsSpan();
            int specialIndex = fieldSpan.IndexOfAny(CsvSpecialChars);

            // Check for leading/trailing whitespace
            bool hasLeadingTrailingSpace = fieldSpan.Length > 0 && (fieldSpan[0] == ' ' || fieldSpan[^1] == ' ');

            if (specialIndex < 0 && !hasLeadingTrailingSpace)
            {
                writer.Write(field);
                return;
            }

            // Escape internal quotes and wrap the field
            writer.Write('"');

            // If we have no quotes, we can construct the quoted string faster
            if (!fieldSpan.Contains('"'))
            {
                writer.Write(field);
            }
            else
            {
                // Process character by character for quotes
                foreach (char c in fieldSpan)
                {
                    if (c == '"') writer.Write("\"\""); // Double up quotes
                    else writer.Write(c);
                }
            }

            writer.Write('"');
        }
    }



    // Handles data transformation and creation of enhanced tables (e.g., for Power BI)

    // Handles data transformation and creation of enhanced tables (e.g., for Power BI)

    public partial class XerTransformer
    {
        private readonly XerDataStore _dataStore;

        // .NET 8: FrozenSet for O(1) lookups on static field sets (faster than HashSet for reads)
        private static readonly FrozenSet<string> HandledTaskFields = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
            FieldNames.StatusCode, FieldNames.Start, FieldNames.Finish, FieldNames.IdName,
            FieldNames.RemainingDuration, FieldNames.OriginalDuration, FieldNames.TotalFloat, FieldNames.FreeFloat,
            FieldNames.PercentComplete, FieldNames.DataDate,
            FieldNames.ActStartDate, FieldNames.ActEndDate,
            FieldNames.EarlyStartDate, FieldNames.EarlyEndDate,
            FieldNames.LateStartDate, FieldNames.LateEndDate,
            FieldNames.CstrDate,
            FieldNames.TargetStartDate, FieldNames.TargetEndDate,
            FieldNames.WbsIdKey, FieldNames.TaskIdKey, FieldNames.CalendarIdKey, FieldNames.ProjIdKey,
            FieldNames.MonthUpdate
        }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

        // Regex to extract YYMM from filename for MonthUpdate column
        [GeneratedRegex("""^(\d{4})""", RegexOptions.Singleline)]
        private static partial Regex MonthUpdateRegex();


        public XerTransformer(XerDataStore dataStore)

        {

            _dataStore = dataStore;

        }



        // New helper method to parse YYMM from filename

        private string ParseMonthUpdateFromFilename(string filename)

        {

            if (string.IsNullOrWhiteSpace(filename)) return string.Empty;



            var match = MonthUpdateRegex().Match(filename.Trim());


            if (match.Success)

            {

                ReadOnlySpan<char> yymm = match.Groups[1].Value.AsSpan();
                // The regex already ensures yymm is 4 digits.
                if (yymm.Length == 4 &&
                    int.TryParse(yymm[..2], out int year) &&
                    int.TryParse(yymm.Slice(2, 2), out int month))
                {
                    // Assuming 2-digit year 'yy' is in the 21st century (20xx)

                    year += 2000;



                    if (month >= 1 && month <= 12)

                    {

                        try

                        {

                            var date = new DateTime(year, month, 1);

                            return date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

                        }

                        catch (ArgumentOutOfRangeException)

                        {

                            return string.Empty; // Handle invalid parsed dates

                        }

                    }

                }

            }

            return string.Empty;

        }



        private bool IsTableValid([NotNullWhen(true)] XerTable? table) => table != null && !table.IsEmpty && table.Headers != null;


        // Creates a unique key combining filename and value (used for cross-file references)

        private static string CreateKey(string filename, string value)

        {

            if (string.IsNullOrEmpty(filename) || string.IsNullOrEmpty(value)) return string.Empty;

            string key = $"{filename.Trim()}.{value.Trim()}";

            return StringInternPool.Intern(key);

        }



        private static string GetFieldValue(string[] row, IReadOnlyDictionary<string, int> indexes, string fieldName)

        {

            if (row == null || indexes == null) return "";

            if (indexes.TryGetValue(fieldName, out int index) && index >= 0 && index < row.Length)

            {

                return row[index] ?? "";

            }

            return "";

        }



        // PERFORMANCE OPTIMIZATION: Optimized field setting using O(1) dictionary lookup.

        // Replaced the original implementation using Array.IndexOf (O(N)) with this optimized version.

        private void SetTransformedField(string[] transformedRow, IReadOnlyDictionary<string, int> finalIndexes, string fieldName, string value)

        {

            // Use TryGetValue for fast, safe lookup

            if (finalIndexes.TryGetValue(fieldName, out int index))

            {

                transformedRow[index] = value ?? string.Empty;

            }

        }



        // Creates the enhanced TASK table (01_XER_TASK)

        internal static readonly string[] TaskColumns01 = {

                FieldNames.TaskId, FieldNames.ProjectId, FieldNames.WbsId, FieldNames.CalendarId,

                FieldNames.TaskType, FieldNames.StatusCode, FieldNames.TaskCode, FieldNames.TaskName,

                FieldNames.RsrcId, FieldNames.ActStartDate, FieldNames.ActEndDate,

                FieldNames.EarlyStartDate, FieldNames.EarlyEndDate, FieldNames.LateStartDate, FieldNames.LateEndDate,

                FieldNames.TargetStartDate, FieldNames.TargetEndDate,

                FieldNames.CstrType, FieldNames.CstrDate, FieldNames.PriorityType, FieldNames.FloatPath,

                FieldNames.FloatPathOrder, FieldNames.DrivingPathFlag,
                FieldNames.RemainDurationHrCnt, FieldNames.PhysCompletePct,

                // Calculated Fields

                FieldNames.Start, FieldNames.Finish, FieldNames.IdName,

                FieldNames.RemainingDuration, FieldNames.OriginalDuration, FieldNames.TotalFloat, FieldNames.FreeFloat,

                FieldNames.PercentComplete, FieldNames.DataDate,

                // Key Fields

                FieldNames.WbsIdKey, FieldNames.TaskIdKey, FieldNames.CalendarIdKey, FieldNames.ProjIdKey,

                FieldNames.MonthUpdate

            };

        public XerTable? Create01XerTaskTable()
        {

            var taskTable = _dataStore.GetTable(TableNames.Task);

            var calendarTable = _dataStore.GetTable(TableNames.Calendar);

            var projectTable = _dataStore.GetTable(TableNames.Project);



            if (!IsTableValid(taskTable) || !IsTableValid(calendarTable) || !IsTableValid(projectTable)) return null;



            try

            {

                var taskIndexes = taskTable.FieldIndexes;

                // Pre-build lookup tables

                var calendarHours = BuildCalendarHoursLookup(calendarTable);

                var projectDataDates = BuildProjectDataDatesLookup(projectTable);



                string[] finalColumns = TaskColumns01;



                // PERFORMANCE OPTIMIZATION: Pre-calculate target indexes for O(1) lookups in the parallel loop

                var finalIndexes = finalColumns

                    .Select((name, index) => new { name, index })

                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);





                var finalTable = new XerTable(EnhancedTableNames.XerTask01, taskTable.RowCount);

                finalTable.SetHeaders(finalColumns.Select(s => StringInternPool.Intern(s) ?? string.Empty).ToArray());



                const string TK_Complete = "TK_Complete";

                const string TK_NotStart = "TK_NotStart";

                const string TK_Active = "TK_Active";



                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };

                var transformedRows = new DataRow[taskTable.RowCount];



                Parallel.For(0, taskTable.RowCount, parallelOptions, rowIndex =>

                {
                    DataRow sourceRowData = taskTable.Rows[rowIndex];

                    string[] row = sourceRowData.Fields;

                    string[] transformed = new string[finalColumns.Length];

                    string originalFilename = sourceRowData.SourceFilename;



                    // Extract key fields

                    string projId = GetFieldValue(row, taskIndexes, FieldNames.ProjectId);

                    string taskId = GetFieldValue(row, taskIndexes, FieldNames.TaskId);

                    string wbsId = GetFieldValue(row, taskIndexes, FieldNames.WbsId);

                    string clndrId = GetFieldValue(row, taskIndexes, FieldNames.CalendarId);

                    string statusCode = GetFieldValue(row, taskIndexes, FieldNames.StatusCode);

                    DateTime? actEndDate = DateParser.TryParse(GetFieldValue(row, taskIndexes, FieldNames.ActEndDate));



                    // Copy and format fields using optimized methods (passing finalIndexes)

                    CopyDirectFields(row, taskIndexes, transformed, finalIndexes);

                    FormatDateFields(row, taskIndexes, transformed, finalIndexes, actEndDate);



                    // Status Code mapping

                    SetTransformedField(transformed, finalIndexes, FieldNames.StatusCode,

                        StringInternPool.Intern(

                            statusCode == TK_Complete ? "Complete" :

                            statusCode == TK_NotStart ? "Not Started" :

                            statusCode == TK_Active ? "In Progress" : statusCode)

                        );



                    // Start/Finish calculation

                    DateTime startDate = CalculateStartDate(row, taskIndexes, statusCode);

                    DateTime finishDate = CalculateFinishDate(row, taskIndexes, statusCode);

                    SetTransformedField(transformed, finalIndexes, FieldNames.Start, DateParser.Format(startDate));

                    SetTransformedField(transformed, finalIndexes, FieldNames.Finish, DateParser.Format(finishDate));



                    // ID_Name concatenation

                    string taskCode = GetFieldValue(row, taskIndexes, FieldNames.TaskCode);

                    string taskName = GetFieldValue(row, taskIndexes, FieldNames.TaskName);

                    SetTransformedField(transformed, finalIndexes, FieldNames.IdName, $"{taskCode} - {taskName}");



                    // Duration/Float calculations (Hours to Days)

                    string calendarKey = CreateKey(originalFilename, clndrId);

                    if (!string.IsNullOrEmpty(clndrId) && calendarHours.TryGetValue((sourceRowData.SourceToken, clndrId.Trim()), out decimal dayHrCnt) && dayHrCnt > 0)

                    {

                        SetTransformedField(transformed, finalIndexes, FieldNames.RemainingDuration,

                            CalculateDaysFromHours(row, taskIndexes, FieldNames.RemainDurationHrCnt, dayHrCnt, 2));

                        SetTransformedField(transformed, finalIndexes, FieldNames.OriginalDuration,

                            CalculateDaysFromHours(row, taskIndexes, FieldNames.TargetDurationHrCnt, dayHrCnt, 2));



                        if (statusCode != TK_Complete)

                        {

                            SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat,

                                CalculateDaysFromHours(row, taskIndexes, FieldNames.TotalFloatHrCnt, dayHrCnt, 2));

                            SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloat,

                                CalculateDaysFromHours(row, taskIndexes, FieldNames.FreeFloatHrCnt, dayHrCnt, 2));

                        }

                        else

                        {

                            // Floats are typically null/empty when complete

                            SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat, "");

                            SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloat, "");

                        }

                    }

                    else

                    {

                        // Handle missing calendar or zero hours/day

                        SetTransformedField(transformed, finalIndexes, FieldNames.RemainingDuration, "");

                        SetTransformedField(transformed, finalIndexes, FieldNames.OriginalDuration, "");

                        SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat, "");

                        SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloat, "");

                    }



                    // Percentage Complete calculation

                    decimal? pct = CalculateCompletionPercentage(row, taskIndexes, statusCode);

                    SetTransformedField(transformed, finalIndexes, FieldNames.PercentComplete,

                        pct?.ToString("F2", CultureInfo.InvariantCulture) ?? "");



                    // Data Date lookup

                    string projectKey = CreateKey(originalFilename, projId);

                    if (!string.IsNullOrEmpty(projId) && projectDataDates.TryGetValue((sourceRowData.SourceToken, projId.Trim()), out DateTime dataDateValue))

                    {

                        SetTransformedField(transformed, finalIndexes, FieldNames.DataDate, DateParser.Format(dataDateValue));

                    }

                    else

                    {

                        SetTransformedField(transformed, finalIndexes, FieldNames.DataDate, "");

                    }



                    // Key generation

                    SetTransformedField(transformed, finalIndexes, FieldNames.WbsIdKey, CreateKey(originalFilename, wbsId));

                    SetTransformedField(transformed, finalIndexes, FieldNames.TaskIdKey, CreateKey(originalFilename, taskId));

                    SetTransformedField(transformed, finalIndexes, FieldNames.CalendarIdKey, CreateKey(originalFilename, clndrId));

                    SetTransformedField(transformed, finalIndexes, FieldNames.ProjIdKey, CreateKey(originalFilename, projId));



                    // Add MonthUpdate value

                    SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(sourceRowData.OriginalSourceFilename));



                    // Intern all resulting strings in the transformed row

                    for (int k = 0; k < transformed.Length; k++)

                    {

                        transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);

                    }



                    transformedRows[rowIndex] = sourceRowData.WithFields(transformed);

                });



                finalTable.AddRows(transformedRows);

                return finalTable;

            }

            catch (Exception ex)

            {

                // Log error (consider using a formal logging framework)

                Console.WriteLine($"Error creating {EnhancedTableNames.XerTask01}: {ex.Message}\n{ex.StackTrace}");

                return null;

            }

        }



        // PERFORMANCE OPTIMIZATION: Updated to use dictionary for target indexes (O(1) lookup)
        // Uses static FrozenSet (HandledTaskFields) for faster field exclusion checks

        private void CopyDirectFields(string[] sourceRow, IReadOnlyDictionary<string, int> sourceIndexes, string[] targetRow, IReadOnlyDictionary<string, int> targetIndexes)

        {

            // Iterate over the target dictionary keys/values

            foreach (var kvp in targetIndexes)

            {

                string colName = kvp.Key;

                int targetIndex = kvp.Value;



                // If the source has the column and it's not handled elsewhere, copy it
                // Uses static FrozenSet for O(1) lookup

                if (sourceIndexes.TryGetValue(colName, out int srcIndex) && !HandledTaskFields.Contains(colName))

                {

                    if (srcIndex < sourceRow.Length)

                    {

                        targetRow[targetIndex] = sourceRow[srcIndex];

                    }

                }

            }

        }



        // PERFORMANCE OPTIMIZATION: Updated to use dictionary for target indexes (O(1) lookup)

        private void FormatDateFields(string[] sourceRow, IReadOnlyDictionary<string, int> sourceIndexes, string[] transformedRow, IReadOnlyDictionary<string, int> targetIndexes, DateTime? actEndDate)

        {

            string[] directFormatCols = {

            FieldNames.CstrDate,

            FieldNames.TargetStartDate,

            FieldNames.TargetEndDate,

            FieldNames.ActStartDate,

            FieldNames.EarlyStartDate,

            FieldNames.EarlyEndDate,

            FieldNames.LateStartDate,

            FieldNames.LateEndDate

        };



            foreach (string colName in directFormatCols)

            {

                // Use dictionary lookup

                if (targetIndexes.TryGetValue(colName, out int targetIndex))

                {

                    string originalValue = GetFieldValue(sourceRow, sourceIndexes, colName);

                    transformedRow[targetIndex] = DateParser.Format(DateParser.TryParse(originalValue));

                }

            }



            // Handle ActEndDate specifically as it's pre-parsed

            if (targetIndexes.TryGetValue(FieldNames.ActEndDate, out int actEndIdxTarget))

            {

                transformedRow[actEndIdxTarget] = DateParser.Format(actEndDate);

            }

        }



        private Dictionary<(string Source, string Id), decimal> BuildCalendarHoursLookup(XerTable? table)
        {
            var result = new Dictionary<(string Source, string Id), decimal>();
            if (!IsTableValid(table)) return result;
            foreach (var group in table.Rows.GroupBy(row => (row.SourceToken,
                         GetFieldValue(row.Fields, table.FieldIndexes, FieldNames.ClndrId).Trim())))
            {
                if (group.Key.Item2.Length == 0 || group.Count() != 1) continue;
                string raw = GetFieldValue(group.Single().Fields, table.FieldIndexes, FieldNames.DayHourCount);
                if (decimal.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal hours) && hours > 0)
                    result.Add(group.Key, hours);
            }
            return result;
        }

        private Dictionary<(string Source, string Id), DateTime> BuildProjectDataDatesLookup(XerTable? table)
        {
            var result = new Dictionary<(string Source, string Id), DateTime>();
            if (!IsTableValid(table)) return result;
            foreach (var group in table.Rows.GroupBy(row => (row.SourceToken,
                         GetFieldValue(row.Fields, table.FieldIndexes, FieldNames.ProjectId).Trim())))
            {
                if (group.Key.Item2.Length == 0 || group.Count() != 1) continue;
                DateTime? date = DateParser.TryParse(GetFieldValue(group.Single().Fields,
                    table.FieldIndexes, FieldNames.LastRecalcDate));
                if (date.HasValue) result.Add(group.Key, date.Value);
            }
            return result;
        }

        // Calculation Logic Helpers

        private static DateTime? ReadPreferredDate(string[] row, IReadOnlyDictionary<string, int> indexes,
            string preferred, string fallback)
        {
            string raw = GetFieldValue(row, indexes, preferred);
            // Only absence permits fallback; malformed preferred values remain unknown.
            return DateParser.TryParse(string.IsNullOrWhiteSpace(raw) ? GetFieldValue(row, indexes, fallback) : raw);
        }

        private DateTime CalculateStartDate(string[] row, IReadOnlyDictionary<string, int> indexes, string statusCode) =>
            (statusCode.Trim().ToUpperInvariant() switch
            {
                "TK_NOTSTART" => ReadPreferredDate(row, indexes, FieldNames.RestartDate, FieldNames.EarlyStartDate),
                "TK_ACTIVE" or "TK_COMPLETE" => DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.ActStartDate)),
                _ => null
            }) ?? DateTime.MinValue;

        private DateTime CalculateFinishDate(string[] row, IReadOnlyDictionary<string, int> indexes, string statusCode) =>
            (statusCode.Trim().ToUpperInvariant() switch
            {
                "TK_COMPLETE" => DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.ActEndDate)),
                "TK_NOTSTART" or "TK_ACTIVE" => ReadPreferredDate(row, indexes, FieldNames.ReendDate, FieldNames.EarlyEndDate),
                _ => null
            }) ?? DateTime.MinValue;

        private string CalculateDaysFromHours(string[] row, IReadOnlyDictionary<string, int> indexes, string hourFieldName, decimal hoursPerDay, int decimalPlaces = 1)

        {

            if (hoursPerDay <= 0) return "";



            string hourStr = GetFieldValue(row, indexes, hourFieldName);

            if (decimal.TryParse(hourStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal hours))

            {

                decimal days = hours / hoursPerDay;

                return days.ToString($"F{decimalPlaces}", CultureInfo.InvariantCulture);

            }

            return "";

        }



        private decimal? CalculateCompletionPercentage(string[] row, IReadOnlyDictionary<string, int> indexes, string statusCode)
        {
            if (string.Equals(statusCode, "TK_Complete", StringComparison.OrdinalIgnoreCase)) return 100m;
            if (string.Equals(statusCode, "TK_NotStart", StringComparison.OrdinalIgnoreCase)) return 0m;
            if (!string.Equals(statusCode, "TK_Active", StringComparison.OrdinalIgnoreCase)) return null;

            decimal? ReadNumber(string field, bool optional = false)
            {
                string raw = GetFieldValue(row, indexes, field);
                if (optional && string.IsNullOrWhiteSpace(raw)) return 0m;
                return decimal.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal number)
                    && number >= 0 ? number : null;
            }
            try
            {
                switch (GetFieldValue(row, indexes, FieldNames.CompletePctType).Trim().ToUpperInvariant())
                {
                    case "CP_PHYS":
                        decimal? physical = ReadNumber(FieldNames.PhysCompletePct);
                        return physical.HasValue ? Math.Clamp(physical.Value, 0m, 100m) : null;
                    case "CP_UNITS":
                        decimal? actual = ReadNumber(FieldNames.ActWorkQty, true) + ReadNumber("act_equip_qty", true);
                        decimal? remaining = ReadNumber(FieldNames.RemainWorkQty, true) + ReadNumber("remain_equip_qty", true);
                        if (!actual.HasValue || !remaining.HasValue) return null;
                        decimal total = actual.Value + remaining.Value;
                        return total == 0 ? 0m : Math.Clamp(actual.Value / total * 100m, 0m, 100m);
                    case "CP_DRTN":
                        decimal? target = ReadNumber(FieldNames.TargetDurationHrCnt);
                        decimal? remainingDuration = ReadNumber(FieldNames.RemainDurationHrCnt);
                        if (!target.HasValue || !remainingDuration.HasValue || target.Value == 0) return null;
                        return Math.Clamp((1m - remainingDuration.Value / target.Value) * 100m, 0m, 100m);
                    default: return null;
                }
            }
            catch (OverflowException) { return null; }
        }

        // Creates the Baseline table (04_XER_BASELINE) by finding the earliest MonthUpdate snapshot

        public XerTable? Create04XerBaselineTable(XerTable task01Table)
        {

            if (!IsTableValid(task01Table)) return null;



            try

            {

                var baselineTable = new XerTable(EnhancedTableNames.XerBaseline04, task01Table.RowCount);

                if (task01Table.Headers is null)
                {
                    return null;
                }
                baselineTable.SetHeaders(task01Table.Headers);



                // --- CHANGE: Use MonthUpdate instead of DataDate ---

                if (!task01Table.FieldIndexes.TryGetValue(FieldNames.MonthUpdate, out int monthUpdateIndex)) return null;



                // Find the minimum MonthUpdate date across all rows

                DateTime? minMonthUpdate = null;

                foreach (var rowData in task01Table.Rows)

                {

                    // --- CHANGE: Get value from MonthUpdate column ---

                    var currentDate = DateParser.TryParse(XerTable.GetFieldValueSafe(rowData, monthUpdateIndex));

                    if (currentDate.HasValue)

                    {

                        // Compare only the Date part, ignoring time

                        if (!minMonthUpdate.HasValue || currentDate.Value.Date < minMonthUpdate.Value.Date)

                        {

                            minMonthUpdate = currentDate.Value.Date;

                        }

                    }

                }



                if (!minMonthUpdate.HasValue)
                {
                    var empty = new XerTable(EnhancedTableNames.XerBaseline04);
                    empty.SetHeaders(task01Table.Headers!.ToArray());
                    return empty;
                }



                // Filter rows matching the minimum MonthUpdate Date

                foreach (var sourceRow in task01Table.Rows)

                {

                    // --- CHANGE: Get value from MonthUpdate column for comparison ---

                    var rowDate = DateParser.TryParse(XerTable.GetFieldValueSafe(sourceRow, monthUpdateIndex));

                    if (rowDate.HasValue && rowDate.Value.Date == minMonthUpdate.Value)

                    {

                        baselineTable.AddRow(sourceRow);

                    }

                }



                return baselineTable;

            }

            catch (Exception ex)

            {

                Console.WriteLine($"Error creating {EnhancedTableNames.XerBaseline04}: {ex.Message}");

                return null;

            }

        }



        // Creates the enhanced PROJWBS table (03_XER_PROJWBS)

        public XerTable? Create03XerProjWbsTable()
        {
            XerTable? source = _dataStore.GetTable(TableNames.ProjWbs);
            if (!IsTableValid(source)) return null;
            try
            {
                var indexes = source.FieldIndexes;
                var nodes = new Dictionary<(string Source, string Id), DataRow>();
                foreach (DataRow row in source.Rows)
                {
                    string id = GetFieldValue(row.Fields, indexes, FieldNames.WbsId).Trim();
                    if (id.Length == 0 || !nodes.TryAdd((row.SourceToken, id), row))
                        throw new InvalidDataException($"Source '{row.SourceToken}' has a blank or duplicate PROJWBS.wbs_id '{id}'.");
                }
                var parents = new Dictionary<(string Source, string Id), (string Source, string Id)>();
                foreach (var pair in nodes)
                {
                    string parent = GetFieldValue(pair.Value.Fields, indexes, FieldNames.ParentWbsId).Trim();
                    var parentKey = (pair.Key.Source, parent);
                    if (parent.Length == 0 || !nodes.TryGetValue(parentKey, out DataRow parentRow)) continue;
                    string project = GetFieldValue(pair.Value.Fields, indexes, FieldNames.ProjectId).Trim();
                    string parentProject = GetFieldValue(parentRow.Fields, indexes, FieldNames.ProjectId).Trim();
                    if (project != parentProject)
                        throw new InvalidDataException($"Source '{pair.Key.Source}' WBS '{pair.Key.Id}' has a parent in another project.");
                    parents.Add(pair.Key, parentKey);
                }
                var complete = new HashSet<(string Source, string Id)>();
                foreach (var key in nodes.Keys)
                {
                    var path = new HashSet<(string Source, string Id)>();
                    var current = key;
                    while (!complete.Contains(current))
                    {
                        if (!path.Add(current))
                            throw new InvalidDataException($"Source '{current.Source}' has a PROJWBS parent cycle at '{current.Id}'.");
                        if (!parents.TryGetValue(current, out current)) break;
                    }
                    complete.UnionWith(path);
                }

                string[] headers = source.Headers!.Concat(new[]
                    { FieldNames.WbsIdKey, FieldNames.ParentWbsIdKey, FieldNames.MonthUpdate }).ToArray();
                var result = new XerTable(EnhancedTableNames.XerProjWbs03, source.RowCount);
                result.SetHeaders(headers);
                foreach (DataRow row in source.Rows)
                {
                    string id = GetFieldValue(row.Fields, indexes, FieldNames.WbsId).Trim();
                    string[] values = new string[headers.Length];
                    Array.Copy(row.Fields, values, row.Fields.Length);
                    values[^3] = CreateKey(row.SourceFilename, id);
                    // Preserve legacy missing-parent clearing; present invalid ancestry fails.
                    values[^2] = parents.TryGetValue((row.SourceToken, id), out var parent)
                        ? CreateKey(row.SourceFilename, parent.Id) : "";
                    values[^1] = ParseMonthUpdateFromFilename(row.OriginalSourceFilename);
                    result.AddRow(row.WithFields(values));
                }
                return result;
            }
            catch (InvalidDataException ex)
            {
                Console.WriteLine($"Error creating {EnhancedTableNames.XerProjWbs03}: {ex.Message}");
                return null;
            }
        }

        // Generic method for creating simple enhanced tables that just add keys

        private XerTable? CreateSimpleKeyedTable(string sourceTableName, string newTableName, List<Tuple<string, string>> keyMappings)
        {

            var sourceTable = _dataStore.GetTable(sourceTableName);

            if (!IsTableValid(sourceTable)) return null;



            try

            {

                if (sourceTable.Headers is not { } sourceHeaders)
                {
                    return null;
                }

                var sourceIndexes = sourceTable.FieldIndexes;
                var finalHeadersList = sourceHeaders.ToList();

                foreach (var mapping in keyMappings)

                {

                    finalHeadersList.Add(mapping.Item1);

                }

                finalHeadersList.Add(FieldNames.MonthUpdate);

                string[] finalHeaders = finalHeadersList.Select(s => StringInternPool.Intern(s) ?? string.Empty).ToArray();



                // PERFORMANCE OPTIMIZATION: Pre-calculate indexes

                var finalIndexes = finalHeaders

                    .Select((name, index) => new { name, index })

                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);





                var resultTable = new XerTable(newTableName, sourceTable.RowCount);

                resultTable.SetHeaders(finalHeaders);



                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };

                var transformedRows = new DataRow[sourceTable.RowCount];



                Parallel.For(0, sourceTable.RowCount, parallelOptions, rowIndex =>

                {
                    DataRow sourceRow = sourceTable.Rows[rowIndex];

                    var row = sourceRow.Fields;

                    string[] transformed = new string[finalHeaders.Length];

                    string originalFilename = sourceRow.SourceFilename;



                    // Copy existing fields

                    int copyLength = Math.Min(row.Length, sourceHeaders.Length);

                    Array.Copy(row, transformed, copyLength);



                    // Generate and set keys

                    foreach (var mapping in keyMappings)

                    {

                        // Check if the source field exists before attempting to access it

                        if (sourceIndexes.ContainsKey(mapping.Item2))

                        {

                            string sourceValue = GetFieldValue(row, sourceIndexes, mapping.Item2);

                            string key = CreateKey(originalFilename, sourceValue);

                            // Use optimized method

                            SetTransformedField(transformed, finalIndexes, mapping.Item1, key);

                        }

                    }



                    // Add MonthUpdate value

                    SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(sourceRow.OriginalSourceFilename));



                    // Intern strings

                    for (int k = 0; k < transformed.Length; k++)

                    {

                        transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);

                    }



                    transformedRows[rowIndex] = sourceRow.WithFields(transformed);

                });



                resultTable.AddRows(transformedRows);

                return resultTable;

            }

            catch (Exception ex)

            {

                Console.WriteLine($"Error creating {newTableName}: {ex.Message}");

                return null;

            }

        }



        // Definitions for simple keyed tables using the generic method

        public XerTable? Create02XerProject() => CreateSimpleKeyedTable(TableNames.Project, EnhancedTableNames.XerProject02,
            new List<Tuple<string, string>> { Tuple.Create(FieldNames.ProjIdKey, FieldNames.ProjectId) });



        public XerTable? Create06XerPredecessor(ConcurrentDictionary<string, XerTable> cache)
        {

            var taskPredTable = _dataStore.GetTable(TableNames.TaskPred);

            var taskTable = _dataStore.GetTable(TableNames.Task);

            var calendarTable = _dataStore.GetTable(TableNames.Calendar);

            // Get the SCHEDOPTIONS table

            var schedOptionsTable = _dataStore.GetTable("SCHEDOPTIONS");



            // We no longer read from 11_DETAILED_CALENDAR cache,

            // BuildRelationshipCalendars reads from the raw CALENDAR table.



            if (!IsTableValid(taskPredTable)) return null;



            try

            {

                if (taskPredTable.Headers is not { } sourceHeaders)
                {
                    return null;
                }

                var sourceIndexes = taskPredTable.FieldIndexes;

                // Define all output columns (existing + new calculated columns)
                var finalHeadersList = sourceHeaders.ToList();

                finalHeadersList.Add(FieldNames.TaskIdKey);

                finalHeadersList.Add(FieldNames.PredTaskIdKey);

                finalHeadersList.Add(FieldNames.CalendarIdKey); // Successor's calendar (used for float calculation)

                finalHeadersList.Add(FieldNames.PredecessorClndrIdKey); // Predecessor's calendar (used for lag conversion)

                finalHeadersList.Add(FieldNames.StatusCode); // Successor's status

                finalHeadersList.Add(FieldNames.PredecessorStatusCode); // Predecessor's status

                finalHeadersList.Add(FieldNames.TaskType); // Successor's task type

                finalHeadersList.Add(FieldNames.PredecessorTaskType); // Predecessor's task type

                finalHeadersList.Add(FieldNames.Lag); // This will remain in DAYS, but calculated from hours

                finalHeadersList.Add(FieldNames.TimePeriodHoursPerDay);

                finalHeadersList.Add(FieldNames.Start);

                finalHeadersList.Add(FieldNames.Finish);

                finalHeadersList.Add(FieldNames.PredecessorStart);

                finalHeadersList.Add(FieldNames.PredecessorFinish);

                finalHeadersList.Add(FieldNames.PredecessorFreeFloat); // This is the new 'free_float' in DAYS

                finalHeadersList.Add(FieldNames.TotalFloat);

                finalHeadersList.Add(FieldNames.MonthUpdate);

                string[] finalHeaders = finalHeadersList.Select(s => StringInternPool.Intern(s) ?? string.Empty).ToArray();



                // Pre-calculate indexes for O(1) lookups

                var finalIndexes = finalHeaders

                    .Select((name, index) => new { name, index })

                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);



                var resultTable = new XerTable(EnhancedTableNames.XerPredecessor06, taskPredTable.RowCount);

                resultTable.SetHeaders(finalHeaders);



                // Build lookup dictionaries

                var taskLookup = BuildTaskLookupDictionary(taskTable); // Includes ActualStartDate

                var calendarHoursLookup = BuildCalendarHoursLookup(calendarTable); // Standard Hours/Day

                var relationshipCalendars = BuildRelationshipCalendars();

                // Strict relationship-only inputs leave the shared task/display/resource lookups unchanged.
                var relationshipTasks = BuildRelationshipTaskLookup(taskTable);

                var schedOptionsLookup = BuildProjectScheduleOptionsLookup(schedOptionsTable);



                // Build Total Float Lookup from 01_XER_TASK if available





                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };

                var transformedRows = new DataRow[taskPredTable.RowCount];



                Parallel.For(0, taskPredTable.RowCount, parallelOptions, rowIndex =>

                {
                    DataRow sourceRow = taskPredTable.Rows[rowIndex];

                    var row = sourceRow.Fields;

                    string[] transformed = new string[finalHeaders.Length];

                    string originalFilename = sourceRow.SourceFilename;



                    // Copy existing fields

                    int copyLength = Math.Min(row.Length, sourceHeaders.Length);

                    Array.Copy(row, transformed, copyLength);



                    // Generate keys

                    string taskId = GetFieldValue(row, sourceIndexes, FieldNames.TaskId);

                    string predTaskId = GetFieldValue(row, sourceIndexes, FieldNames.PredTaskId);

                    TaskData succTask = ResolveDisplayTask(taskLookup, sourceRow.SourceToken, taskId,
                        GetFieldValue(row, sourceIndexes, FieldNames.ProjectId));
                    TaskData predTask = ResolveDisplayTask(taskLookup, sourceRow.SourceToken, predTaskId,
                        GetFieldValue(row, sourceIndexes, "pred_proj_id"));
                    // An unresolved external/ambiguous endpoint is not a local activity.
                    string taskIdKey = succTask.ProjectId is null ? "" : CreateKey(originalFilename, taskId);
                    string predTaskIdKey = predTask.ProjectId is null ? "" : CreateKey(originalFilename, predTaskId);



                    SetTransformedField(transformed, finalIndexes, FieldNames.TaskIdKey, taskIdKey);

                    SetTransformedField(transformed, finalIndexes, FieldNames.PredTaskIdKey, predTaskIdKey);



                    // Lookup successor task data

                    // Successor identity was resolved with its exported project context above.

                    // Lookup predecessor task data

                    // Predecessor identity was resolved with its exported project context above.



                    // --- FIX IS HERE ---

                    // Define the variables for the successor's and predecessor's calendar keys

                    string succClndrIdKey = succTask.ClndrIdKey ?? string.Empty;

                    string predClndrIdKey = predTask.ClndrIdKey ?? string.Empty;

                    // --- END FIX ---



                    // Set lookup columns

                    SetTransformedField(transformed, finalIndexes, FieldNames.CalendarIdKey, succClndrIdKey);

                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorClndrIdKey, predClndrIdKey);

                    SetTransformedField(transformed, finalIndexes, FieldNames.StatusCode, succTask.StatusCode ?? string.Empty);

                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorStatusCode, predTask.StatusCode ?? string.Empty);

                    SetTransformedField(transformed, finalIndexes, FieldNames.TaskType, succTask.TaskType ?? string.Empty);

                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorTaskType, predTask.TaskType ?? string.Empty);

                    SetTransformedField(transformed, finalIndexes, FieldNames.Start, DateParser.Format(succTask.DisplayStartDate));

                    SetTransformedField(transformed, finalIndexes, FieldNames.Finish, DateParser.Format(succTask.DisplayFinishDate));

                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorStart, DateParser.Format(predTask.DisplayStartDate));

                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorFinish, DateParser.Format(predTask.DisplayFinishDate));



                    // Calculate TimePeriod (hours per day) using PREDECESSOR's calendar

                    decimal hoursPerDay = 0;

                    if (!string.IsNullOrEmpty(predClndrIdKey) && calendarHoursLookup.TryGetValue((sourceRow.SourceToken, predTask.CalendarId ?? ""), out decimal hpd))

                    {

                        hoursPerDay = hpd;

                    }

                    SetTransformedField(transformed, finalIndexes, FieldNames.TimePeriodHoursPerDay, hoursPerDay > 0 ? hoursPerDay.ToString("F2", CultureInfo.InvariantCulture) : "");



                    // Get Lag in HOURS

                    string lagHrCntStr = GetFieldValue(row, sourceIndexes, FieldNames.LagHrCnt);

                    decimal lagHours = 0m;
                    bool validLag = string.IsNullOrWhiteSpace(lagHrCntStr)
                        || decimal.TryParse(lagHrCntStr, NumberStyles.Float, CultureInfo.InvariantCulture, out lagHours);



                    // Calculate Lag in DAYS for the 'Lag' column

                    string lagDays = !validLag ? "" : lagHours == 0 ? "0"
                        : FormatRelationshipDays(lagHours, hoursPerDay, "F2");

                    SetTransformedField(transformed, finalIndexes, FieldNames.Lag, lagDays);



                    // --- NEW: Get Successor's HPD for final conversion ---

                    decimal hoursPerDayForSuccessor = 0;

                    if (calendarHoursLookup.TryGetValue((sourceRow.SourceToken, succTask.CalendarId ?? ""), out decimal succHpd))

                    {

                        hoursPerDayForSuccessor = succHpd;

                    }

                    // Unresolved conversion factors remain unknown; never invent an eight-hour day.





                    // Calculate Free Float (passing lag in HOURS)

                    string freeFloatInDays = CalculateFreeFloat(sourceRow, sourceIndexes,
                        relationshipTasks, relationshipCalendars, schedOptionsLookup);

                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorFreeFloat, freeFloatInDays);



                    // --- NEW: Calculate Total Float for Successor ---

                    string totalFloatVal = "";



                    // Fallback to calculation if not found

                    if (!string.Equals(succTask.StatusCode, "TK_Complete", StringComparison.OrdinalIgnoreCase) &&

                        !string.IsNullOrEmpty(succTask.TotalFloatHrCnt) && hoursPerDayForSuccessor > 0)

                    {

                         if (decimal.TryParse(succTask.TotalFloatHrCnt, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal tfHours))

                         {

                             totalFloatVal = FormatRelationshipDays(tfHours, hoursPerDayForSuccessor, "F1");

                         }

                    }



                    SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat, totalFloatVal);



                    // Add MonthUpdate value

                    SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(sourceRow.OriginalSourceFilename));



                    // Intern strings

                    for (int k = 0; k < transformed.Length; k++)

                    {

                        transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);

                    }



                    transformedRows[rowIndex] = sourceRow.WithFields(transformed);

                });



                resultTable.AddRows(transformedRows);

                return resultTable;

            }

            catch (Exception ex)

            {

                Console.WriteLine($"Error creating {EnhancedTableNames.XerPredecessor06}: {ex.Message}");

                return null;

            }

        }



        private struct TaskData
        {
            public string? ProjectId;
            public string? CalendarId;
            public string? ClndrIdKey;
            public string? StatusCode;
            public string? TaskType;
            public DateTime? DisplayStartDate;
            public DateTime? DisplayFinishDate;
            public string? TotalFloatHrCnt;
        }

        private Dictionary<(string Source, string Task), TaskData[]> BuildTaskLookupDictionary(XerTable? table)
        {
            var result = new Dictionary<(string Source, string Task), TaskData[]>();
            if (!IsTableValid(table)) return result;
            foreach (var group in table.Rows.GroupBy(row => (row.SourceToken,
                         GetFieldValue(row.Fields, table.FieldIndexes, FieldNames.TaskId).Trim())))
            {
                if (group.Key.Item2.Length == 0) continue;
                result.Add(group.Key, group.Select(row =>
                {
                    string Read(string field) => GetFieldValue(row.Fields, table.FieldIndexes, field);
                    string status = Read(FieldNames.StatusCode).Trim();
                    DateTime start = CalculateStartDate(row.Fields, table.FieldIndexes, status);
                    DateTime finish = CalculateFinishDate(row.Fields, table.FieldIndexes, status);
                    return new TaskData
                    {
                        ProjectId = Read(FieldNames.ProjectId).Trim(),
                        CalendarId = Read(FieldNames.ClndrId).Trim(),
                        ClndrIdKey = CreateKey(row.SourceFilename, Read(FieldNames.ClndrId)),
                        StatusCode = status,
                        TaskType = Read(FieldNames.TaskType),
                        DisplayStartDate = start == DateTime.MinValue ? null : start,
                        DisplayFinishDate = finish == DateTime.MinValue ? null : finish,
                        TotalFloatHrCnt = Read(FieldNames.TotalFloatHrCnt)
                    };
                }).ToArray());
            }
            return result;
        }

        private static TaskData ResolveDisplayTask(Dictionary<(string Source, string Task), TaskData[]> lookup,
            string source, string taskId, string projectId)
        {
            if (!lookup.TryGetValue((source, taskId.Trim()), out var rows) || rows.Length != 1) return new();
            TaskData task = rows[0];
            if (string.IsNullOrWhiteSpace(task.ProjectId)
                || (!string.IsNullOrWhiteSpace(projectId) && task.ProjectId != projectId.Trim())) return new();
            return task;
        }

    public XerTable? Create07XerActvType() => CreateSimpleKeyedTable(TableNames.ActvType, EnhancedTableNames.XerActvType07,
            new List<Tuple<string, string>> { Tuple.Create(FieldNames.ActvCodeTypeIdKey, FieldNames.ActvCodeTypeId) });



        public XerTable? Create08XerActvCode() => CreateSimpleKeyedTable(TableNames.ActvCode, EnhancedTableNames.XerActvCode08,
            new List<Tuple<string, string>> {

        Tuple.Create(FieldNames.ActvCodeIdKey, FieldNames.ActvCodeId),

        Tuple.Create(FieldNames.ActvCodeTypeIdKey, FieldNames.ActvCodeTypeId)

            });



        public XerTable? Create09XerTaskActv() => CreateSimpleKeyedTable(TableNames.TaskActv, EnhancedTableNames.XerTaskActv09,
            new List<Tuple<string, string>> {

        Tuple.Create(FieldNames.ActvCodeIdKey, FieldNames.ActvCodeId),

        Tuple.Create(FieldNames.TaskIdKey, FieldNames.TaskId)

            });



        public XerTable? Create10XerCalendar() => CreateSimpleKeyedTable(TableNames.Calendar, EnhancedTableNames.XerCalendar10,
            new List<Tuple<string, string>> { Tuple.Create(FieldNames.ClndrIdKey, FieldNames.ClndrId) });



        public XerTable? Create12XerRsrc() => CreateSimpleKeyedTable(TableNames.Rsrc, EnhancedTableNames.XerRsrc12,
                new List<Tuple<string, string>> {

        Tuple.Create(FieldNames.RsrcIdKey, FieldNames.RsrcId),

        Tuple.Create(FieldNames.ClndrIdKey, FieldNames.ClndrId),

        Tuple.Create(FieldNames.UnitIdKey, FieldNames.UnitId)

                });



        public XerTable? Create13XerTaskRsrc() => CreateSimpleKeyedTable(TableNames.TaskRsrc, EnhancedTableNames.XerTaskRsrc13,
            new List<Tuple<string, string>> {

        Tuple.Create(FieldNames.RsrcIdKey, FieldNames.RsrcId),

        Tuple.Create(FieldNames.TaskIdKey, FieldNames.TaskId)

            });



        public XerTable? Create14XerUmeasure() => CreateSimpleKeyedTable(TableNames.Umeasure, EnhancedTableNames.XerUmeasure14,
            new List<Tuple<string, string>> { Tuple.Create(FieldNames.UnitIdKey, FieldNames.UnitId) });






        // Resource distribution is implemented in XerTransformer.ResourceDistribution.cs.
    }
    // Orchestrates the overall processing workflow (Parsing, Transformation, Exporting)

    public class ProcessingService

    {

        private readonly XerParser _parser;

        private readonly CsvExporter _exporter;



        public ProcessingService()

        {

            _parser = new XerParser();

            _exporter = new CsvExporter();

        }



        // Data structure for detailed progress reporting including file status visualization

        public struct DetailedProgress
        {
            public int Percent;
            public string? Message;
            // Fields for UI status visualization
            public string? FilePath;
            public string? FileStatus;
            public string? StatusColor;
            public int? InputIndex;
            public string? SourceToken;
        }




        // Allocate occurrence identity before parallel work, then merge in caller input order.
        public async Task<XerDataStore> ParseMultipleXerFilesAsync(List<string> filePaths, IProgress<DetailedProgress>? progress, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(filePaths);
            StringInternPool.Clear();
            DateParser.ClearCache();
            string[] orderedPaths = filePaths.ToArray();
            int count = orderedPaths.Length;
            if (count == 0) return new XerDataStore();
            XerSourceIdentity[] identities = XerSourceIdentity.CreateOrdered(
                orderedPaths.Select(path => Path.GetFileName(path)).ToArray());
            var stores = new XerDataStore[count];
            var options = new ParallelOptions
            {
                MaxDegreeOfParallelism = Math.Max(1, Math.Min(PerformanceConfig.MaxParallelFiles, count)),
                CancellationToken = cancellationToken
            };
            await Parallel.ForEachAsync(Enumerable.Range(0, count), options, (index, ct) =>
            {
                string path = orderedPaths[index];
                string name = identities[index].OriginalFilename;
                ct.ThrowIfCancellationRequested();
                progress?.Report(new DetailedProgress
                {
                    Percent = index * 100 / count, Message = $"Starting: {name}",
                    FilePath = path, FileStatus = "Processing", StatusColor = "Blue",
                    InputIndex = index, SourceToken = identities[index].SourceToken
                });
                try
                {
                    if (!File.Exists(path))
                        throw new FileNotFoundException("A requested XER input does not exist.", path);
                    XerDataStore parsed = _parser.ParseXerFile(path, (percent, message) =>
                        progress?.Report(new DetailedProgress
                        {
                            Percent = (index * 100 + percent) / count, Message = message,
                            FilePath = path, FileStatus = "Processing", StatusColor = "Blue",
                            InputIndex = index, SourceToken = identities[index].SourceToken
                        }), ct);
                    stores[index] = identities[index].ApplyTo(parsed);
                    progress?.Report(new DetailedProgress
                    {
                        Percent = (index + 1) * 100 / count, Message = $"Completed: {name}",
                        FilePath = path, FileStatus = "Success", StatusColor = "DarkGreen",
                        InputIndex = index, SourceToken = identities[index].SourceToken
                    });
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex)
                {
                    progress?.Report(new DetailedProgress
                    {
                        Percent = (index + 1) * 100 / count, Message = $"Failed: {name}",
                        FilePath = path, FileStatus = "Error", StatusColor = "Red",
                        InputIndex = index, SourceToken = identities[index].SourceToken
                    });
                    throw new InvalidDataException($"Failed to parse file '{name}': {ex.Message}", ex);
                }
                return ValueTask.CompletedTask;
            }).ConfigureAwait(false);

            cancellationToken.ThrowIfCancellationRequested();
            var result = new XerDataStore();
            foreach (XerDataStore store in stores)
            {
                cancellationToken.ThrowIfCancellationRequested();
                result.MergeStore(store);
            }
            progress?.Report(new DetailedProgress { Percent = 100, Message = "Merged all requested inputs." });
            return result;
        }


        public async Task<List<string>> ExportTablesAsync(XerDataStore dataStore, List<string> tablesToExport,
            string outputDirectory, IProgress<(int percent, string message)>? progress, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(outputDirectory);
            XerTable[] tables = await ResolveRequestedTablesAsync(dataStore, tablesToExport, progress, cancellationToken);
            return await Task.Run(() => StandardExportPublication.Write(
                tables, outputDirectory, _exporter, progress, cancellationToken), cancellationToken).ConfigureAwait(false);
        }

        // Parse XER from streams (for Blazor WASM / in-memory scenarios)
        public async Task<XerDataStore> ParseXerStreamsAsync(
            IEnumerable<(Stream stream, string fileName)> files,
            IProgress<DetailedProgress>? progress,
            CancellationToken cancellationToken)
        {
            StringInternPool.Clear();
            DateParser.ClearCache();

            var fileList = files.ToList();
            int fileCount = fileList.Count;
            XerSourceIdentity[] identities = XerSourceIdentity.CreateOrdered(
                fileList.Select(file => file.fileName).ToArray());
            var masterStore = new XerDataStore();

            for (int i = 0; i < fileCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(1, cancellationToken);
                var (stream, fileName) = fileList[i];

                progress?.Report(new DetailedProgress
                {
                    Percent = i * 100 / fileCount,
                    Message = $"Parsing: {fileName}",
                    InputIndex = i,
                    SourceToken = identities[i].SourceToken,
                    FilePath = fileName,
                    FileStatus = "Processing",
                    StatusColor = "Blue"
                });

                try
                {
                    int fileIndex = i;
                    Action<int, string> parserProgress = (percent, message) =>
                    {
                        int overall = (fileIndex * 100 + Math.Clamp(percent, 0, 100)) / fileCount;
                        progress?.Report(new DetailedProgress
                        {
                            Percent = overall,
                            Message = message,
                            InputIndex = fileIndex,
                            SourceToken = identities[fileIndex].SourceToken,
                            FilePath = fileName,
                            FileStatus = "Processing",
                            StatusColor = "Blue"
                        });
                    };
                    var singleStore = await _parser.ParseXerStreamAsync(
                        stream, fileName, parserProgress, cancellationToken);
                    masterStore.MergeStore(identities[i].ApplyTo(singleStore));

                    progress?.Report(new DetailedProgress
                    {
                        Percent = (i + 1) * 100 / fileCount,
                        Message = $"Completed: {fileName}",
                        InputIndex = i,
                        SourceToken = identities[i].SourceToken,
                        FilePath = fileName,
                        FileStatus = "Success",
                        StatusColor = "DarkGreen"
                    });
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex)
                {
                    progress?.Report(new DetailedProgress
                    {
                        Percent = (i + 1) * 100 / fileCount,
                        Message = $"Failed: {fileName}",
                        InputIndex = i,
                        SourceToken = identities[i].SourceToken,
                        FilePath = fileName,
                        FileStatus = "Error",
                        StatusColor = "Red"
                    });
                    throw new Exception($"Failed to parse file '{fileName}': {ex.Message}", ex);
                }
            }

            return masterStore;
        }

        // Output names are unique by table, while input occurrences remain an ordered sequence.
        public async Task<Dictionary<string, byte[]>> ExportTablesToMemoryAsync(
            XerDataStore dataStore, List<string> tablesToExport,
            IProgress<(int percent, string message)>? progress, CancellationToken cancellationToken)
        {
            XerTable[] tables = await ResolveRequestedTablesAsync(dataStore, tablesToExport, progress, cancellationToken);
            var result = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < tables.Length; i++)
            {
                await Task.Delay(1, cancellationToken);
                using var stream = new MemoryStream();
                _exporter.WriteTableToStream(tables[i], stream);
                result.Add(tables[i].Name, stream.ToArray());
                progress?.Report((80 + (i + 1) * 20 / Math.Max(1, tables.Length), $"Exported: {tables[i].Name} ({i + 1}/{tables.Length})"));
            }
            cancellationToken.ThrowIfCancellationRequested();
            return result;
        }

        private static async Task<XerTable[]> ResolveRequestedTablesAsync(
            XerDataStore dataStore, IEnumerable<string> requested,
            IProgress<(int percent, string message)>? progress, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(dataStore);
            ArgumentNullException.ThrowIfNull(requested);
            string[] names = requested.Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(name => name, StringComparer.Ordinal).ToArray();
            foreach (string name in names) StandardExportPublication.ValidateTableName(name);
            cancellationToken.ThrowIfCancellationRequested();
            foreach (string tableName in dataStore.TableNames)
                XerSourceSchema.ValidateHeaders(tableName, dataStore.GetTable(tableName)!.Headers);
            // This is validation of source metadata, not a filename-keyed input collection.
            // Separate independently parsed batches must not silently alias public reporting keys.
            var sourcePairs = dataStore.TableNames.SelectMany(name => dataStore.GetTable(name)!.Rows)
                .Select(row => (Namespace: row.SourceFilename.Trim(), row.SourceToken)).Distinct();
            foreach (var scope in sourcePairs.GroupBy(source => source.Namespace, StringComparer.OrdinalIgnoreCase))
                if (scope.Select(source => source.SourceToken).Distinct(StringComparer.Ordinal).Skip(1).Any())
                    throw new InvalidDataException($"Standard public source namespace '{scope.Key}' belongs to multiple input occurrences. Parse the sources together in one ordered batch so repeated filenames receive distinct public keys. No CSV files have been published by this export.");
            progress?.Report((0, "Generating and validating every requested table..."));
            var transformer = new XerTransformer(dataStore);
            var cache = new ConcurrentDictionary<string, XerTable>(StringComparer.OrdinalIgnoreCase);
            var result = new List<XerTable>(names.Length);

            foreach (string requestedName in names)
            {
                await Task.Delay(1, cancellationToken);
                XerTable? table = dataStore.GetTable(requestedName);
                if (table is null)
                {
                    string name = requestedName.ToUpperInvariant();
                    string? sourceName = StandardExportSchema.SourceTable(name);
                    if (sourceName is null)
                        throw Missing(requestedName, "the requested raw table is absent or the enhanced table is unsupported");
                    if (dataStore.GetTable(sourceName)?.Headers is null)
                        throw Missing(requestedName, $"required source table '{sourceName}' or its headers are absent");

                    if (name is EnhancedTableNames.XerTask01 or EnhancedTableNames.XerBaseline04)
                    {
                        foreach (string dependency in new[] { TableNames.Task, TableNames.Calendar, TableNames.Project })
                            if (dataStore.GetTable(dependency)?.Headers is null)
                                throw Missing(requestedName, $"required source table '{dependency}' or its headers are absent");
                        if (!cache.ContainsKey(EnhancedTableNames.XerTask01))
                        {
                            XerTable? tasks = StandardExportSchema.CreateIfSourceEmpty(dataStore, EnhancedTableNames.XerTask01)
                                ?? transformer.Create01XerTaskTable();
                            if (tasks is null)
                                throw Missing(requestedName, "required activity-table calculation failed");
                            cache.TryAdd(EnhancedTableNames.XerTask01, tasks);
                        }
                    }

                    if (!cache.TryGetValue(name, out table))
                    {
                        table = StandardExportSchema.CreateIfSourceEmpty(dataStore, name)
                            ?? GenerateEnhancedTable(transformer, name, cache);
                        if (table is not null) cache.TryAdd(name, table);
                    }
                }
                if (table?.Headers is not { Length: > 0 })
                    throw Missing(requestedName, "generation failed or no valid output schema is available");
                if (table.Headers.Any(string.IsNullOrWhiteSpace)
                    || table.Headers.Distinct(StringComparer.OrdinalIgnoreCase).Count() != table.Headers.Length
                    || table.Headers.Contains(FieldNames.FileName, StringComparer.OrdinalIgnoreCase))
                    throw Missing(requestedName, "the output schema contains blank or ambiguous duplicate column names");
                StandardExportPublication.ValidateTableName(table.Name);
                result.Add(table);
            }
            cancellationToken.ThrowIfCancellationRequested();
            return result.ToArray();

            static InvalidDataException Missing(string name, string detail) =>
                new($"Cannot export '{name}': {detail}. No CSV files have been published by this export.");
        }

        private static XerTable? GenerateEnhancedTable(XerTransformer transformer, string tableName, ConcurrentDictionary<string, XerTable> cache)
        {
            switch (tableName)
            {
                case EnhancedTableNames.XerBaseline04:
                    if (cache.TryGetValue(EnhancedTableNames.XerTask01, out XerTable? task01) && task01 != null)
                    {
                        return transformer.Create04XerBaselineTable(task01);
                    }
                    return null;
                case EnhancedTableNames.XerPredecessor06: return transformer.Create06XerPredecessor(cache);
                case EnhancedTableNames.XerProject02: return transformer.Create02XerProject();
                case EnhancedTableNames.XerProjWbs03: return transformer.Create03XerProjWbsTable();
                case EnhancedTableNames.XerActvType07: return transformer.Create07XerActvType();
                case EnhancedTableNames.XerActvCode08: return transformer.Create08XerActvCode();
                case EnhancedTableNames.XerTaskActv09: return transformer.Create09XerTaskActv();
                case EnhancedTableNames.XerCalendar10: return transformer.Create10XerCalendar();
                case EnhancedTableNames.XerCalendarDetailed11: return transformer.Create11XerCalendarDetailed();
                case EnhancedTableNames.XerRsrc12: return transformer.Create12XerRsrc();
                case EnhancedTableNames.XerTaskRsrc13: return transformer.Create13XerTaskRsrc();
                case EnhancedTableNames.XerUmeasure14: return transformer.Create14XerUmeasure();
                case EnhancedTableNames.XerResourceDist15: return transformer.Create15XerResourceDistribution(); // ** NEW **
                case EnhancedTableNames.XerTask01: return transformer.Create01XerTaskTable();
            }
            return null;
        }
    }
