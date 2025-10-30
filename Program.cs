using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Globalization;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime;
using System.Text.RegularExpressions;
using System.Drawing;

namespace XerToCsvConverter
{
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
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            // LatencyMode is managed by the MainForm: Batch during processing, Interactive when idle.
        }
    }

    internal static class TableNames { public const string Task = "TASK"; public const string Calendar = "CALENDAR"; public const string Project = "PROJECT"; public const string ProjWbs = "PROJWBS"; public const string TaskActv = "TASKACTV"; public const string ActvCode = "ACTVCODE"; public const string ActvType = "ACTVTYPE"; public const string TaskPred = "TASKPRED"; public const string Rsrc = "RSRC"; public const string TaskRsrc = "TASKRSRC"; public const string Umeasure = "UMEASURE"; }

    internal static class FieldNames
    {
        public const string TaskId = "task_id"; public const string ProjectId = "proj_id"; public const string WbsId = "wbs_id"; public const string CalendarId = "clndr_id"; public const string TaskType = "task_type"; public const string StatusCode = "status_code"; public const string TaskCode = "task_code"; public const string TaskName = "task_name"; public const string RsrcId = "rsrc_id"; public const string ActStartDate = "act_start_date"; public const string ActEndDate = "act_end_date"; public const string EarlyStartDate = "early_start_date"; public const string EarlyEndDate = "early_end_date"; public const string LateEndDate = "late_end_date"; public const string LateStartDate = "late_start_date"; public const string TargetStartDate = "target_start_date"; public const string TargetEndDate = "target_end_date"; public const string CstrType = "cstr_type"; public const string CstrDate = "cstr_date"; public const string PriorityType = "priority_type"; public const string FloatPath = "float_path"; public const string FloatPathOrder = "float_path_order"; public const string DrivingPathFlag = "driving_path_flag"; public const string RemainDurationHrCnt = "remain_drtn_hr_cnt"; public const string TotalFloatHrCnt = "total_float_hr_cnt"; public const string FreeFloatHrCnt = "free_float_hr_cnt"; public const string CompletePctType = "complete_pct_type"; public const string PhysCompletePct = "phys_complete_pct"; public const string ActWorkQty = "act_work_qty"; public const string RemainWorkQty = "remain_work_qty"; public const string TargetDurationHrCnt = "target_drtn_hr_cnt"; public const string ClndrId = "clndr_id"; public const string DayHourCount = "day_hr_cnt"; public const string LastRecalcDate = "last_recalc_date";
        public const string ParentWbsId = "parent_wbs_id";
        public const string ActvCodeTypeId = "actv_code_type_id"; public const string ActvCodeId = "actv_code_id"; public const string FileName = "FileName"; public const string Start = "Start"; public const string Finish = "Finish"; public const string IdName = "ID_Name"; public const string RemainingWorkingDays = "Remaining Working Days"; public const string OriginalDuration = "Original Duration"; public const string TotalFloat = "Total Float"; public const string FreeFloat = "Free Float"; public const string PercentComplete = "%"; public const string DataDate = "Data Date"; public const string WbsIdKey = "wbs_id_key"; public const string TaskIdKey = "task_id_key"; public const string ParentWbsIdKey = "parent_wbs_id_key"; public const string CalendarIdKey = "calendar_id_key"; public const string ProjIdKey = "proj_id_key"; public const string ActvCodeIdKey = "actv_code_id_key"; public const string ActvCodeTypeIdKey = "actv_code_type_id_key"; public const string ClndrIdKey = "clndr_id_key"; public const string PredTaskId = "pred_task_id"; public const string PredTaskIdKey = "pred_task_id_key"; public const string CalendarName = "clndr_name"; public const string CalendarData = "clndr_data"; public const string CalendarType = "clndr_type";
        public const string Date = "date"; public const string DayOfWeek = "day_of_week"; public const string WorkingDay = "working_day"; public const string WorkHours = "work_hours"; public const string ExceptionType = "exception_type"; public const string RsrcIdKey = "rsrc_id_key";
        public const string MonthUpdate = "MonthUpdate";
        public const string UnitId = "unit_id"; public const string UnitIdKey = "unit_id_key";
    }

    internal static class EnhancedTableNames { public const string XerTask01 = "01_XER_TASK"; public const string XerProject02 = "02_XER_PROJECT"; public const string XerProjWbs03 = "03_XER_PROJWBS"; public const string XerBaseline04 = "04_XER_BASELINE"; public const string XerPredecessor06 = "06_XER_PREDECESSOR"; public const string XerActvType07 = "07_XER_ACTVTYPE"; public const string XerActvCode08 = "08_XER_ACTVCODE"; public const string XerTaskActv09 = "09_XER_TASKACTV"; public const string XerCalendar10 = "10_XER_CALENDAR"; public const string XerCalendarDetailed11 = "11_XER_CALENDAR_DETAILED"; public const string XerRsrc12 = "12_XER_RSRC"; public const string XerTaskRsrc13 = "13_XER_TASKRSRC"; public const string XerUmeasure14 = "14_XER_UMEASURE"; }

    // Custom String Interning Pool to reduce memory footprint by reusing identical strings
    public static class StringInternPool
    {
        // Initialize pool with estimated capacity
        private static readonly ConcurrentDictionary<string, string> Pool = new ConcurrentDictionary<string, string>(
            Environment.ProcessorCount * 2, PerformanceConfig.DefaultTableCapacity * 20);

        public static string Intern(string str)
        {
            if (!PerformanceConfig.EnableGlobalStringInterning) return str;
            if (str == null) return null;
            if (string.IsNullOrEmpty(str)) return string.Empty;
            // Avoid interning very long strings
            if (str.Length > PerformanceConfig.MaxStringInternLength) return str;

            return Pool.GetOrAdd(str, str);
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

        public static DateTime? TryParse(string dateStr)
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

    // Lightweight struct for data rows
    public readonly struct DataRow
    {
        public string[] Fields { get; }
        public string SourceFilename { get; }

        public DataRow(string[] fields, string sourceFilename)
        {
            Fields = fields;
            SourceFilename = sourceFilename;
        }
    }

    // Represents a table extracted from the XER file
    public class XerTable
    {
        public string Name { get; }
        public string[] Headers { get; private set; }
        private readonly List<DataRow> _rows;
        private IReadOnlyDictionary<string, int> _fieldIndexes;

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
            if (Headers == null) throw new InvalidOperationException($"Headers must be set for table {Name} before adding rows.");

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
            var newValues = new string[Headers.Length];
            int copyLength = Math.Min(values.Length, Headers.Length);
            Array.Copy(values, newValues, copyLength);

            // Fill remaining fields with empty strings
            for (int i = copyLength; i < Headers.Length; i++)
            {
                newValues[i] = string.Empty;
            }
            return new DataRow(newValues, row.SourceFilename);
        }

        public IReadOnlyList<DataRow> Rows => _rows;
        public int RowCount => _rows.Count;
        public bool IsEmpty => _rows.Count == 0;

        // Builds a dictionary for fast field index lookup (Case-insensitive)
        private void BuildFieldIndexes()
        {
            if (Headers == null)
            {
                _fieldIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                return;
            }

            var dict = new Dictionary<string, int>(Headers.Length, StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < Headers.Length; i++)
            {
                // Handle potential duplicate or empty headers
                if (!string.IsNullOrEmpty(Headers[i]) && !dict.ContainsKey(Headers[i]))
                {
                    dict[Headers[i]] = i;
                }
            }
            _fieldIndexes = dict;
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

        // Merges data from another store (used during parallel parsing)
        public void MergeStore(XerDataStore otherStore)
        {
            foreach (var kvp in otherStore._tables)
            {
                if (_tables.TryGetValue(kvp.Key, out XerTable existingTable))
                {
                    // Assumes header consistency across files for the same table name
                    existingTable.AddRows(kvp.Value.Rows);
                }
                else
                {
                    _tables.Add(kvp.Key, kvp.Value);
                }
            }
        }

        public void AddTable(XerTable table) => _tables[table.Name] = table;

        public XerTable GetTable(string tableName)
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

        // PERFORMANCE/UX OPTIMIZATION: Added CancellationToken, Optimized Encoding and Progress Reporting
        public XerDataStore ParseXerFile(string xerFilePath, Action<int, string> reportProgressAction, CancellationToken cancellationToken)
        {
            var fileStore = new XerDataStore();
            string filename = StringInternPool.Intern(Path.GetFileName(xerFilePath));
            int bufferSize = PerformanceConfig.FileReadBufferSize;
            int progressReportInterval = PerformanceConfig.ProgressReportIntervalLines;

            var localTables = new Dictionary<string, XerTable>(StringComparer.OrdinalIgnoreCase);
            XerTable currentTable = null;
            int lineCount = 0;
            long bytesRead = 0;

            // Default to UTF8, generally safer and more common than Encoding.Default (ANSI)
            Encoding encoding = Encoding.UTF8;

            try
            {
                var fileInfo = new FileInfo(xerFilePath);
                long fileSize = fileInfo.Length;
                if (fileSize == 0) return fileStore;

                int lastReportedProgress = 0;

                // Use FileOptions.SequentialScan for optimized reading of large files
                using (var fileStream = new FileStream(xerFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, FileOptions.SequentialScan))
                // Enable BOM detection, fallback to defined encoding (UTF8)
                using (var reader = new StreamReader(fileStream, encoding, true, bufferSize))
                {
                    string line;

                    // Get the actual encoding used by the reader (might change if BOM is detected)
                    encoding = reader.CurrentEncoding;

                    // PERFORMANCE OPTIMIZATION: Estimate average bytes per char for faster progress calculation
                    // Calculating exact byte count for every line is slow.
                    double avgBytesPerChar = 1.0; // Default assumption for UTF8/ASCII
                    if (encoding is UnicodeEncoding || encoding is UTF32Encoding)
                    {
                        // More accurate estimation for wide encodings
                        avgBytesPerChar = encoding.GetEncoder().GetByteCount(new char[] { 'a' }, 0, 1, true);
                    }

                    while ((line = reader.ReadLine()) != null)
                    {
                        // UI/UX OPTIMIZATION: Check for cancellation
                        cancellationToken.ThrowIfCancellationRequested();

                        // PERFORMANCE OPTIMIZATION: Optimized progress calculation using estimation
                        bytesRead += (long)(line.Length * avgBytesPerChar) + Environment.NewLine.Length;

                        lineCount++;

                        if (lineCount % progressReportInterval == 0)
                        {
                            // Ensure progress doesn't exceed 100% due to estimation inaccuracies
                            int progress = Math.Min(100, (int)((double)bytesRead * 100 / fileSize));
                            if (progress > lastReportedProgress)
                            {
                                reportProgressAction?.Invoke(progress, $"Parsing {filename}: {progress}%");
                                lastReportedProgress = progress;
                            }
                        }

                        if (string.IsNullOrWhiteSpace(line)) continue;

                        // XER format lines start with %T (Table), %F (Fields), or %R (Row)
                        if (line.Length < 2 || line[0] != '%') continue;

                        char typeChar = line[1];

                        switch (typeChar)
                        {
                            case 'T': // Table Definition
                                string currentTableName = line.Substring(2).Trim();
                                if (!string.IsNullOrEmpty(currentTableName))
                                {
                                    currentTable = new XerTable(currentTableName);
                                    localTables[currentTable.Name] = currentTable;
                                }
                                break;

                            case 'F': // Field Definitions (Headers)
                                if (currentTable != null)
                                {
                                    string fieldsLine = line.Substring(2);
                                    // Handle potential leading delimiter
                                    if (fieldsLine.Length > 0 && fieldsLine[0] == Delimiter) fieldsLine = fieldsLine.Substring(1);
                                    string[] headers = FastSplitAndIntern(fieldsLine, Delimiter, trim: true);
                                    currentTable.SetHeaders(headers);
                                }
                                break;

                            case 'R': // Row Data
                                if (currentTable != null && currentTable.Headers != null)
                                {
                                    string dataLine = line.Substring(2);
                                    if (dataLine.Length > 0 && dataLine[0] == Delimiter) dataLine = dataLine.Substring(1);
                                    string[] values = FastSplitAndIntern(dataLine, Delimiter, trim: false);
                                    currentTable.AddRow(new DataRow(values, filename));
                                }
                                break;
                        }
                    }
                }

                // Add non-empty tables to the store
                foreach (var kvp in localTables)
                {
                    if (!kvp.Value.IsEmpty)
                    {
                        fileStore.AddTable(kvp.Value);
                    }
                }

                reportProgressAction?.Invoke(100, $"Finished parsing {filename}");
                return fileStore;
            }
            catch (OperationCanceledException)
            {
                // Rethrow cancellation to be handled by the caller (ProcessingService)
                throw;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error reading file {filename} at line {lineCount}. Bytes read: {bytesRead}.", ex);
            }
        }

        // Optimized string splitting and interning implementation
        // Note: While Span<T> (available in modern .NET) could further optimize this by reducing Substring allocations,
        // this implementation balances performance and compatibility with older .NET Framework versions.
        private static string[] FastSplitAndIntern(string text, char delimiter, bool trim)
        {
            if (string.IsNullOrEmpty(text)) return Array.Empty<string>();

            // Count delimiters to pre-size the array
            int count = 1;
            for (int i = 0; i < text.Length; i++)
            {
                if (text[i] == delimiter) count++;
            }

            string[] result = new string[count];
            int resultIndex = 0;
            int start = 0;

            for (int i = 0; i < text.Length; i++)
            {
                if (text[i] == delimiter)
                {
                    string segment = text.Substring(start, i - start);
                    if (trim) segment = segment.Trim();
                    result[resultIndex++] = StringInternPool.Intern(segment);
                    start = i + 1;
                }
            }

            // Handle the last segment
            string lastSegment = text.Substring(start);
            if (trim) lastSegment = lastSegment.Trim();
            result[resultIndex] = StringInternPool.Intern(lastSegment);

            return result;
        }
    }

    // Handles exporting data to CSV format
    public class CsvExporter
    {
        // PERFORMANCE OPTIMIZATION: Removed explicit batching (List<string>), relying solely on StreamWriter buffering
        public void WriteTableToCsv(XerTable table, string csvFilePath)
        {
            if (table == null || table.Headers == null) return;
            if (table.IsEmpty && table.Headers.Length == 0) return;

            int bufferSize = PerformanceConfig.CsvWriteBufferSize;

            try
            {
                // Use buffered FileStream and StreamWriter
                using (var fileStream = new FileStream(csvFilePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize))
                // Use UTF8 encoding for output
                using (var writer = new StreamWriter(fileStream, Encoding.UTF8, bufferSize))
                {
                    string[] headerRow = table.Headers;
                    // Use a reusable StringBuilder for line construction
                    var lineBuilder = new StringBuilder(PerformanceConfig.StringBuilderInitialCapacity);

                    // Write Headers
                    for (int i = 0; i < headerRow.Length; i++)
                    {
                        if (i > 0) lineBuilder.Append(',');
                        lineBuilder.Append(EscapeCsvField(headerRow[i]));
                    }
                    // Add FileName column
                    if (headerRow.Length > 0) lineBuilder.Append(',');
                    lineBuilder.Append(EscapeCsvField(FieldNames.FileName));

                    writer.WriteLine(lineBuilder.ToString());
                    lineBuilder.Clear();

                    // Write Data Rows
                    foreach (var dataRow in table.Rows)
                    {
                        string[] rowFields = dataRow.Fields;
                        if (rowFields == null) continue;

                        for (int j = 0; j < rowFields.Length; j++)
                        {
                            if (j > 0) lineBuilder.Append(',');
                            lineBuilder.Append(EscapeCsvField(rowFields[j] ?? string.Empty));
                        }
                        // Add FileName data
                        if (rowFields.Length > 0) lineBuilder.Append(',');
                        lineBuilder.Append(EscapeCsvField(dataRow.SourceFilename));

                        // Write directly to the writer, leveraging its internal buffer.
                        // This avoids allocating intermediate strings for every row in a batch list.
                        writer.WriteLine(lineBuilder.ToString());
                        lineBuilder.Clear();
                    }

                    writer.Flush();
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to write CSV file '{Path.GetFileName(csvFilePath)}'. {ex.Message}", ex);
            }
        }

        // Efficient CSV field escaping
        private static string EscapeCsvField(string field)
        {
            if (string.IsNullOrEmpty(field)) return "";

            bool needsQuotes = false;
            bool hasQuotes = false;

            // Check if quoting is necessary (contains comma, newline, or quotes)
            for (int i = 0; i < field.Length; i++)
            {
                char c = field[i];
                if (c == ',' || c == '\r' || c == '\n') needsQuotes = true;
                else if (c == '"') { needsQuotes = true; hasQuotes = true; }

                if (needsQuotes && hasQuotes) break;
            }

            // Check for leading/trailing whitespace (also requires quotes)
            if (!needsQuotes && field.Length > 0 && (field[0] == ' ' || field[field.Length - 1] == ' '))
            {
                needsQuotes = true;
            }

            if (!needsQuotes) return field;

            // Escape internal quotes and wrap the field
            if (hasQuotes)
            {
                // Estimate capacity to avoid resizing
                var sb = new StringBuilder(field.Length + 10);
                sb.Append('"');
                foreach (char c in field)
                {
                    if (c == '"') sb.Append("\"\""); // Double up quotes
                    else sb.Append(c);
                }
                sb.Append('"');
                return sb.ToString();
            }
            else
            {
                return $"\"{field}\"";
            }
        }
    }

    // Handles data transformation and creation of enhanced tables (e.g., for Power BI)
    // Handles data transformation and creation of enhanced tables (e.g., for Power BI)
    public class XerTransformer
    {
        private readonly XerDataStore _dataStore;

        // Compiled Regex for optimized pattern matching in calendar parsing
        private static readonly RegexOptions RegexCompiledOptions = RegexOptions.Singleline | RegexOptions.Compiled;
        // Regex for Exceptions (Holidays)
        private static readonly Regex ExcPatternRegex = new Regex(@"\(0\|\|(\d+)\(d\|(\d+)\)\s*\((.*?)\)\s*\)", RegexCompiledOptions);
        private static readonly Regex ExcPatternRegex2 = new Regex(@"\(0\|\|(\d+)\(d\|(\d+)\|0\)\s*\((.*?)\)\s*\)", RegexCompiledOptions);
        // Regex for cleaning data strings
        private static readonly Regex CleanRegex1 = new Regex(@"(\|\|)\s+", RegexCompiledOptions);
        private static readonly Regex CleanRegex2 = new Regex(@"\s+(\|\|)", RegexCompiledOptions);

        // PERFORMANCE OPTIMIZATION: New static compiled Regex for DayOfWeek parsing (replaces dynamic Regex generated in loop)
        // Captures day number (Group 1) and content (Group 2). Handles nested parentheses in content.
        private static readonly Regex DayPatternRegex = new Regex(
            @"\(0\|\|(\d)\(\)\s*\(((?:[^()]|\((?:[^()]|\([^()]*\))*\))*)\)\s*\)",
                RegexCompiledOptions);

        // New static compiled Regex for empty DayOfWeek definitions
        private static readonly Regex EmptyDayPatternRegex = new Regex(
            @"\(0\|\|(\d)\(\)\s*\(\s*\)\s*\)",
                RegexCompiledOptions);

        // Regex to extract YYMM from filename for MonthUpdate column
        private static readonly Regex MonthUpdateRegex = new Regex(@"^(\d{4})", RegexCompiledOptions);

        public XerTransformer(XerDataStore dataStore)
        {
            _dataStore = dataStore;
        }

        // New helper method to parse YYMM from filename
        private string ParseMonthUpdateFromFilename(string filename)
        {
            if (string.IsNullOrWhiteSpace(filename)) return string.Empty;

            var match = MonthUpdateRegex.Match(filename.Trim());

            if (match.Success)
            {
                string yymm = match.Groups[1].Value;
                // The regex already ensures yymm is 4 digits.
                if (int.TryParse(yymm.Substring(0, 2), out int year) &&
                    int.TryParse(yymm.Substring(2, 2), out int month))
                {
                    // Assuming 2-digit year 'yy' is in the 21st century (20xx)
                    year += 2000;

                    if (month >= 1 && month <= 12)
                    {
                        try
                        {
                            var date = new DateTime(year, month, 1);
                            return date.ToString("yyyy-MM-dd"); // Use a standard, sortable format
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

        private bool IsTableValid(XerTable table) => table != null && !table.IsEmpty && table.Headers != null;

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
        public XerTable Create01XerTaskTable()
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

                string[] finalColumns = {
                FieldNames.TaskId, FieldNames.ProjectId, FieldNames.WbsId, FieldNames.CalendarId,
                FieldNames.TaskType, FieldNames.StatusCode, FieldNames.TaskCode, FieldNames.TaskName,
                FieldNames.RsrcId, FieldNames.ActStartDate, FieldNames.ActEndDate,
                FieldNames.EarlyStartDate, FieldNames.EarlyEndDate, FieldNames.LateStartDate, FieldNames.LateEndDate,
                FieldNames.TargetStartDate, FieldNames.TargetEndDate,
                FieldNames.CstrType, FieldNames.CstrDate, FieldNames.PriorityType, FieldNames.FloatPath,
                FieldNames.FloatPathOrder, FieldNames.DrivingPathFlag,
                // Calculated Fields
                FieldNames.Start, FieldNames.Finish, FieldNames.IdName,
                FieldNames.RemainingWorkingDays, FieldNames.OriginalDuration, FieldNames.TotalFloat, FieldNames.FreeFloat,
                FieldNames.PercentComplete, FieldNames.DataDate,
                // Key Fields
                FieldNames.WbsIdKey, FieldNames.TaskIdKey, FieldNames.CalendarIdKey, FieldNames.ProjIdKey,
                FieldNames.MonthUpdate
            };

                // PERFORMANCE OPTIMIZATION: Pre-calculate target indexes for O(1) lookups in the parallel loop
                var finalIndexes = finalColumns
                    .Select((name, index) => new { name, index })
                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);


                var finalTable = new XerTable(EnhancedTableNames.XerTask01, taskTable.RowCount);
                finalTable.SetHeaders(finalColumns.Select(StringInternPool.Intern).ToArray());

                const string TK_Complete = "TK_Complete";
                const string TK_NotStart = "TK_NotStart";
                const string TK_Active = "TK_Active";

                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };
                var transformedRowsBag = new ConcurrentBag<DataRow>();

                Parallel.ForEach(taskTable.Rows, parallelOptions, sourceRowData =>
                {
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
                    if (!string.IsNullOrEmpty(clndrId) && calendarHours.TryGetValue(calendarKey, out decimal dayHrCnt) && dayHrCnt > 0)
                    {
                        SetTransformedField(transformed, finalIndexes, FieldNames.RemainingWorkingDays,
                            CalculateDaysFromHours(row, taskIndexes, FieldNames.RemainDurationHrCnt, dayHrCnt));
                        SetTransformedField(transformed, finalIndexes, FieldNames.OriginalDuration,
                            CalculateDaysFromHours(row, taskIndexes, FieldNames.TargetDurationHrCnt, dayHrCnt));

                        if (statusCode != TK_Complete)
                        {
                            SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat,
                                CalculateDaysFromHours(row, taskIndexes, FieldNames.TotalFloatHrCnt, dayHrCnt));
                            SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloat,
                                CalculateDaysFromHours(row, taskIndexes, FieldNames.FreeFloatHrCnt, dayHrCnt));
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
                        SetTransformedField(transformed, finalIndexes, FieldNames.RemainingWorkingDays, "");
                        SetTransformedField(transformed, finalIndexes, FieldNames.OriginalDuration, "");
                        SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat, "");
                        SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloat, "");
                    }

                    // Percentage Complete calculation
                    double pct = CalculateCompletionPercentage(row, taskIndexes, statusCode);
                    SetTransformedField(transformed, finalIndexes, FieldNames.PercentComplete,
                        pct.ToString("F2", CultureInfo.InvariantCulture));

                    // Data Date lookup
                    string projectKey = CreateKey(originalFilename, projId);
                    if (!string.IsNullOrEmpty(projId) && projectDataDates.TryGetValue(projectKey, out DateTime dataDateValue))
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
                    SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(originalFilename));

                    // Intern all resulting strings in the transformed row
                    for (int k = 0; k < transformed.Length; k++)
                    {
                        transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);
                    }

                    transformedRowsBag.Add(new DataRow(transformed, originalFilename));
                });

                finalTable.AddRows(transformedRowsBag);
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
        private void CopyDirectFields(string[] sourceRow, IReadOnlyDictionary<string, int> sourceIndexes, string[] targetRow, IReadOnlyDictionary<string, int> targetIndexes)
        {
            // Fields handled by specific transformation logic and should not be copied directly
            var handledFields = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
            FieldNames.StatusCode, FieldNames.Start, FieldNames.Finish, FieldNames.IdName,
            FieldNames.RemainingWorkingDays, FieldNames.OriginalDuration, FieldNames.TotalFloat, FieldNames.FreeFloat,
            FieldNames.PercentComplete, FieldNames.DataDate,
            // Date fields are handled by FormatDateFields
            FieldNames.ActStartDate, FieldNames.ActEndDate,
            FieldNames.EarlyStartDate, FieldNames.EarlyEndDate,
            FieldNames.LateStartDate, FieldNames.LateEndDate,
            FieldNames.CstrDate,
            FieldNames.TargetStartDate, FieldNames.TargetEndDate,
            // Key fields are generated separately
            FieldNames.WbsIdKey, FieldNames.TaskIdKey, FieldNames.CalendarIdKey, FieldNames.ProjIdKey,
            FieldNames.MonthUpdate // Don't copy this directly
        };

            // Iterate over the target dictionary keys/values
            foreach (var kvp in targetIndexes)
            {
                string colName = kvp.Key;
                int targetIndex = kvp.Value;

                // If the source has the column and it's not handled elsewhere, copy it
                if (sourceIndexes.TryGetValue(colName, out int srcIndex) && !handledFields.Contains(colName))
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

        private Dictionary<string, decimal> BuildCalendarHoursLookup(XerTable calendarTable)
        {
            var lookup = new Dictionary<string, decimal>(calendarTable.RowCount, StringComparer.OrdinalIgnoreCase);
            var indexes = calendarTable.FieldIndexes;

            if (!indexes.ContainsKey(FieldNames.ClndrId) || !indexes.ContainsKey(FieldNames.DayHourCount)) return lookup;

            foreach (var rowData in calendarTable.Rows)
            {
                var row = rowData.Fields;
                string clndrId = GetFieldValue(row, indexes, FieldNames.ClndrId);
                string dayHrCntStr = GetFieldValue(row, indexes, FieldNames.DayHourCount);

                if (!string.IsNullOrEmpty(clndrId) && decimal.TryParse(dayHrCntStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal dayHrCnt) && dayHrCnt > 0)
                {
                    string compositeKey = CreateKey(rowData.SourceFilename, clndrId);
                    lookup[compositeKey] = dayHrCnt;
                }
            }
            return lookup;
        }

        private Dictionary<string, DateTime> BuildProjectDataDatesLookup(XerTable projectTable)
        {
            var lookup = new Dictionary<string, DateTime>(projectTable.RowCount, StringComparer.OrdinalIgnoreCase);
            var indexes = projectTable.FieldIndexes;

            // P6 uses last_recalc_date as the Data Date (DD)
            if (!indexes.ContainsKey(FieldNames.ProjectId) || !indexes.ContainsKey(FieldNames.LastRecalcDate)) return lookup;

            foreach (var rowData in projectTable.Rows)
            {
                var row = rowData.Fields;
                string projId = GetFieldValue(row, indexes, FieldNames.ProjectId);
                var dataDate = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.LastRecalcDate));

                if (!string.IsNullOrEmpty(projId) && dataDate.HasValue)
                {
                    string compositeKey = CreateKey(rowData.SourceFilename, projId);
                    lookup[compositeKey] = dataDate.Value;
                }
            }
            return lookup;
        }

        // Calculation Logic Helpers
        private DateTime CalculateStartDate(string[] row, IReadOnlyDictionary<string, int> indexes, string statusCode)
        {
            if (statusCode == "TK_NotStart")
            {
                var parsed = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.EarlyStartDate));
                if (parsed.HasValue) return parsed.Value;
                // Fallback for milestones (Start Milestone)
                parsed = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.EarlyEndDate));
                if (parsed.HasValue) return parsed.Value;
            }
            else // In Progress or Complete
            {
                var parsed = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.ActStartDate));
                if (parsed.HasValue) return parsed.Value;
                // Fallback for milestones
                parsed = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.ActEndDate));
                if (parsed.HasValue) return parsed.Value;
            }
            return DateTime.MinValue;
        }

        private DateTime CalculateFinishDate(string[] row, IReadOnlyDictionary<string, int> indexes, string statusCode)
        {
            if (statusCode == "TK_Complete")
            {
                var parsed = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.ActEndDate));
                if (parsed.HasValue) return parsed.Value;
            }
            else // Not Started or In Progress
            {
                // Use Early Finish (Remaining Finish)
                var parsed = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.EarlyEndDate));
                if (parsed.HasValue) return parsed.Value;
                // Fallback to Late Finish if Early Finish is missing (less common but possible)
                parsed = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.LateEndDate));
                if (parsed.HasValue) return parsed.Value;
            }
            return DateTime.MinValue;
        }

        private string CalculateDaysFromHours(string[] row, IReadOnlyDictionary<string, int> indexes, string hourFieldName, decimal hoursPerDay)
        {
            if (hoursPerDay <= 0) return "";

            string hourStr = GetFieldValue(row, indexes, hourFieldName);
            if (decimal.TryParse(hourStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal hours))
            {
                decimal days = hours / hoursPerDay;
                return days.ToString("F1", CultureInfo.InvariantCulture); // Format to 1 decimal place
            }
            return "";
        }

        private double CalculateCompletionPercentage(string[] row, IReadOnlyDictionary<string, int> indexes, string statusCode)
        {
            if (statusCode == "TK_Complete") return 100.0;
            if (statusCode == "TK_NotStart") return 0.0;

            string completePctType = GetFieldValue(row, indexes, FieldNames.CompletePctType);
            NumberStyles numStyle = NumberStyles.Any;
            IFormatProvider formatProvider = CultureInfo.InvariantCulture;

            switch (completePctType)
            {
                case "CP_Phys": // Physical % Complete
                    if (double.TryParse(GetFieldValue(row, indexes, FieldNames.PhysCompletePct), numStyle, formatProvider, out double pct))
                    {
                        return Math.Max(0.0, Math.Min(100.0, pct));
                    }
                    break;

                case "CP_Units": // Units % Complete (Actual / (Actual + Remaining))
                    if (double.TryParse(GetFieldValue(row, indexes, FieldNames.ActWorkQty), numStyle, formatProvider, out double actVal) &&
                        double.TryParse(GetFieldValue(row, indexes, FieldNames.RemainWorkQty), numStyle, formatProvider, out double remVal))
                    {
                        double total = actVal + remVal;
                        if (total > 0.0001) // Avoid division by zero
                        {
                            double calculatedPct = (actVal / total) * 100.0;
                            return Math.Max(0.0, Math.Min(100.0, calculatedPct));
                        }
                    }
                    break;

                case "CP_Drtn": // Duration % Complete ((Target - Remaining) / Target)
                    if (double.TryParse(GetFieldValue(row, indexes, FieldNames.TargetDurationHrCnt), numStyle, formatProvider, out double targVal) &&
                        double.TryParse(GetFieldValue(row, indexes, FieldNames.RemainDurationHrCnt), numStyle, formatProvider, out double remDurVal))
                    {
                        if (targVal > 0.0001)
                        {
                            double calculatedPct = ((targVal - remDurVal) / targVal) * 100.0;
                            return Math.Max(0.0, Math.Min(100.0, calculatedPct));
                        }
                    }
                    break;
            }

            return 0.0;
        }

        // Creates the Baseline table (04_XER_BASELINE) by finding the earliest MonthUpdate snapshot
        public XerTable Create04XerBaselineTable(XerTable task01Table)
        {
            if (!IsTableValid(task01Table)) return null;

            try
            {
                var baselineTable = new XerTable(EnhancedTableNames.XerBaseline04, task01Table.RowCount);
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

                if (!minMonthUpdate.HasValue) return null; // No valid dates found

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
        public XerTable Create03XerProjWbsTable()
        {
            var projwbsTable = _dataStore.GetTable(TableNames.ProjWbs);
            if (!IsTableValid(projwbsTable)) return null;

            try
            {
                var sourceHeaders = projwbsTable.Headers;
                var idx = projwbsTable.FieldIndexes;

                var finalHeadersList = sourceHeaders.ToList();
                finalHeadersList.Add(FieldNames.WbsIdKey);
                finalHeadersList.Add(FieldNames.ParentWbsIdKey);
                finalHeadersList.Add(FieldNames.MonthUpdate);
                string[] finalHeaders = finalHeadersList.Select(StringInternPool.Intern).ToArray();

                // PERFORMANCE OPTIMIZATION: Pre-calculate indexes
                var finalIndexes = finalHeaders
                    .Select((name, index) => new { name, index })
                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);


                var resultTable = new XerTable(EnhancedTableNames.XerProjWbs03, projwbsTable.RowCount);
                resultTable.SetHeaders(finalHeaders);

                // Track all valid WBS ID Keys to validate parent keys later
                var allWbsIdKeys = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);
                var transformedRowsBag = new ConcurrentBag<DataRow>();

                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };

                Parallel.ForEach(projwbsTable.Rows, parallelOptions, sourceRow =>
                {
                    var row = sourceRow.Fields;
                    var transformed = new string[finalHeaders.Length];
                    string originalFilename = sourceRow.SourceFilename;

                    string wbsIdKey = CreateKey(originalFilename, GetFieldValue(row, idx, FieldNames.WbsId));
                    string parentWbsIdKey = CreateKey(originalFilename, GetFieldValue(row, idx, FieldNames.ParentWbsId));

                    allWbsIdKeys.TryAdd(wbsIdKey, 0); // Use ConcurrentDictionary as a ConcurrentHashSet

                    // Copy existing fields
                    int copyLength = Math.Min(row.Length, sourceHeaders.Length);
                    Array.Copy(row, transformed, copyLength);
                    // Initialize potentially missing fields if source row was shorter
                    for (int i = copyLength; i < sourceHeaders.Length; i++)
                    {
                        transformed[i] = string.Empty;
                    }

                    // Set new key fields using optimized method
                    SetTransformedField(transformed, finalIndexes, FieldNames.WbsIdKey, wbsIdKey);
                    SetTransformedField(transformed, finalIndexes, FieldNames.ParentWbsIdKey, parentWbsIdKey);

                    // Add MonthUpdate value
                    SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(originalFilename));

                    // Intern strings
                    for (int k = 0; k < transformed.Length; k++)
                    {
                        transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);
                    }

                    transformedRowsBag.Add(new DataRow(transformed, originalFilename));
                });

                // Post-processing: Validate ParentWbsIdKey
                // If a parent key doesn't exist as a primary WbsIdKey in the dataset, it's invalid (e.g., reference to an external project WBS or orphaned node)
                int parentKeyIndex = finalIndexes[FieldNames.ParentWbsIdKey];
                foreach (var dataRow in transformedRowsBag)
                {
                    var fields = dataRow.Fields;
                    string parentKey = fields[parentKeyIndex];
                    if (!string.IsNullOrEmpty(parentKey) && !allWbsIdKeys.ContainsKey(parentKey))
                    {
                        fields[parentKeyIndex] = string.Empty; // Nullify invalid parent key
                    }
                    resultTable.AddRow(dataRow);
                }

                return resultTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {EnhancedTableNames.XerProjWbs03}: {ex.Message}");
                return null;
            }
        }

        // Generic method for creating simple enhanced tables that just add keys
        private XerTable CreateSimpleKeyedTable(string sourceTableName, string newTableName, List<Tuple<string, string>> keyMappings)
        {
            var sourceTable = _dataStore.GetTable(sourceTableName);
            if (!IsTableValid(sourceTable)) return null;

            try
            {
                var sourceHeaders = sourceTable.Headers;
                var sourceIndexes = sourceTable.FieldIndexes;

                var finalHeadersList = sourceHeaders.ToList();
                foreach (var mapping in keyMappings)
                {
                    finalHeadersList.Add(mapping.Item1);
                }
                finalHeadersList.Add(FieldNames.MonthUpdate);
                string[] finalHeaders = finalHeadersList.Select(StringInternPool.Intern).ToArray();

                // PERFORMANCE OPTIMIZATION: Pre-calculate indexes
                var finalIndexes = finalHeaders
                    .Select((name, index) => new { name, index })
                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);


                var resultTable = new XerTable(newTableName, sourceTable.RowCount);
                resultTable.SetHeaders(finalHeaders);

                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };
                var transformedRowsBag = new ConcurrentBag<DataRow>();

                Parallel.ForEach(sourceTable.Rows, parallelOptions, sourceRow =>
                {
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
                    SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(originalFilename));

                    // Intern strings
                    for (int k = 0; k < transformed.Length; k++)
                    {
                        transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);
                    }

                    transformedRowsBag.Add(new DataRow(transformed, originalFilename));
                });

                resultTable.AddRows(transformedRowsBag);
                return resultTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {newTableName}: {ex.Message}");
                return null;
            }
        }

        // Definitions for simple keyed tables using the generic method
        public XerTable Create02XerProject() => CreateSimpleKeyedTable(TableNames.Project, EnhancedTableNames.XerProject02,
            new List<Tuple<string, string>> { Tuple.Create(FieldNames.ProjIdKey, FieldNames.ProjectId) });

        public XerTable Create06XerPredecessor() => CreateSimpleKeyedTable(TableNames.TaskPred, EnhancedTableNames.XerPredecessor06,
            new List<Tuple<string, string>> {
        Tuple.Create(FieldNames.TaskIdKey, FieldNames.TaskId),
        Tuple.Create(FieldNames.PredTaskIdKey, FieldNames.PredTaskId)
            });

        public XerTable Create07XerActvType() => CreateSimpleKeyedTable(TableNames.ActvType, EnhancedTableNames.XerActvType07,
            new List<Tuple<string, string>> { Tuple.Create(FieldNames.ActvCodeTypeIdKey, FieldNames.ActvCodeTypeId) });

        public XerTable Create08XerActvCode() => CreateSimpleKeyedTable(TableNames.ActvCode, EnhancedTableNames.XerActvCode08,
            new List<Tuple<string, string>> {
        Tuple.Create(FieldNames.ActvCodeIdKey, FieldNames.ActvCodeId),
        Tuple.Create(FieldNames.ActvCodeTypeIdKey, FieldNames.ActvCodeTypeId)
            });

        public XerTable Create09XerTaskActv() => CreateSimpleKeyedTable(TableNames.TaskActv, EnhancedTableNames.XerTaskActv09,
            new List<Tuple<string, string>> {
        Tuple.Create(FieldNames.ActvCodeIdKey, FieldNames.ActvCodeId),
        Tuple.Create(FieldNames.TaskIdKey, FieldNames.TaskId)
            });

        public XerTable Create10XerCalendar() => CreateSimpleKeyedTable(TableNames.Calendar, EnhancedTableNames.XerCalendar10,
            new List<Tuple<string, string>> { Tuple.Create(FieldNames.ClndrIdKey, FieldNames.ClndrId) });

        public XerTable Create12XerRsrc() => CreateSimpleKeyedTable(TableNames.Rsrc, EnhancedTableNames.XerRsrc12,
                new List<Tuple<string, string>> {
        Tuple.Create(FieldNames.RsrcIdKey, FieldNames.RsrcId),
        Tuple.Create(FieldNames.ClndrIdKey, FieldNames.ClndrId),
        Tuple.Create(FieldNames.UnitIdKey, FieldNames.UnitId)
                });

        public XerTable Create13XerTaskRsrc() => CreateSimpleKeyedTable(TableNames.TaskRsrc, EnhancedTableNames.XerTaskRsrc13,
            new List<Tuple<string, string>> {
        Tuple.Create(FieldNames.RsrcIdKey, FieldNames.RsrcId),
        Tuple.Create(FieldNames.TaskIdKey, FieldNames.TaskId)
            });

        public XerTable Create14XerUmeasure() => CreateSimpleKeyedTable(TableNames.Umeasure, EnhancedTableNames.XerUmeasure14,
            new List<Tuple<string, string>> { Tuple.Create(FieldNames.UnitIdKey, FieldNames.UnitId) });

        // Creates the detailed calendar table (11_XER_CALENDAR_DETAILED) by parsing clndr_data
        public XerTable Create11XerCalendarDetailed()
        {
            var calendarTable = _dataStore.GetTable(TableNames.Calendar);
            if (!IsTableValid(calendarTable)) return null;

            try
            {
                var sourceIndexes = calendarTable.FieldIndexes;
                string[] detailedColumns = {
            FieldNames.ClndrId, FieldNames.CalendarName, FieldNames.CalendarType,
            FieldNames.Date, FieldNames.DayOfWeek, FieldNames.WorkingDay,
            FieldNames.WorkHours, FieldNames.ExceptionType, FieldNames.ClndrIdKey,
            FieldNames.MonthUpdate
        };

                // PERFORMANCE OPTIMIZATION: Pre-calculate indexes
                var finalIndexes = detailedColumns
                    .Select((name, index) => new { name, index })
                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);


                var resultTable = new XerTable(EnhancedTableNames.XerCalendarDetailed11);
                resultTable.SetHeaders(detailedColumns.Select(StringInternPool.Intern).ToArray());

                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };
                var detailedRowsBag = new ConcurrentBag<DataRow>();

                Parallel.ForEach(calendarTable.Rows, parallelOptions, sourceRow =>
                {
                    var row = sourceRow.Fields;
                    string originalFilename = sourceRow.SourceFilename;
                    string clndrId = GetFieldValue(row, sourceIndexes, FieldNames.ClndrId);
                    string clndrData = GetFieldValue(row, sourceIndexes, FieldNames.CalendarData);
                    string dayHrCnt = GetFieldValue(row, sourceIndexes, FieldNames.DayHourCount);
                    string clndrIdKey = CreateKey(originalFilename, clndrId);

                    // Parse the complex clndr_data field
                    var calendarEntries = ParseCalendarData(clndrData, dayHrCnt);

                    foreach (var entry in calendarEntries)
                    {
                        string[] detailedRow = new string[detailedColumns.Length];

                        // Use optimized method
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.ClndrId, clndrId);
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.CalendarName, GetFieldValue(row, sourceIndexes, FieldNames.CalendarName));
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.CalendarType, GetFieldValue(row, sourceIndexes, FieldNames.CalendarType));
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.Date, entry.Date);
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.DayOfWeek, entry.DayOfWeek);
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.WorkingDay, entry.WorkingDay);
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.WorkHours, entry.WorkHours);
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.ExceptionType, entry.ExceptionType);
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.ClndrIdKey, clndrIdKey);

                        // Add MonthUpdate value
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(originalFilename));

                        // Intern strings
                        for (int k = 0; k < detailedRow.Length; k++)
                        {
                            detailedRow[k] = StringInternPool.Intern(detailedRow[k] ?? string.Empty);
                        }

                        detailedRowsBag.Add(new DataRow(detailedRow, originalFilename));
                    }
                });

                resultTable.AddRows(detailedRowsBag);
                return resultTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {EnhancedTableNames.XerCalendarDetailed11}: {ex.Message}");
                return null;
            }
        }

        private class CalendarEntry
        {
            public string Date { get; set; }
            public string DayOfWeek { get; set; }
            public string WorkingDay { get; set; }
            public string WorkHours { get; set; }
            public string ExceptionType { get; set; }
        }

        private List<CalendarEntry> ParseCalendarData(string clndrData, string defaultDayHours)
        {
            if (string.IsNullOrWhiteSpace(clndrData))
                return GetDefaultWorkWeek(defaultDayHours);

            try
            {
                // Check for P6 structured data markers
                bool hasStructuredData = clndrData.Contains("CalendarData") || clndrData.Contains("DaysOfWeek") || clndrData.Contains("(0||");

                if (hasStructuredData)
                {
                    var result = ParseP6CalendarFormat(clndrData, defaultDayHours);
                    if (result.Count > 0) return result;
                }

                // Fallback if structured data is present but parsing fails or yields no results
                return GetDefaultWorkWeek(defaultDayHours);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing calendar data: {ex.Message}");
                return GetDefaultWorkWeek(defaultDayHours);
            }
        }

        // PERFORMANCE OPTIMIZATION: Refactored to use static compiled Regex patterns instead of dynamic Regex in a loop
        private List<CalendarEntry> ParseP6CalendarFormat(string clndrData, string defaultDayHours)
        {
            var entries = new List<CalendarEntry>();
            clndrData = CleanUnicodeData(clndrData);

            // Parse DaysOfWeek section
            int daysStart = clndrData.IndexOf("DaysOfWeek");
            if (daysStart > -1)
            {
                int daysEnd = FindSectionEnd(clndrData, daysStart);
                if (daysEnd > daysStart)
                {
                    string daysSection = clndrData.Substring(daysStart, daysEnd - daysStart);

                    // Use dictionaries to store results from Regex matches
                    var dayContents = new Dictionary<int, string>();
                    var emptyDays = new HashSet<int>();

                    // Match all occurrences using the static compiled Regex (DayPatternRegex)
                    foreach (Match match in DayPatternRegex.Matches(daysSection))
                    {
                        if (int.TryParse(match.Groups[1].Value, out int dayNum) && dayNum >= 1 && dayNum <= 7)
                        {
                            dayContents[dayNum] = match.Groups[2].Value.Trim();
                        }
                    }

                    // Match all occurrences using the static compiled Regex (EmptyDayPatternRegex)
                    foreach (Match match in EmptyDayPatternRegex.Matches(daysSection))
                    {
                        if (int.TryParse(match.Groups[1].Value, out int dayNum) && dayNum >= 1 && dayNum <= 7)
                        {
                            emptyDays.Add(dayNum);
                            // Ensure it's removed from dayContents if somehow matched by both
                            dayContents.Remove(dayNum);
                        }
                    }

                    // Process the results for all 7 days
                    for (int dayNum = 1; dayNum <= 7; dayNum++)
                    {
                        string dayName = GetDayName(dayNum);
                        decimal totalHours = 0;

                        if (dayContents.TryGetValue(dayNum, out string dayContent) && !string.IsNullOrWhiteSpace(dayContent))
                        {
                            // Day has defined work hours
                            totalHours = ParseDayWorkHours(dayContent);
                        }
                        else if (emptyDays.Contains(dayNum))
                        {
                            // Day is explicitly defined as non-working
                            totalHours = 0;
                        }
                        else
                        {
                            // Default P6 logic (if not explicitly defined in DaysOfWeek section)
                            // Sunday (1) and Saturday (7) default to non-work; others use default hours.
                            if (dayNum == 1 || dayNum == 7)
                            {
                                totalHours = 0;
                            }
                            else
                            {
                                if (decimal.TryParse(defaultDayHours, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal defHours))
                                {
                                    totalHours = defHours;
                                }
                                else
                                {
                                    totalHours = 8; // Standard fallback if defaultDayHours is invalid
                                }
                            }
                        }

                        entries.Add(new CalendarEntry
                        {
                            Date = "",
                            DayOfWeek = dayName,
                            WorkingDay = totalHours > 0 ? "Y" : "N",
                            WorkHours = FormatHours(totalHours),
                            ExceptionType = "Standard"
                        });
                    }
                }
            }

            // If DaysOfWeek parsing failed to produce a full week, use the default week definition
            if (entries.Count == 0)
                entries.AddRange(GetDefaultWorkWeek(defaultDayHours));

            // Parse Exceptions/Holidays (which overlay the standard week)
            ParseExceptions(clndrData, entries);

            return entries;
        }

        private void ParseExceptions(string clndrData, List<CalendarEntry> entries)
        {
            var exceptionSections = new[] { "Exceptions", "HolidayOrExceptions", "HolidayOrException" };

            foreach (var sectionName in exceptionSections)
            {
                int excStart = clndrData.IndexOf(sectionName);
                if (excStart > -1)
                {
                    int excEnd = FindSectionEnd(clndrData, excStart);
                    string excSection = excEnd > excStart ?
                        clndrData.Substring(excStart, excEnd - excStart) :
                        clndrData.Substring(excStart);

                    // Use pre-compiled Regex patterns
                    ParseExceptionMatches(ExcPatternRegex.Matches(excSection), entries);
                    ParseExceptionMatches(ExcPatternRegex2.Matches(excSection), entries);
                }
            }
        }

        private void ParseExceptionMatches(MatchCollection matches, List<CalendarEntry> entries)
        {
            foreach (Match excMatch in matches)
            {
                if (excMatch.Success && excMatch.Groups.Count >= 3)
                {
                    // Group 2 contains the OLE date serial
                    if (int.TryParse(excMatch.Groups[2].Value, out int dateSerial))
                    {
                        DateTime excDate = ConvertFromOleDate(dateSerial);
                        // Group 3 contains the work content (time slots)
                        string workContent = excMatch.Groups.Count > 3 ? excMatch.Groups[3].Value : "";
                        decimal hours = ParseDayWorkHours(workContent);

                        entries.Add(new CalendarEntry
                        {
                            Date = excDate.ToString("yyyy-MM-dd"),
                            DayOfWeek = excDate.DayOfWeek.ToString(),
                            WorkingDay = hours > 0 ? "Y" : "N",
                            WorkHours = FormatHours(hours),
                            ExceptionType = DetermineExceptionType(hours)
                        });
                    }
                }
            }
        }

        private string DetermineExceptionType(decimal hours)
        {
            if (hours > 0)
                return "Exception - Working";
            return "Holiday"; // Non-working exception
        }

        private int FindSectionEnd(string data, int sectionStart)
        {
            // Defines markers for subsequent sections in clndr_data
            var nextSections = new[] { "VIEW", "Exceptions", "HolidayOrExceptions", "Resources", "DaysOfWeek" };
            int minEnd = data.Length;

            foreach (var section in nextSections)
            {
                // Search for the next section marker after the current section start
                // Ensure we don't find the same section marker again
                int pos = data.IndexOf(section, sectionStart + 5);
                if (pos > -1 && pos < minEnd && pos != sectionStart)
                    minEnd = pos;
            }
            return minEnd;
        }

        // Cleans up potentially corrupted or Unicode characters in clndr_data
        private string CleanUnicodeData(string data)
        {
            if (string.IsNullOrEmpty(data)) return data;

            var cleaned = new StringBuilder(data.Length);
            foreach (char c in data)
            {
                // Keep printable ASCII characters and common punctuation/symbols
                if ((c >= 32 && c <= 126) || char.IsPunctuation(c) || char.IsSymbol(c))
                {
                    cleaned.Append(c);
                }
                // Replace control characters (like newlines, tabs) and others with spaces
                else
                {
                    cleaned.Append(' ');
                }
            }

            string result = cleaned.ToString();
            // Use pre-compiled Regex to clean up spacing around delimiters
            result = CleanRegex1.Replace(result, "$1");
            result = CleanRegex2.Replace(result, "$1");
            // Collapse multiple spaces (using non-compiled Regex here as it's run once per calendar)
            result = Regex.Replace(result, @"\s{2,}", " ");

            return result;
        }

        // Parses work hours from time slot definitions within clndr_data
        // This logic handles various P6 time slot formats (s=start, f=finish)
        private decimal ParseDayWorkHours(string content)
        {
            if (string.IsNullOrWhiteSpace(content)) return 0;

            decimal totalHours = 0;

            // Pattern 1: (0||N (s|HH:MM|f|HH:MM) ()) - Most common format
            // Regex is defined locally here as these patterns are less critical than the main parsing ones,
            // and compiling them adds overhead if they aren't used frequently.
            string timeSlotPattern = @"\(0\|\|\d+\s*\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)\s*\(\s*\)\s*\)";
            var timeSlotRegex = new Regex(timeSlotPattern, RegexOptions.Singleline);
            var matches = timeSlotRegex.Matches(content);

            if (matches.Count > 0)
            {
                foreach (Match match in matches)
                {
                    totalHours += ExtractHoursFromMatch(match);
                }
                return totalHours;
            }

            // Pattern 2: (s|HH:MM|f|HH:MM) - Alternative format
            string altPattern1 = @"\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)";
            var altRegex1 = new Regex(altPattern1, RegexOptions.Singleline);
            matches = altRegex1.Matches(content);

            if (matches.Count > 0)
            {
                foreach (Match match in matches)
                {
                    totalHours += ExtractHoursFromMatch(match);
                }
                return totalHours;
            }

            // Pattern 3: s|HH:MM|f|HH:MM (Simplified format)
            string simplePattern = @"([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})";
            var simpleRegex = new Regex(simplePattern, RegexOptions.Singleline);
            matches = simpleRegex.Matches(content);

            if (matches.Count > 0)
            {
                // Handle potential duplicates in simplified format parsing
                var processedPairs = new HashSet<string>();
                foreach (Match match in matches)
                {
                    string timeKey = $"{match.Groups[2].Value}-{match.Groups[4].Value}";
                    if (processedPairs.Contains(timeKey)) continue;
                    processedPairs.Add(timeKey);

                    totalHours += ExtractHoursFromMatch(match);
                }
            }

            return totalHours;
        }

        private decimal ExtractHoursFromMatch(Match match)
        {
            if (match.Groups.Count >= 5)
            {
                if (TimeSpan.TryParse(match.Groups[2].Value, out TimeSpan time1) && TimeSpan.TryParse(match.Groups[4].Value, out TimeSpan time2))
                {
                    string type1 = match.Groups[1].Value;
                    string type2 = match.Groups[3].Value;
                    TimeSpan start, end;

                    // Determine start and end times based on 's' (start) and 'f' (finish) markers
                    if (type1 == "s" && type2 == "f") { start = time1; end = time2; }
                    else if (type1 == "f" && type2 == "s") { start = time2; end = time1; }
                    else
                    {
                        // Handle ambiguous cases (e.g., both 's' or both 'f') by ordering them temporally
                        start = time1 < time2 ? time1 : time2;
                        end = time1 < time2 ? time2 : time1;
                    }

                    return CalculateHoursBetween(start, end);
                }
            }
            return 0;
        }


        private decimal CalculateHoursBetween(TimeSpan start, TimeSpan end)
        {
            if (start == TimeSpan.Zero && end == TimeSpan.Zero) return 0;
            if (start == end) return 0;

            if (end > start) return (decimal)(end - start).TotalHours;
            else
            {
                // Handle overnight shifts (e.g., 22:00 to 06:00)
                return (decimal)(TimeSpan.FromHours(24) - start + end).TotalHours;
            }
        }

        // Converts OLE Automation Date (used in P6 clndr_data) to DateTime
        private DateTime ConvertFromOleDate(int oleDate)
        {
            try
            {
                // OLE Date base is December 30, 1899
                DateTime baseDate = new DateTime(1899, 12, 30);

                if (oleDate < 1) return new DateTime(1900, 1, 1); // Handle invalid dates

                // Note: DateTime.FromOADate handles the conversion correctly, including the historical leap year intricacies.
                // However, P6 XER files typically use integer OLE dates (days only).
                return baseDate.AddDays(oleDate);

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error converting OLE date {oleDate}: {ex.Message}");
                return new DateTime(2000, 1, 1); // Fallback date
            }
        }

        private string GetDayName(int dayNumber)
        {
            // P6 convention: 1=Sunday, 7=Saturday
            switch (dayNumber)
            {
                case 1: return "Sunday";
                case 2: return "Monday";
                case 3: return "Tuesday";
                case 4: return "Wednesday";
                case 5: return "Thursday";
                case 6: return "Friday";
                case 7: return "Saturday";
                default: return "Unknown";
            }
        }

        private string FormatHours(decimal hours)
        {
            if (hours == 0) return "0";
            if (hours == Math.Floor(hours)) return hours.ToString("0");
            return hours.ToString("0.##"); // Format with up to 2 decimal places if needed
        }

        // Provides a default work week definition if parsing fails
        private List<CalendarEntry> GetDefaultWorkWeek(string defaultDayHours)
        {
            decimal defaultHours = 8; // Standard fallback
            if (!string.IsNullOrWhiteSpace(defaultDayHours))
            {
                if (decimal.TryParse(defaultDayHours, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal parsed))
                    defaultHours = Math.Max(0, parsed);
            }

            // Heuristic to detect 24x7 calendars: if default hours are high (e.g., >= 12), assume weekends work too
            bool is24x7 = defaultHours >= 12;

            var days = new[]
            {
        ("Sunday", is24x7 ? "Y" : "N", is24x7 ? defaultHours : 0m),
        ("Monday", "Y", defaultHours),
        ("Tuesday", "Y", defaultHours),
        ("Wednesday", "Y", defaultHours),
        ("Thursday", "Y", defaultHours),
        ("Friday", "Y", defaultHours),
        ("Saturday", is24x7 ? "Y" : "N", is24x7 ? defaultHours : 0m)
    };

            return days.Select(d => new CalendarEntry
            {
                DayOfWeek = d.Item1,
                WorkingDay = d.Item2,
                WorkHours = FormatHours(d.Item3),
                ExceptionType = "Standard",
                Date = ""
            }).ToList();
        }
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
            public string Message;
            // Fields for UI status visualization
            public string FilePath;
            public string FileStatus;
            public Color? StatusColor;
        }


        // UI/UX OPTIMIZATION: Added CancellationToken and detailed progress reporting
        public async Task<XerDataStore> ParseMultipleXerFilesAsync(List<string> filePaths, IProgress<DetailedProgress> progress, CancellationToken cancellationToken)
        {
            // Clear caches before starting a new batch
            StringInternPool.Clear();
            DateParser.ClearCache();

            int fileCount = filePaths.Count;
            // Configure parallel options including the CancellationToken
            var parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = Math.Min(PerformanceConfig.MaxParallelFiles, fileCount),
                CancellationToken = cancellationToken
            };

            var fileIndex = 0;
            var intermediateStores = new ConcurrentBag<XerDataStore>();

            // Run parsing in a background task
            var combinedStore = await Task.Run<XerDataStore>(() =>
            {
                try
                {
                    Parallel.ForEach(filePaths, parallelOptions, file =>
                    {
                        // Check for cancellation before starting the file
                        cancellationToken.ThrowIfCancellationRequested();

                        var currentIndex = Interlocked.Increment(ref fileIndex);
                        string shortFileName = Path.GetFileName(file);

                        // Report starting status for the file visualization
                        progress?.Report(new DetailedProgress { Percent = (currentIndex - 1) * 100 / fileCount, Message = $"Starting: {shortFileName}", FilePath = file, FileStatus = "Processing", StatusColor = Color.Blue });

                        try
                        {
                            if (!File.Exists(file))
                            {
                                progress?.Report(new DetailedProgress { Percent = currentIndex * 100 / fileCount, Message = $"Skipped (Missing): {shortFileName}", FilePath = file, FileStatus = "Skipped", StatusColor = Color.Orange });
                                return; // Continue to the next iteration in Parallel.ForEach
                            }

                            // Define the progress callback for the parser
                            Action<int, string> parserProgressCallback = (p, s) =>
                            {
                                // Calculate overall progress based on file progress
                                int overallProgress = ((currentIndex - 1) * 100 + p) / fileCount;
                                // Report detailed progress without changing file visualization status yet
                                progress?.Report(new DetailedProgress { Percent = overallProgress, Message = s });
                            };

                            // Parse the file, passing the cancellation token down
                            var singleFileStore = _parser.ParseXerFile(file, parserProgressCallback, cancellationToken);
                            intermediateStores.Add(singleFileStore);

                            // Report success status for the file visualization
                            progress?.Report(new DetailedProgress { Percent = currentIndex * 100 / fileCount, Message = $"Completed: {shortFileName}", FilePath = file, FileStatus = "Success", StatusColor = Color.DarkGreen });

                        }
                        catch (Exception ex)
                        {
                            if (ex is OperationCanceledException) throw; // Propagate cancellation immediately

                            // Report failure status for the file visualization
                            progress?.Report(new DetailedProgress { Percent = currentIndex * 100 / fileCount, Message = $"Failed: {shortFileName}", FilePath = file, FileStatus = "Error", StatusColor = Color.Red });

                            // If a file fails fundamentally (e.g., I/O error, corrupted format), throw an exception to stop the process
                            // This maintains the behavior of the original code where one failure stops the batch.
                            throw new Exception($"Failed to parse file '{shortFileName}': {ex.Message}", ex);
                        }
                    });
                }
                catch (OperationCanceledException)
                {
                    progress?.Report(new DetailedProgress { Percent = 0, Message = "Parsing canceled by user." });
                    // Return whatever data was successfully parsed before cancellation
                    var canceledStore = new XerDataStore();
                    foreach (var store in intermediateStores)
                    {
                        canceledStore.MergeStore(store);
                    }
                    return canceledStore;
                }

                // Merge results (if not canceled)
                progress?.Report(new DetailedProgress { Percent = 100, Message = "Merging parsed data..." });
                var storeToReturn = new XerDataStore();
                foreach (var store in intermediateStores)
                {
                    storeToReturn.MergeStore(store);
                }
                return storeToReturn;

            }, cancellationToken); // Pass token to Task.Run as well

            return combinedStore;
        }

        // UI/UX OPTIMIZATION: Added CancellationToken
        public async Task<List<string>> ExportTablesAsync(XerDataStore dataStore, List<string> tablesToExport, string outputDirectory, IProgress<(int percent, string message)> progress, CancellationToken cancellationToken)
        {
            var exportedFiles = new ConcurrentBag<string>();
            var transformer = new XerTransformer(dataStore);
            // Cache for generated enhanced tables
            var enhancedCache = new ConcurrentDictionary<string, XerTable>(StringComparer.OrdinalIgnoreCase);

            // Configure parallel options including the CancellationToken
            var parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = PerformanceConfig.MaxParallelFiles,
                CancellationToken = cancellationToken
            };

            progress?.Report((0, "Generating enhanced tables..."));

            // Identify enhanced tables requested for export (those not already in the raw data store)
            var enhancedTablesToGenerate = tablesToExport.Where(t => !dataStore.ContainsTable(t)).Distinct().ToList();

            if (enhancedTablesToGenerate.Any())
            {
                await Task.Run(() =>
                {
                    try
                    {
                        // Dependency Management: Pre-generate TASK01 if needed by others (e.g., BASELINE04), as it cannot be generated in the parallel loop if others depend on it.
                        bool needsTask01 = enhancedTablesToGenerate.Contains(EnhancedTableNames.XerTask01) || enhancedTablesToGenerate.Contains(EnhancedTableNames.XerBaseline04);
                        if (needsTask01)
                        {
                            // Check dependencies for TASK01
                            if (dataStore.ContainsTable(TableNames.Task) && dataStore.ContainsTable(TableNames.Calendar) && dataStore.ContainsTable(TableNames.Project))
                            {
                                var task01Data = transformer.Create01XerTaskTable();
                                if (task01Data != null) enhancedCache.TryAdd(EnhancedTableNames.XerTask01, task01Data);
                            }
                        }

                        // Generate other enhanced tables in parallel
                        Parallel.ForEach(enhancedTablesToGenerate, parallelOptions, tableName =>
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            if (enhancedCache.ContainsKey(tableName)) return; // Already generated (e.g., TASK01)

                            var generatedTable = GenerateEnhancedTable(transformer, tableName, enhancedCache);
                            if (generatedTable != null)
                            {
                                enhancedCache.TryAdd(tableName, generatedTable);
                            }
                        });
                    }
                    catch (OperationCanceledException)
                    {
                        progress?.Report((0, "Enhanced table generation canceled."));
                        return; // Stop generation if canceled
                    }

                }, cancellationToken);
            }

            // If canceled during generation, stop the export process
            if (cancellationToken.IsCancellationRequested)
            {
                return exportedFiles.ToList();
            }


            progress?.Report((80, "Exporting tables to CSV..."));
            var exportProgress = new ProgressCounter(tablesToExport.Count, progress);

            await Task.Run(() =>
            {
                try
                {
                    Parallel.ForEach(tablesToExport.OrderBy(n => n), parallelOptions, tableName =>
                    {
                        // Check for cancellation before starting export of the table
                        cancellationToken.ThrowIfCancellationRequested();

                        exportProgress.UpdateStatus($"Exporting: {tableName}");

                        XerTable tableToExport = null;
                        try
                        {
                            // Determine source of the table (raw data or enhanced cache)
                            if (dataStore.ContainsTable(tableName))
                            {
                                tableToExport = dataStore.GetTable(tableName);
                            }
                            else if (enhancedCache.TryGetValue(tableName, out XerTable cachedTable))
                            {
                                tableToExport = cachedTable;
                            }

                            // Export if table exists and is not empty
                            if (tableToExport != null && !tableToExport.IsEmpty)
                            {
                                string csvFilePath = Path.Combine(outputDirectory, $"{tableName}.csv");
                                _exporter.WriteTableToCsv(tableToExport, csvFilePath);
                                exportedFiles.Add(csvFilePath);
                            }
                        }
                        catch (Exception ex)
                        {
                            // If an export fails, throw an exception to stop the parallel loop
                            throw new Exception($"Error exporting table '{tableName}': {ex.Message}", ex);
                        }

                        exportProgress.Increment($"Exported: {tableName}");
                    });
                }
                catch (OperationCanceledException)
                {
                    // Handle cancellation during the export loop
                    progress?.Report((0, "Export canceled by user."));
                }

            }, cancellationToken);

            // Clean up memory after export
            StringInternPool.Clear();
            DateParser.ClearCache();

            if (cancellationToken.IsCancellationRequested)
            {
                progress?.Report((0, $"Export process canceled. {exportedFiles.Count} files written."));
            }
            else
            {
                progress?.Report((100, $"Export process complete. {exportedFiles.Count} files written."));
            }

            return exportedFiles.ToList();
        }

        private XerTable GenerateEnhancedTable(XerTransformer transformer, string tableName, ConcurrentDictionary<string, XerTable> cache)
        {
            switch (tableName)
            {
                case EnhancedTableNames.XerProject02: return transformer.Create02XerProject();
                case EnhancedTableNames.XerProjWbs03: return transformer.Create03XerProjWbsTable();
                case EnhancedTableNames.XerBaseline04:
                    // Depends on TASK01 (must be present in cache)
                    if (cache.TryGetValue(EnhancedTableNames.XerTask01, out XerTable task01))
                    {
                        return transformer.Create04XerBaselineTable(task01);
                    }
                    return null;
                case EnhancedTableNames.XerPredecessor06: return transformer.Create06XerPredecessor();
                case EnhancedTableNames.XerActvType07: return transformer.Create07XerActvType();
                case EnhancedTableNames.XerActvCode08: return transformer.Create08XerActvCode();
                case EnhancedTableNames.XerTaskActv09: return transformer.Create09XerTaskActv();
                case EnhancedTableNames.XerCalendar10: return transformer.Create10XerCalendar();
                case EnhancedTableNames.XerCalendarDetailed11: return transformer.Create11XerCalendarDetailed();
                case EnhancedTableNames.XerRsrc12: return transformer.Create12XerRsrc();
                case EnhancedTableNames.XerTaskRsrc13: return transformer.Create13XerTaskRsrc();
                case EnhancedTableNames.XerUmeasure14: return transformer.Create14XerUmeasure();
                // TASK01 is handled separately due to dependencies, but included here for completeness
                case EnhancedTableNames.XerTask01: return transformer.Create01XerTaskTable();
            }
            return null;
        }
    }

    // Helper class for thread-safe progress counting
    internal class ProgressCounter
    {
        private readonly int _total;
        private readonly IProgress<(int percent, string message)> _progress;
        private int _count;
        private readonly object _lock = new object();

        public ProgressCounter(int total, IProgress<(int percent, string message)> progress)
        {
            _total = total;
            _progress = progress;
            _count = 0;
        }

        public void Increment(string message)
        {
            lock (_lock)
            {
                _count++;
                Report(message);
            }
        }

        public void UpdateStatus(string message)
        {
            lock (_lock)
            {
                Report(message);
            }
        }

        private void Report(string message)
        {
            if (_progress != null && _total > 0)
            {
                int percent = (int)((double)_count / _total * 100);
                percent = Math.Min(100, percent);
                // Report progress relative to the total (80% base for generation + 20% for export)
                int adjustedPercent = 80 + (int)(percent * 0.2);
                _progress.Report((adjustedPercent, $"{message} ({_count}/{_total})"));
            }
        }
    }

    // Main Application Form (UI)
    public partial class MainForm : Form
    {
        // UI Components (Declarations)
        private System.Windows.Forms.SplitContainer splitContainerMain;
        private System.Windows.Forms.SplitContainer splitContainerResults;
        private System.Windows.Forms.StatusStrip statusStrip;
        private System.Windows.Forms.ToolStripStatusLabel toolStripStatusLabel;
        private System.Windows.Forms.ToolStripProgressBar toolStripProgressBar;
        private System.Windows.Forms.MenuStrip menuStrip;
        private System.Windows.Forms.ToolStripMenuItem fileToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem exitToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem helpToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem aboutToolStripMenuItem;
        private System.Windows.Forms.GroupBox grpInputFiles;
        private System.Windows.Forms.ListView lvwXerFiles;
        private System.Windows.Forms.ColumnHeader colFileName;
        private System.Windows.Forms.ColumnHeader colFilePath;
        private System.Windows.Forms.ColumnHeader colStatus; // UI/UX OPTIMIZATION: Added Status column
        private System.Windows.Forms.ToolStrip toolStripFileActions;
        private System.Windows.Forms.ToolStripButton btnAddFile;
        private System.Windows.Forms.ToolStripButton btnRemoveFile;
        private System.Windows.Forms.ToolStripButton btnClearFiles;
        private System.Windows.Forms.Label lblDragDropHint;
        private System.Windows.Forms.GroupBox grpOutput;
        private System.Windows.Forms.TextBox txtOutputPath;
        private System.Windows.Forms.Button btnSelectOutput;
        private System.Windows.Forms.Button btnParseXer;
        private System.Windows.Forms.GroupBox grpExtractedTables;

        // UI/UX OPTIMIZATION: Replaced ListBox with CheckedListBox
        private System.Windows.Forms.CheckedListBox lstTables;

        private System.Windows.Forms.TextBox txtTableFilter;
        private System.Windows.Forms.Label lblFilter;
        private System.Windows.Forms.GroupBox grpActivityLog;
        private System.Windows.Forms.TextBox txtActivityLog;
        private System.Windows.Forms.GroupBox grpPowerBI;
        private System.Windows.Forms.CheckBox chkCreatePowerBiTables;
        private System.Windows.Forms.Label lblPbiStatus;
        private System.Windows.Forms.Button btnPbiDetails;
        private System.Windows.Forms.Panel panelExportActions;
        private System.Windows.Forms.Button btnExportAll;
        private System.Windows.Forms.Button btnExportSelected;
        private System.Windows.Forms.Button btnCancelOperation; // UI/UX OPTIMIZATION: Added Cancel button
        private System.Windows.Forms.ToolTip toolTip;

        // Application State
        private readonly List<string> _xerFilePaths = new List<string>();
        private string _outputDirectory;
        private List<string> _allTableNames = new List<string>();
        private XerDataStore _dataStore;
        private readonly ProcessingService _processingService;
        private CancellationTokenSource _cancellationTokenSource; // UI/UX OPTIMIZATION: Cancellation token source

        // Power BI Dependency Flags
        private bool _canCreateTask01 = false; private bool _canCreateProjWbs03 = false; private bool _canCreateBaseline04 = false; private bool _canCreateProject02 = false; private bool _canCreatePredecessor06 = false; private bool _canCreateActvType07 = false; private bool _canCreateActvCode08 = false; private bool _canCreateTaskActv09 = false; private bool _canCreateCalendar10 = false; private bool _canCreateCalendarDetailed11 = false; private bool _canCreateRsrc12 = false; private bool _canCreateTaskRsrc13 = false; private bool _canCreateUmeasure14 = false;

        public MainForm()
        {
            _processingService = new ProcessingService();
            _dataStore = new XerDataStore();
            InitializeComponent();
            InitializeUIState();
            LoadSettings(); // UI/UX OPTIMIZATION: Load persisted settings
        }

        // UI/UX OPTIMIZATION: Placeholder for loading settings (e.g., from Properties.Settings.Default)
        private void LoadSettings()
        {
            // Example Implementation (Requires defining settings in Project Properties):
            /*
            if (!string.IsNullOrEmpty(Properties.Settings.Default.LastOutputPath) && Directory.Exists(Properties.Settings.Default.LastOutputPath))
            {
                _outputDirectory = Properties.Settings.Default.LastOutputPath;
                txtOutputPath.Text = _outputDirectory;
            }
            chkCreatePowerBiTables.Checked = Properties.Settings.Default.GeneratePowerBI;
            */

            // If no settings exist, default output to a subfolder in Documents
            if (string.IsNullOrEmpty(_outputDirectory))
            {
                try
                {
                    string defaultPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "XER_Exports");
                    if (!Directory.Exists(defaultPath))
                    {
                        Directory.CreateDirectory(defaultPath);
                    }
                    _outputDirectory = defaultPath;
                    txtOutputPath.Text = _outputDirectory;
                }
                catch (Exception ex)
                {
                    LogActivity($"Could not set default output directory: {ex.Message}");
                }
            }
        }

        // UI/UX OPTIMIZATION: Placeholder for saving settings
        private void SaveSettings()
        {
            // Example Implementation:
            /*
            Properties.Settings.Default.LastOutputPath = _outputDirectory;
            Properties.Settings.Default.GeneratePowerBI = chkCreatePowerBiTables.Checked;
            Properties.Settings.Default.Save();
            */
        }

        private void InitializeUIState()
        {
            // Enable Drag and Drop
            this.lvwXerFiles.AllowDrop = true;
            this.lvwXerFiles.DragEnter += new DragEventHandler(LvwXerFiles_DragEnter);
            this.lvwXerFiles.DragDrop += new DragEventHandler(LvwXerFiles_DragDrop);

            this.Resize += new System.EventHandler(this.MainForm_Resize);

            // Initialize CheckedListBox specific properties
            this.lstTables.ItemCheck += new ItemCheckEventHandler(this.LstTables_ItemCheck);
            this.lstTables.CheckOnClick = true; // Allows checking with a single click

            UpdateStatus("Ready");
            LogActivity("Application started.");
            UpdateInputButtonsState();
            UpdateExportButtonState();
            UpdatePbiCheckboxState();
            ResizeListViewColumns();
        }

        // --- Event Handlers ---

        private void ExitToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Application.Exit();
        }

        private void AboutToolStripMenuItem_Click(object sender, EventArgs e)
        {
            ShowInfo("Primavera P6 XER to CSV Converter\nVersion: 2.3 (Optimized)\n\nSOFTWARE COPYRIGHT NOTICE\r\nCopyright © 2025 Ricardo Aguirre. All Rights Reserved.\r\nUnauthorized use, copying, or sharing of this software is strictly prohibited. Written permission required for any use.\r\nTHE SOFTWARE IS PROVIDED \"AS IS\" WITHOUT WARRANTY OF ANY KIND. RICARDO AGUIRRE SHALL NOT BE LIABLE FOR ANY DAMAGES ARISING FROM USE OF THIS SOFTWARE.\r\nBy using this software, you agree to these terms.", "About");
        }

        private void BtnAddFile_Click(object sender, EventArgs e)
        {
            using (OpenFileDialog openFileDialog = new OpenFileDialog
            {
                Filter = "XER files (*.xer)|*.xer|All files (*.*)|*.*",
                Title = "Select Primavera P6 XER File(s)",
                Multiselect = true
            })
            {
                if (openFileDialog.ShowDialog() == DialogResult.OK)
                {
                    AddXerFiles(openFileDialog.FileNames);
                }
            }
        }

        private void BtnRemoveFile_Click(object sender, EventArgs e)
        {
            RemoveSelectedXerFiles();
        }

        private void BtnClearFiles_Click(object sender, EventArgs e)
        {
            if (MessageBox.Show("Are you sure you want to clear all files and results?", "Confirm Clear All", MessageBoxButtons.YesNo, MessageBoxIcon.Question) == DialogResult.Yes)
            {
                _xerFilePaths.Clear();
                lvwXerFiles.Items.Clear();
                ClearResults();
                UpdateInputButtonsState();
                LogActivity("File list and results cleared by user.");
            }
        }

        private void LvwXerFiles_SelectedIndexChanged(object sender, EventArgs e)
        {
            UpdateInputButtonsState();
        }

        private void BtnSelectOutput_Click(object sender, EventArgs e)
        {
            using (FolderBrowserDialog folderDialog = new FolderBrowserDialog
            {
                Description = "Select Output Directory for CSV Files",
                SelectedPath = _outputDirectory // Start browsing from the current directory
            })
            {
                if (folderDialog.ShowDialog() == DialogResult.OK)
                {
                    _outputDirectory = folderDialog.SelectedPath;
                    txtOutputPath.Text = _outputDirectory;
                    UpdateInputButtonsState();
                    UpdateExportButtonState();
                    LogActivity($"Output directory set: {_outputDirectory}");
                    SaveSettings(); // Persist the change
                }
            }
        }

        // Main Parsing Logic
        private async void BtnParseXer_Click(object sender, EventArgs e)
        {
            if (!ValidateInputs()) return;

            ClearResults(false); // Clear previous results without forcing GC immediately
            LogActivity("Starting XER parsing...");

            // Reset status visualization for all files
            foreach (ListViewItem item in lvwXerFiles.Items)
            {
                item.SubItems[2].Text = "Pending"; // colStatus index
                item.ForeColor = SystemColors.ControlText;
            }

            SetUIEnabled(false); // Disables UI, enables Cancel button
            UpdateStatus("Parsing XER File(s)...");
            toolStripProgressBar.Visible = true;

            // Initialize CancellationTokenSource for the operation
            _cancellationTokenSource = new CancellationTokenSource();
            var token = _cancellationTokenSource.Token;

            // Define progress reporting using the detailed structure
            var progress = new Progress<ProcessingService.DetailedProgress>(p =>
            {
                UpdateStatus(p.Message);
                toolStripProgressBar.Value = p.Percent;
                // Update file-specific status in the ListView
                if (!string.IsNullOrEmpty(p.FilePath) && !string.IsNullOrEmpty(p.FileStatus))
                {
                    UpdateFileStatus(p.FilePath, p.FileStatus, p.StatusColor);
                }
            });

            try
            {
                var stopwatch = Stopwatch.StartNew();
                // Call the async service with the token and detailed progress
                var result = await _processingService.ParseMultipleXerFilesAsync(_xerFilePaths, progress, token);
                stopwatch.Stop();

                _dataStore = result;

                // Handle UI updates based on results and cancellation status
                if (token.IsCancellationRequested && _dataStore.TableCount == 0)
                {
                    UpdateStatus("Parsing canceled.");
                    LogActivity("Parsing canceled by user. No data extracted.");
                }
                else
                {
                    UpdateTableList();
                    CheckEnhancedTableDependencies();
                    UpdatePbiCheckboxState();
                    UpdateExportButtonState();
                    toolStripProgressBar.Value = 100;

                    string summary = $"Parsing complete. Extracted {_dataStore.TableCount} tables. Time elapsed: {stopwatch.Elapsed.TotalSeconds:F2}s.";
                    if (token.IsCancellationRequested)
                    {
                        summary = $"Parsing canceled. {summary}"; // Canceled but partial data exists
                    }
                    UpdateStatus(summary);
                    LogActivity(summary);
                }
            }
            catch (Exception ex)
            {
                // Handle errors (including those thrown from ProcessingService if a specific file fails)
                ShowError($"Error parsing XER file(s): {ex.Message}\n\nDetails: {ex.InnerException?.Message}");
                LogActivity($"ERROR during parsing: {ex.Message}");
                ClearResults(); // Clear partial data on failure
                UpdateStatus("Parsing failed.");
            }
            finally
            {
                SetUIEnabled(true); // Re-enable UI
                toolStripProgressBar.Visible = false;
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
            }
        }

        // Export Handlers
        private async void BtnExportAll_Click(object sender, EventArgs e)
        {
            LogActivity("Starting export of ALL tables...");
            await ExportTablesAsync(GetAllTableNamesToExport());
        }

        private async void BtnExportSelected_Click(object sender, EventArgs e)
        {
            LogActivity("Starting export of SELECTED tables...");
            await ExportTablesAsync(GetSelectedTableNamesToExport());
        }

        // UI/UX OPTIMIZATION: New Cancel button handler
        private void BtnCancelOperation_Click(object sender, EventArgs e)
        {
            if (_cancellationTokenSource != null && !_cancellationTokenSource.IsCancellationRequested)
            {
                LogActivity("Cancellation requested by user.");
                UpdateStatus("Cancelling operation...");
                _cancellationTokenSource.Cancel();
                btnCancelOperation.Enabled = false; // Disable button while cancellation propagates
            }
        }

        // UI/UX OPTIMIZATION: Event handler for CheckedListBox
        private void LstTables_ItemCheck(object sender, ItemCheckEventArgs e)
        {
            // Use BeginInvoke to ensure the UI state (CheckedItems.Count) is updated *after* the check event completes
            this.BeginInvoke(new Action(() => UpdateExportButtonState()));
        }

        private void TxtTableFilter_TextChanged(object sender, EventArgs e)
        {
            FilterTableList(txtTableFilter.Text);
        }

        // Power BI Handlers
        private void ChkCreatePowerBiTables_CheckedChanged(object sender, EventArgs e)
        {
            UpdateExportButtonState();
            SaveSettings(); // Persist the change

            if (chkCreatePowerBiTables.Checked && !AnyPbiDependencyMet())
            {
                // Provide feedback if the user enables it but dependencies are missing
                ShowWarning(GetMissingDependenciesMessage("Cannot create any Power BI tables due to missing dependencies."));
            }
        }

        private void BtnPbiDetails_Click(object sender, EventArgs e)
        {
            // Generate detailed status report
            string message = "Power BI Enhanced Table Generation Status:\n\n";
            message += GetPbiTableStatusDetail(EnhancedTableNames.XerTask01, _canCreateTask01);
            message += GetPbiTableStatusDetail(EnhancedTableNames.XerProject02, _canCreateProject02);
            message += GetPbiTableStatusDetail(EnhancedTableNames.XerProjWbs03, _canCreateProjWbs03);
            message += GetPbiTableStatusDetail(EnhancedTableNames.XerBaseline04, _canCreateBaseline04);
            message += GetPbiTableStatusDetail(EnhancedTableNames.XerPredecessor06, _canCreatePredecessor06);
            message += GetPbiTableStatusDetail(EnhancedTableNames.XerActvType07, _canCreateActvType07);
            message += GetPbiTableStatusDetail(EnhancedTableNames.XerActvCode08, _canCreateActvCode08);
            message += GetPbiTableStatusDetail(EnhancedTableNames.XerTaskActv09, _canCreateTaskActv09);
            message += GetPbiTableStatusDetail(EnhancedTableNames.XerCalendar10, _canCreateCalendar10);
            message += GetPbiTableStatusDetail(EnhancedTableNames.XerCalendarDetailed11, _canCreateCalendarDetailed11);
            message += GetPbiTableStatusDetail(EnhancedTableNames.XerRsrc12, _canCreateRsrc12);
            message += GetPbiTableStatusDetail(EnhancedTableNames.XerTaskRsrc13, _canCreateTaskRsrc13);
            message += GetPbiTableStatusDetail(EnhancedTableNames.XerUmeasure14, _canCreateUmeasure14);

            message += "\n" + GetMissingDependenciesMessage("Summary:");
            ShowInfo(message, "Power BI Details");
        }

        private string GetPbiTableStatusDetail(string tableName, bool canCreate)
        {
            return $"[{(canCreate ? "\u2713" : "\u2717")}] {tableName}\n"; // Unicode Checkmark or Cross
        }

        // Drag and Drop Handlers
        private void LvwXerFiles_DragEnter(object sender, DragEventArgs e)
        {
            if (e.Data.GetDataPresent(DataFormats.FileDrop)) e.Effect = DragDropEffects.Copy;
            else e.Effect = DragDropEffects.None;
        }

        private void LvwXerFiles_DragDrop(object sender, DragEventArgs e)
        {
            string[] files = (string[])e.Data.GetData(DataFormats.FileDrop);
            var xerFiles = files.Where(f => Path.GetExtension(f).Equals(".xer", StringComparison.OrdinalIgnoreCase)).ToArray();

            if (xerFiles.Length > 0) AddXerFiles(xerFiles);
            else ShowWarning("No valid .xer files found in the dropped items.");
        }

        // Keyboard Handlers
        private void LvwXerFiles_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Delete && lvwXerFiles.SelectedItems.Count > 0)
            {
                RemoveSelectedXerFiles();
            }
        }

        // Form Resize Handler
        private void MainForm_Resize(object sender, EventArgs e)
        {
            ResizeListViewColumns();
        }

        // --- Helper Methods ---

        private bool ValidateInputs()
        {
            if (_xerFilePaths.Count == 0) { ShowError("Please select at least one XER file first."); return false; }
            if (string.IsNullOrEmpty(_outputDirectory) || !Directory.Exists(_outputDirectory)) { ShowError("Please select a valid output directory first."); return false; }

            // Check for missing files and automatically remove them from the list
            var missingFiles = _xerFilePaths.Where(f => !File.Exists(f)).ToList();
            if (missingFiles.Any())
            {
                ShowWarning($"The following file(s) could not be found and were removed from the list:\n{string.Join("\n", missingFiles)}");
                lvwXerFiles.BeginUpdate();
                foreach (var missingFile in missingFiles)
                {
                    _xerFilePaths.Remove(missingFile);
                    foreach (ListViewItem item in lvwXerFiles.Items)
                    {
                        if (item.Tag.ToString().Equals(missingFile, StringComparison.OrdinalIgnoreCase))
                        {
                            lvwXerFiles.Items.Remove(item);
                            break;
                        }
                    }
                }
                lvwXerFiles.EndUpdate();
                UpdateInputButtonsState();
                // Check if any files remain after cleanup
                if (_xerFilePaths.Count == 0) return false;
            }

            return true;
        }

        private List<string> GetAllTableNamesToExport()
        {
            var names = _allTableNames.ToList();
            AddAvailablePbiTables(names);
            return names.Distinct().OrderBy(n => n).ToList();
        }

        // UI/UX OPTIMIZATION: Updated for CheckedListBox
        private List<string> GetSelectedTableNamesToExport()
        {
            // Get checked items from the CheckedListBox
            var names = lstTables.CheckedItems.Cast<string>().ToList();

            if (names.Count == 0 && _dataStore.TableCount > 0 && !chkCreatePowerBiTables.Checked)
            {
                ShowWarning("Please check at least one table from the list, or enable Power BI table generation.");
                return new List<string>();
            }

            AddAvailablePbiTables(names);
            return names.Distinct().OrderBy(n => n).ToList();
        }

        private void AddAvailablePbiTables(List<string> names)
        {
            // Include enhanced tables if the checkbox is checked and dependencies are met
            if (chkCreatePowerBiTables.Checked)
            {
                if (_canCreateTask01) names.Add(EnhancedTableNames.XerTask01);
                if (_canCreateProject02) names.Add(EnhancedTableNames.XerProject02);
                if (_canCreateProjWbs03) names.Add(EnhancedTableNames.XerProjWbs03);
                if (_canCreateBaseline04) names.Add(EnhancedTableNames.XerBaseline04);
                if (_canCreatePredecessor06) names.Add(EnhancedTableNames.XerPredecessor06);
                if (_canCreateActvType07) names.Add(EnhancedTableNames.XerActvType07);
                if (_canCreateActvCode08) names.Add(EnhancedTableNames.XerActvCode08);
                if (_canCreateTaskActv09) names.Add(EnhancedTableNames.XerTaskActv09);
                if (_canCreateCalendar10) names.Add(EnhancedTableNames.XerCalendar10);
                if (_canCreateCalendarDetailed11) names.Add(EnhancedTableNames.XerCalendarDetailed11);
                if (_canCreateRsrc12) names.Add(EnhancedTableNames.XerRsrc12);
                if (_canCreateTaskRsrc13) names.Add(EnhancedTableNames.XerTaskRsrc13);
                if (_canCreateUmeasure14) names.Add(EnhancedTableNames.XerUmeasure14);
            }
        }

        // Main Export Logic
        private async Task ExportTablesAsync(List<string> tablesToExport)
        {
            if (tablesToExport.Count == 0)
            {
                return;
            }

            if (string.IsNullOrEmpty(_outputDirectory) || !Directory.Exists(_outputDirectory)) { ShowError("Please select a valid output directory first."); return; }

            // Validate dependencies and determine the final list, informing the user of any skipped tables
            var (finalTablesToExport, skippedPbi) = DetermineFinalExportList(tablesToExport);

            if (skippedPbi.Any())
            {
                string warningMsg = $"Cannot create requested Power BI tables due to missing dependencies: {string.Join(", ", skippedPbi.Distinct().OrderBy(n => n))}. Exporting available tables.";
                ShowWarning(warningMsg);
                LogActivity($"WARNING: {warningMsg}");
            }

            if (finalTablesToExport.Count == 0)
            {
                ShowError("No tables available for export after checking selections and dependencies.");
                return;
            }

            SetUIEnabled(false); // Disables UI, enables Cancel button
            UpdateStatus("Exporting Tables...");
            toolStripProgressBar.Visible = true;

            // Initialize CancellationTokenSource for the operation
            _cancellationTokenSource = new CancellationTokenSource();
            var token = _cancellationTokenSource.Token;

            var progress = new Progress<(int percent, string message)>(p =>
            {
                UpdateStatus(p.message);
                toolStripProgressBar.Value = p.percent;
            });

            try
            {
                var stopwatch = Stopwatch.StartNew();
                // Call the async service with the token
                var exportedFiles = await _processingService.ExportTablesAsync(_dataStore, finalTablesToExport, _outputDirectory, progress, token);
                stopwatch.Stop();

                toolStripProgressBar.Value = 100;

                string summary = $"{exportedFiles.Count} tables exported. Time elapsed: {stopwatch.Elapsed.TotalSeconds:F2}s.";

                // Handle UI updates based on results and cancellation status
                if (token.IsCancellationRequested)
                {
                    UpdateStatus($"Export canceled. {summary}");
                    LogActivity($"Export canceled. {summary}");
                    ShowWarning($"Export operation was canceled. {exportedFiles.Count} files were written.", "Export Canceled");
                }
                else
                {
                    UpdateStatus($"Export complete. {summary}");
                    LogActivity($"Export complete. {summary}");
                    ShowExportCompleteMessage(exportedFiles);
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error exporting CSV files: {ex.Message}\n\nDetails: {ex.InnerException?.Message}");
                LogActivity($"ERROR during export: {ex.Message}");
                UpdateStatus("Export failed.");
            }
            finally
            {
                SetUIEnabled(true); // Re-enable UI
                toolStripProgressBar.Visible = false;
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
            }
        }

        private (List<string> FinalList, List<string> Skipped) DetermineFinalExportList(List<string> requestedTables)
        {
            List<string> finalTablesToExport = new List<string>();
            List<string> skippedPbi = new List<string>();

            foreach (string tableName in requestedTables)
            {
                if (_dataStore.ContainsTable(tableName))
                {
                    // Raw table exists
                    finalTablesToExport.Add(tableName);
                }
                else if (IsEnhancedTableName(tableName))
                {
                    // Enhanced table requested
                    if (chkCreatePowerBiTables.Checked)
                    {
                        if (CheckSpecificPbiDependency(tableName))
                        {
                            finalTablesToExport.Add(tableName);
                        }
                        else
                        {
                            // Dependency missing
                            skippedPbi.Add(tableName);
                        }
                    }
                }
            }
            return (finalTablesToExport.Distinct().ToList(), skippedPbi);
        }

        // --- Dependency Management ---

        private void CheckEnhancedTableDependencies()
        {
            bool hasTask = _dataStore.ContainsTable(TableNames.Task);
            bool hasCalendar = _dataStore.ContainsTable(TableNames.Calendar);
            bool hasProject = _dataStore.ContainsTable(TableNames.Project);
            bool hasProjWbs = _dataStore.ContainsTable(TableNames.ProjWbs);
            bool hasTaskActv = _dataStore.ContainsTable(TableNames.TaskActv);
            bool hasActvCode = _dataStore.ContainsTable(TableNames.ActvCode);
            bool hasActvType = _dataStore.ContainsTable(TableNames.ActvType);
            bool hasTaskPred = _dataStore.ContainsTable(TableNames.TaskPred);
            bool hasRsrc = _dataStore.ContainsTable(TableNames.Rsrc);
            bool hasTaskRsrc = _dataStore.ContainsTable(TableNames.TaskRsrc);
            bool hasUmeasure = _dataStore.ContainsTable(TableNames.Umeasure);

            _canCreateTask01 = hasTask && hasCalendar && hasProject;
            _canCreateProjWbs03 = hasProjWbs;
            _canCreateBaseline04 = _canCreateTask01; // Depends on Task01 being possible
            _canCreateProject02 = hasProject;
            _canCreatePredecessor06 = hasTaskPred;
            _canCreateActvType07 = hasActvType;
            _canCreateActvCode08 = hasActvCode;
            _canCreateTaskActv09 = hasTaskActv;
            _canCreateCalendar10 = hasCalendar;
            _canCreateCalendarDetailed11 = hasCalendar;
            _canCreateRsrc12 = hasRsrc;
            _canCreateTaskRsrc13 = hasTaskRsrc;
            _canCreateUmeasure14 = hasUmeasure;
        }

        private bool CheckSpecificPbiDependency(string tableName)
        {
            switch (tableName)
            {
                case EnhancedTableNames.XerTask01: return _canCreateTask01;
                case EnhancedTableNames.XerProject02: return _canCreateProject02;
                case EnhancedTableNames.XerProjWbs03: return _canCreateProjWbs03;
                case EnhancedTableNames.XerBaseline04: return _canCreateBaseline04;
                case EnhancedTableNames.XerPredecessor06: return _canCreatePredecessor06;
                case EnhancedTableNames.XerActvType07: return _canCreateActvType07;
                case EnhancedTableNames.XerActvCode08: return _canCreateActvCode08;
                case EnhancedTableNames.XerTaskActv09: return _canCreateTaskActv09;
                case EnhancedTableNames.XerCalendar10: return _canCreateCalendar10;
                case EnhancedTableNames.XerCalendarDetailed11: return _canCreateCalendarDetailed11;
                case EnhancedTableNames.XerRsrc12: return _canCreateRsrc12;
                case EnhancedTableNames.XerTaskRsrc13: return _canCreateTaskRsrc13;
                case EnhancedTableNames.XerUmeasure14: return _canCreateUmeasure14;
                default: return false;
            }
        }

        private bool AnyPbiDependencyMet()
        {
            return _canCreateTask01 || _canCreateProjWbs03 || _canCreateProject02 ||
                   _canCreatePredecessor06 || _canCreateActvType07 || _canCreateActvCode08 ||
                   _canCreateTaskActv09 || _canCreateCalendar10 || _canCreateCalendarDetailed11 ||
                   _canCreateRsrc12 || _canCreateTaskRsrc13 || _canCreateUmeasure14;
        }

        private string GetMissingDependenciesMessage(string prefix)
        {
            var missing = new List<string>();

            // Check dependencies for key tables (Task01 and Baseline04 rely on Task, Calendar, Project)
            if (!_canCreateTask01)
            {
                if (!_dataStore.ContainsTable(TableNames.Task)) missing.Add(TableNames.Task);
                if (!_dataStore.ContainsTable(TableNames.Calendar)) missing.Add(TableNames.Calendar);
                if (!_dataStore.ContainsTable(TableNames.Project)) missing.Add(TableNames.Project);
            }

            // Check others (only if not already covered by the checks above)
            if (!_canCreateProjWbs03 && !_dataStore.ContainsTable(TableNames.ProjWbs)) missing.Add(TableNames.ProjWbs);
            // Project02 dependency on Project is already checked above
            if (!_canCreatePredecessor06 && !_dataStore.ContainsTable(TableNames.TaskPred)) missing.Add(TableNames.TaskPred);
            if (!_canCreateActvType07 && !_dataStore.ContainsTable(TableNames.ActvType)) missing.Add(TableNames.ActvType);
            if (!_canCreateActvCode08 && !_dataStore.ContainsTable(TableNames.ActvCode)) missing.Add(TableNames.ActvCode);
            if (!_canCreateTaskActv09 && !_dataStore.ContainsTable(TableNames.TaskActv)) missing.Add(TableNames.TaskActv);
            // Calendar10/11 dependency on Calendar is already checked above
            if (!_canCreateRsrc12 && !_dataStore.ContainsTable(TableNames.Rsrc)) missing.Add(TableNames.Rsrc);
            if (!_canCreateTaskRsrc13 && !_dataStore.ContainsTable(TableNames.TaskRsrc)) missing.Add(TableNames.TaskRsrc);
            if (!_canCreateUmeasure14 && !_dataStore.ContainsTable(TableNames.Umeasure)) missing.Add(TableNames.Umeasure);

            var distinctMissing = missing.Distinct().OrderBy(s => s).ToList();

            if (distinctMissing.Any())
            {
                return $"{prefix} Required source tables missing: {string.Join(", ", distinctMissing)}.";
            }
            return prefix + " All dependencies met.";
        }

        private bool IsEnhancedTableName(string name)
        {
            var enhancedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
            EnhancedTableNames.XerTask01, EnhancedTableNames.XerProject02,
            EnhancedTableNames.XerProjWbs03, EnhancedTableNames.XerBaseline04,
            EnhancedTableNames.XerPredecessor06, EnhancedTableNames.XerActvType07,
            EnhancedTableNames.XerActvCode08, EnhancedTableNames.XerTaskActv09,
            EnhancedTableNames.XerCalendar10, EnhancedTableNames.XerCalendarDetailed11,
            EnhancedTableNames.XerRsrc12, EnhancedTableNames.XerTaskRsrc13,
            EnhancedTableNames.XerUmeasure14
        };
            return enhancedNames.Contains(name);
        }

        // --- UI Management Methods ---

        private void AddXerFiles(IEnumerable<string> filesToAdd)
        {
            int addedCount = 0;
            lvwXerFiles.BeginUpdate();
            foreach (var file in filesToAdd)
            {
                // Prevent duplicates
                if (!_xerFilePaths.Any(existing => string.Equals(existing, file, StringComparison.OrdinalIgnoreCase)))
                {
                    _xerFilePaths.Add(file);
                    var item = new ListViewItem(Path.GetFileName(file));
                    item.SubItems.Add(Path.GetDirectoryName(file));
                    item.SubItems.Add("Pending"); // Initialize Status
                    item.Tag = file;
                    lvwXerFiles.Items.Add(item);
                    addedCount++;
                }
            }
            lvwXerFiles.EndUpdate();

            if (addedCount > 0)
            {
                UpdateInputButtonsState();
                ResizeListViewColumns();
                LogActivity($"Added {addedCount} XER file(s).");
            }
        }

        private void ResizeListViewColumns()
        {
            // Adjust column widths based on available space
            if (lvwXerFiles.Width > 300)
            {
                colFileName.Width = 150;
                colStatus.Width = 80;
                // Fill remaining space with Path column
                colFilePath.Width = lvwXerFiles.ClientSize.Width - colFileName.Width - colStatus.Width - 5;
            }
        }

        // UI/UX OPTIMIZATION: Method to update file status visualization in the ListView (Thread-safe)
        private void UpdateFileStatus(string filePath, string status, Color? color = null)
        {
            if (lvwXerFiles.InvokeRequired)
            {
                lvwXerFiles.Invoke(new Action(() => UpdateFileStatus(filePath, status, color)));
                return;
            }

            foreach (ListViewItem item in lvwXerFiles.Items)
            {
                if (item.Tag.ToString().Equals(filePath, StringComparison.OrdinalIgnoreCase))
                {
                    item.SubItems[2].Text = status; // colStatus index
                    if (color.HasValue)
                    {
                        item.ForeColor = color.Value;
                    }
                    break;
                }
            }
        }


        private void RemoveSelectedXerFiles()
        {
            if (lvwXerFiles.SelectedItems.Count > 0)
            {
                int count = lvwXerFiles.SelectedItems.Count;
                if (MessageBox.Show($"Remove {count} selected file(s)?", "Confirm Removal", MessageBoxButtons.YesNo, MessageBoxIcon.Question) == DialogResult.Yes)
                {
                    lvwXerFiles.BeginUpdate();
                    foreach (ListViewItem item in lvwXerFiles.SelectedItems)
                    {
                        string path = item.Tag.ToString();
                        _xerFilePaths.Remove(path);
                        lvwXerFiles.Items.Remove(item);
                    }
                    lvwXerFiles.EndUpdate();
                    UpdateInputButtonsState();
                    LogActivity($"Removed {count} file(s).");
                }
            }
        }

        private void ClearResults(bool forceGC = true)
        {
            _dataStore = new XerDataStore();
            _allTableNames.Clear();
            lstTables.Items.Clear();
            txtTableFilter.Clear();

            // Reset dependency flags
            _canCreateTask01 = false; _canCreateProjWbs03 = false; _canCreateBaseline04 = false;
            _canCreateProject02 = false; _canCreatePredecessor06 = false; _canCreateActvType07 = false;
            _canCreateActvCode08 = false; _canCreateTaskActv09 = false; _canCreateCalendar10 = false;
            _canCreateCalendarDetailed11 = false;
            _canCreateRsrc12 = false; _canCreateTaskRsrc13 = false;
            _canCreateUmeasure14 = false;

            UpdatePbiCheckboxState();
            UpdateExportButtonState();

            // Force garbage collection to release memory from the previous data store
            if (forceGC)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        }

        private void UpdateTableList()
        {
            _allTableNames = _dataStore.TableNames.ToList();
            FilterTableList(txtTableFilter.Text);
        }

        // UI/UX OPTIMIZATION: Updated for CheckedListBox to preserve checked state during filtering
        private void FilterTableList(string filter)
        {
            // Store currently checked items before clearing the list
            var checkedItems = new HashSet<string>(lstTables.CheckedItems.Cast<string>());

            lstTables.BeginUpdate();
            lstTables.Items.Clear();

            IEnumerable<string> filteredNames;
            if (string.IsNullOrWhiteSpace(filter))
            {
                filteredNames = _allTableNames;
            }
            else
            {
                filteredNames = _allTableNames.Where(name => name.IndexOf(filter, StringComparison.OrdinalIgnoreCase) >= 0);
            }

            foreach (var name in filteredNames)
            {
                // Add item and restore its checked state if it was previously checked
                bool isChecked = checkedItems.Contains(name);
                lstTables.Items.Add(name, isChecked);
            }
            lstTables.EndUpdate();
        }

        private void UpdatePbiCheckboxState()
        {
            bool dependenciesMet = AnyPbiDependencyMet();
            chkCreatePowerBiTables.Enabled = dependenciesMet;

            if (_dataStore == null || _dataStore.TableCount == 0)
            {
                lblPbiStatus.Text = "Status: Parse XER file(s) first.";
                lblPbiStatus.ForeColor = System.Drawing.Color.Gray;
                btnPbiDetails.Enabled = false;
            }
            else if (dependenciesMet)
            {
                lblPbiStatus.Text = "Status: Dependencies met. Ready to generate.";
                lblPbiStatus.ForeColor = System.Drawing.Color.DarkGreen;
                btnPbiDetails.Enabled = true;
            }
            else
            {
                // If dependencies are missing, ensure the box is unchecked and disabled
                chkCreatePowerBiTables.Checked = false;
                lblPbiStatus.Text = "Status: Missing required source tables.";
                lblPbiStatus.ForeColor = System.Drawing.Color.DarkRed;
                btnPbiDetails.Enabled = true; // Allow user to see why it's disabled
            }
        }

        private void UpdateInputButtonsState()
        {
            bool hasFiles = _xerFilePaths.Count > 0;
            bool hasValidOutput = !string.IsNullOrEmpty(_outputDirectory) && Directory.Exists(_outputDirectory);
            bool hasSelection = lvwXerFiles.SelectedItems.Count > 0;

            btnAddFile.Enabled = true;
            btnParseXer.Enabled = hasFiles && hasValidOutput;
            btnClearFiles.Enabled = hasFiles;
            btnRemoveFile.Enabled = hasSelection;
        }

        // UI/UX OPTIMIZATION: Updated for CheckedListBox
        private void UpdateExportButtonState()
        {
            bool hasOutput = !string.IsNullOrEmpty(_outputDirectory) && Directory.Exists(_outputDirectory);
            bool hasParsedData = _dataStore.TableCount > 0;
            bool pbiTablesSelectedAndPossible = chkCreatePowerBiTables.Checked && AnyPbiDependencyMet();

            // Check if any items are checked in the CheckedListBox
            bool hasCheckedTables = lstTables.CheckedItems.Count > 0;

            btnExportAll.Enabled = hasOutput && (hasParsedData || pbiTablesSelectedAndPossible);
            // Export Selected is enabled if raw tables are checked OR if PBI generation is enabled and possible
            btnExportSelected.Enabled = hasOutput && (hasCheckedTables || pbiTablesSelectedAndPossible);
        }

        private void UpdateStatus(string message)
        {
            // Ensure updates happen on the UI thread
            if (statusStrip.InvokeRequired)
            {
                statusStrip.Invoke(new Action(() => toolStripStatusLabel.Text = message));
            }
            else
            {
                toolStripStatusLabel.Text = message;
            }
        }

        private void LogActivity(string message)
        {
            string timestampedMessage = $"{DateTime.Now:HH:mm:ss}: {message}\r\n";
            // Ensure updates happen on the UI thread
            if (txtActivityLog.InvokeRequired)
            {
                txtActivityLog.Invoke(new Action(() =>
                {
                    txtActivityLog.AppendText(timestampedMessage);
                    txtActivityLog.ScrollToCaret();
                }));
            }
            else
            {
                txtActivityLog.AppendText(timestampedMessage);
                txtActivityLog.ScrollToCaret();
            }
        }

        // UI/UX OPTIMIZATION: Modified to handle the Cancel button state and GC Latency Mode
        private void SetUIEnabled(bool enabled)
        {
            this.UseWaitCursor = !enabled;
            grpInputFiles.Enabled = enabled;
            grpOutput.Enabled = enabled;
            grpExtractedTables.Enabled = enabled;
            grpPowerBI.Enabled = enabled;
            menuStrip.Enabled = enabled;

            if (enabled)
            {
                // Restore button states when idle
                UpdateInputButtonsState();
                UpdateExportButtonState();
                UpdatePbiCheckboxState();
                btnCancelOperation.Visible = false; // Hide cancel button when idle
                // Restore GC latency mode for responsiveness
                GCSettings.LatencyMode = GCLatencyMode.Interactive;
            }
            else
            {
                // Disable main action buttons during operation
                btnParseXer.Enabled = false;
                btnExportAll.Enabled = false;
                btnExportSelected.Enabled = false;
                btnAddFile.Enabled = false;
                btnRemoveFile.Enabled = false;
                btnClearFiles.Enabled = false;

                // Enable Cancel button
                btnCancelOperation.Visible = true;
                btnCancelOperation.Enabled = true;
                // Set GC latency mode for throughput
                GCSettings.LatencyMode = GCLatencyMode.Batch;
            }
        }

        private void ShowExportCompleteMessage(List<string> exportedFiles)
        {
            var createdEnhanced = exportedFiles.Select(Path.GetFileNameWithoutExtension).Where(IsEnhancedTableName).ToList();
            string message = $"Successfully exported {exportedFiles.Count} tables to CSV files.";

            if (createdEnhanced.Any())
            {
                message += "\n\nEnhanced tables created:";
                foreach (var name in createdEnhanced.OrderBy(n => n))
                {
                    message += $"\n- {name}";
                }
            }

            ShowInfo(message, "Export Complete");

            // Ask user to open the output folder
            if (MessageBox.Show("Do you want to open the output folder?", "Open folder", MessageBoxButtons.YesNo, MessageBoxIcon.Question) == DialogResult.Yes)
            {
                try
                {
                    // Open Explorer at the specified path
                    ProcessStartInfo startInfo = new ProcessStartInfo
                    {
                        Arguments = _outputDirectory,
                        FileName = "explorer.exe",
                        UseShellExecute = true
                    };
                    Process.Start(startInfo);
                }
                catch (Exception ex)
                {
                    ShowError($"Could not open folder: {ex.Message}");
                }
            }
        }

        // MessageBox wrappers (thread-safe)
        private void ShowError(string message, string title = "Error")
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new Action(() => MessageBox.Show(this, message, title, MessageBoxButtons.OK, MessageBoxIcon.Error)));
            }
            else
            {
                MessageBox.Show(this, message, title, MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private void ShowWarning(string message, string title = "Warning")
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new Action(() => MessageBox.Show(this, message, title, MessageBoxButtons.OK, MessageBoxIcon.Warning)));
            }
            else
            {
                MessageBox.Show(this, message, title, MessageBoxButtons.OK, MessageBoxIcon.Warning);
            }
        }

        private void ShowInfo(string message, string title = "Information")
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new Action(() => MessageBox.Show(this, message, title, MessageBoxButtons.OK, MessageBoxIcon.Information)));
            }
            else
            {
                MessageBox.Show(this, message, title, MessageBoxButtons.OK, MessageBoxIcon.Information);
            }
        }

        // --- Designer Generated Code (Modified) ---

        private System.ComponentModel.IContainer components = null;

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                SaveSettings(); // Save settings before closing
                if (components != null)
                {
                    components.Dispose();
                }
                // Ensure CancellationTokenSource is disposed
                if (_cancellationTokenSource != null)
                {
                    _cancellationTokenSource.Dispose();
                }
            }
            base.Dispose(disposing);
        }

        // InitializeComponent - Modified to include UI/UX enhancements
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            this.toolTip = new System.Windows.Forms.ToolTip(this.components);
            this.statusStrip = new System.Windows.Forms.StatusStrip();
            this.toolStripStatusLabel = new System.Windows.Forms.ToolStripStatusLabel();
            this.toolStripProgressBar = new System.Windows.Forms.ToolStripProgressBar();
            this.menuStrip = new System.Windows.Forms.MenuStrip();
            this.fileToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.exitToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.helpToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.aboutToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.splitContainerMain = new System.Windows.Forms.SplitContainer();
            this.btnParseXer = new System.Windows.Forms.Button();
            this.grpOutput = new System.Windows.Forms.GroupBox();
            this.btnSelectOutput = new System.Windows.Forms.Button();
            this.txtOutputPath = new System.Windows.Forms.TextBox();
            this.grpInputFiles = new System.Windows.Forms.GroupBox();
            this.lblDragDropHint = new System.Windows.Forms.Label();
            this.lvwXerFiles = new System.Windows.Forms.ListView();
            this.colFileName = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
            this.colFilePath = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
            this.colStatus = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader())); // Added Status Column
            this.toolStripFileActions = new System.Windows.Forms.ToolStrip();
            this.btnAddFile = new System.Windows.Forms.ToolStripButton();
            this.btnRemoveFile = new System.Windows.Forms.ToolStripButton();
            this.btnClearFiles = new System.Windows.Forms.ToolStripButton();
            this.splitContainerResults = new System.Windows.Forms.SplitContainer();
            this.grpExtractedTables = new System.Windows.Forms.GroupBox();
            this.lblFilter = new System.Windows.Forms.Label();
            this.txtTableFilter = new System.Windows.Forms.TextBox();
            // Changed ListBox to CheckedListBox
            this.lstTables = new System.Windows.Forms.CheckedListBox();
            this.grpActivityLog = new System.Windows.Forms.GroupBox();
            this.txtActivityLog = new System.Windows.Forms.TextBox();
            this.panelExportActions = new System.Windows.Forms.Panel();
            this.btnCancelOperation = new System.Windows.Forms.Button(); // Added Cancel Button
            this.btnExportSelected = new System.Windows.Forms.Button();
            this.btnExportAll = new System.Windows.Forms.Button();
            this.grpPowerBI = new System.Windows.Forms.GroupBox();
            this.btnPbiDetails = new System.Windows.Forms.Button();
            this.lblPbiStatus = new System.Windows.Forms.Label();
            this.chkCreatePowerBiTables = new System.Windows.Forms.CheckBox();
            this.statusStrip.SuspendLayout();
            this.menuStrip.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainerMain)).BeginInit();
            this.splitContainerMain.Panel1.SuspendLayout();
            this.splitContainerMain.Panel2.SuspendLayout();
            this.splitContainerMain.SuspendLayout();
            this.grpOutput.SuspendLayout();
            this.grpInputFiles.SuspendLayout();
            this.toolStripFileActions.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainerResults)).BeginInit();
            this.splitContainerResults.Panel1.SuspendLayout();
            this.splitContainerResults.Panel2.SuspendLayout();
            this.splitContainerResults.SuspendLayout();
            this.grpExtractedTables.SuspendLayout();
            this.grpActivityLog.SuspendLayout();
            this.panelExportActions.SuspendLayout();
            this.grpPowerBI.SuspendLayout();
            this.SuspendLayout();
            //
            // statusStrip
            //
            this.statusStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.toolStripStatusLabel,
            this.toolStripProgressBar});
            this.statusStrip.Location = new System.Drawing.Point(0, 539);
            this.statusStrip.Name = "statusStrip";
            this.statusStrip.Size = new System.Drawing.Size(884, 22);
            this.statusStrip.TabIndex = 0;
            this.statusStrip.Text = "statusStrip1";
            //
            // toolStripStatusLabel
            //
            this.toolStripStatusLabel.Name = "toolStripStatusLabel";
            this.toolStripStatusLabel.Size = new System.Drawing.Size(869, 17);
            this.toolStripStatusLabel.Spring = true;
            this.toolStripStatusLabel.Text = "Ready";
            this.toolStripStatusLabel.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            //
            // toolStripProgressBar
            //
            this.toolStripProgressBar.Name = "toolStripProgressBar";
            this.toolStripProgressBar.Size = new System.Drawing.Size(100, 16);
            this.toolStripProgressBar.Visible = false;
            //
            // menuStrip
            //
            this.menuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.fileToolStripMenuItem,
            this.helpToolStripMenuItem});
            this.menuStrip.Location = new System.Drawing.Point(0, 0);
            this.menuStrip.Name = "menuStrip";
            this.menuStrip.Size = new System.Drawing.Size(884, 24);
            this.menuStrip.TabIndex = 1;
            this.menuStrip.Text = "menuStrip1";
            //
            // fileToolStripMenuItem
            //
            this.fileToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.exitToolStripMenuItem});
            this.fileToolStripMenuItem.Name = "fileToolStripMenuItem";
            this.fileToolStripMenuItem.Size = new System.Drawing.Size(37, 20);
            this.fileToolStripMenuItem.Text = "&File";
            //
            // exitToolStripMenuItem
            //
            this.exitToolStripMenuItem.Name = "exitToolStripMenuItem";
            this.exitToolStripMenuItem.Size = new System.Drawing.Size(93, 22);
            this.exitToolStripMenuItem.Text = "E&xit";
            this.exitToolStripMenuItem.Click += new System.EventHandler(this.ExitToolStripMenuItem_Click);
            //
            // helpToolStripMenuItem
            //
            this.helpToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.aboutToolStripMenuItem});
            this.helpToolStripMenuItem.Name = "helpToolStripMenuItem";
            this.helpToolStripMenuItem.Size = new System.Drawing.Size(44, 20);
            this.helpToolStripMenuItem.Text = "&Help";
            //
            // aboutToolStripMenuItem
            //
            this.aboutToolStripMenuItem.Name = "aboutToolStripMenuItem";
            this.aboutToolStripMenuItem.Size = new System.Drawing.Size(107, 22);
            this.aboutToolStripMenuItem.Text = "&About";
            this.aboutToolStripMenuItem.Click += new System.EventHandler(this.AboutToolStripMenuItem_Click);
            //
            // splitContainerMain
            //
            this.splitContainerMain.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitContainerMain.Location = new System.Drawing.Point(0, 24);
            this.splitContainerMain.Name = "splitContainerMain";
            //
            // splitContainerMain.Panel1
            //
            this.splitContainerMain.Panel1.Controls.Add(this.btnParseXer);
            this.splitContainerMain.Panel1.Controls.Add(this.grpOutput);
            this.splitContainerMain.Panel1.Controls.Add(this.grpInputFiles);
            this.splitContainerMain.Panel1.Padding = new System.Windows.Forms.Padding(5);
            //
            // splitContainerMain.Panel2
            //
            this.splitContainerMain.Panel2.Controls.Add(this.splitContainerResults);
            this.splitContainerMain.Panel2.Controls.Add(this.panelExportActions);
            this.splitContainerMain.Panel2.Controls.Add(this.grpPowerBI);
            this.splitContainerMain.Panel2.Padding = new System.Windows.Forms.Padding(5);
            this.splitContainerMain.Size = new System.Drawing.Size(884, 515);
            this.splitContainerMain.SplitterDistance = 420;
            this.splitContainerMain.TabIndex = 2;
            //
            // btnParseXer
            //
            this.btnParseXer.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.btnParseXer.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnParseXer.Location = new System.Drawing.Point(8, 467);
            this.btnParseXer.Name = "btnParseXer";
            this.btnParseXer.Size = new System.Drawing.Size(404, 40);
            this.btnParseXer.TabIndex = 2;
            this.btnParseXer.Text = "Parse XER File(s)";
            this.btnParseXer.UseVisualStyleBackColor = true;
            this.btnParseXer.Click += new System.EventHandler(this.BtnParseXer_Click);
            //
            // grpOutput
            //
            this.grpOutput.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.grpOutput.Controls.Add(this.btnSelectOutput);
            this.grpOutput.Controls.Add(this.txtOutputPath);
            this.grpOutput.Location = new System.Drawing.Point(8, 396);
            this.grpOutput.Name = "grpOutput";
            this.grpOutput.Size = new System.Drawing.Size(404, 65);
            this.grpOutput.TabIndex = 1;
            this.grpOutput.TabStop = false;
            this.grpOutput.Text = "Output Directory";
            //
            // btnSelectOutput
            //
            this.btnSelectOutput.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnSelectOutput.Location = new System.Drawing.Point(323, 25);
            this.btnSelectOutput.Name = "btnSelectOutput";
            this.btnSelectOutput.Size = new System.Drawing.Size(75, 23);
            this.btnSelectOutput.TabIndex = 1;
            this.btnSelectOutput.Text = "Browse...";
            this.btnSelectOutput.UseVisualStyleBackColor = true;
            this.btnSelectOutput.Click += new System.EventHandler(this.BtnSelectOutput_Click);
            //
            // txtOutputPath
            //
            this.txtOutputPath.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.txtOutputPath.Location = new System.Drawing.Point(6, 27);
            this.txtOutputPath.Name = "txtOutputPath";
            this.txtOutputPath.ReadOnly = true;
            this.txtOutputPath.Size = new System.Drawing.Size(311, 20);
            this.txtOutputPath.TabIndex = 0;
            //
            // grpInputFiles
            //
            this.grpInputFiles.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom)
            | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.grpInputFiles.Controls.Add(this.lblDragDropHint);
            this.grpInputFiles.Controls.Add(this.lvwXerFiles);
            this.grpInputFiles.Controls.Add(this.toolStripFileActions);
            this.grpInputFiles.Location = new System.Drawing.Point(8, 8);
            this.grpInputFiles.Name = "grpInputFiles";
            this.grpInputFiles.Size = new System.Drawing.Size(404, 382);
            this.grpInputFiles.TabIndex = 0;
            this.grpInputFiles.TabStop = false;
            this.grpInputFiles.Text = "Primavera P6 XER Files";
            //
            // lblDragDropHint
            //
            this.lblDragDropHint.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.lblDragDropHint.ForeColor = System.Drawing.SystemColors.GrayText;
            this.lblDragDropHint.Location = new System.Drawing.Point(6, 358);
            this.lblDragDropHint.Name = "lblDragDropHint";
            this.lblDragDropHint.Size = new System.Drawing.Size(392, 18);
            this.lblDragDropHint.TabIndex = 2;
            this.lblDragDropHint.Text = "Tip: Drag and drop XER files onto the list above.";
            this.lblDragDropHint.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            //
            // lvwXerFiles
            //
            this.lvwXerFiles.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom)
            | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.lvwXerFiles.Columns.AddRange(new System.Windows.Forms.ColumnHeader[] {
            this.colFileName,
            this.colFilePath,
            this.colStatus}); // Added Status Column
            this.lvwXerFiles.FullRowSelect = true;
            this.lvwXerFiles.HideSelection = false;
            this.lvwXerFiles.Location = new System.Drawing.Point(6, 44);
            this.lvwXerFiles.Name = "lvwXerFiles";
            this.lvwXerFiles.Size = new System.Drawing.Size(392, 311);
            this.lvwXerFiles.TabIndex = 1;
            this.lvwXerFiles.UseCompatibleStateImageBehavior = false;
            this.lvwXerFiles.View = System.Windows.Forms.View.Details;
            this.lvwXerFiles.SelectedIndexChanged += new System.EventHandler(this.LvwXerFiles_SelectedIndexChanged);
            this.lvwXerFiles.KeyDown += new System.Windows.Forms.KeyEventHandler(this.LvwXerFiles_KeyDown);
            //
            // colFileName
            //
            this.colFileName.Text = "File Name";
            this.colFileName.Width = 150;
            //
            // colFilePath
            //
            this.colFilePath.Text = "Path";
            this.colFilePath.Width = 150;
            //
            // colStatus
            //
            this.colStatus.Text = "Status";
            this.colStatus.Width = 80;
            //
            // toolStripFileActions
            //
            this.toolStripFileActions.GripStyle = System.Windows.Forms.ToolStripGripStyle.Hidden;
            this.toolStripFileActions.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.btnAddFile,
            this.btnRemoveFile,
            this.btnClearFiles});
            this.toolStripFileActions.Location = new System.Drawing.Point(3, 16);
            this.toolStripFileActions.Name = "toolStripFileActions";
            this.toolStripFileActions.Size = new System.Drawing.Size(398, 25);
            this.toolStripFileActions.TabIndex = 0;
            this.toolStripFileActions.Text = "toolStrip1";
            //
            // btnAddFile
            //
            // Note: To add icons, set the DisplayStyle to ImageAndText and assign an Image (e.g., using Resources)
            this.btnAddFile.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.btnAddFile.Name = "btnAddFile";
            this.btnAddFile.Size = new System.Drawing.Size(58, 22);
            this.btnAddFile.Text = "Add Files";
            this.btnAddFile.ToolTipText = "Add XER files";
            this.btnAddFile.Click += new System.EventHandler(this.BtnAddFile_Click);
            //
            // btnRemoveFile
            //
            this.btnRemoveFile.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.btnRemoveFile.Name = "btnRemoveFile";
            this.btnRemoveFile.Size = new System.Drawing.Size(54, 22);
            this.btnRemoveFile.Text = "Remove";
            this.btnRemoveFile.ToolTipText = "Remove selected files (Del)";
            this.btnRemoveFile.Click += new System.EventHandler(this.BtnRemoveFile_Click);
            //
            // btnClearFiles
            //
            this.btnClearFiles.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.btnClearFiles.Name = "btnClearFiles";
            this.btnClearFiles.Size = new System.Drawing.Size(57, 22);
            this.btnClearFiles.Text = "Clear All";
            this.btnClearFiles.ToolTipText = "Clear all files and results";
            this.btnClearFiles.Click += new System.EventHandler(this.BtnClearFiles_Click);
            //
            // splitContainerResults
            //
            this.splitContainerResults.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom)
            | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.splitContainerResults.Location = new System.Drawing.Point(8, 8);
            this.splitContainerResults.Name = "splitContainerResults";
            this.splitContainerResults.Orientation = System.Windows.Forms.Orientation.Horizontal;
            //
            // splitContainerResults.Panel1
            //
            this.splitContainerResults.Panel1.Controls.Add(this.grpExtractedTables);
            //
            // splitContainerResults.Panel2
            //
            this.splitContainerResults.Panel2.Controls.Add(this.grpActivityLog);
            this.splitContainerResults.Size = new System.Drawing.Size(444, 362);
            this.splitContainerResults.SplitterDistance = 230;
            this.splitContainerResults.TabIndex = 3;
            //
            // grpExtractedTables
            //
            this.grpExtractedTables.Controls.Add(this.lblFilter);
            this.grpExtractedTables.Controls.Add(this.txtTableFilter);
            this.grpExtractedTables.Controls.Add(this.lstTables);
            this.grpExtractedTables.Dock = System.Windows.Forms.DockStyle.Fill;
            this.grpExtractedTables.Location = new System.Drawing.Point(0, 0);
            this.grpExtractedTables.Name = "grpExtractedTables";
            this.grpExtractedTables.Size = new System.Drawing.Size(444, 230);
            this.grpExtractedTables.TabIndex = 0;
            this.grpExtractedTables.TabStop = false;
            this.grpExtractedTables.Text = "Extracted Tables (Check to select)"; // Updated Text
            //
            // lblFilter
            //
            this.lblFilter.AutoSize = true;
            this.lblFilter.Location = new System.Drawing.Point(6, 22);
            this.lblFilter.Name = "lblFilter";
            this.lblFilter.Size = new System.Drawing.Size(32, 13);
            this.lblFilter.TabIndex = 2;
            this.lblFilter.Text = "Filter:";
            //
            // txtTableFilter
            //
            this.txtTableFilter.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.txtTableFilter.Location = new System.Drawing.Point(44, 19);
            this.txtTableFilter.Name = "txtTableFilter";
            this.txtTableFilter.Size = new System.Drawing.Size(394, 20);
            this.txtTableFilter.TabIndex = 1;
            this.txtTableFilter.TextChanged += new System.EventHandler(this.TxtTableFilter_TextChanged);
            //
            // lstTables (CheckedListBox)
            //
            this.lstTables.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom)
            | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.lstTables.FormattingEnabled = true;
            this.lstTables.IntegralHeight = false;
            this.lstTables.Location = new System.Drawing.Point(3, 45);
            this.lstTables.Name = "lstTables";
            this.lstTables.Size = new System.Drawing.Size(438, 182);
            this.lstTables.Sorted = true;
            this.lstTables.TabIndex = 0;
            //
            // grpActivityLog
            //
            this.grpActivityLog.Controls.Add(this.txtActivityLog);
            this.grpActivityLog.Dock = System.Windows.Forms.DockStyle.Fill;
            this.grpActivityLog.Location = new System.Drawing.Point(0, 0);
            this.grpActivityLog.Name = "grpActivityLog";
            this.grpActivityLog.Size = new System.Drawing.Size(444, 128);
            this.grpActivityLog.TabIndex = 0;
            this.grpActivityLog.TabStop = false;
            this.grpActivityLog.Text = "Activity Log";
            //
            // txtActivityLog
            //
            this.txtActivityLog.Dock = System.Windows.Forms.DockStyle.Fill;
            this.txtActivityLog.Location = new System.Drawing.Point(3, 16);
            this.txtActivityLog.Multiline = true;
            this.txtActivityLog.Name = "txtActivityLog";
            this.txtActivityLog.ReadOnly = true;
            this.txtActivityLog.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.txtActivityLog.Size = new System.Drawing.Size(438, 109);
            this.txtActivityLog.TabIndex = 0;
            //
            // panelExportActions
            //
            this.panelExportActions.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.panelExportActions.Controls.Add(this.btnCancelOperation);
            this.panelExportActions.Controls.Add(this.btnExportSelected);
            this.panelExportActions.Controls.Add(this.btnExportAll);
            this.panelExportActions.Location = new System.Drawing.Point(8, 467);
            this.panelExportActions.Name = "panelExportActions";
            this.panelExportActions.Size = new System.Drawing.Size(444, 40);
            this.panelExportActions.TabIndex = 2;
            //
            // btnCancelOperation
            //
            this.btnCancelOperation.Anchor = System.Windows.Forms.AnchorStyles.Top;
            this.btnCancelOperation.BackColor = System.Drawing.Color.MistyRose;
            this.btnCancelOperation.Location = new System.Drawing.Point(186, 5);
            this.btnCancelOperation.Name = "btnCancelOperation";
            this.btnCancelOperation.Size = new System.Drawing.Size(72, 30);
            this.btnCancelOperation.TabIndex = 2;
            this.btnCancelOperation.Text = "Cancel";
            this.btnCancelOperation.UseVisualStyleBackColor = false;
            this.btnCancelOperation.Visible = false;
            this.btnCancelOperation.Click += new System.EventHandler(this.BtnCancelOperation_Click);
            //
            // btnExportSelected
            //
            this.btnExportSelected.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnExportSelected.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnExportSelected.Location = new System.Drawing.Point(264, 0);
            this.btnExportSelected.Name = "btnExportSelected";
            this.btnExportSelected.Size = new System.Drawing.Size(180, 40);
            this.btnExportSelected.TabIndex = 1;
            this.btnExportSelected.Text = "Export Selected Tables"; // Renamed
            this.btnExportSelected.UseVisualStyleBackColor = true;
            this.btnExportSelected.Click += new System.EventHandler(this.BtnExportSelected_Click);
            //
            // btnExportAll
            //
            this.btnExportAll.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnExportAll.Location = new System.Drawing.Point(0, 0);
            this.btnExportAll.Name = "btnExportAll";
            this.btnExportAll.Size = new System.Drawing.Size(180, 40);
            this.btnExportAll.TabIndex = 0;
            this.btnExportAll.Text = "Export All Tables"; // Renamed
            this.btnExportAll.UseVisualStyleBackColor = true;
            this.btnExportAll.Click += new System.EventHandler(this.BtnExportAll_Click);
            //
            // grpPowerBI
            //
            this.grpPowerBI.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.grpPowerBI.Controls.Add(this.btnPbiDetails);
            this.grpPowerBI.Controls.Add(this.lblPbiStatus);
            this.grpPowerBI.Controls.Add(this.chkCreatePowerBiTables);
            this.grpPowerBI.Location = new System.Drawing.Point(8, 376);
            this.grpPowerBI.Name = "grpPowerBI";
            this.grpPowerBI.Size = new System.Drawing.Size(444, 85);
            this.grpPowerBI.TabIndex = 1;
            this.grpPowerBI.TabStop = false;
            this.grpPowerBI.Text = "Power BI Enhanced Tables (Included in Exports)"; // Updated Text
            //
            // btnPbiDetails
            //
            this.btnPbiDetails.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnPbiDetails.Location = new System.Drawing.Point(363, 24);
            this.btnPbiDetails.Name = "btnPbiDetails";
            this.btnPbiDetails.Size = new System.Drawing.Size(75, 23);
            this.btnPbiDetails.TabIndex = 2;
            this.btnPbiDetails.Text = "Details...";
            this.btnPbiDetails.UseVisualStyleBackColor = true;
            this.btnPbiDetails.Click += new System.EventHandler(this.BtnPbiDetails_Click);
            //
            // lblPbiStatus
            //
            this.lblPbiStatus.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.lblPbiStatus.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblPbiStatus.Location = new System.Drawing.Point(6, 54);
            this.lblPbiStatus.Name = "lblPbiStatus";
            this.lblPbiStatus.Size = new System.Drawing.Size(432, 21);
            this.lblPbiStatus.TabIndex = 1;
            this.lblPbiStatus.Text = "Status: Unknown";
            //
            // chkCreatePowerBiTables
            //
            this.chkCreatePowerBiTables.AutoSize = true;
            this.chkCreatePowerBiTables.Location = new System.Drawing.Point(9, 28);
            this.chkCreatePowerBiTables.Name = "chkCreatePowerBiTables";
            this.chkCreatePowerBiTables.Size = new System.Drawing.Size(193, 17);
            this.chkCreatePowerBiTables.TabIndex = 0;
            this.chkCreatePowerBiTables.Text = "Generate Enhanced Tables on Export";
            this.chkCreatePowerBiTables.UseVisualStyleBackColor = true;
            this.chkCreatePowerBiTables.CheckedChanged += new System.EventHandler(this.ChkCreatePowerBiTables_CheckedChanged);
            //
            // MainForm
            //
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(884, 561);
            this.Controls.Add(this.splitContainerMain);
            this.Controls.Add(this.menuStrip);
            this.Controls.Add(this.statusStrip);
            this.MainMenuStrip = this.menuStrip;
            this.MinimumSize = new System.Drawing.Size(800, 550);
            this.Name = "MainForm";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Primavera P6 XER to CSV Converter v2.3";
            this.statusStrip.ResumeLayout(false);
            this.statusStrip.PerformLayout();
            this.menuStrip.ResumeLayout(false);
            this.menuStrip.PerformLayout();
            this.splitContainerMain.Panel1.ResumeLayout(false);
            this.splitContainerMain.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.splitContainerMain)).EndInit();
            this.splitContainerMain.ResumeLayout(false);
            this.grpOutput.ResumeLayout(false);
            this.grpOutput.PerformLayout();
            this.grpInputFiles.ResumeLayout(false);
            this.grpInputFiles.PerformLayout();
            this.toolStripFileActions.ResumeLayout(false);
            this.toolStripFileActions.PerformLayout();
            this.splitContainerResults.Panel1.ResumeLayout(false);
            this.splitContainerResults.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.splitContainerResults)).EndInit();
            this.splitContainerResults.ResumeLayout(false);
            this.grpExtractedTables.ResumeLayout(false);
            this.grpExtractedTables.PerformLayout();
            this.grpActivityLog.ResumeLayout(false);
            this.grpActivityLog.PerformLayout();
            this.panelExportActions.ResumeLayout(false);
            this.grpPowerBI.ResumeLayout(false);
            this.grpPowerBI.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }
    }

    static class Program
    {
        private static readonly bool ENABLE_EXPIRATION = false;
        // Note: The current date provided in the environment context is 2025-08-22.
        private static readonly DateTime EXPIRATION_DATE = new DateTime(2025, 09, 01, 23, 59, 59);
        private const string EXPIRATION_TITLE = "Application Expired";
        private const string EXPIRATION_MESSAGE = "This application has expired and is no longer available for use.\n\n" +
                                                  "Please contact the software provider for a renewed version.\n\n" +
                                                  "Thank you for using this software.";
        [STAThread]
        static void Main()
        {
            if (ENABLE_EXPIRATION)
            {
                // Use the actual system time for expiration checks
                DateTime currentDate = DateTime.Now;
                if (currentDate > EXPIRATION_DATE)
                {
                    MessageBox.Show(EXPIRATION_MESSAGE, EXPIRATION_TITLE,
                                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                    return;
                }

                TimeSpan timeUntilExpiration = EXPIRATION_DATE - currentDate;
                // Show warning within 7 days
                if (timeUntilExpiration.TotalDays <= 7 && timeUntilExpiration.TotalDays > 0)
                {
                    string warningMessage = $"Warning: This application will expire in {(int)Math.Ceiling(timeUntilExpiration.TotalDays)} day(s).\n\n" +
                                            "Please contact the software provider for a renewed version.";
                    MessageBox.Show(warningMessage, "Expiration Warning",
                                    MessageBoxButtons.OK, MessageBoxIcon.Warning);
                }
            }

            // UI/UX OPTIMIZATION: Removed legacy DPI awareness P/Invoke call.
            // Modern DPI awareness should be configured via app.manifest (for PerMonitorV2 support)
            // or app.config (for SystemAware support in older .NET Framework versions).
            /*
            if (Environment.OSVersion.Version.Major >= 6)
            {
                SetProcessDPIAware();
            }
            */

            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new MainForm());
        }

        // Removed P/Invoke definition for SetProcessDPIAware
        // [System.Runtime.InteropServices.DllImport("user32.dll")]
        // private static extern bool SetProcessDPIAware();
    }
}