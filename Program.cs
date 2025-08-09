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
using System.Drawing; // Added for UI enhancements

// =====================================================================================================================
// Application: Optimized Primavera P6 XER to CSV Converter v2.1 (Enhanced UI)
// Overview: Performance Optimized Backend with Enhanced User Interface.
// CRITICAL: All original transformations and business logic are preserved 100%.
// =====================================================================================================================

namespace XerToCsvConverter
{
    // -----------------------------------------------------------------------------------------------------------------
    // Configuration, Constants, Utilities, Data Model (Preserved from Optimized Backend)
    // -----------------------------------------------------------------------------------------------------------------
    #region Backend Core (Configuration, Constants, Utilities, DataModel)

    public static class PerformanceConfig
    {
        public static int FileReadBufferSize { get; set; } = 131072; // 128KB
        public static int CsvWriteBufferSize { get; set; } = 131072; // 128KB
        public static int ProgressReportIntervalLines { get; set; } = 5000;
        public static int BatchProcessingSize { get; set; } = 5000;
        public static int MaxParallelFiles { get; set; } = Environment.ProcessorCount;
        public static int MaxParallelTransformations { get; set; } = Environment.ProcessorCount;
        public static int StringBuilderInitialCapacity { get; set; } = 8192;
        public static int DefaultTableCapacity { get; set; } = 50000;
        public static bool EnableGlobalStringInterning { get; set; } = true;
        public static int MaxStringInternLength { get; set; } = 512;

        static PerformanceConfig()
        {
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            GCSettings.LatencyMode = GCLatencyMode.Batch;
        }
    }

    internal static class TableNames { public const string Task = "TASK"; public const string Calendar = "CALENDAR"; public const string Project = "PROJECT"; public const string ProjWbs = "PROJWBS"; public const string TaskActv = "TASKACTV"; public const string ActvCode = "ACTVCODE"; public const string ActvType = "ACTVTYPE"; public const string TaskPred = "TASKPRED"; public const string Rsrc = "RSRC"; public const string TaskRsrc = "TASKRSRC"; }
    internal static class FieldNames
    {
        public const string TaskId = "task_id"; public const string ProjectId = "proj_id"; public const string WbsId = "wbs_id"; public const string CalendarId = "clndr_id"; public const string TaskType = "task_type"; public const string StatusCode = "status_code"; public const string TaskCode = "task_code"; public const string TaskName = "task_name"; public const string RsrcId = "rsrc_id"; public const string ActStartDate = "act_start_date"; public const string ActEndDate = "act_end_date"; public const string EarlyStartDate = "early_start_date"; public const string EarlyEndDate = "early_end_date"; public const string LateEndDate = "late_end_date"; public const string LateStartDate = "late_start_date"; public const string TargetStartDate = "target_start_date"; public const string TargetEndDate = "target_end_date"; public const string CstrType = "cstr_type"; public const string CstrDate = "cstr_date"; public const string PriorityType = "priority_type"; public const string FloatPath = "float_path"; public const string FloatPathOrder = "float_path_order"; public const string DrivingPathFlag = "driving_path_flag"; public const string RemainDurationHrCnt = "remain_drtn_hr_cnt"; public const string TotalFloatHrCnt = "total_float_hr_cnt"; public const string FreeFloatHrCnt = "free_float_hr_cnt"; public const string CompletePctType = "complete_pct_type"; public const string PhysCompletePct = "phys_complete_pct"; public const string ActWorkQty = "act_work_qty"; public const string RemainWorkQty = "remain_work_qty"; public const string TargetDurationHrCnt = "target_drtn_hr_cnt"; public const string ClndrId = "clndr_id"; public const string DayHourCount = "day_hr_cnt"; public const string LastRecalcDate = "last_recalc_date";
        public const string ParentWbsId = "parent_wbs_id";
        public const string ActvCodeTypeId = "actv_code_type_id"; public const string ActvCodeId = "actv_code_id"; public const string FileName = "FileName"; public const string Start = "Start"; public const string Finish = "Finish"; public const string IdName = "ID_Name"; public const string RemainingWorkingDays = "Remaining Working Days"; public const string OriginalDuration = "Original Duration"; public const string TotalFloat = "Total Float"; public const string FreeFloat = "Free Float"; public const string PercentComplete = "%"; public const string DataDate = "Data Date"; public const string WbsIdKey = "wbs_id_key"; public const string TaskIdKey = "task_id_key"; public const string ParentWbsIdKey = "parent_wbs_id_key"; public const string CalendarIdKey = "calendar_id_key"; public const string ProjIdKey = "proj_id_key"; public const string ActvCodeIdKey = "actv_code_id_key"; public const string ActvCodeTypeIdKey = "actv_code_type_id_key"; public const string ClndrIdKey = "clndr_id_key"; public const string PredTaskId = "pred_task_id"; public const string PredTaskIdKey = "pred_task_id_key"; public const string CalendarName = "clndr_name"; public const string CalendarData = "clndr_data"; public const string CalendarType = "clndr_type";
        public const string Date = "date"; public const string DayOfWeek = "day_of_week"; public const string WorkingDay = "working_day"; public const string WorkHours = "work_hours"; public const string ExceptionType = "exception_type"; public const string RsrcIdKey = "rsrc_id_key";
    }
    internal static class EnhancedTableNames { public const string XerTask01 = "01_XER_TASK"; public const string XerProject02 = "02_XER_PROJECT"; public const string XerProjWbs03 = "03_XER_PROJWBS"; public const string XerBaseline04 = "04_XER_BASELINE"; public const string XerPredecessor06 = "06_XER_PREDECESSOR"; public const string XerActvType07 = "07_XER_ACTVTYPE"; public const string XerActvCode08 = "08_XER_ACTVCODE"; public const string XerTaskActv09 = "09_XER_TASKACTV"; public const string XerCalendar10 = "10_XER_CALENDAR"; public const string XerCalendarDetailed11 = "11_XER_CALENDAR_DETAILED"; public const string XerRsrc12 = "12_XER_RSRC"; public const string XerTaskRsrc13 = "13_XER_TASKRSRC"; }

    public static class StringInternPool
    {
        private static readonly ConcurrentDictionary<string, string> Pool = new ConcurrentDictionary<string, string>(
            Environment.ProcessorCount * 2, PerformanceConfig.DefaultTableCapacity * 20);

        public static string Intern(string str)
        {
            if (!PerformanceConfig.EnableGlobalStringInterning) return str;
            if (str == null) return null;
            if (string.IsNullOrEmpty(str)) return string.Empty;
            if (str.Length > PerformanceConfig.MaxStringInternLength) return str;
            return Pool.GetOrAdd(str, str);
        }

        public static void Clear() => Pool.Clear();
    }

    public static class DateParser
    {
        private static readonly ConcurrentDictionary<string, DateTime?> DateCache = new ConcurrentDictionary<string, DateTime?>();
        private static readonly string[] P6Formats = {
            "d/M/yyyy", "dd/MM/yyyy", "M/d/yyyy", "MM/dd/yyyy",
            "yyyy-MM-dd", "dd-MMM-yy", "dd-MMM-yyyy"
        };
        public const string OutputFormat = "yyyy-MM-dd HH:mm:ss";

        public static DateTime? TryParse(string dateStr)
        {
            if (string.IsNullOrWhiteSpace(dateStr)) return null;
            return DateCache.GetOrAdd(dateStr, str =>
            {
                if (DateTime.TryParseExact(str, P6Formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime result))
                {
                    if (result.Year > 1900) return result;
                }
                if (DateTime.TryParse(str, CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
                {
                    if (result.Year > 1900) return result;
                }
                return null;
            });
        }

        public static string Format(DateTime? date) => date.HasValue && date.Value != DateTime.MinValue ? date.Value.ToString(OutputFormat, CultureInfo.InvariantCulture) : "";
        public static string Format(DateTime date) => date != DateTime.MinValue ? date.ToString(OutputFormat, CultureInfo.InvariantCulture) : "";
        public static void ClearCache() => DateCache.Clear();
    }

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
            for (int i = copyLength; i < Headers.Length; i++)
            {
                newValues[i] = string.Empty;
            }
            return new DataRow(newValues, row.SourceFilename);
        }

        public IReadOnlyList<DataRow> Rows => _rows;
        public int RowCount => _rows.Count;
        public bool IsEmpty => _rows.Count == 0;

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

    public class XerDataStore
    {
        private readonly Dictionary<string, XerTable> _tables = new Dictionary<string, XerTable>(StringComparer.OrdinalIgnoreCase);

        public void MergeStore(XerDataStore otherStore)
        {
            foreach (var kvp in otherStore._tables)
            {
                if (_tables.TryGetValue(kvp.Key, out XerTable existingTable))
                {
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

    #endregion

    // -----------------------------------------------------------------------------------------------------------------
    // Services (Parser, Exporter, Transformer, ProcessingService) (Preserved from Optimized Backend)
    // -----------------------------------------------------------------------------------------------------------------
    #region Backend Services

    public class XerParser
    {
        private const char Delimiter = '\t';

        public XerDataStore ParseXerFile(string xerFilePath, Action<int, string> reportProgressAction)
        {
            var fileStore = new XerDataStore();
            string filename = StringInternPool.Intern(Path.GetFileName(xerFilePath));
            int bufferSize = PerformanceConfig.FileReadBufferSize;
            int progressReportInterval = PerformanceConfig.ProgressReportIntervalLines;
            var localTables = new Dictionary<string, XerTable>(StringComparer.OrdinalIgnoreCase);
            XerTable currentTable = null;
            int lineCount = 0;
            long bytesRead = 0;

            try
            {
                var fileInfo = new FileInfo(xerFilePath);
                long fileSize = fileInfo.Length;
                if (fileSize == 0) return fileStore;

                int lastReportedProgress = 0;

                using (var fileStream = new FileStream(xerFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize))
                using (var reader = new StreamReader(fileStream, Encoding.Default, true, bufferSize))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        bytesRead += Encoding.Default.GetByteCount(line) + Environment.NewLine.Length;
                        lineCount++;

                        if (lineCount % progressReportInterval == 0)
                        {
                            int progress = (int)((double)bytesRead * 100 / fileSize);
                            if (progress > lastReportedProgress)
                            {
                                reportProgressAction?.Invoke(progress, $"Parsing {filename}: {progress}%");
                                lastReportedProgress = progress;
                            }
                        }

                        if (string.IsNullOrWhiteSpace(line)) continue;
                        if (line.Length < 2 || line[0] != '%') continue;
                        char typeChar = line[1];

                        switch (typeChar)
                        {
                            case 'T':
                                string currentTableName = line.Substring(2).Trim();
                                if (!string.IsNullOrEmpty(currentTableName))
                                {
                                    currentTable = new XerTable(currentTableName);
                                    localTables[currentTable.Name] = currentTable;
                                }
                                break;

                            case 'F':
                                if (currentTable != null)
                                {
                                    string fieldsLine = line.Substring(2);
                                    if (fieldsLine.Length > 0 && fieldsLine[0] == Delimiter) fieldsLine = fieldsLine.Substring(1);
                                    string[] headers = FastSplitAndIntern(fieldsLine, Delimiter, trim: true);
                                    currentTable.SetHeaders(headers);
                                }
                                break;

                            case 'R':
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
            catch (Exception ex)
            {
                throw new Exception($"Error reading file {filename} at line {lineCount}. Bytes read: {bytesRead}.", ex);
            }
        }

        private static string[] FastSplitAndIntern(string text, char delimiter, bool trim)
        {
            if (string.IsNullOrEmpty(text)) return Array.Empty<string>();
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

            string lastSegment = text.Substring(start);
            if (trim) lastSegment = lastSegment.Trim();
            result[resultIndex] = StringInternPool.Intern(lastSegment);
            return result;
        }
    }

    public class CsvExporter
    {
        public void WriteTableToCsv(XerTable table, string csvFilePath)
        {
            if (table == null || table.Headers == null) return;
            if (table.IsEmpty && table.Headers.Length == 0) return;

            int bufferSize = PerformanceConfig.CsvWriteBufferSize;

            try
            {
                using (var fileStream = new FileStream(csvFilePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize))
                using (var writer = new StreamWriter(fileStream, Encoding.UTF8, bufferSize))
                {
                    string[] headerRow = table.Headers;
                    var lineBuilder = new StringBuilder(PerformanceConfig.StringBuilderInitialCapacity);

                    // Write header
                    for (int i = 0; i < headerRow.Length; i++)
                    {
                        if (i > 0) lineBuilder.Append(',');
                        lineBuilder.Append(EscapeCsvField(headerRow[i]));
                    }
                    if (headerRow.Length > 0) lineBuilder.Append(',');
                    lineBuilder.Append(EscapeCsvField(FieldNames.FileName));

                    writer.WriteLine(lineBuilder.ToString());
                    lineBuilder.Clear();

                    // Write data rows in batches
                    int batchSize = PerformanceConfig.BatchProcessingSize;
                    var batch = new List<string>(batchSize);

                    foreach (var dataRow in table.Rows)
                    {
                        string[] rowFields = dataRow.Fields;
                        if (rowFields == null) continue;

                        // Build CSV line
                        for (int j = 0; j < rowFields.Length; j++)
                        {
                            if (j > 0) lineBuilder.Append(',');
                            lineBuilder.Append(EscapeCsvField(rowFields[j] ?? string.Empty));
                        }
                        if (rowFields.Length > 0) lineBuilder.Append(',');
                        lineBuilder.Append(EscapeCsvField(dataRow.SourceFilename));

                        batch.Add(lineBuilder.ToString());
                        lineBuilder.Clear();

                        if (batch.Count >= batchSize)
                        {
                            WriteBatch(writer, batch);
                            batch.Clear();
                        }
                    }

                    WriteBatch(writer, batch);
                    writer.Flush();
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to write CSV file '{Path.GetFileName(csvFilePath)}'. {ex.Message}", ex);
            }
        }

        private void WriteBatch(StreamWriter writer, List<string> batch)
        {
            foreach (var line in batch)
            {
                writer.WriteLine(line);
            }
        }

        private static string EscapeCsvField(string field)
        {
            if (string.IsNullOrEmpty(field)) return "";

            bool needsQuotes = false;
            bool hasQuotes = false;

            for (int i = 0; i < field.Length; i++)
            {
                char c = field[i];
                if (c == ',' || c == '\r' || c == '\n') needsQuotes = true;
                else if (c == '"') { needsQuotes = true; hasQuotes = true; }
                if (needsQuotes && hasQuotes) break;
            }

            if (!needsQuotes && field.Length > 0 && (field[0] == ' ' || field[field.Length - 1] == ' '))
            {
                needsQuotes = true;
            }

            if (!needsQuotes) return field;

            if (hasQuotes)
            {
                var sb = new StringBuilder(field.Length + 10);
                sb.Append('"');
                foreach (char c in field)
                {
                    if (c == '"') sb.Append("\"\"");
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

    public class XerTransformer
    {
        private readonly XerDataStore _dataStore;
        // Enhanced regex patterns with better accuracy
        private static readonly RegexOptions RegexCompiledOptions = RegexOptions.Singleline | RegexOptions.Compiled;

        // Enhanced exception pattern to handle both old and new formats
        private static readonly Regex ExcPatternRegex = new Regex(
            @"\(0\|\|(\d+)\(d\|(\d+)\)\s*\((.*?)\)\s*\)",
            RegexCompiledOptions);

        // Additional pattern for alternative exception format
        private static readonly Regex ExcPatternRegex2 = new Regex(
            @"\(0\|\|(\d+)\(d\|(\d+)\|0\)\s*\((.*?)\)\s*\)",
            RegexCompiledOptions);

        private static readonly Regex CleanRegex1 = new Regex(@"(\|\|)\s+", RegexCompiledOptions);
        private static readonly Regex CleanRegex2 = new Regex(@"\s+(\|\|)", RegexCompiledOptions);

        // Enhanced time slot patterns to handle various formats
        private static readonly Regex[] TimeSlotPatterns = {
            // Standard format: (0||0(s|08:00|f|12:00)())
            new Regex(@"\(0\|\|\d+\s*\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)\s*\(\s*\)\s*\)", RegexCompiledOptions),
            // Without trailing parentheses: (0||0(s|08:00|f|12:00))
            new Regex(@"\(0\|\|\d+\s*\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)\s*\)", RegexCompiledOptions),
            // Simplified format: s|08:00|f|12:00
            new Regex(@"([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})", RegexCompiledOptions),
            // Alternative format with different delimiters
            new Regex(@"\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)", RegexCompiledOptions)
        };

        public XerTransformer(XerDataStore dataStore)
        {
            _dataStore = dataStore;
        }

        private bool IsTableValid(XerTable table) => table != null && !table.IsEmpty && table.Headers != null;

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

        private void SetTransformedField(string[] transformedRow, string[] finalColumns, string fieldName, string value)
        {
            int index = Array.IndexOf(finalColumns, fieldName);
            if (index != -1)
            {
                transformedRow[index] = value ?? string.Empty;
            }
        }

        // 01_XER_TASK (Parallelized)
        public XerTable Create01XerTaskTable()
        {
            var taskTable = _dataStore.GetTable(TableNames.Task);
            var calendarTable = _dataStore.GetTable(TableNames.Calendar);
            var projectTable = _dataStore.GetTable(TableNames.Project);

            if (!IsTableValid(taskTable) || !IsTableValid(calendarTable) || !IsTableValid(projectTable)) return null;

            try
            {
                var taskIndexes = taskTable.FieldIndexes;
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
                    FieldNames.Start, FieldNames.Finish, FieldNames.IdName,
                    FieldNames.RemainingWorkingDays, FieldNames.OriginalDuration, FieldNames.TotalFloat, FieldNames.FreeFloat,
                    FieldNames.PercentComplete, FieldNames.DataDate,
                    FieldNames.WbsIdKey, FieldNames.TaskIdKey, FieldNames.CalendarIdKey, FieldNames.ProjIdKey
                };

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

                    string projId = GetFieldValue(row, taskIndexes, FieldNames.ProjectId);
                    string taskId = GetFieldValue(row, taskIndexes, FieldNames.TaskId);
                    string wbsId = GetFieldValue(row, taskIndexes, FieldNames.WbsId);
                    string clndrId = GetFieldValue(row, taskIndexes, FieldNames.CalendarId);
                    string statusCode = GetFieldValue(row, taskIndexes, FieldNames.StatusCode);
                    DateTime? actEndDate = DateParser.TryParse(GetFieldValue(row, taskIndexes, FieldNames.ActEndDate));

                    // 1. Copy direct fields.
                    CopyDirectFields(row, taskIndexes, transformed, finalColumns);

                    // 2. Format date fields.
                    FormatDateFields(row, taskIndexes, transformed, finalColumns, actEndDate);

                    // 3. Transform status code.
                    SetTransformedField(transformed, finalColumns, FieldNames.StatusCode,
                        StringInternPool.Intern(
                            statusCode == TK_Complete ? "Complete" :
                            statusCode == TK_NotStart ? "Not Started" :
                            statusCode == TK_Active ? "In Progress" : statusCode)
                        );

                    // 4. Calculate Start/Finish dates.
                    DateTime startDate = CalculateStartDate(row, taskIndexes, statusCode);
                    DateTime finishDate = CalculateFinishDate(row, taskIndexes, statusCode);
                    SetTransformedField(transformed, finalColumns, FieldNames.Start, DateParser.Format(startDate));
                    SetTransformedField(transformed, finalColumns, FieldNames.Finish, DateParser.Format(finishDate));

                    // 5. Create ID_Name.
                    string taskCode = GetFieldValue(row, taskIndexes, FieldNames.TaskCode);
                    string taskName = GetFieldValue(row, taskIndexes, FieldNames.TaskName);
                    SetTransformedField(transformed, finalColumns, FieldNames.IdName, $"{taskCode} - {taskName}");

                    // 6. Calculate Durations and Floats.
                    string calendarKey = CreateKey(originalFilename, clndrId);
                    if (!string.IsNullOrEmpty(clndrId) && calendarHours.TryGetValue(calendarKey, out decimal dayHrCnt) && dayHrCnt > 0)
                    {
                        SetTransformedField(transformed, finalColumns, FieldNames.RemainingWorkingDays,
                            CalculateDaysFromHours(row, taskIndexes, FieldNames.RemainDurationHrCnt, dayHrCnt));
                        SetTransformedField(transformed, finalColumns, FieldNames.OriginalDuration,
                            CalculateDaysFromHours(row, taskIndexes, FieldNames.TargetDurationHrCnt, dayHrCnt));

                        if (statusCode != TK_Complete)
                        {
                            SetTransformedField(transformed, finalColumns, FieldNames.TotalFloat,
                                CalculateDaysFromHours(row, taskIndexes, FieldNames.TotalFloatHrCnt, dayHrCnt));
                            SetTransformedField(transformed, finalColumns, FieldNames.FreeFloat,
                                CalculateDaysFromHours(row, taskIndexes, FieldNames.FreeFloatHrCnt, dayHrCnt));
                        }
                        else
                        {
                            SetTransformedField(transformed, finalColumns, FieldNames.TotalFloat, "");
                            SetTransformedField(transformed, finalColumns, FieldNames.FreeFloat, "");
                        }
                    }
                    else
                    {
                        SetTransformedField(transformed, finalColumns, FieldNames.RemainingWorkingDays, "");
                        SetTransformedField(transformed, finalColumns, FieldNames.OriginalDuration, "");
                        SetTransformedField(transformed, finalColumns, FieldNames.TotalFloat, "");
                        SetTransformedField(transformed, finalColumns, FieldNames.FreeFloat, "");
                    }

                    // 7. Calculate completion percentage.
                    double pct = CalculateCompletionPercentage(row, taskIndexes, statusCode);
                    SetTransformedField(transformed, finalColumns, FieldNames.PercentComplete,
                        pct.ToString("F2", CultureInfo.InvariantCulture));

                    // 8. Set Data Date.
                    string projectKey = CreateKey(originalFilename, projId);
                    if (!string.IsNullOrEmpty(projId) && projectDataDates.TryGetValue(projectKey, out DateTime dataDateValue))
                    {
                        SetTransformedField(transformed, finalColumns, FieldNames.DataDate, DateParser.Format(dataDateValue));
                    }
                    else
                    {
                        SetTransformedField(transformed, finalColumns, FieldNames.DataDate, "");
                    }

                    // 9. Create Keys.
                    SetTransformedField(transformed, finalColumns, FieldNames.WbsIdKey, CreateKey(originalFilename, wbsId));
                    SetTransformedField(transformed, finalColumns, FieldNames.TaskIdKey, CreateKey(originalFilename, taskId));
                    SetTransformedField(transformed, finalColumns, FieldNames.CalendarIdKey, CreateKey(originalFilename, clndrId));
                    SetTransformedField(transformed, finalColumns, FieldNames.ProjIdKey, CreateKey(originalFilename, projId));

                    // Intern results
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
                Console.WriteLine($"Error creating {EnhancedTableNames.XerTask01}: {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        // TASK01 Helpers
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

                if (!string.IsNullOrEmpty(clndrId) &&
                    decimal.TryParse(dayHrCntStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal dayHrCnt) &&
                    dayHrCnt > 0)
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

        private void CopyDirectFields(string[] sourceRow, IReadOnlyDictionary<string, int> sourceIndexes, string[] targetRow, string[] targetColumns)
        {
            var handledFields = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
                FieldNames.StatusCode, FieldNames.Start, FieldNames.Finish, FieldNames.IdName,
                FieldNames.RemainingWorkingDays, FieldNames.OriginalDuration, FieldNames.TotalFloat, FieldNames.FreeFloat,
                FieldNames.PercentComplete, FieldNames.DataDate,
                FieldNames.ActStartDate, FieldNames.ActEndDate, FieldNames.EarlyStartDate, FieldNames.EarlyEndDate,
                FieldNames.LateStartDate, FieldNames.LateEndDate, FieldNames.CstrDate,
                FieldNames.TargetStartDate, FieldNames.TargetEndDate,
                FieldNames.WbsIdKey, FieldNames.TaskIdKey, FieldNames.CalendarIdKey, FieldNames.ProjIdKey
            };

            for (int i = 0; i < targetColumns.Length; i++)
            {
                string colName = targetColumns[i];
                if (sourceIndexes.TryGetValue(colName, out int srcIndex) && !handledFields.Contains(colName))
                {
                    if (srcIndex < sourceRow.Length)
                    {
                        targetRow[i] = sourceRow[srcIndex];
                    }
                }
            }
        }

        private void FormatDateFields(string[] sourceRow, IReadOnlyDictionary<string, int> sourceIndexes, string[] transformedRow, string[] finalColumns, DateTime? actEndDate)
        {
            string[] directFormatCols = {
                FieldNames.CstrDate, FieldNames.TargetStartDate, FieldNames.TargetEndDate,
                FieldNames.ActStartDate, FieldNames.EarlyStartDate, FieldNames.LateStartDate
            };

            foreach (string colName in directFormatCols)
            {
                int targetIndex = Array.IndexOf(finalColumns, colName);
                if (targetIndex != -1)
                {
                    string originalValue = GetFieldValue(sourceRow, sourceIndexes, colName);
                    transformedRow[targetIndex] = DateParser.Format(DateParser.TryParse(originalValue));
                }
            }

            int actEndIdxTarget = Array.IndexOf(finalColumns, FieldNames.ActEndDate);
            if (actEndIdxTarget != -1)
            {
                transformedRow[actEndIdxTarget] = DateParser.Format(actEndDate);
            }

            // Handle EarlyEndDate with fallback
            int earlyEndIdxTarget = Array.IndexOf(finalColumns, FieldNames.EarlyEndDate);
            if (earlyEndIdxTarget != -1)
            {
                string originalValue = GetFieldValue(sourceRow, sourceIndexes, FieldNames.EarlyEndDate);
                DateTime? earlyEndDate = DateParser.TryParse(originalValue);

                if (earlyEndDate.HasValue)
                {
                    transformedRow[earlyEndIdxTarget] = DateParser.Format(earlyEndDate);
                }
                else if (string.IsNullOrWhiteSpace(originalValue) && actEndDate.HasValue)
                {
                    transformedRow[earlyEndIdxTarget] = DateParser.Format(actEndDate);
                }
                else
                {
                    transformedRow[earlyEndIdxTarget] = originalValue ?? string.Empty;
                }
            }

            // Handle LateEndDate with fallback
            int lateEndIdxTarget = Array.IndexOf(finalColumns, FieldNames.LateEndDate);
            if (lateEndIdxTarget != -1)
            {
                string originalValue = GetFieldValue(sourceRow, sourceIndexes, FieldNames.LateEndDate);
                DateTime? lateEndDate = DateParser.TryParse(originalValue);

                if (lateEndDate.HasValue)
                {
                    transformedRow[lateEndIdxTarget] = DateParser.Format(lateEndDate);
                }
                else if (string.IsNullOrWhiteSpace(originalValue) && actEndDate.HasValue)
                {
                    transformedRow[lateEndIdxTarget] = DateParser.Format(actEndDate);
                }
                else
                {
                    transformedRow[lateEndIdxTarget] = originalValue ?? string.Empty;
                }
            }
        }

        private DateTime CalculateStartDate(string[] row, IReadOnlyDictionary<string, int> indexes, string statusCode)
        {
            if (statusCode == "TK_NotStart")
            {
                var parsed = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.EarlyStartDate));
                if (parsed.HasValue) return parsed.Value;
                parsed = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.EarlyEndDate));
                if (parsed.HasValue) return parsed.Value;
            }
            else
            {
                var parsed = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.ActStartDate));
                if (parsed.HasValue) return parsed.Value;
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
            else
            {
                var parsed = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.EarlyEndDate));
                if (parsed.HasValue) return parsed.Value;
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
                return days.ToString("F1", CultureInfo.InvariantCulture);
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
                case "CP_Phys":
                    if (double.TryParse(GetFieldValue(row, indexes, FieldNames.PhysCompletePct), numStyle, formatProvider, out double pct))
                    {
                        return Math.Max(0.0, Math.Min(100.0, pct));
                    }
                    break;

                case "CP_Units":
                    if (double.TryParse(GetFieldValue(row, indexes, FieldNames.ActWorkQty), numStyle, formatProvider, out double actVal) &&
                        double.TryParse(GetFieldValue(row, indexes, FieldNames.RemainWorkQty), numStyle, formatProvider, out double remVal))
                    {
                        double total = actVal + remVal;
                        if (total > 0.0001)
                        {
                            double calculatedPct = (actVal / total) * 100.0;
                            return Math.Max(0.0, Math.Min(100.0, calculatedPct));
                        }
                    }
                    break;

                case "CP_Drtn":
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

        // 04_XER_BASELINE
        public XerTable Create04XerBaselineTable(XerTable task01Table)
        {
            if (!IsTableValid(task01Table)) return null;

            try
            {
                var baselineTable = new XerTable(EnhancedTableNames.XerBaseline04, task01Table.RowCount);
                baselineTable.SetHeaders(task01Table.Headers);

                if (!task01Table.FieldIndexes.TryGetValue(FieldNames.DataDate, out int dataDateIndex)) return null;

                // Find minimum Data Date
                DateTime? minDataDate = null;
                foreach (var rowData in task01Table.Rows)
                {
                    var currentDate = DateParser.TryParse(XerTable.GetFieldValueSafe(rowData, dataDateIndex));
                    if (currentDate.HasValue)
                    {
                        if (!minDataDate.HasValue || currentDate.Value.Date < minDataDate.Value.Date)
                        {
                            minDataDate = currentDate.Value.Date;
                        }
                    }
                }

                if (!minDataDate.HasValue) return null;

                // Filter rows
                foreach (var sourceRow in task01Table.Rows)
                {
                    var rowDate = DateParser.TryParse(XerTable.GetFieldValueSafe(sourceRow, dataDateIndex));
                    if (rowDate.HasValue && rowDate.Value.Date == minDataDate.Value)
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

        // 03_XER_PROJWBS (Parallelized)
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
                string[] finalHeaders = finalHeadersList.Select(StringInternPool.Intern).ToArray();

                var resultTable = new XerTable(EnhancedTableNames.XerProjWbs03, projwbsTable.RowCount);
                resultTable.SetHeaders(finalHeaders);

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

                    allWbsIdKeys.TryAdd(wbsIdKey, 0);

                    Array.Copy(row, transformed, sourceHeaders.Length);
                    SetTransformedField(transformed, finalHeaders, FieldNames.WbsIdKey, wbsIdKey);
                    SetTransformedField(transformed, finalHeaders, FieldNames.ParentWbsIdKey, parentWbsIdKey);

                    for (int k = 0; k < transformed.Length; k++)
                    {
                        transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);
                    }
                    transformedRowsBag.Add(new DataRow(transformed, originalFilename));
                });

                // Validate ParentWbsIdKeys
                int parentKeyIndex = Array.IndexOf(finalHeaders, FieldNames.ParentWbsIdKey);
                foreach (var dataRow in transformedRowsBag)
                {
                    var fields = dataRow.Fields;
                    string parentKey = fields[parentKeyIndex];

                    if (!string.IsNullOrEmpty(parentKey) && !allWbsIdKeys.ContainsKey(parentKey))
                    {
                        fields[parentKeyIndex] = string.Empty;
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

        // Simple Keyed Tables (Generic Parallel Implementation)
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
                string[] finalHeaders = finalHeadersList.Select(StringInternPool.Intern).ToArray();

                var resultTable = new XerTable(newTableName, sourceTable.RowCount);
                resultTable.SetHeaders(finalHeaders);

                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };
                var transformedRowsBag = new ConcurrentBag<DataRow>();

                Parallel.ForEach(sourceTable.Rows, parallelOptions, sourceRow =>
                {
                    var row = sourceRow.Fields;
                    string[] transformed = new string[finalHeaders.Length];
                    string originalFilename = sourceRow.SourceFilename;

                    Array.Copy(row, transformed, sourceHeaders.Length);

                    foreach (var mapping in keyMappings)
                    {
                        if (sourceIndexes.ContainsKey(mapping.Item2))
                        {
                            string sourceValue = GetFieldValue(row, sourceIndexes, mapping.Item2);
                            string key = CreateKey(originalFilename, sourceValue);
                            SetTransformedField(transformed, finalHeaders, mapping.Item1, key);
                        }
                    }

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

        public XerTable Create06XerPredecessor() => CreateSimpleKeyedTable(TableNames.TaskPred, EnhancedTableNames.XerPredecessor06,
            new List<Tuple<string, string>> {
                Tuple.Create(FieldNames.TaskIdKey, FieldNames.TaskId),
                Tuple.Create(FieldNames.PredTaskIdKey, FieldNames.PredTaskId)
            });

        public XerTable Create02XerProject() => CreateSimpleKeyedTable(TableNames.Project, EnhancedTableNames.XerProject02,
            new List<Tuple<string, string>> { Tuple.Create(FieldNames.ProjIdKey, FieldNames.ProjectId) });

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
                Tuple.Create(FieldNames.ClndrIdKey, FieldNames.ClndrId)
            });

        public XerTable Create13XerTaskRsrc() => CreateSimpleKeyedTable(TableNames.TaskRsrc, EnhancedTableNames.XerTaskRsrc13,
            new List<Tuple<string, string>> {
                Tuple.Create(FieldNames.RsrcIdKey, FieldNames.RsrcId),
                Tuple.Create(FieldNames.TaskIdKey, FieldNames.TaskId)
            });

        // 11_XER_CALENDAR_DETAILED (Parallelized)
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
            FieldNames.WorkHours, FieldNames.ExceptionType, FieldNames.ClndrIdKey
        };

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

                    var calendarEntries = ParseCalendarData(clndrData, dayHrCnt);

                    foreach (var entry in calendarEntries)
                    {
                        string[] detailedRow = new string[detailedColumns.Length];
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.ClndrId, clndrId);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.CalendarName, GetFieldValue(row, sourceIndexes, FieldNames.CalendarName));
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.CalendarType, GetFieldValue(row, sourceIndexes, FieldNames.CalendarType));
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.Date, entry.Date);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.DayOfWeek, entry.DayOfWeek);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.WorkingDay, entry.WorkingDay);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.WorkHours, entry.WorkHours);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.ExceptionType, entry.ExceptionType);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.ClndrIdKey, clndrIdKey);

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

        // Calendar Parsing Helpers
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
                // Check for structured calendar data
                bool hasStructuredData = clndrData.Contains("CalendarData") ||
                                         clndrData.Contains("DaysOfWeek") ||
                                         clndrData.Contains("(0||");

                if (hasStructuredData)
                {
                    var result = ParseP6CalendarFormat(clndrData, defaultDayHours);
                    if (result.Count > 0) return result;
                }

                return GetDefaultWorkWeek(defaultDayHours);
            }
            catch (Exception ex)
            {
                // Log the error for debugging
                Console.WriteLine($"Error parsing calendar data: {ex.Message}");
                return GetDefaultWorkWeek(defaultDayHours);
            }
        }

        private List<CalendarEntry> ParseP6CalendarFormat(string clndrData, string defaultDayHours)
        {
            var entries = new List<CalendarEntry>();
            clndrData = CleanUnicodeData(clndrData);

            // Parse DaysOfWeek
            int daysStart = clndrData.IndexOf("DaysOfWeek");
            if (daysStart > -1)
            {
                // Find the end of DaysOfWeek section
                int daysEnd = FindSectionEnd(clndrData, daysStart);

                if (daysEnd > daysStart)
                {
                    string daysSection = clndrData.Substring(daysStart, daysEnd - daysStart);

                    // Parse each day (1=Sunday through 7=Saturday)
                    for (int dayNum = 1; dayNum <= 7; dayNum++)
                    {
                        string dayName = GetDayName(dayNum);
                        decimal totalHours = 0;

                        // Look for the day's complete section
                        // Pattern: (0||{dayNum}()( ... ))
                        string dayPatternFull = $@"\(0\|\|{dayNum}\(\)\s*\(((?:[^()]|\((?:[^()]|\([^()]*\))*\))*)\)\s*\)";
                        var dayMatch = Regex.Match(daysSection, dayPatternFull, RegexOptions.Singleline);

                        if (dayMatch.Success && dayMatch.Groups.Count > 1)
                        {
                            string dayContent = dayMatch.Groups[1].Value.Trim();

                            if (!string.IsNullOrWhiteSpace(dayContent))
                            {
                                // Parse the work hours for this specific day
                                totalHours = ParseDayWorkHours(dayContent);
                            }
                        }
                        else
                        {
                            // Try simpler pattern for empty days
                            string emptyDayPattern = $@"\(0\|\|{dayNum}\(\)\s*\(\s*\)\s*\)";
                            if (Regex.IsMatch(daysSection, emptyDayPattern))
                            {
                                totalHours = 0;
                            }
                            else
                            {
                                // Day not found - use default based on day of week
                                if (dayNum == 1 || dayNum == 7) // Sunday or Saturday
                                {
                                    totalHours = 0; // Weekend by default
                                }
                                else
                                {
                                    // Use default hours for weekdays if not found
                                    if (decimal.TryParse(defaultDayHours, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal defHours))
                                    {
                                        totalHours = defHours;
                                    }
                                    else
                                    {
                                        totalHours = 8; // Standard 8-hour day
                                    }
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

            // If no days were parsed, use default work week
            if (entries.Count == 0)
                entries.AddRange(GetDefaultWorkWeek(defaultDayHours));

            // Parse Exceptions/Holidays
            ParseExceptions(clndrData, entries);

            return entries;
        }

        private void ParseExceptions(string clndrData, List<CalendarEntry> entries)
        {
            // Try to find exceptions in various sections
            var exceptionSections = new[] { "Exceptions", "HolidayOrExceptions", "HolidayOrException" };

            foreach (var sectionName in exceptionSections)
            {
                int excStart = clndrData.IndexOf(sectionName);
                if (excStart > -1)
                {
                    // Find the end of this section
                    int excEnd = FindSectionEnd(clndrData, excStart);
                    string excSection = excEnd > excStart ?
                        clndrData.Substring(excStart, excEnd - excStart) :
                        clndrData.Substring(excStart);

                    // Try both exception patterns
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
                    if (int.TryParse(excMatch.Groups[2].Value, out int dateSerial))
                    {
                        DateTime excDate = ConvertFromOleDate(dateSerial);
                        string workContent = excMatch.Groups.Count > 3 ? excMatch.Groups[3].Value : "";
                        decimal hours = ParseDayWorkHours(workContent);

                        entries.Add(new CalendarEntry
                        {
                            Date = excDate.ToString("yyyy-MM-dd"),
                            DayOfWeek = excDate.DayOfWeek.ToString(),
                            WorkingDay = hours > 0 ? "Y" : "N",
                            WorkHours = FormatHours(hours),
                            ExceptionType = DetermineExceptionType(hours, excDate)
                        });
                    }
                }
            }
        }

        private string DetermineExceptionType(decimal hours, DateTime date)
        {
            if (hours > 0)
                return "Exception - Working";

            // You could add logic here to identify specific holidays based on date
            // For now, just mark as Holiday
            return "Holiday";
        }

        private int FindSectionEnd(string data, int sectionStart)
        {
            // Find the end of a section by looking for the next major section or end of data
            var nextSections = new[] { "VIEW", "Exceptions", "HolidayOrExceptions", "Resources" };
            int minEnd = data.Length;

            foreach (var section in nextSections)
            {
                int pos = data.IndexOf(section, sectionStart + 10); // +10 to skip current section name
                if (pos > -1 && pos < minEnd)
                    minEnd = pos;
            }

            return minEnd;
        }
        private string CleanUnicodeData(string data)
        {
            if (string.IsNullOrEmpty(data)) return data;

            var cleaned = new StringBuilder(data.Length);
            foreach (char c in data)
            {
                // Keep printable ASCII characters and common symbols
                if ((c >= 32 && c <= 126) || char.IsLetterOrDigit(c) ||
                    char.IsPunctuation(c) || char.IsSymbol(c))
                {
                    cleaned.Append(c);
                }
                else if (c == '\r' || c == '\n' || c == '\t')
                {
                    cleaned.Append(' ');
                }
                else
                {
                    // Replace other characters with space
                    cleaned.Append(' ');
                }
            }

            string result = cleaned.ToString();

            // Clean up extra spaces around pipe delimiters
            result = CleanRegex1.Replace(result, "$1");
            result = CleanRegex2.Replace(result, "$1");

            // Remove multiple consecutive spaces
            result = Regex.Replace(result, @"\s{2,}", " ");

            return result;
        }

        private decimal ParseDayWorkHours(string content)
        {
            if (string.IsNullOrWhiteSpace(content)) return 0;

            decimal totalHours = 0;

            // First, try to find all time slot patterns in the content
            // Look for patterns like (0||0(s|08:00|f|12:00)()) or (0||1(s|13:00|f|17:00)())
            // These represent individual time slots for the day

            // Pattern specifically for P6 time slots with index
            string timeSlotPattern = @"\(0\|\|\d+\s*\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)\s*\(\s*\)\s*\)";
            var timeSlotRegex = new Regex(timeSlotPattern, RegexOptions.Singleline);
            var matches = timeSlotRegex.Matches(content);

            if (matches.Count > 0)
            {
                foreach (Match match in matches)
                {
                    if (match.Groups.Count >= 5)
                    {
                        if (TimeSpan.TryParse(match.Groups[2].Value, out TimeSpan time1) &&
                            TimeSpan.TryParse(match.Groups[4].Value, out TimeSpan time2))
                        {
                            string type1 = match.Groups[1].Value;
                            string type2 = match.Groups[3].Value;

                            TimeSpan start, end;

                            // Determine which is start and which is finish
                            if (type1 == "s" && type2 == "f")
                            {
                                start = time1;
                                end = time2;
                            }
                            else if (type1 == "f" && type2 == "s")
                            {
                                start = time2;
                                end = time1;
                            }
                            else if (type1 == "s" && type2 == "s")
                            {
                                // Both marked as start - take the earlier as start, later as end
                                start = time1 < time2 ? time1 : time2;
                                end = time1 < time2 ? time2 : time1;
                            }
                            else // both 'f' or other combination
                            {
                                // Assume first is start, second is end
                                start = time1;
                                end = time2;
                            }

                            // Calculate hours for this time slot
                            decimal slotHours = CalculateHoursBetween(start, end);
                            totalHours += slotHours;
                        }
                    }
                }

                return totalHours;
            }

            // If no matches with the primary pattern, try alternative patterns
            // Pattern without the outer wrapper
            string altPattern1 = @"\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)";
            var altRegex1 = new Regex(altPattern1, RegexOptions.Singleline);
            matches = altRegex1.Matches(content);

            if (matches.Count > 0)
            {
                foreach (Match match in matches)
                {
                    if (match.Groups.Count >= 5)
                    {
                        if (TimeSpan.TryParse(match.Groups[2].Value, out TimeSpan time1) &&
                            TimeSpan.TryParse(match.Groups[4].Value, out TimeSpan time2))
                        {
                            string type1 = match.Groups[1].Value;
                            string type2 = match.Groups[3].Value;

                            TimeSpan start, end;

                            if (type1 == "s" && type2 == "f")
                            {
                                start = time1;
                                end = time2;
                            }
                            else if (type1 == "f" && type2 == "s")
                            {
                                start = time2;
                                end = time1;
                            }
                            else
                            {
                                start = time1;
                                end = time2;
                            }

                            decimal slotHours = CalculateHoursBetween(start, end);
                            totalHours += slotHours;
                        }
                    }
                }

                return totalHours;
            }

            // Last resort - look for simple time patterns
            string simplePattern = @"([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})";
            var simpleRegex = new Regex(simplePattern, RegexOptions.Singleline);
            matches = simpleRegex.Matches(content);

            if (matches.Count > 0)
            {
                // To avoid duplicates in simple pattern, track what we've already processed
                var processedPairs = new HashSet<string>();

                foreach (Match match in matches)
                {
                    if (match.Groups.Count >= 5)
                    {
                        string timeKey = $"{match.Groups[2].Value}-{match.Groups[4].Value}";
                        if (processedPairs.Contains(timeKey))
                            continue;

                        processedPairs.Add(timeKey);

                        if (TimeSpan.TryParse(match.Groups[2].Value, out TimeSpan time1) &&
                            TimeSpan.TryParse(match.Groups[4].Value, out TimeSpan time2))
                        {
                            string type1 = match.Groups[1].Value;
                            string type2 = match.Groups[3].Value;

                            TimeSpan start, end;

                            if (type1 == "s" && type2 == "f")
                            {
                                start = time1;
                                end = time2;
                            }
                            else if (type1 == "f" && type2 == "s")
                            {
                                start = time2;
                                end = time1;
                            }
                            else
                            {
                                start = time1;
                                end = time2;
                            }

                            decimal slotHours = CalculateHoursBetween(start, end);
                            totalHours += slotHours;
                        }
                    }
                }
            }

            return totalHours;
        }

        // Helper method to calculate hours between two time spans
        private decimal CalculateHoursBetween(TimeSpan start, TimeSpan end)
        {
            // Handle special cases
            if (start == TimeSpan.Zero && end == TimeSpan.Zero)
            {
                // Both midnight - no work
                return 0;
            }

            if (start == end)
            {
                // Same time - could be 24 hours or 0 hours
                // If it's not midnight, assume 0 hours
                return start == TimeSpan.Zero ? 0 : 0;
            }

            if (end > start)
            {
                // Normal case - end is after start
                return (decimal)(end - start).TotalHours;
            }
            else
            {
                // Overnight shift - end is before start
                // e.g., 22:00 to 06:00
                return (decimal)(TimeSpan.FromHours(24) - start + end).TotalHours;
            }
        }
        private DateTime ConvertFromOleDate(int oleDate)
        {
            try
            {
                // OLE Automation date starts from December 30, 1899
                // Note: There's a known bug in Excel/OLE where it treats 1900 as a leap year

                if (oleDate < 1)
                    return new DateTime(1900, 1, 1);

                // Adjust for the Excel/OLE leap year bug
                if (oleDate < 60)
                {
                    return new DateTime(1899, 12, 30).AddDays(oleDate);
                }
                else
                {
                    // After Feb 28, 1900, we need to subtract 1 to account for the leap year bug
                    return new DateTime(1899, 12, 30).AddDays(oleDate - 1);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error converting OLE date {oleDate}: {ex.Message}");
                return new DateTime(2000, 1, 1);
            }
        }

        private string GetDayName(int dayNumber)
        {
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
            // Format hours to 2 decimal places, but remove trailing zeros
            if (hours == 0) return "0";
            if (hours == Math.Floor(hours)) return hours.ToString("0");
            return hours.ToString("0.##");
        }

        private List<CalendarEntry> GetDefaultWorkWeek(string defaultDayHours)
        {
            decimal defaultHours = 8;
            if (!string.IsNullOrWhiteSpace(defaultDayHours))
            {
                if (decimal.TryParse(defaultDayHours, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal parsed))
                    defaultHours = Math.Max(0, parsed);
            }

            // Check if it's a 24x7 calendar (usually 12 hours per day for continuous operations)
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

    public class ProcessingService
    {
        private readonly XerParser _parser;
        private readonly CsvExporter _exporter;

        public ProcessingService()
        {
            _parser = new XerParser();
            _exporter = new CsvExporter();
        }

        public async Task<XerDataStore> ParseMultipleXerFilesAsync(List<string> filePaths, IProgress<(int percent, string message)> progress)
        {
            StringInternPool.Clear();
            DateParser.ClearCache();

            int fileCount = filePaths.Count;
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = Math.Min(PerformanceConfig.MaxParallelFiles, fileCount) };
            var fileIndex = 0;
            var intermediateStores = new ConcurrentBag<XerDataStore>();

            var combinedStore = await Task.Run<XerDataStore>(() =>
            {
                Parallel.ForEach(filePaths, parallelOptions, file =>
                {
                    var currentIndex = Interlocked.Increment(ref fileIndex);
                    string shortFileName = Path.GetFileName(file);

                    try
                    {
                        if (!File.Exists(file))
                        {
                            progress?.Report((currentIndex * 100 / fileCount, $"Skipping missing file: {shortFileName}"));
                            return;
                        }

                        var singleFileStore = _parser.ParseXerFile(file, (p, s) =>
                        {
                            int overallProgress = ((currentIndex - 1) * 100 + p) / fileCount;
                            progress?.Report((overallProgress, s));
                        });

                        intermediateStores.Add(singleFileStore);
                    }
                    catch (Exception ex)
                    {
                        throw new Exception($"Failed to parse file '{shortFileName}': {ex.Message}", ex);
                    }
                });

                progress?.Report((100, "Merging parsed data..."));
                var storeToReturn = new XerDataStore();
                foreach (var store in intermediateStores)
                {
                    storeToReturn.MergeStore(store);
                }
                return storeToReturn;
            });

            return combinedStore;
        }

        public async Task<List<string>> ExportTablesAsync(XerDataStore dataStore, List<string> tablesToExport, string outputDirectory, IProgress<(int percent, string message)> progress)
        {
            var exportedFiles = new ConcurrentBag<string>();
            var transformer = new XerTransformer(dataStore);
            ConcurrentDictionary<string, XerTable> enhancedCache = new ConcurrentDictionary<string, XerTable>(StringComparer.OrdinalIgnoreCase);

            bool needsTask01 = tablesToExport.Contains(EnhancedTableNames.XerTask01, StringComparer.OrdinalIgnoreCase) ||
                               tablesToExport.Contains(EnhancedTableNames.XerBaseline04, StringComparer.OrdinalIgnoreCase);

            if (needsTask01 && dataStore.ContainsTable(TableNames.Task) && dataStore.ContainsTable(TableNames.Calendar) && dataStore.ContainsTable(TableNames.Project))
            {
                progress?.Report((0, "Preprocessing: Generating 01_XER_TASK..."));
                var task01Data = transformer.Create01XerTaskTable();
                if (task01Data != null)
                {
                    enhancedCache.TryAdd(EnhancedTableNames.XerTask01, task01Data);
                }
            }

            var progressCounter = new ProgressCounter(tablesToExport.Count, progress);

            await Task.Run(() =>
            {
                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelFiles };

                Parallel.ForEach(tablesToExport.OrderBy(n => n), parallelOptions, tableName =>
                {
                    progressCounter.UpdateStatus($"Processing table: {tableName}");
                    string csvFilePath = Path.Combine(outputDirectory, $"{tableName}.csv");
                    XerTable tableToExport = null;

                    try
                    {
                        if (dataStore.ContainsTable(tableName))
                        {
                            tableToExport = dataStore.GetTable(tableName);
                        }
                        else if (enhancedCache.TryGetValue(tableName, out XerTable cachedTable))
                        {
                            tableToExport = cachedTable;
                        }
                        else
                        {
                            tableToExport = GenerateEnhancedTable(transformer, tableName, enhancedCache);
                        }

                        if (tableToExport != null && !tableToExport.IsEmpty)
                        {
                            _exporter.WriteTableToCsv(tableToExport, csvFilePath);
                            exportedFiles.Add(csvFilePath);
                        }

                        progressCounter.Increment($"Exported table: {tableName}");
                    }
                    catch (Exception ex)
                    {
                        throw new Exception($"Error exporting table '{tableName}': {ex.Message}", ex);
                    }
                });
            });

            StringInternPool.Clear();
            DateParser.ClearCache();
            return exportedFiles.ToList();
        }

        private XerTable GenerateEnhancedTable(XerTransformer transformer, string tableName, ConcurrentDictionary<string, XerTable> cache)
        {
            switch (tableName)
            {
                case EnhancedTableNames.XerProject02: return transformer.Create02XerProject();
                case EnhancedTableNames.XerProjWbs03: return transformer.Create03XerProjWbsTable();
                case EnhancedTableNames.XerBaseline04:
                    if (cache.TryGetValue(EnhancedTableNames.XerTask01, out XerTable task01))
                    {
                        return transformer.Create04XerBaselineTable(task01);
                    }
                    break;
                case EnhancedTableNames.XerPredecessor06: return transformer.Create06XerPredecessor();
                case EnhancedTableNames.XerActvType07: return transformer.Create07XerActvType();
                case EnhancedTableNames.XerActvCode08: return transformer.Create08XerActvCode();
                case EnhancedTableNames.XerTaskActv09: return transformer.Create09XerTaskActv();
                case EnhancedTableNames.XerCalendar10: return transformer.Create10XerCalendar();
                case EnhancedTableNames.XerCalendarDetailed11: return transformer.Create11XerCalendarDetailed();
                case EnhancedTableNames.XerRsrc12: return transformer.Create12XerRsrc();
                case EnhancedTableNames.XerTaskRsrc13: return transformer.Create13XerTaskRsrc();
            }
            return null;
        }
    }

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
                _progress.Report((percent, $"{message} ({_count}/{_total})"));
            }
        }
    }

    #endregion

    // -----------------------------------------------------------------------------------------------------------------
    // UI Layer (MainForm) - Enhanced UI Implementation
    // -----------------------------------------------------------------------------------------------------------------
    #region UI Layer (Enhanced)

    public partial class MainForm : Form
    {
        // --- UI Controls (Declarations) ---
        private System.Windows.Forms.SplitContainer splitContainerMain;
        private System.Windows.Forms.SplitContainer splitContainerResults; // Nested SplitContainer for Tables/Log
        private System.Windows.Forms.StatusStrip statusStrip;
        private System.Windows.Forms.ToolStripStatusLabel toolStripStatusLabel;
        private System.Windows.Forms.ToolStripProgressBar toolStripProgressBar;
        private System.Windows.Forms.MenuStrip menuStrip;
        private System.Windows.Forms.ToolStripMenuItem fileToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem exitToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem helpToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem aboutToolStripMenuItem;

        // Input Panel Controls
        private System.Windows.Forms.GroupBox grpInputFiles;
        // UI Enhancement: Using ListView for better file display
        private System.Windows.Forms.ListView lvwXerFiles;
        private System.Windows.Forms.ColumnHeader colFileName;
        private System.Windows.Forms.ColumnHeader colFilePath;

        private System.Windows.Forms.ToolStrip toolStripFileActions;
        private System.Windows.Forms.ToolStripButton btnAddFile;
        private System.Windows.Forms.ToolStripButton btnRemoveFile;
        private System.Windows.Forms.ToolStripButton btnClearFiles;
        private System.Windows.Forms.Label lblDragDropHint;

        private System.Windows.Forms.GroupBox grpOutput;
        private System.Windows.Forms.TextBox txtOutputPath;
        private System.Windows.Forms.Button btnSelectOutput;

        private System.Windows.Forms.Button btnParseXer;

        // Results Panel Controls
        private System.Windows.Forms.GroupBox grpExtractedTables;
        private System.Windows.Forms.ListBox lstTables;
        // UI Enhancement: Table filtering
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

        private System.Windows.Forms.ToolTip toolTip;
        // ------------------------------------------

        private List<string> _xerFilePaths = new List<string>();
        private string _outputDirectory;
        private List<string> _allTableNames = new List<string>(); // Used for filtering

        // Backend Services
        private XerDataStore _dataStore;
        private readonly ProcessingService _processingService;

        // Dependency flags
        private bool _canCreateTask01 = false; private bool _canCreateProjWbs03 = false; private bool _canCreateBaseline04 = false; private bool _canCreateProject02 = false; private bool _canCreatePredecessor06 = false; private bool _canCreateActvType07 = false; private bool _canCreateActvCode08 = false; private bool _canCreateTaskActv09 = false; private bool _canCreateCalendar10 = false; private bool _canCreateCalendarDetailed11 = false; private bool _canCreateRsrc12 = false; private bool _canCreateTaskRsrc13 = false;


        public MainForm()
        {
            // Initialize backend
            _processingService = new ProcessingService();
            _dataStore = new XerDataStore();

            // Initialize UI Components
            InitializeComponent();

            // UI Initialization (Post-generation setup)
            InitializeUIState();
        }

        private void InitializeUIState()
        {
            // Setup ListView interaction
            this.lvwXerFiles.AllowDrop = true;
            this.lvwXerFiles.DragEnter += new DragEventHandler(LvwXerFiles_DragEnter);
            this.lvwXerFiles.DragDrop += new DragEventHandler(LvwXerFiles_DragDrop);
            this.Resize += new System.EventHandler(this.MainForm_Resize); // Handle resizing for ListView columns

            UpdateStatus("Ready");
            LogActivity("Application started.");
            UpdateInputButtonsState();
            UpdateExportButtonState();
            UpdatePbiCheckboxState(); // Initialize PBI status label
            ResizeListViewColumns(); // Initial column sizing
        }

        #region UI Event Handlers

        // --- Menu Handlers ---
        private void ExitToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Application.Exit();
        }

        private void AboutToolStripMenuItem_Click(object sender, EventArgs e)
        {
            ShowInfo("Primavera P6 XER to CSV Converter\nVersion: 2.1\n\nSOFTWARE COPYRIGHT NOTICE\r\nCopyright © 2025 Ricardo Aguirre. All Rights Reserved.\r\nUnauthorized use, copying, or sharing of this software is strictly prohibited. Written permission required for any use.\r\nTHE SOFTWARE IS PROVIDED \"AS IS\" WITHOUT WARRANTY OF ANY KIND. RICARDO AGUIRRE SHALL NOT BE LIABLE FOR ANY DAMAGES ARISING FROM USE OF THIS SOFTWARE.\r\nBy using this software, you agree to these terms.", "About");
        }

        // --- File Input Handlers ---

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

        // --- Output Selection Handlers ---
        private void BtnSelectOutput_Click(object sender, EventArgs e)
        {
            using (FolderBrowserDialog folderDialog = new FolderBrowserDialog
            {
                Description = "Select Output Directory for CSV Files"
            })
            {
                if (folderDialog.ShowDialog() == DialogResult.OK)
                {
                    _outputDirectory = folderDialog.SelectedPath;
                    txtOutputPath.Text = _outputDirectory;
                    UpdateInputButtonsState();
                    UpdateExportButtonState();
                    LogActivity($"Output directory set: {_outputDirectory}");
                }
            }
        }

        // --- Parsing Handler ---
        private async void BtnParseXer_Click(object sender, EventArgs e)
        {
            if (!ValidateInputs()) return;

            // Clear previous results before parsing new files
            ClearResults(false); // Don't force GC yet, wait until after parsing
            LogActivity("Starting XER parsing...");

            SetUIEnabled(false);
            UpdateStatus("Parsing XER File(s)...");
            toolStripProgressBar.Visible = true;

            // Use IProgress for updates back to the UI thread (integrated into StatusStrip)
            var progress = new Progress<(int percent, string message)>(p =>
            {
                UpdateStatus(p.message);
                toolStripProgressBar.Value = p.percent;
                // Optionally log detailed progress
                // LogActivity(p.message);
            });

            try
            {
                var stopwatch = Stopwatch.StartNew();

                // Execute optimized backend service
                var result = await _processingService.ParseMultipleXerFilesAsync(_xerFilePaths, progress);
                stopwatch.Stop();

                // Update state
                _dataStore = result;
                UpdateTableList();
                CheckEnhancedTableDependencies();
                UpdatePbiCheckboxState();
                UpdateExportButtonState();

                // Final status update
                toolStripProgressBar.Value = 100;
                string summary = $"Parsing complete. Extracted {_dataStore.TableCount} tables. Time elapsed: {stopwatch.Elapsed.TotalSeconds:F2}s.";
                UpdateStatus(summary);
                LogActivity(summary);
            }
            catch (Exception ex)
            {
                ShowError($"Error parsing XER file(s): {ex.Message}\n\nDetails: {ex.InnerException?.Message}");
                LogActivity($"ERROR during parsing: {ex.Message}");
                ClearResults();
                UpdateStatus("Parsing failed.");
            }
            finally
            {
                SetUIEnabled(true);
                toolStripProgressBar.Visible = false;
            }
        }

        // --- Export Handlers ---

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

        private void LstTables_SelectedIndexChanged(object sender, EventArgs e)
        {
            UpdateExportButtonState();
        }

        // UI Enhancement: Table filtering handler
        private void TxtTableFilter_TextChanged(object sender, EventArgs e)
        {
            FilterTableList(txtTableFilter.Text);
        }

        // --- Power BI Handlers ---

        private void ChkCreatePowerBiTables_CheckedChanged(object sender, EventArgs e)
        {
            UpdateExportButtonState();
            if (chkCreatePowerBiTables.Checked && !AnyPbiDependencyMet())
            {
                // This state should ideally not happen if UpdatePbiCheckboxState is working correctly, but kept as a safeguard.
                ShowWarning(GetMissingDependenciesMessage("Cannot create any Power BI tables due to missing dependencies."));
            }
        }

        private void BtnPbiDetails_Click(object sender, EventArgs e)
        {
            // Show a detailed message box explaining the PBI status
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

            message += "\n" + GetMissingDependenciesMessage("Summary:");

            ShowInfo(message, "Power BI Details");
        }

        private string GetPbiTableStatusDetail(string tableName, bool canCreate)
        {
            // Unicode checkmark or cross
            return $"[{(canCreate ? "\u2713" : "\u2717")}] {tableName}\n";
        }

        // --- Drag/Drop and KeyDown handlers (Adapted for ListView) ---
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

        private void LvwXerFiles_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Delete && lvwXerFiles.SelectedItems.Count > 0)
            {
                RemoveSelectedXerFiles();
            }
        }

        private void MainForm_Resize(object sender, EventArgs e)
        {
            ResizeListViewColumns();
        }

        #endregion

        #region Core Logic - (Validation, Export Orchestration, Dependencies)

        // (Methods preserved exactly as they contain critical business logic)

        private bool ValidateInputs()
        {
            if (_xerFilePaths.Count == 0) { ShowError("Please select at least one XER file first."); return false; }
            if (string.IsNullOrEmpty(_outputDirectory) || !Directory.Exists(_outputDirectory)) { ShowError("Please select a valid output directory first."); return false; }

            var missingFiles = _xerFilePaths.Where(f => !File.Exists(f)).ToList();
            if (missingFiles.Any())
            {
                ShowError($"The following file(s) could not be found:\n{string.Join("\n", missingFiles)}");

                // Logic to remove missing files from ListView and _xerFilePaths
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
                if (_xerFilePaths.Count == 0) return false;
            }
            return true;
        }

        private List<string> GetAllTableNamesToExport()
        {
            // Use the full list, not the filtered view
            var names = _allTableNames.ToList();
            AddAvailablePbiTables(names);
            return names.Distinct().OrderBy(n => n).ToList();
        }

        private List<string> GetSelectedTableNamesToExport()
        {
            var names = lstTables.SelectedItems.Cast<string>().ToList();
            // If nothing is selected in the list, we cannot export selected raw tables.
            if (names.Count == 0 && _dataStore.TableCount > 0 && !chkCreatePowerBiTables.Checked)
            {
                ShowWarning("Please select at least one table from the list, or enable Power BI table generation.");
                return new List<string>();
            }
            AddAvailablePbiTables(names);
            return names.Distinct().OrderBy(n => n).ToList();
        }

        private void AddAvailablePbiTables(List<string> names)
        {
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
            }
        }

        private async Task ExportTablesAsync(List<string> tablesToExport)
        {
            if (tablesToExport.Count == 0)
            {
                // Message already shown in GetSelectedTableNamesToExport or if validation fails earlier.
                return;
            }
            if (string.IsNullOrEmpty(_outputDirectory) || !Directory.Exists(_outputDirectory)) { ShowError("Please select a valid output directory first."); return; }

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

            SetUIEnabled(false);
            UpdateStatus("Exporting Tables...");
            toolStripProgressBar.Visible = true;

            // Use IProgress for updates back to the UI thread (integrated into StatusStrip)
            var progress = new Progress<(int percent, string message)>(p =>
            {
                UpdateStatus(p.message);
                toolStripProgressBar.Value = p.percent;
            });

            try
            {
                var stopwatch = Stopwatch.StartNew();

                // Execute optimized backend service
                var exportedFiles = await _processingService.ExportTablesAsync(_dataStore, finalTablesToExport, _outputDirectory, progress);
                stopwatch.Stop();

                // Final status update
                toolStripProgressBar.Value = 100;
                string summary = $"Export complete. {exportedFiles.Count} tables exported. Time elapsed: {stopwatch.Elapsed.TotalSeconds:F2}s.";
                UpdateStatus(summary);
                LogActivity(summary);
                ShowExportCompleteMessage(exportedFiles);
            }
            catch (Exception ex)
            {
                ShowError($"Error exporting CSV files: {ex.Message}\n\nDetails: {ex.InnerException?.Message}");
                LogActivity($"ERROR during export: {ex.Message}");
                UpdateStatus("Export failed.");
            }
            finally
            {
                SetUIEnabled(true);
                toolStripProgressBar.Visible = false;
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
                    finalTablesToExport.Add(tableName);
                }
                else if (IsEnhancedTableName(tableName))
                {
                    if (chkCreatePowerBiTables.Checked)
                    {
                        if (CheckSpecificPbiDependency(tableName))
                        {
                            finalTablesToExport.Add(tableName);
                        }
                        else
                        {
                            skippedPbi.Add(tableName);
                        }
                    }
                }
            }
            return (finalTablesToExport.Distinct().ToList(), skippedPbi);
        }

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

            _canCreateTask01 = hasTask && hasCalendar && hasProject;
            _canCreateProjWbs03 = hasProjWbs;
            _canCreateBaseline04 = _canCreateTask01;
            _canCreateProject02 = hasProject;
            _canCreatePredecessor06 = hasTaskPred;
            _canCreateActvType07 = hasActvType;
            _canCreateActvCode08 = hasActvCode;
            _canCreateTaskActv09 = hasTaskActv;
            _canCreateCalendar10 = hasCalendar;
            _canCreateCalendarDetailed11 = hasCalendar;
            _canCreateRsrc12 = hasRsrc;
            _canCreateTaskRsrc13 = hasTaskRsrc;
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
                default: return false;
            }
        }

        private bool AnyPbiDependencyMet()
        {
            return _canCreateTask01 || _canCreateProjWbs03 || _canCreateProject02 ||
                   _canCreatePredecessor06 || _canCreateActvType07 || _canCreateActvCode08 ||
                   _canCreateTaskActv09 || _canCreateCalendar10 || _canCreateCalendarDetailed11 ||
                   _canCreateRsrc12 || _canCreateTaskRsrc13;
        }

        private string GetMissingDependenciesMessage(string prefix)
        {
            var missing = new List<string>();

            if (!_canCreateTask01 && !_canCreateBaseline04)
            {
                if (!_dataStore.ContainsTable(TableNames.Task)) missing.Add(TableNames.Task);
                if (!_dataStore.ContainsTable(TableNames.Calendar)) missing.Add(TableNames.Calendar);
                if (!_dataStore.ContainsTable(TableNames.Project)) missing.Add(TableNames.Project);
            }
            if (!_canCreateProjWbs03 && !_dataStore.ContainsTable(TableNames.ProjWbs)) missing.Add(TableNames.ProjWbs);
            if (!_canCreateProject02 && !_dataStore.ContainsTable(TableNames.Project)) missing.Add(TableNames.Project);
            if (!_canCreatePredecessor06 && !_dataStore.ContainsTable(TableNames.TaskPred)) missing.Add(TableNames.TaskPred);
            if (!_canCreateActvType07 && !_dataStore.ContainsTable(TableNames.ActvType)) missing.Add(TableNames.ActvType);
            if (!_canCreateActvCode08 && !_dataStore.ContainsTable(TableNames.ActvCode)) missing.Add(TableNames.ActvCode);
            if (!_canCreateTaskActv09 && !_dataStore.ContainsTable(TableNames.TaskActv)) missing.Add(TableNames.TaskActv);
            if (!_canCreateCalendar10 && !_canCreateCalendarDetailed11 && !_dataStore.ContainsTable(TableNames.Calendar)) missing.Add(TableNames.Calendar);
            if (!_canCreateRsrc12 && !_dataStore.ContainsTable(TableNames.Rsrc)) missing.Add(TableNames.Rsrc);
            if (!_canCreateTaskRsrc13 && !_dataStore.ContainsTable(TableNames.TaskRsrc)) missing.Add(TableNames.TaskRsrc);

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
            EnhancedTableNames.XerRsrc12, EnhancedTableNames.XerTaskRsrc13
        };
            return enhancedNames.Contains(name);
        }

        #endregion

        #region UI Helper Methods (Enhanced)

        // UI Enhancement: Adapted for ListView
        private void AddXerFiles(IEnumerable<string> filesToAdd)
        {
            int addedCount = 0;
            lvwXerFiles.BeginUpdate();
            foreach (var file in filesToAdd)
            {
                if (!_xerFilePaths.Any(existing => string.Equals(existing, file, StringComparison.OrdinalIgnoreCase)))
                {
                    _xerFilePaths.Add(file);
                    // Create ListViewItem with FileName and Path
                    var item = new ListViewItem(Path.GetFileName(file));
                    item.SubItems.Add(Path.GetDirectoryName(file));
                    item.Tag = file; // Store full path in Tag for easy retrieval
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

        // UI Enhancement: Helper to resize ListView columns
        private void ResizeListViewColumns()
        {
            if (lvwXerFiles.Width > 200)
            {
                // Set Filename column to a reasonable width, and Path to fill the rest
                colFileName.Width = 180;
                colFilePath.Width = lvwXerFiles.ClientSize.Width - colFileName.Width - 5;
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

            // Reset flags
            _canCreateTask01 = false; _canCreateProjWbs03 = false; _canCreateBaseline04 = false;
            _canCreateProject02 = false; _canCreatePredecessor06 = false; _canCreateActvType07 = false;
            _canCreateActvCode08 = false; _canCreateTaskActv09 = false; _canCreateCalendar10 = false;
            _canCreateCalendarDetailed11 = false;
            _canCreateRsrc12 = false; _canCreateTaskRsrc13 = false;

            UpdatePbiCheckboxState();
            UpdateExportButtonState();
            // Status update handled by calling method usually

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

        // UI Enhancement: Implements filtering logic
        private void FilterTableList(string filter)
        {
            lstTables.BeginUpdate();
            lstTables.Items.Clear();

            IEnumerable<string> filteredNames;
            if (string.IsNullOrWhiteSpace(filter))
            {
                filteredNames = _allTableNames;
            }
            else
            {
                // Case-insensitive filtering
                filteredNames = _allTableNames.Where(name => name.IndexOf(filter, StringComparison.OrdinalIgnoreCase) >= 0);
            }

            foreach (var name in filteredNames)
            {
                lstTables.Items.Add(name);
            }
            lstTables.EndUpdate();
        }

        // UI Enhancement: Improved PBI status feedback
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
                chkCreatePowerBiTables.Checked = false;
                lblPbiStatus.Text = "Status: Missing required source tables.";
                lblPbiStatus.ForeColor = System.Drawing.Color.DarkRed;
                btnPbiDetails.Enabled = true;
            }
        }


        private void UpdateInputButtonsState()
        {
            bool hasFiles = _xerFilePaths.Count > 0;
            bool hasValidOutput = !string.IsNullOrEmpty(_outputDirectory) && Directory.Exists(_outputDirectory);
            bool hasSelection = lvwXerFiles.SelectedItems.Count > 0;

            btnParseXer.Enabled = hasFiles && hasValidOutput;
            btnClearFiles.Enabled = hasFiles;
            btnRemoveFile.Enabled = hasSelection;
        }

        private void UpdateExportButtonState()
        {
            bool hasOutput = !string.IsNullOrEmpty(_outputDirectory) && Directory.Exists(_outputDirectory);
            bool hasParsedData = _dataStore.TableCount > 0;
            bool pbiTablesSelectedAndPossible = chkCreatePowerBiTables.Checked && AnyPbiDependencyMet();

            // Export All is enabled if there is output and either raw data exists or PBI is possible
            btnExportAll.Enabled = hasOutput && (hasParsedData || pbiTablesSelectedAndPossible);

            // Export Selected is enabled if there is output AND (something is selected OR PBI is checked/possible)
            // Note: The logic for whether Export Selected should proceed if nothing is selected in the list but PBI is checked is handled in GetSelectedTableNamesToExport
            btnExportSelected.Enabled = hasOutput && (lstTables.SelectedItems.Count > 0 || pbiTablesSelectedAndPossible);
        }

        private void UpdateStatus(string message)
        {
            // Thread-safe UI update for StatusStrip.
            if (statusStrip.InvokeRequired)
            {
                statusStrip.Invoke(new Action(() => toolStripStatusLabel.Text = message));
            }
            else
            {
                toolStripStatusLabel.Text = message;
            }
        }

        // UI Enhancement: Log activity to the TextBox log
        private void LogActivity(string message)
        {
            string timestampedMessage = $"{DateTime.Now:HH:mm:ss}: {message}\r\n";
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

        private void SetUIEnabled(bool enabled)
        {
            this.UseWaitCursor = !enabled;

            // Toggle major control areas
            grpInputFiles.Enabled = enabled;
            grpOutput.Enabled = enabled;
            grpExtractedTables.Enabled = enabled;
            grpPowerBI.Enabled = enabled;
            menuStrip.Enabled = enabled;

            // Ensure buttons respect input requirements even when re-enabled.
            if (enabled)
            {
                UpdateInputButtonsState();
                UpdateExportButtonState();
                UpdatePbiCheckboxState(); // Ensure PBI checkbox state is correct
            }
            else
            {
                btnParseXer.Enabled = false;
                btnExportAll.Enabled = false;
                btnExportSelected.Enabled = false;
                // Toolstrip buttons
                btnAddFile.Enabled = false;
                btnRemoveFile.Enabled = false;
                btnClearFiles.Enabled = false;
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

            if (MessageBox.Show("Do you want to open the output folder?", "Open folder", MessageBoxButtons.YesNo, MessageBoxIcon.Question) == DialogResult.Yes)
            {
                try
                {
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

        // Thread-safe MessageBox helpers
        private void ShowError(string message)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new Action(() => MessageBox.Show(this, message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Error)));
            }
            else
            {
                MessageBox.Show(this, message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private void ShowWarning(string message)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new Action(() => MessageBox.Show(this, message, "Warning", MessageBoxButtons.OK, MessageBoxIcon.Warning)));
            }
            else
            {
                MessageBox.Show(this, message, "Warning", MessageBoxButtons.OK, MessageBoxIcon.Warning);
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

        #endregion

        #region Windows Form Designer generated code (Rewritten for Enhanced UI)
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
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
            this.toolStripFileActions = new System.Windows.Forms.ToolStrip();
            this.btnAddFile = new System.Windows.Forms.ToolStripButton();
            this.btnRemoveFile = new System.Windows.Forms.ToolStripButton();
            this.btnClearFiles = new System.Windows.Forms.ToolStripButton();
            this.splitContainerResults = new System.Windows.Forms.SplitContainer();
            this.grpExtractedTables = new System.Windows.Forms.GroupBox();
            this.lblFilter = new System.Windows.Forms.Label();
            this.txtTableFilter = new System.Windows.Forms.TextBox();
            this.lstTables = new System.Windows.Forms.ListBox();
            this.grpActivityLog = new System.Windows.Forms.GroupBox();
            this.txtActivityLog = new System.Windows.Forms.TextBox();
            this.panelExportActions = new System.Windows.Forms.Panel();
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
            this.toolStripStatusLabel.Size = new System.Drawing.Size(767, 17);
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
            this.colFilePath});
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
            this.colFilePath.Width = 200;
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
            this.btnAddFile.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.btnAddFile.Name = "btnAddFile";
            this.btnAddFile.Size = new System.Drawing.Size(58, 22);
            this.btnAddFile.Text = "Add Files";
            this.btnAddFile.ToolTipText = "Add XER files (Ctrl+O)";
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
            this.grpExtractedTables.Text = "Extracted Tables";
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
            // lstTables
            //
            this.lstTables.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom)
            | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.lstTables.FormattingEnabled = true;
            this.lstTables.IntegralHeight = false;
            this.lstTables.Location = new System.Drawing.Point(3, 45);
            this.lstTables.Name = "lstTables";
            this.lstTables.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended;
            this.lstTables.Size = new System.Drawing.Size(438, 182);
            this.lstTables.Sorted = true;
            this.lstTables.TabIndex = 0;
            this.lstTables.SelectedIndexChanged += new System.EventHandler(this.LstTables_SelectedIndexChanged);
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
            this.panelExportActions.Controls.Add(this.btnExportSelected);
            this.panelExportActions.Controls.Add(this.btnExportAll);
            this.panelExportActions.Location = new System.Drawing.Point(8, 467);
            this.panelExportActions.Name = "panelExportActions";
            this.panelExportActions.Size = new System.Drawing.Size(444, 40);
            this.panelExportActions.TabIndex = 2;
            //
            // btnExportSelected
            //
            this.btnExportSelected.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnExportSelected.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnExportSelected.Location = new System.Drawing.Point(264, 0);
            this.btnExportSelected.Name = "btnExportSelected";
            this.btnExportSelected.Size = new System.Drawing.Size(180, 40);
            this.btnExportSelected.TabIndex = 1;
            this.btnExportSelected.Text = "Export Selected (+ Power BI)";
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
            this.btnExportAll.Text = "Export All";
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
            this.grpPowerBI.Text = "Power BI Enhanced Tables";
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
            this.Text = "Primavera P6 XER to CSV Converter v2.1";
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
        #endregion

    }

    #endregion

    // -----------------------------------------------------------------------------------------------------------------
    // Program Entry Point
    // -----------------------------------------------------------------------------------------------------------------
    #region Program Entry Point

    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            // UI Enhancement: Add High DPI support (Windows Vista+)
            if (Environment.OSVersion.Version.Major >= 6)
            {
                SetProcessDPIAware();
            }

            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new MainForm());
        }

        // P/Invoke for DPI awareness
        [System.Runtime.InteropServices.DllImport("user32.dll")]
        private static extern bool SetProcessDPIAware();
    }
    #endregion
}