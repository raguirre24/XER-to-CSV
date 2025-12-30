using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Drawing;
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
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            // LatencyMode is managed by the MainForm: Batch during processing, Interactive when idle.
        }
    }

    internal sealed class UserSettings
    {
        public string LastOutputPath { get; set; } = string.Empty;
        public bool GeneratePowerBI { get; set; }
    }

    // .NET 8: JSON Source Generator for AOT-friendly, reflection-free serialization
    [JsonSerializable(typeof(UserSettings))]
    [JsonSourceGenerationOptions(WriteIndented = true)]
    internal partial class UserSettingsJsonContext : JsonSerializerContext { }

    internal static class UserSettingsStore
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


    internal static class TableNames { public const string Task = "TASK"; public const string Calendar = "CALENDAR"; public const string Project = "PROJECT"; public const string ProjWbs = "PROJWBS"; public const string TaskActv = "TASKACTV"; public const string ActvCode = "ACTVCODE"; public const string ActvType = "ACTVTYPE"; public const string TaskPred = "TASKPRED"; public const string Rsrc = "RSRC"; public const string TaskRsrc = "TASKRSRC"; public const string Umeasure = "UMEASURE"; }



    internal static class FieldNames

    {

        // Common Fields

        public const string TaskId = "task_id"; public const string ProjectId = "proj_id"; public const string WbsId = "wbs_id"; public const string CalendarId = "clndr_id"; public const string TaskType = "task_type"; public const string StatusCode = "status_code"; public const string TaskCode = "task_code"; public const string TaskName = "task_name"; public const string RsrcId = "rsrc_id"; public const string ActStartDate = "act_start_date"; public const string ActEndDate = "act_end_date"; public const string EarlyStartDate = "early_start_date"; public const string EarlyEndDate = "early_end_date"; public const string LateEndDate = "late_end_date"; public const string LateStartDate = "late_start_date"; public const string TargetStartDate = "target_start_date"; public const string TargetEndDate = "target_end_date"; public const string CstrType = "cstr_type"; public const string CstrDate = "cstr_date"; public const string PriorityType = "priority_type"; public const string FloatPath = "float_path"; public const string FloatPathOrder = "float_path_order"; public const string DrivingPathFlag = "driving_path_flag"; public const string RemainDurationHrCnt = "remain_drtn_hr_cnt"; public const string TotalFloatHrCnt = "total_float_hr_cnt"; public const string FreeFloatHrCnt = "free_float_hr_cnt"; public const string CompletePctType = "complete_pct_type"; public const string PhysCompletePct = "phys_complete_pct"; public const string ActWorkQty = "act_work_qty"; public const string RemainWorkQty = "remain_work_qty"; public const string TargetDurationHrCnt = "target_drtn_hr_cnt"; public const string ClndrId = "clndr_id"; public const string DayHourCount = "day_hr_cnt"; public const string LastRecalcDate = "last_recalc_date";

        public const string ParentWbsId = "parent_wbs_id";

        public const string ActvCodeTypeId = "actv_code_type_id"; public const string ActvCodeId = "actv_code_id"; public const string FileName = "FileName"; public const string Start = "Start"; public const string Finish = "Finish"; public const string IdName = "ID_Name"; public const string RemainingWorkingDays = "Remaining Working Days"; public const string OriginalDuration = "Original Duration"; public const string TotalFloat = "Total Float"; public const string FreeFloat = "Free Float"; public const string PercentComplete = "%"; public const string DataDate = "Data Date"; public const string WbsIdKey = "wbs_id_key"; public const string TaskIdKey = "task_id_key"; public const string ParentWbsIdKey = "parent_wbs_id_key"; public const string CalendarIdKey = "calendar_id_key"; public const string ProjIdKey = "proj_id_key"; public const string ActvCodeIdKey = "actv_code_id_key"; public const string ActvCodeTypeIdKey = "actv_code_type_id_key"; public const string ClndrIdKey = "clndr_id_key"; public const string PredTaskId = "pred_task_id"; public const string PredTaskIdKey = "pred_task_id_key"; public const string CalendarName = "clndr_name"; public const string CalendarData = "clndr_data"; public const string CalendarType = "clndr_type";

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

    internal static class EnhancedTableNames

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

    public class WorkingDayCalculator

    {

        // --- Data Structures ---

        // These are pre-populated by the XerTransformer

        private readonly Dictionary<DateTime, decimal> _exceptionHours;

        private readonly Dictionary<DateTime, List<(TimeSpan Start, TimeSpan End)>> _exceptionTimeSlots;

        private readonly decimal[] _standardWeekHours; // Index 0=Sunday, 1=Monday... 6=Saturday

        private readonly List<(TimeSpan Start, TimeSpan End)>[] _standardWeekTimeSlots; // Index 0=Sunday...

        private static readonly WorkingDayCalculator _default = CreateDefaultCalendar();



        public WorkingDayCalculator(

            Dictionary<DateTime, decimal> exceptionHours,

            Dictionary<DateTime, List<(TimeSpan Start, TimeSpan End)>> exceptionTimeSlots,

            decimal[] standardWeekHours,

            List<(TimeSpan Start, TimeSpan End)>[] standardWeekTimeSlots)

        {

            _exceptionHours = exceptionHours ?? new Dictionary<DateTime, decimal>();

            _exceptionTimeSlots = exceptionTimeSlots ?? new Dictionary<DateTime, List<(TimeSpan, TimeSpan)>>();

            _standardWeekHours = standardWeekHours ?? new decimal[7];

            _standardWeekTimeSlots = standardWeekTimeSlots ?? new List<(TimeSpan, TimeSpan)>[7];

        }



        // Creates a default 5-day work week calendar (Mon-Fri, 8-12, 13-17)

        private static WorkingDayCalculator CreateDefaultCalendar()

        {

            var stdHours = new decimal[7]; // 0=Sun, 6=Sat

            var stdSlots = new List<(TimeSpan, TimeSpan)>[7];

            var defaultSlots = new List<(TimeSpan, TimeSpan)>

            {

                (new TimeSpan(8, 0, 0), new TimeSpan(12, 0, 0)),

                (new TimeSpan(13, 0, 0), new TimeSpan(17, 0, 0))

            };



            for (int i = 0; i < 7; i++)

            {

                stdSlots[i] = new List<(TimeSpan, TimeSpan)>(); // Initialize all lists

            }



            for (int i = 1; i <= 5; i++) // 1=Monday to 5=Friday

            {

                stdHours[i] = 8m;

                stdSlots[i] = defaultSlots;

            }

            // Sunday (0) and Saturday (6)

            stdHours[0] = 0m;

            stdHours[6] = 0m;



            return new WorkingDayCalculator(

                new Dictionary<DateTime, decimal>(),

                new Dictionary<DateTime, List<(TimeSpan, TimeSpan)>>(),

                stdHours,

                stdSlots);

        }



        public static WorkingDayCalculator Default => _default;



        // --- Helper Methods ---



        /// <summary>

        /// Gets the defined work hours for a specific date, checking exceptions first.

        /// </summary>

        private decimal GetWorkHours(DateTime date)

        {

            date = date.Date;

            if (_exceptionHours.TryGetValue(date, out decimal hours))

            {

                return hours; // Return specific exception hours (could be 0)

            }

            return _standardWeekHours[(int)date.DayOfWeek]; // Return standard week hours

        }



        /// <summary>

        /// Gets the list of defined time slots for a specific date.

        /// </summary>

        private List<(TimeSpan Start, TimeSpan End)> GetTimeSlots(DateTime date)

        {

            date = date.Date;

            if (_exceptionTimeSlots.TryGetValue(date, out var slots))

            {

                return slots; // Return specific exception slots

            }

            var stdSlots = _standardWeekTimeSlots[(int)date.DayOfWeek];

            return stdSlots ?? new List<(TimeSpan, TimeSpan)>(); // Ensure list is never null

        }



        /// <summary>

        /// Checks if a date is a working day (has > 0 work hours).

        /// </summary>

        public bool IsWorkingDay(DateTime date)

        {

            return GetWorkHours(date.Date) > 0;

        }



        /// <summary>

        /// Calculates remaining work hours on a given day from a specific time.

        /// </summary>

        private decimal GetRemainingHoursOnDay(DateTime startDateTime)

        {

            var slots = GetTimeSlots(startDateTime.Date);

            if (slots == null || slots.Count == 0)

            {

                return 0m;

            }



            TimeSpan startTime = startDateTime.TimeOfDay;

            decimal remainingHours = 0m;



            foreach (var (slotStart, slotEnd) in slots)

            {

                if (startTime < slotEnd) // If the time is before the end of the slot

                {

                    // Find the effective start time (either the slot start or the current time, whichever is later)

                    TimeSpan effectiveStart = (startTime > slotStart) ? startTime : slotStart;

                    remainingHours += (decimal)(slotEnd - effectiveStart).TotalHours;

                }

            }

            return Math.Max(0, remainingHours);

        }



        /// <summary>

        /// Calculates hours worked on a given day up to a specific time.

        /// </summary>

        private decimal GetHoursWorkedOnDay(DateTime endDateTime)

        {

            var slots = GetTimeSlots(endDateTime.Date);

            if (slots == null || slots.Count == 0)

            {

                return 0m;

            }



            TimeSpan endTime = endDateTime.TimeOfDay;

            decimal hoursWorked = 0m;



            foreach (var (slotStart, slotEnd) in slots)

            {

                if (endTime > slotStart) // If the time is after the start of the slot

                {

                    // Find the effective end time (either the slot end or the current time, whichever is earlier)

                    TimeSpan effectiveEnd = (endTime < slotEnd) ? endTime : slotEnd;

                    hoursWorked += (decimal)(effectiveEnd - slotStart).TotalHours;

                }

            }

            return Math.Max(0, hoursWorked);

        }



        /// <summary>

        /// Finds the exact time on a given day when a certain amount of work has been completed.

        /// </summary>

        private TimeSpan FindTimeForHoursWorked(DateTime date, decimal hoursWorked)

        {

            var slots = GetTimeSlots(date.Date);

            if (slots == null || slots.Count == 0)

            {

                return TimeSpan.Zero;

            }



            decimal hoursAccumulated = 0m;



            foreach (var (slotStart, slotEnd) in slots)

            {

                decimal slotDuration = (decimal)(slotEnd - slotStart).TotalHours;

                if (hoursAccumulated + slotDuration >= hoursWorked)

                {

                    // The time is in this slot

                    decimal hoursNeededInSlot = hoursWorked - hoursAccumulated;

                    return slotStart + TimeSpan.FromHours((double)hoursNeededInSlot);

                }

                // Time is after this slot, add this slot's full duration

                hoursAccumulated += slotDuration;

            }

            // If hoursWorked > total work hours, return end of last slot

            return slots.LastOrDefault().End;

        }



        // --- NEW CALCULATION METHODS (WITH NEGATIVE LAG FIX) ---



        /// <summary>

        /// Projects a date forward or backward by a specific number of working hours.

        /// </summary>

        public DateTime AddWorkingHours(DateTime startDate, decimal lagHours)

        {

            if (lagHours == 0) return startDate;



            if (lagHours > 0)

            {

                return ProjectForward(startDate, lagHours);

            }

            else

            {

                // Call new method for projecting backward

                return ProjectBackward(startDate, -lagHours); // Pass a positive duration

            }

        }



        private DateTime ProjectForward(DateTime startDate, decimal hoursToAdd)

        {

            DateTime currentDate = startDate;

            decimal hoursRemaining = hoursToAdd;



            // 1. Spend remaining hours on the start day

            decimal remainingDayHours = GetRemainingHoursOnDay(currentDate);

            if (remainingDayHours > hoursRemaining)

            {

                // Lag finishes on the same day.

                // We need to find the exact finish time.

                var slots = GetTimeSlots(currentDate.Date);

                TimeSpan currentTime = currentDate.TimeOfDay;

                foreach (var (slotStart, slotEnd) in slots)

                {

                    if (currentTime < slotEnd)

                    {

                        TimeSpan effectiveStart = (currentTime > slotStart) ? currentTime : slotStart;

                        decimal slotDuration = (decimal)(slotEnd - effectiveStart).TotalHours;



                        if (hoursRemaining <= slotDuration)

                        {

                            // Finishes in this slot

                            return currentDate.Date + effectiveStart + TimeSpan.FromHours((double)hoursRemaining);

                        }

                        // Finishes after this slot, consume this slot's hours

                        hoursRemaining -= slotDuration;

                        currentTime = slotEnd; // Move time to end of this slot

                    }

                }

                // Should not be reachable if GetRemainingHoursOnDay was correct

                return currentDate.Date.AddDays(1);

            }



            // Lag does not finish on the start day

            hoursRemaining -= remainingDayHours;

            currentDate = currentDate.Date.AddDays(1); // Move to start of next day



            // 2. Spend hours on full days

            while (true)

            {

                decimal dayHours = GetWorkHours(currentDate);

                if (dayHours > 0)

                {

                    if (hoursRemaining <= dayHours)

                    {

                        // Lag finishes on this day

                        break;

                    }

                    // Consume the full day and move to the next

                    hoursRemaining -= dayHours;

                }

                currentDate = currentDate.AddDays(1);

            }



            // 3. Find the exact finish time on the final day

            var finalDaySlots = GetTimeSlots(currentDate.Date);

            if (finalDaySlots == null || finalDaySlots.Count == 0)

            {

                // This case should be impossible if GetWorkHours(currentDate) > 0

                return currentDate;

            }



            // This is just `GetHoursWorkedOnDay` in reverse, which is `FindTimeForHoursWorked`

            TimeSpan finalTime = FindTimeForHoursWorked(currentDate, hoursRemaining);

            return currentDate.Date + finalTime;

        }



        private DateTime ProjectBackward(DateTime startDate, decimal hoursToSubtract)

        {

            DateTime currentDate = startDate;

            decimal hoursRemainingToSubtract = hoursToSubtract;



            // 1. "Un-spend" hours on the start day

            decimal hoursWorkedToday = GetHoursWorkedOnDay(currentDate);



            if (hoursWorkedToday > hoursRemainingToSubtract)

            {

                // Lag finishes (starts) on the same day.

                decimal targetHoursWorked = hoursWorkedToday - hoursRemainingToSubtract;

                TimeSpan finalTime = FindTimeForHoursWorked(currentDate, targetHoursWorked);

                return currentDate.Date + finalTime;

            }



            // Lag does not finish on the start day

            hoursRemainingToSubtract -= hoursWorkedToday;

            currentDate = currentDate.Date.AddDays(-1); // Move to previous day



            // 2. "Un-spend" hours on full days

            while (true)

            {

                decimal dayHours = GetWorkHours(currentDate);

                if (dayHours > 0)

                {

                    if (hoursRemainingToSubtract <= dayHours)

                    {

                        // Lag finishes (starts) on this day

                        break;

                    }

                    // Consume the full day and move to the previous

                    hoursRemainingToSubtract -= dayHours;

                }

                currentDate = currentDate.AddDays(-1);

            }



            // 3. Find the exact start time on the final day

            decimal hoursToWorkOnFinalDay = GetWorkHours(currentDate) - hoursRemainingToSubtract;

            TimeSpan finalTimeOnDay = FindTimeForHoursWorked(currentDate, hoursToWorkOnFinalDay);

            return currentDate.Date + finalTimeOnDay;

        }



        /// <summary>

        /// Counts the precise working hours between two DateTimes.

        /// </summary>

        public decimal CountWorkingHours(DateTime startDate, DateTime endDate)

        {

            if (Math.Abs((startDate - endDate).TotalSeconds) < 1) return 0m;



            // Handle reverse

            if (endDate < startDate)

            {

                return -CountWorkingHours(endDate, startDate);

            }



            decimal totalHours = 0m;

            DateTime currentDate = startDate.Date;



            // --- 1. Handle Start Day (Partial Day) ---

            if (currentDate == endDate.Date)

            {

                // Start and End are on the same day

                var slots = GetTimeSlots(currentDate);

                if (slots == null) return 0m;



                TimeSpan startTime = startDate.TimeOfDay;

                TimeSpan endTime = endDate.TimeOfDay;



                foreach (var (slotStart, slotEnd) in slots)

                {

                    TimeSpan effectiveStart = (startTime > slotStart) ? startTime : slotStart;

                    TimeSpan effectiveEnd = (endTime < slotEnd) ? endTime : slotEnd;



                    if (effectiveEnd > effectiveStart)

                    {

                        totalHours += (decimal)(effectiveEnd - effectiveStart).TotalHours;

                    }

                }

                return totalHours;

            }



            // Start and End are on different days

            totalHours += GetRemainingHoursOnDay(startDate);

            currentDate = currentDate.AddDays(1);



            // --- 2. Handle Full Days Between ---

            while (currentDate < endDate.Date)

            {

                totalHours += GetWorkHours(currentDate);

                currentDate = currentDate.AddDays(1);

            }



            // --- 3. Handle End Day (Partial Day) ---

            totalHours += GetHoursWorkedOnDay(endDate);



            return totalHours;

        }

    }



    // Lightweight struct for data rows
    public readonly record struct DataRow(string[] Fields, string SourceFilename);


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
            return new DataRow(newValues, row.SourceFilename);
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


        // Merges data from another store (used during parallel parsing)

        public void MergeStore(XerDataStore otherStore)

        {

            foreach (var kvp in otherStore._tables)
            {
                if (_tables.TryGetValue(kvp.Key, out XerTable? existingTable) && existingTable != null)
                {
                    var incomingTable = kvp.Value;
                    if (existingTable.Headers == null)
                    {
                        if (incomingTable.Headers != null)
                        {
                            existingTable.SetHeaders(incomingTable.Headers);
                        }

                        existingTable.AddRows(incomingTable.Rows);
                        continue;
                    }

                    if (incomingTable.Headers == null)
                    {
                        existingTable.AddRows(incomingTable.Rows);
                        continue;
                    }

                    if (HeadersMatch(existingTable.Headers, incomingTable.Headers))
                    {
                        existingTable.AddRows(incomingTable.Rows);
                    }
                    else
                    {
                        MergeRowsByHeader(existingTable, incomingTable);
                    }
                }
                else
                {
                    _tables.Add(kvp.Key, kvp.Value);
                }
            }

        }

        private static bool HeadersMatch(string[] existingHeaders, string[] incomingHeaders)
        {
            if (existingHeaders.Length != incomingHeaders.Length)
            {
                return false;
            }

            for (int i = 0; i < existingHeaders.Length; i++)
            {
                if (!string.Equals(existingHeaders[i], incomingHeaders[i], StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }

            return true;
        }

        private static void MergeRowsByHeader(XerTable targetTable, XerTable sourceTable)
        {
            if (targetTable.Headers == null || sourceTable.Headers == null)
            {
                targetTable.AddRows(sourceTable.Rows);
                return;
            }

            var targetIndexes = targetTable.FieldIndexes;
            string[] sourceHeaders = sourceTable.Headers;
            int targetLength = targetTable.Headers.Length;

            foreach (var row in sourceTable.Rows)
            {
                string[] merged = new string[targetLength];
                Array.Fill(merged, string.Empty);

                string[] sourceFields = row.Fields;
                int copyLimit = Math.Min(sourceHeaders.Length, sourceFields.Length);

                for (int i = 0; i < copyLimit; i++)
                {
                    string header = sourceHeaders[i];
                    if (string.IsNullOrEmpty(header))
                    {
                        continue;
                    }

                    if (targetIndexes.TryGetValue(header, out int targetIndex))
                    {
                        merged[targetIndex] = sourceFields[i] ?? string.Empty;
                    }
                }

                targetTable.AddRow(new DataRow(merged, row.SourceFilename));
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



        // PERFORMANCE/UX OPTIMIZATION: Added CancellationToken, Optimized Encoding and Progress Reporting

        public XerDataStore ParseXerFile(string xerFilePath, Action<int, string>? reportProgressAction, CancellationToken cancellationToken)
        {

            var fileStore = new XerDataStore();

            string filename = StringInternPool.Intern(Path.GetFileName(xerFilePath));

            int bufferSize = PerformanceConfig.FileReadBufferSize;

            int progressReportInterval = PerformanceConfig.ProgressReportIntervalLines;



            var localTables = new Dictionary<string, XerTable>(StringComparer.OrdinalIgnoreCase);
            XerTable? currentTable = null;
            int lineCount = 0;

            long bytesRead = 0;



            // Default to UTF8, generally safer and more common than Encoding.Default (ANSI)

            Encoding encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
            bool retryWithDefaultEncoding = false;

            while (true)
            {
                fileStore = new XerDataStore();
                localTables = new Dictionary<string, XerTable>(StringComparer.OrdinalIgnoreCase);
                currentTable = null;
                lineCount = 0;
                bytesRead = 0;



            try

            {

                var fileInfo = new FileInfo(xerFilePath);

                long fileSize = fileInfo.Length;

                if (fileSize == 0) return fileStore;



                int lastReportedProgress = 0;



                // Use FileStreamOptions for optimized sequential reading of large files
                var fileOptions = new FileStreamOptions
                {
                    Mode = FileMode.Open,
                    Access = FileAccess.Read,
                    Share = FileShare.Read,
                    BufferSize = bufferSize,
                    Options = FileOptions.SequentialScan
                };
                using var fileStream = new FileStream(xerFilePath, fileOptions);
                // Enable BOM detection, fallback to defined encoding (UTF8)
                using var reader = new StreamReader(fileStream, encoding, detectEncodingFromByteOrderMarks: true, bufferSize);
                {
                    string? line;



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

                        ReadOnlySpan<char> lineSpan = line.AsSpan();
                        // XER format lines start with %T (Table), %F (Fields), or %R (Row)
                        if (lineSpan.Length < 2 || lineSpan[0] != '%') continue;

                        char typeChar = lineSpan[1];


                        switch (typeChar)

                        {

                            case 'T': // Table Definition
                                ReadOnlySpan<char> tableNameSpan = lineSpan[2..].Trim();
                                if (!tableNameSpan.IsEmpty)
                                {
                                    string currentTableName = StringInternPool.Intern(tableNameSpan);
                                    currentTable = new XerTable(currentTableName);
                                    localTables[currentTable.Name] = currentTable;
                                }
                                break;


                            case 'F': // Field Definitions (Headers)
                                if (currentTable != null)
                                {
                                    ReadOnlySpan<char> fieldsLine = lineSpan[2..];
                                    // Handle potential leading delimiter
                                    if (!fieldsLine.IsEmpty && fieldsLine[0] == Delimiter) fieldsLine = fieldsLine[1..];
                                    string[] headers = FastSplitAndIntern(fieldsLine, Delimiter, trim: true);
                                    currentTable.SetHeaders(headers);
                                }
                                break;


                            case 'R': // Row Data
                                if (currentTable != null && currentTable.Headers is not null)
                                {
                                    ReadOnlySpan<char> dataLine = lineSpan[2..];
                                    if (!dataLine.IsEmpty && dataLine[0] == Delimiter) dataLine = dataLine[1..];
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

            catch (DecoderFallbackException)
            {
                if (!retryWithDefaultEncoding)
                {
                    encoding = Encoding.Default;
                    retryWithDefaultEncoding = true;
                    continue;
                }

                throw;
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



                    // PERFORMANCE: Write StringBuilder directly to avoid ToString() allocation
                    writer.Write(lineBuilder);
                    writer.WriteLine();

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



                        // PERFORMANCE: Write StringBuilder directly to TextWriter without intermediate string allocation.
                        // TextWriter.Write(StringBuilder) iterates through StringBuilder chunks and writes directly,
                        // avoiding the heap allocation that ToString() would cause for every row.
                        writer.Write(lineBuilder);
                        writer.WriteLine();

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



        // Efficient CSV field escaping using .NET 8 SearchValues for SIMD-accelerated character search

        private static string EscapeCsvField(string field)

        {

            if (string.IsNullOrEmpty(field)) return "";


            // .NET 8: Use SearchValues with SIMD vectorization for fast special character detection
            ReadOnlySpan<char> fieldSpan = field.AsSpan();
            int specialIndex = fieldSpan.IndexOfAny(CsvSpecialChars);

            // Check for leading/trailing whitespace
            bool hasLeadingTrailingSpace = fieldSpan[0] == ' ' || fieldSpan[^1] == ' ';

            if (specialIndex < 0 && !hasLeadingTrailingSpace) return field;

            // Check if field contains quotes (need to escape them)
            bool hasQuotes = fieldSpan.Contains('"');

            // Escape internal quotes and wrap the field

            if (hasQuotes)

            {

                // Estimate capacity to avoid resizing

                var sb = new StringBuilder(field.Length + 10);

                sb.Append('"');

                foreach (char c in fieldSpan)

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

    public partial class XerTransformer
    {
        private readonly XerDataStore _dataStore;

        // .NET 8: FrozenSet for O(1) lookups on static field sets (faster than HashSet for reads)
        private static readonly FrozenSet<string> HandledTaskFields = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
            FieldNames.StatusCode, FieldNames.Start, FieldNames.Finish, FieldNames.IdName,
            FieldNames.RemainingWorkingDays, FieldNames.OriginalDuration, FieldNames.TotalFloat, FieldNames.FreeFloat,
            FieldNames.PercentComplete, FieldNames.DataDate,
            FieldNames.ActStartDate, FieldNames.ActEndDate,
            FieldNames.EarlyStartDate, FieldNames.EarlyEndDate,
            FieldNames.LateStartDate, FieldNames.LateEndDate,
            FieldNames.CstrDate,
            FieldNames.TargetStartDate, FieldNames.TargetEndDate,
            FieldNames.WbsIdKey, FieldNames.TaskIdKey, FieldNames.CalendarIdKey, FieldNames.ProjIdKey,
            FieldNames.MonthUpdate
        }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

        // Generated Regex for optimized pattern matching in calendar parsing
        [GeneratedRegex("""\(0\|\|(\d+)\(d\|(\d+)\)\s*\((.*?)\)\s*\)""", RegexOptions.Singleline)]
        private static partial Regex ExcPatternRegex();

        [GeneratedRegex("""\(0\|\|(\d+)\(d\|(\d+)\|0\)\s*\((.*?)\)\s*\)""", RegexOptions.Singleline)]
        private static partial Regex ExcPatternRegex2();

        [GeneratedRegex("""(\|\|)\s+""", RegexOptions.Singleline)]
        private static partial Regex CleanRegex1();

        [GeneratedRegex("""\s+(\|\|)""", RegexOptions.Singleline)]
        private static partial Regex CleanRegex2();

        // Captures day number (Group 1) and content (Group 2). Handles nested parentheses in content.
        [GeneratedRegex("""\(0\|\|(\d)\(\)\s*\(((?:[^()]|\((?:[^()]|\([^()]*\))*\))*)\)\s*\)""", RegexOptions.Singleline)]
        private static partial Regex DayPatternRegex();

        [GeneratedRegex("""\(0\|\|(\d)\(\)\s*\(\s*\)\s*\)""", RegexOptions.Singleline)]
        private static partial Regex EmptyDayPatternRegex();

        // Regex to extract YYMM from filename for MonthUpdate column
        [GeneratedRegex("""^(\d{4})""", RegexOptions.Singleline)]
        private static partial Regex MonthUpdateRegex();

        // .NET 8: GeneratedRegex for time slot parsing in calendar work hours
        // Pattern 1: (0||N (s|HH:MM|f|HH:MM) ()) - Most common format
        [GeneratedRegex(@"\(0\|\|\d+\s*\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)\s*\(\s*\)\s*\)", RegexOptions.Singleline)]
        private static partial Regex TimeSlotPattern1Regex();

        // Pattern 2: (s|HH:MM|f|HH:MM) - Alternative format
        [GeneratedRegex(@"\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)", RegexOptions.Singleline)]
        private static partial Regex TimeSlotPattern2Regex();

        // Pattern 3: s|HH:MM|f|HH:MM (Simplified format)
        [GeneratedRegex(@"([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})", RegexOptions.Singleline)]
        private static partial Regex TimeSlotPattern3Regex();

        // Regex for collapsing multiple whitespace characters
        [GeneratedRegex(@"\s{2,}", RegexOptions.Singleline)]
        private static partial Regex MultiSpaceRegex();


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

                finalTable.SetHeaders(finalColumns.Select(s => StringInternPool.Intern(s) ?? string.Empty).ToArray());



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



        private Dictionary<string, decimal> BuildCalendarHoursLookup(XerTable? calendarTable)

        {

            var lookup = new Dictionary<string, decimal>(calendarTable?.RowCount ?? 0, StringComparer.OrdinalIgnoreCase);

            if (!IsTableValid(calendarTable)) return lookup;

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



        private Dictionary<string, DateTime> BuildProjectDataDatesLookup(XerTable? projectTable)

        {

            var lookup = new Dictionary<string, DateTime>(projectTable?.RowCount ?? 0, StringComparer.OrdinalIgnoreCase);

            if (!IsTableValid(projectTable)) return lookup;

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



        private Dictionary<string, string> BuildProjectScheduleOptionsLookup(XerTable? schedOptionsTable)

        {

            var lookup = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            if (!IsTableValid(schedOptionsTable)) return lookup;



            var indexes = schedOptionsTable.FieldIndexes;

            const string lagSettingCol = "sched_calendar_on_relationship_lag";



            if (!indexes.ContainsKey(FieldNames.ProjectId) || !indexes.ContainsKey(lagSettingCol))

            {

                // Log or notify that the required column is missing

                Console.WriteLine($"Warning: SCHEDOPTIONS table is missing '{FieldNames.ProjectId}' or '{lagSettingCol}'. Defaulting to Predecessor calendar for lag.");

                return lookup;

            }



            foreach (var rowData in schedOptionsTable.Rows)

            {

                var row = rowData.Fields;

                string projId = GetFieldValue(row, indexes, FieldNames.ProjectId);

                string lagSetting = GetFieldValue(row, indexes, lagSettingCol); // e.g., "rcal_Predecessor"



                if (!string.IsNullOrEmpty(projId))

                {

                    string compositeKey = CreateKey(rowData.SourceFilename, projId);

                    lookup[compositeKey] = lagSetting;

                }

            }

            return lookup;

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

        public XerTable? Create03XerProjWbsTable()
        {

            var projwbsTable = _dataStore.GetTable(TableNames.ProjWbs);

            if (!IsTableValid(projwbsTable)) return null;



            try

            {

                if (projwbsTable.Headers is not { } sourceHeaders)
                {
                    return null;
                }

                var idx = projwbsTable.FieldIndexes;
                var finalHeadersList = sourceHeaders.ToList();

                finalHeadersList.Add(FieldNames.WbsIdKey);

                finalHeadersList.Add(FieldNames.ParentWbsIdKey);

                finalHeadersList.Add(FieldNames.MonthUpdate);

                string[] finalHeaders = finalHeadersList.Select(s => StringInternPool.Intern(s) ?? string.Empty).ToArray();



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

            // BuildCalendarCalculators now reads from the raw CALENDAR table.



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

                var calendarCalculators = BuildCalendarCalculators(null); // Pass null, it now reads from raw CALENDAR

                var schedOptionsLookup = BuildProjectScheduleOptionsLookup(schedOptionsTable);



                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };

                var transformedRowsBag = new ConcurrentBag<DataRow>();



                Parallel.ForEach(taskPredTable.Rows, parallelOptions, sourceRow =>

                {

                    var row = sourceRow.Fields;

                    string[] transformed = new string[finalHeaders.Length];

                    string originalFilename = sourceRow.SourceFilename;



                    // Copy existing fields

                    int copyLength = Math.Min(row.Length, sourceHeaders.Length);

                    Array.Copy(row, transformed, copyLength);



                    // Generate keys

                    string taskId = GetFieldValue(row, sourceIndexes, FieldNames.TaskId);

                    string predTaskId = GetFieldValue(row, sourceIndexes, FieldNames.PredTaskId);

                    string taskIdKey = CreateKey(originalFilename, taskId);

                    string predTaskIdKey = CreateKey(originalFilename, predTaskId);



                    SetTransformedField(transformed, finalIndexes, FieldNames.TaskIdKey, taskIdKey);

                    SetTransformedField(transformed, finalIndexes, FieldNames.PredTaskIdKey, predTaskIdKey);



                    // Lookup successor task data

                    TaskData succTask = taskLookup.TryGetValue(taskIdKey, out var st) ? st : new TaskData();

                    // Lookup predecessor task data

                    TaskData predTask = taskLookup.TryGetValue(predTaskIdKey, out var pt) ? pt : new TaskData();



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

                    SetTransformedField(transformed, finalIndexes, FieldNames.Start, DateParser.Format(succTask.EarlyStartDate));

                    SetTransformedField(transformed, finalIndexes, FieldNames.Finish, DateParser.Format(succTask.EarlyEndDate));

                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorStart, DateParser.Format(predTask.EarlyStartDate));

                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorFinish, DateParser.Format(predTask.EarlyEndDate));



                    // Calculate TimePeriod (hours per day) using PREDECESSOR's calendar

                    decimal hoursPerDay = 0;

                    if (!string.IsNullOrEmpty(predClndrIdKey) && calendarHoursLookup.TryGetValue(predClndrIdKey, out decimal hpd))

                    {

                        hoursPerDay = hpd;

                    }

                    SetTransformedField(transformed, finalIndexes, FieldNames.TimePeriodHoursPerDay, hoursPerDay > 0 ? hoursPerDay.ToString("F2", CultureInfo.InvariantCulture) : "");



                    // Get Lag in HOURS

                    string lagHrCntStr = GetFieldValue(row, sourceIndexes, FieldNames.LagHrCnt);

                    decimal.TryParse(lagHrCntStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal lagHours);



                    // Calculate Lag in DAYS for the 'Lag' column

                    decimal lagDays = 0;

                    if (lagHours != 0 && hoursPerDay > 0)

                    {

                        lagDays = lagHours / hoursPerDay;

                    }

                    SetTransformedField(transformed, finalIndexes, FieldNames.Lag, lagDays != 0 ? lagDays.ToString("F2", CultureInfo.InvariantCulture) : "0");



                    // --- NEW: Get Successor's HPD for final conversion ---

                    decimal hoursPerDayForSuccessor = 0;

                    if (calendarHoursLookup.TryGetValue(succClndrIdKey, out decimal succHpd))

                    {

                        hoursPerDayForSuccessor = succHpd;

                    }

                    if (hoursPerDayForSuccessor <= 0) hoursPerDayForSuccessor = 8m; // Fallback





                    // Calculate Free Float (passing lag in HOURS)

                    string freeFloatInDays = CalculateFreeFloat(

                        row, sourceIndexes,

                        succTask, predTask,

                        lagHours, // Pass hours, not days

                        predClndrIdKey,

                        succClndrIdKey,

                        calendarCalculators,

                        schedOptionsLookup,

                        hoursPerDayForSuccessor // Pass successor's HPD

                    );

                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorFreeFloat, freeFloatInDays);



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

                Console.WriteLine($"Error creating {EnhancedTableNames.XerPredecessor06}: {ex.Message}");

                return null;

            }

        }



        // Helper structure for task data used in predecessor calculations

        private struct TaskData
        {
            public string? ProjIdKey;
            public string? ClndrIdKey;
            public string? StatusCode;
            public string? TaskType;
            public string? TaskCode;

            public DateTime? EarlyStartDate;

            public DateTime? EarlyEndDate;

            public DateTime? ActualStartDate;

            public DateTime? ActualEndDate;

        }



        // Builds a lookup dictionary for task data needed by predecessor table

        private Dictionary<string, TaskData> BuildTaskLookupDictionary(XerTable? taskTable)

        {

            var lookup = new Dictionary<string, TaskData>(StringComparer.OrdinalIgnoreCase);

            if (!IsTableValid(taskTable)) return lookup;



            var indexes = taskTable.FieldIndexes;

            foreach (var rowData in taskTable.Rows)

            {

                var row = rowData.Fields;

                string taskId = GetFieldValue(row, indexes, FieldNames.TaskId);

                string clndrId = GetFieldValue(row, indexes, FieldNames.CalendarId);

                string projId = GetFieldValue(row, indexes, FieldNames.ProjectId); // Get ProjId



                string taskIdKey = CreateKey(rowData.SourceFilename, taskId);

                string clndrIdKey = CreateKey(rowData.SourceFilename, clndrId);

                string projIdKey = CreateKey(rowData.SourceFilename, projId); // Create ProjIdKey



                var taskData = new TaskData

                {

                    ProjIdKey = projIdKey, // Store ProjIdKey

                    ClndrIdKey = clndrIdKey,

                    StatusCode = GetFieldValue(row, indexes, FieldNames.StatusCode),

                    TaskType = GetFieldValue(row, indexes, FieldNames.TaskType),

                    TaskCode = GetFieldValue(row, indexes, FieldNames.TaskCode),

                    EarlyStartDate = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.EarlyStartDate)),

                    EarlyEndDate = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.EarlyEndDate)),

                    ActualStartDate = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.ActStartDate)),

                    ActualEndDate = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.ActEndDate))

                };



                lookup[taskIdKey] = taskData;

            }

            return lookup;

        }



        private Dictionary<string, WorkingDayCalculator> BuildCalendarCalculators(XerTable? calendarDetailedTable)

        {

            var calculators = new Dictionary<string, WorkingDayCalculator>(StringComparer.OrdinalIgnoreCase);



            // This is the main method that reads the raw CALENDAR table.

            Console.WriteLine("Building Calendar Calculators for Free Float...");

            var calendarTable = _dataStore.GetTable(TableNames.Calendar);

            if (!IsTableValid(calendarTable))

            {

                Console.WriteLine("Warning: Raw CALENDAR table not found. Free Float calculations may be incorrect.");

                return calculators;

            }



            var calIndexes = calendarTable.FieldIndexes;

            int rawClndrIdIdx = calIndexes.ContainsKey(FieldNames.ClndrId) ? calIndexes[FieldNames.ClndrId] : -1;

            int rawDataIdx = calIndexes.ContainsKey(FieldNames.CalendarData) ? calIndexes[FieldNames.CalendarData] : -1;

            int rawDayHrCntIdx = calIndexes.ContainsKey(FieldNames.DayHourCount) ? calIndexes[FieldNames.DayHourCount] : -1;



            if (rawClndrIdIdx == -1 || rawDataIdx == -1 || rawDayHrCntIdx == -1)

            {

                Console.WriteLine("Warning: Raw CALENDAR table is missing required columns. Free Float calculations may be incorrect.");

                return calculators;

            }



            // Regex for time slots (Main pattern)
            var timeSlotRegex = new Regex(
                @"\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)",
                RegexOptions.Singleline | RegexOptions.Compiled);


            // Parse the raw CALENDAR table for each calendar

            foreach (var rowData in calendarTable.Rows)

            {

                var row = rowData.Fields;

                string clndrId = XerTable.GetFieldValueSafe(rowData, rawClndrIdIdx);

                string clndrIdKey = CreateKey(rowData.SourceFilename, clndrId);

                string clndrData = XerTable.GetFieldValueSafe(rowData, rawDataIdx);

                string defaultDayHrCntStr = XerTable.GetFieldValueSafe(rowData, rawDayHrCntIdx);



                decimal.TryParse(defaultDayHrCntStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal defaultDayHours);

                if (defaultDayHours <= 0) defaultDayHours = 8m; // Fallback



                var exceptionHours = new Dictionary<DateTime, decimal>();

                var exceptionTimeSlots = new Dictionary<DateTime, List<(TimeSpan, TimeSpan)>>();

                var standardWeekHours = new decimal[7]; // 0=Sun ... 6=Sat

                var standardWeekTimeSlots = new List<(TimeSpan, TimeSpan)>[7];

                for (int i = 0; i < 7; i++) standardWeekTimeSlots[i] = new List<(TimeSpan, TimeSpan)>();



                bool hasStandardWeekDefined = false;



                // --- 1. Parse Standard Week ---

                int daysStart = clndrData.IndexOf("DaysOfWeek");

                if (daysStart > -1)

                {

                    int daysEnd = FindSectionEnd(clndrData, daysStart);

                    string daysSection = clndrData.AsSpan(daysStart, daysEnd - daysStart).ToString();


                    // Use dictionaries to store results from Regex matches

                    var dayContents = new Dictionary<int, string>();

                    var emptyDays = new HashSet<int>();



                    // Match all occurrences using the static compiled Regex (dayPatternRegex)

                    foreach (Match match in DayPatternRegex().Matches(daysSection))
                    {
                        if (int.TryParse(match.Groups[1].Value, out int p6DayNum) && p6DayNum >= 1 && p6DayNum <= 7) // P6 Day (1-7)

                        {

                            dayContents[p6DayNum] = match.Groups[2].Value.Trim();

                        }

                    }



                    // Match all occurrences using the static compiled Regex (emptyDayPatternRegex)

                    foreach (Match match in EmptyDayPatternRegex().Matches(daysSection))
                    {
                        if (int.TryParse(match.Groups[1].Value, out int p6DayNum) && p6DayNum >= 1 && p6DayNum <= 7)

                        {

                            emptyDays.Add(p6DayNum);

                            dayContents.Remove(p6DayNum); // Ensure it's not in both

                        }

                    }



                    // Only proceed if we actually found standard week definitions

                    if (dayContents.Count > 0 || emptyDays.Count > 0)

                    {

                        hasStandardWeekDefined = true;

                        // Process the results for all 7 days

                        for (int p6DayNum = 1; p6DayNum <= 7; p6DayNum++) // P6: 1=Sun, 2=Mon... 7=Sat

                        {

                            int dotNetDay = p6DayNum - 1; // .NET: 0=Sun, 1=Mon... 6=Sat



                            if (dayContents.TryGetValue(p6DayNum, out string? dayContent) && !string.IsNullOrWhiteSpace(dayContent))

                            {

                                // Day has defined work hours

                                standardWeekHours[dotNetDay] = ParseDayWorkHours(dayContent, timeSlotRegex, out var slots);

                                standardWeekTimeSlots[dotNetDay] = slots;

                            }

                            else if (emptyDays.Contains(p6DayNum))

                            {

                                // Day is explicitly defined as non-working

                                standardWeekHours[dotNetDay] = 0m;

                            }

                            else

                            {

                                // Defined in DaysOfWeek section but missing specific day content? 

                                // P6 convention: 1 (Sun) and 7 (Sat) default to non-work; others default to standard.

                                if (p6DayNum == 1 || p6DayNum == 7)

                                {

                                    standardWeekHours[dotNetDay] = 0m;

                                }

                                else

                                {

                                    // Weekday default

                                    standardWeekHours[dotNetDay] = defaultDayHours;

                                    // Add default time slots

                                    AddDefaultTimeSlots(standardWeekTimeSlots[dotNetDay], defaultDayHours);

                                }

                            }

                        }

                    }

                }



                // --- FALLBACK: If no standard week defined, use Defaults (Mon-Fri) ---

                // This fixes the issue where missing DaysOfWeek blocks resulted in 0 working days

                if (!hasStandardWeekDefined)

                {

                    for (int p6DayNum = 1; p6DayNum <= 7; p6DayNum++)

                    {

                        int dotNetDay = p6DayNum - 1;

                        if (p6DayNum == 1 || p6DayNum == 7) // Sun/Sat

                        {

                            standardWeekHours[dotNetDay] = 0m;

                        }

                        else // Mon-Fri

                        {

                            standardWeekHours[dotNetDay] = defaultDayHours;

                            AddDefaultTimeSlots(standardWeekTimeSlots[dotNetDay], defaultDayHours);

                        }

                    }

                }



                // --- 2. Parse Exceptions ---

                string[] exceptionSections = ["Exceptions", "HolidayOrExceptions", "HolidayOrException"];

                foreach (var sectionName in exceptionSections)

                {

                    int excStart = clndrData.IndexOf(sectionName);

                    if (excStart > -1)

                    {

                        int excEnd = FindSectionEnd(clndrData, excStart);

                        ReadOnlySpan<char> excSpan = excEnd > excStart
                            ? clndrData.AsSpan(excStart, excEnd - excStart)
                            : clndrData.AsSpan(excStart);
                        string excSection = excSpan.ToString();


                        var excMatches = ExcPatternRegex().Matches(excSection);
                        ParseAndStoreExceptions(excMatches, exceptionHours, exceptionTimeSlots, timeSlotRegex);

                        excMatches = ExcPatternRegex2().Matches(excSection);
                        ParseAndStoreExceptions(excMatches, exceptionHours, exceptionTimeSlots, timeSlotRegex);
                    }

                }



                // --- 3. Create the Calculator ---

                var calculator = new WorkingDayCalculator(exceptionHours, exceptionTimeSlots, standardWeekHours, standardWeekTimeSlots);

                calculators[clndrIdKey] = calculator;

            }



            Console.WriteLine($"Built {calculators.Count} hour-aware calendar calculators.");

            return calculators;

        }

        /// <summary>

        /// Helper for BuildCalendarCalculators to parse exception data and store it

        /// </summary>

        private void ParseAndStoreExceptions(MatchCollection matches,

            Dictionary<DateTime, decimal> exceptionHours,

            Dictionary<DateTime, List<(TimeSpan, TimeSpan)>> exceptionTimeSlots,

            Regex timeSlotRegex)

        {

            foreach (Match excMatch in matches)

            {

                if (excMatch.Success && excMatch.Groups.Count >= 3)

                {

                    if (int.TryParse(excMatch.Groups[2].Value, out int dateSerial))

                    {

                        DateTime excDate = ConvertFromOleDate(dateSerial).Date;

                        string workContent = excMatch.Groups.Count > 3 ? excMatch.Groups[3].Value.Trim() : "()";



                        if (string.IsNullOrEmpty(workContent) || workContent == "()")

                        {

                            exceptionHours[excDate] = 0m; // Holiday

                            exceptionTimeSlots[excDate] = new List<(TimeSpan, TimeSpan)>();

                        }

                        else

                        {

                            decimal hours = ParseDayWorkHours(workContent, timeSlotRegex, out var slots);

                            exceptionHours[excDate] = hours;

                            exceptionTimeSlots[excDate] = slots;

                        }

                    }

                }

            }

        }



        /// <summary>

        /// This is an OVERLOAD for the existing ParseDayWorkHours.

        /// It is used by the new BuildCalendarCalculators.

        /// Updated to include fallback regex patterns for robustness.

        /// </summary>

        private decimal ParseDayWorkHours(string content, Regex timeSlotRegex, out List<(TimeSpan Start, TimeSpan End)> slots)

        {

            slots = new List<(TimeSpan, TimeSpan)>();

            if (string.IsNullOrWhiteSpace(content)) return 0;



            decimal totalHours = 0;



            // 1. Try Standard Pattern (with parentheses) - e.g. (s|08:00|f|17:00)

            var matches = timeSlotRegex.Matches(content);



            if (matches.Count > 0)

            {

                foreach (Match match in matches)

                {

                    ProcessTimeSlotMatch(match, slots, ref totalHours);

                }

                return totalHours;

            }



            // 2. Try Fallback Pattern (bare, no parentheses) - e.g. s|08:00|f|17:00

            // This handles legacy/corrupted formats that the main pattern misses

            var simpleRegex = new Regex(@"([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})", RegexOptions.Singleline);

            matches = simpleRegex.Matches(content);



            if (matches.Count > 0)

            {

                var processedPairs = new HashSet<string>();

                foreach (Match match in matches)

                {

                    // Prevent duplicates if regex overlaps

                    string timeKey = $"{match.Groups[2].Value}-{match.Groups[4].Value}";

                    if (processedPairs.Contains(timeKey)) continue;

                    processedPairs.Add(timeKey);



                    ProcessTimeSlotMatch(match, slots, ref totalHours);

                }

            }



            return totalHours;

        }



        private void ProcessTimeSlotMatch(Match match, List<(TimeSpan Start, TimeSpan End)> slots, ref decimal totalHours)

        {

            if (match.Groups.Count >= 5)

            {

                if (TimeSpan.TryParse(match.Groups[2].Value, out TimeSpan time1) && TimeSpan.TryParse(match.Groups[4].Value, out TimeSpan time2))

                {

                    string type1 = match.Groups[1].Value;

                    string type2 = match.Groups[3].Value;

                    TimeSpan start, end;



                    if (type1 == "s" && type2 == "f") { start = time1; end = time2; }

                    else if (type1 == "f" && type2 == "s") { start = time2; end = time1; }

                    else { start = time1 < time2 ? time1 : time2; end = time1 < time2 ? time2 : time1; }



                    // Handle 24:00 as end of day

                    // Case 1: (s|HH:MM|f|00:00) - work ends at midnight

                    if (end == TimeSpan.Zero && start != TimeSpan.Zero)

                    {

                        end = new TimeSpan(24, 0, 0);

                    }

                    // Case 2: (s|00:00|f|00:00) - 24-hour workday (full day)

                    else if (start == TimeSpan.Zero && end == TimeSpan.Zero)

                    {

                        end = new TimeSpan(24, 0, 0);

                    }



                    slots.Add((start, end));

                    totalHours += CalculateHoursBetween(start, end);

                }

            }

        }



        private void AddDefaultTimeSlots(List<(TimeSpan, TimeSpan)> slots, decimal defaultDayHours)

        {

            if (defaultDayHours == 8m)

            {

                slots.Add((new TimeSpan(8, 0, 0), new TimeSpan(12, 0, 0)));

                slots.Add((new TimeSpan(13, 0, 0), new TimeSpan(17, 0, 0)));

            }

            else if (defaultDayHours > 0)

            {

                slots.Add((new TimeSpan(8, 0, 0), new TimeSpan(8, 0, 0).Add(TimeSpan.FromHours((double)defaultDayHours))));

            }

        }



        private string CalculateFreeFloat(

            string[] predRow,

            IReadOnlyDictionary<string, int> predIndexes,

            TaskData succTask,

            TaskData predTask,

            decimal lagHours,

            string predClndrIdKey,

            string succClndrIdKey,

            Dictionary<string, WorkingDayCalculator> calendarCalculators,

            Dictionary<string, string> schedOptionsLookup,

            decimal hoursPerDayForSuccessor)

        {

            const string Complete = "TK_Complete";

            const string LOE = "TT_LOE";

            const string WBS = "TT_WBS";





            // Rule 1: Exclude LOE and WBS Summary tasks (no meaningful float)

            if (succTask.TaskType == LOE || succTask.TaskType == WBS ||

                predTask.TaskType == LOE || predTask.TaskType == WBS)

            {

                return "";

            }



            // Rule 2: Exclude completed successors (float no longer meaningful)

            if (succTask.StatusCode == Complete)

            {

                return "";

            }



            // Rule 3: Exclude completed predecessors (P6 shows blank)

            // The constraint is historical - predecessor already finished

            if (predTask.StatusCode == Complete)

            {

                return "";

            }



            WorkingDayCalculator predCalendar = WorkingDayCalculator.Default;

            if (!string.IsNullOrEmpty(predClndrIdKey) && calendarCalculators.TryGetValue(predClndrIdKey, out var predCal))

            {

                predCalendar = predCal;

            }



            WorkingDayCalculator succCalendar = WorkingDayCalculator.Default;

            if (!string.IsNullOrEmpty(succClndrIdKey) && calendarCalculators.TryGetValue(succClndrIdKey, out var succCal))

            {

                succCalendar = succCal;

            }



            // Determine lag projection calendar based on project settings

            string lagCalendarSetting = "rcal_Predecessor";

            if (schedOptionsLookup != null && !string.IsNullOrEmpty(succTask.ProjIdKey) &&

                schedOptionsLookup.TryGetValue(succTask.ProjIdKey, out string? setting))

            {

                if (!string.IsNullOrEmpty(setting))

                {

                    lagCalendarSetting = setting;

                }

            }



            WorkingDayCalculator lagProjectionCalendar =

                (lagCalendarSetting == "rcal_Successor") ? succCalendar : predCalendar;



            // Get the relationship type from the predecessor row

            string predType = GetFieldValue(predRow, predIndexes, FieldNames.PredType);



            DateTime? predBaseDate = null;

            DateTime? succTargetDate = null;



            switch (predType)

            {

                case "PR_FS": // Finish-to-Start

                              // Free Float = Succ.EarlyStart - (Pred.EarlyFinish + Lag)

                    predBaseDate = predTask.EarlyEndDate;

                    succTargetDate = succTask.EarlyStartDate;

                    break;



                case "PR_SS": // Start-to-Start

                              // Free Float = Succ.EarlyStart - (Pred.EarlyStart + Lag)

                    predBaseDate = predTask.EarlyStartDate;

                    succTargetDate = succTask.EarlyStartDate;

                    break;



                case "PR_FF": // Finish-to-Finish

                              // Free Float = Succ.EarlyFinish - (Pred.EarlyFinish + Lag)

                    predBaseDate = predTask.EarlyEndDate;

                    succTargetDate = succTask.EarlyEndDate;

                    break;



                case "PR_SF": // Start-to-Finish

                              // Free Float = Succ.EarlyFinish - (Pred.EarlyStart + Lag)

                    predBaseDate = predTask.EarlyStartDate;

                    succTargetDate = succTask.EarlyEndDate;

                    break;



                default:

                    return "";

            }



            // Validate dates exist

            if (!predBaseDate.HasValue || !succTargetDate.HasValue)

            {

                return "";

            }





            DateTime projectedConstraintDate = lagProjectionCalendar.AddWorkingHours(predBaseDate.Value, lagHours);





            decimal freeFloatInHours = succCalendar.CountWorkingHours(projectedConstraintDate, succTargetDate.Value);





            decimal freeFloatInDays = 0;

            if (hoursPerDayForSuccessor > 0)

            {

                freeFloatInDays = freeFloatInHours / hoursPerDayForSuccessor;

            }



            return freeFloatInDays.ToString("F2", CultureInfo.InvariantCulture);

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



        // Creates the detailed calendar table (11_XER_CALENDAR_DETAILED) by parsing clndr_data

        public XerTable? Create11XerCalendarDetailed()
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

            FieldNames.MonthUpdate, FieldNames.DayOfWeekNum, FieldNames.WorkingDayInt

        };



                // PERFORMANCE OPTIMIZATION: Pre-calculate indexes

                var finalIndexes = detailedColumns

                    .Select((name, index) => new { name, index })

                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);





                var resultTable = new XerTable(EnhancedTableNames.XerCalendarDetailed11);

                resultTable.SetHeaders(detailedColumns.Select(s => StringInternPool.Intern(s) ?? string.Empty).ToArray());



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



                        // Add calculated columns: day_of_week_num and working_day_int

                        SetTransformedField(detailedRow, finalIndexes, FieldNames.DayOfWeekNum, GetDayOfWeekNumber(entry.DayOfWeek));

                        SetTransformedField(detailedRow, finalIndexes, FieldNames.WorkingDayInt, entry.WorkingDay == "Y" ? "1" : "0");



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



        private sealed class CalendarEntry
        {
            public required string Date { get; init; }
            public required string DayOfWeek { get; init; }
            public required string WorkingDay { get; init; }
            public required string WorkHours { get; init; }
            public required string ExceptionType { get; init; }
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

                    string daysSection = clndrData.AsSpan(daysStart, daysEnd - daysStart).ToString();


                    // Use dictionaries to store results from Regex matches

                    var dayContents = new Dictionary<int, string>();

                    var emptyDays = new HashSet<int>();



                    // Match all occurrences using the static compiled Regex (DayPatternRegex)

                    foreach (Match match in DayPatternRegex().Matches(daysSection))
                    {
                        if (int.TryParse(match.Groups[1].Value, out int dayNum) && dayNum >= 1 && dayNum <= 7)

                        {

                            dayContents[dayNum] = match.Groups[2].Value.Trim();

                        }

                    }



                    // Match all occurrences using the static compiled Regex (EmptyDayPatternRegex)

                    foreach (Match match in EmptyDayPatternRegex().Matches(daysSection))
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



                        if (dayContents.TryGetValue(dayNum, out string? dayContent) && !string.IsNullOrWhiteSpace(dayContent))

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

            string[] exceptionSections = ["Exceptions", "HolidayOrExceptions", "HolidayOrException"];



            foreach (var sectionName in exceptionSections)

            {

                int excStart = clndrData.IndexOf(sectionName);

                if (excStart > -1)

                {

                    int excEnd = FindSectionEnd(clndrData, excStart);

                    ReadOnlySpan<char> excSpan = excEnd > excStart
                        ? clndrData.AsSpan(excStart, excEnd - excStart)
                        : clndrData.AsSpan(excStart);
                    string excSection = excSpan.ToString();


                    // Use pre-compiled Regex patterns

                    ParseExceptionMatches(ExcPatternRegex().Matches(excSection), entries);
                    ParseExceptionMatches(ExcPatternRegex2().Matches(excSection), entries);
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

                        int dayNum = (int)excDate.DayOfWeek; // .NET: 0=Sun, 1=Mon... 6=Sat

                        if (dayNum == 0) dayNum = 7; // P6: 1=Sun... 7=Sat. We use 1=Mon... 7=Sun in our GetDayName/GetDayOfWeekNumber.

                                                     // Let's stick to .NET's DayOfWeek and adjust GetDayName



                        string dayName = excDate.DayOfWeek.ToString(); // e.g., "Monday"

                        string p6DayName = GetDayName((int)excDate.DayOfWeek + 1); // Get P6 day name (1-7)



                        // Find the standard entry for this day of the week to use as a template

                        var standardDayEntry = entries.FirstOrDefault(e => e.DayOfWeek == p6DayName && string.IsNullOrEmpty(e.Date));



                        if (standardDayEntry == null) continue; // Should not happen if standard week was parsed



                        // Group 3 contains the work content (time slots)

                        string workContent = excMatch.Groups.Count > 3 ? excMatch.Groups[3].Value.Trim() : "()";



                        decimal hours;

                        string entryType;



                        if (string.IsNullOrEmpty(workContent) || workContent == "()")

                        {

                            // This is explicitly a non-working day (Holiday)

                            hours = 0;

                            entryType = "Exception - Non-Working";

                        }

                        else

                        {

                            // This is a working exception with its own hours

                            hours = ParseDayWorkHours(workContent);

                            entryType = "Exception - Working";

                        }



                        // Add a NEW entry for this specific date, using the standard day as a base

                        // but overriding with exception data.

                        entries.Add(new CalendarEntry

                        {

                            Date = excDate.ToString("yyyy-MM-dd"), // Specific date

                            DayOfWeek = p6DayName,                 // Day name (e.g., "Monday")

                            WorkingDay = hours > 0 ? "Y" : "N",    // Y/N based on exception hours

                            WorkHours = FormatHours(hours),        // Exception hours

                            ExceptionType = entryType              // "Exception - Working" or "Exception - Non-Working"

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

            string[] nextSections = ["VIEW", "Exceptions", "HolidayOrExceptions", "Resources", "DaysOfWeek"];

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

            result = CleanRegex1().Replace(result, "$1");
            result = CleanRegex2().Replace(result, "$1");
            // .NET 8: Use source-generated regex for collapsing multiple spaces

            result = MultiSpaceRegex().Replace(result, " ");



            return result;

        }



        // Parses work hours from time slot definitions within clndr_data

        // This logic handles various P6 time slot formats (s=start, f=finish)
        // .NET 8: Uses GeneratedRegex for source-generated, compiled regex patterns

        private decimal ParseDayWorkHours(string content)

        {

            if (string.IsNullOrWhiteSpace(content)) return 0;



            decimal totalHours = 0;



            // Pattern 1: (0||N (s|HH:MM|f|HH:MM) ()) - Most common format
            // Uses source-generated regex for optimal performance

            var matches = TimeSlotPattern1Regex().Matches(content);



            if (matches.Count > 0)

            {

                foreach (Match match in matches)

                {

                    totalHours += ExtractHoursFromMatch(match);

                }

                return totalHours;

            }



            // Pattern 2: (s|HH:MM|f|HH:MM) - Alternative format

            matches = TimeSlotPattern2Regex().Matches(content);



            if (matches.Count > 0)

            {

                foreach (Match match in matches)

                {

                    totalHours += ExtractHoursFromMatch(match);

                }

                return totalHours;

            }



            // Pattern 3: s|HH:MM|f|HH:MM (Simplified format)

            matches = TimeSlotPattern3Regex().Matches(content);



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

            if (start == end)

            {

                // FIX: P6 uses (s|00:00|f|00:00) to represent a full 24-hour workday.

                if (start == TimeSpan.Zero)

                {

                    return 24m;

                }



                // Any other identical time (e.g., s|08:00|f|08:00) is 0 hours.

                return 0m;

            }



            if (end > start)

            {

                // Standard day shift (e.g., 08:00 to 17:00)

                return (decimal)(end - start).TotalHours;

            }

            else

            {

                // Overnight shift (e.g., 22:00 to 06:00)

                // This also handles (s|08:00|f|00:00), which is 16 hours.

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

            // P6 convention: 1=Sunday, 2=Monday... 7=Saturday

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



        private static string GetDayOfWeekNumber(string dayName)

        {

            // Converts day name to number (Monday=1, Sunday=7) for Power BI compatibility

            switch (dayName)

            {

                case "Monday": return "1";

                case "Tuesday": return "2";

                case "Wednesday": return "3";

                case "Thursday": return "4";

                case "Friday": return "5";

                case "Saturday": return "6";

                case "Sunday": return "7";

                default: return "";

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



        // Creates the Resource Distribution table (15_XER_RESOURCE_DISTRIBUTION)

        public XerTable? Create15XerResourceDistribution()
        {

            var taskRsrcTable = _dataStore.GetTable(TableNames.TaskRsrc);

            var taskTable = _dataStore.GetTable(TableNames.Task);

            var rsrcTable = _dataStore.GetTable(TableNames.Rsrc);

            var projectTable = _dataStore.GetTable(TableNames.Project);

            var calendarTable = _dataStore.GetTable(TableNames.Calendar);

            var umeasureTable = _dataStore.GetTable(TableNames.Umeasure);



            if (!IsTableValid(taskRsrcTable) || !IsTableValid(taskTable) || !IsTableValid(projectTable)) return null;



            try

            {

                // UPDATED: Column definitions with new hour-based fields

                string[] distColumns = {

            FieldNames.TaskIdKey, FieldNames.RsrcIdKey, FieldNames.ClndrIdKey, FieldNames.ProjIdKey,

            FieldNames.DistributionMonth, FieldNames.MonthStartDate, FieldNames.MonthEndDate,

            FieldNames.MonthlyQuantity, FieldNames.DistributionType,

            

            // PRIMARY DIAGNOSTIC COLUMNS (Hour-Based)

            FieldNames.MonthWorkingHours,      // NEW: Actual working hours in month slice

            FieldNames.TotalWorkingHours,      // NEW: Total working hours in full period

            FieldNames.CalendarHoursPerDay,    // NEW: Calendar's standard hours per day

            

            // DERIVED COLUMNS (For backward compatibility)

            FieldNames.MonthWorkingDays,       // Now calculated: hours / calendar_hpd

            FieldNames.TotalWorkingDays,       // Now calculated: hours / calendar_hpd

            FieldNames.MonthCalendarDays,

            FieldNames.TotalCalendarDays,



            FieldNames.Start, FieldNames.Finish,

            FieldNames.IsActual, FieldNames.StatusCode,

            FieldNames.Unit,



            // Descriptive

            FieldNames.TaskCode,

            FieldNames.RsrcShortName, FieldNames.RsrcName, FieldNames.RsrcType,

            FieldNames.MonthUpdate

        };



                var finalIndexes = distColumns

                    .Select((name, index) => new { name, index })

                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);



                var resultTable = new XerTable(EnhancedTableNames.XerResourceDist15, taskRsrcTable.RowCount * 5);

                resultTable.SetHeaders(distColumns.Select(s => StringInternPool.Intern(s) ?? string.Empty).ToArray());



                // 1. Build Lookups

                Console.WriteLine("Building lookups for Hour-Based Resource Distribution...");



                var taskLookup = BuildTaskLookupDictionary(taskTable);

                var projDataDates = BuildProjectDataDatesLookup(projectTable);



                var projNameLookup = new Dictionary<string, string>();

                var pIdx = projectTable.FieldIndexes;

                foreach (var row in projectTable.Rows)

                {

                    string id = GetFieldValue(row.Fields, pIdx, FieldNames.ProjectId);

                    string name = GetFieldValue(row.Fields, pIdx, "proj_short_name");

                    if (string.IsNullOrEmpty(name)) name = id;

                    if (!string.IsNullOrEmpty(id)) projNameLookup[CreateKey(row.SourceFilename, id)] = name;

                }



                var rsrcLookup = BuildResourceLookup(rsrcTable, umeasureTable);

                var calendars = BuildCalendarCalculators(null);

                var calendarHours = BuildCalendarHoursLookup(calendarTable);



                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };

                var resultBag = new ConcurrentBag<DataRow>();

                var trIdx = taskRsrcTable.FieldIndexes;



                Parallel.ForEach(taskRsrcTable.Rows, parallelOptions, sourceRow =>

                {

                    var row = sourceRow.Fields;

                    string filename = sourceRow.SourceFilename;



                    // 2. Parse Quantities using Native Columns

                    double.TryParse(GetFieldValue(row, trIdx, FieldNames.ActRegQty), NumberStyles.Any, CultureInfo.InvariantCulture, out double actReg);

                    double.TryParse(GetFieldValue(row, trIdx, FieldNames.ActOtQty), NumberStyles.Any, CultureInfo.InvariantCulture, out double actOt);

                    double actualQty = actReg + actOt;

                    double.TryParse(GetFieldValue(row, trIdx, FieldNames.RemainQty), NumberStyles.Any, CultureInfo.InvariantCulture, out double remainQty);



                    // 3. Get Keys

                    string taskId = GetFieldValue(row, trIdx, FieldNames.TaskId);

                    string rsrcId = GetFieldValue(row, trIdx, FieldNames.RsrcId);

                    string taskIdKey = CreateKey(filename, taskId);

                    string rsrcIdKey = CreateKey(filename, rsrcId);



                    // 4. Lookups (Mimic RELATED())

                    if (!taskLookup.TryGetValue(taskIdKey, out TaskData taskData)) taskData = new TaskData();



                    string clndrIdKey = taskData.ClndrIdKey ?? string.Empty;

                    string statusCode = taskData.StatusCode ?? string.Empty;

                    string projIdKey = taskData.ProjIdKey ?? string.Empty;

                    DateTime dataDate = projDataDates.TryGetValue(projIdKey, out var dd) ? dd : DateTime.MinValue;



                    string rsrcName = "", rsrcShort = "", rsrcType = "", unitName = "";

                    if (rsrcLookup.TryGetValue(rsrcIdKey, out var rInfo))

                    {

                        rsrcName = rInfo.Name;

                        rsrcShort = rInfo.ShortName;

                        rsrcType = rInfo.Type;

                        unitName = rInfo.Unit;

                    }



                    // --- LOGIC BRANCH 1: ACTUALS ---

                    if (actualQty > 0 && (statusCode == "TK_Complete" || statusCode == "TK_Active"))

                    {

                        DateTime rsrcActStart = DateParser.TryParse(GetFieldValue(row, trIdx, FieldNames.ActStartDate)) ?? DateTime.MinValue;

                        DateTime rsrcActEnd = DateParser.TryParse(GetFieldValue(row, trIdx, FieldNames.ActEndDate)) ?? DateTime.MinValue;



                        DateTime finalEndDate = (statusCode == "TK_Complete") ? rsrcActEnd : dataDate;



                        if (rsrcActStart != DateTime.MinValue && finalEndDate != DateTime.MinValue && finalEndDate >= rsrcActStart)

                        {

                            GenerateDistributionRows(resultBag, filename, finalIndexes, taskIdKey, rsrcIdKey, clndrIdKey, projIdKey, rsrcActStart, finalEndDate, actualQty, true, statusCode, taskData.TaskType ?? string.Empty, taskData.TaskCode ?? string.Empty, row, trIdx, calendars, calendarHours, projNameLookup, rsrcName, rsrcShort, rsrcType, unitName);

                        }

                    }



                    // --- LOGIC BRANCH 2: REMAINING ---

                    if (remainQty > 0 && (statusCode == "TK_NotStart" || statusCode == "TK_Active"))

                    {

                        DateTime restartDate = DateParser.TryParse(GetFieldValue(row, trIdx, FieldNames.RestartDate)) ?? DateTime.MinValue;

                        DateTime reendDate = DateParser.TryParse(GetFieldValue(row, trIdx, FieldNames.ReendDate)) ?? DateTime.MinValue;



                        if (restartDate != DateTime.MinValue && reendDate != DateTime.MinValue && reendDate >= restartDate)

                        {

                            GenerateDistributionRows(resultBag, filename, finalIndexes, taskIdKey, rsrcIdKey, clndrIdKey, projIdKey, restartDate, reendDate, remainQty, false, statusCode, taskData.TaskType ?? string.Empty, taskData.TaskCode ?? string.Empty, row, trIdx, calendars, calendarHours, projNameLookup, rsrcName, rsrcShort, rsrcType, unitName);

                        }

                    }

                });



                resultTable.AddRows(resultBag);

                Console.WriteLine($"Generated {resultTable.RowCount} hour-based resource distribution rows.");

                return resultTable;

            }

            catch (Exception ex)

            {

                Console.WriteLine($"Error creating {EnhancedTableNames.XerResourceDist15}: {ex.Message}");

                return null;

            }

        }

        private Dictionary<string, (string ShortName, string Name, string Type, string Unit)> BuildResourceLookup(XerTable? rsrcTable, XerTable? umeasureTable)

        {

            var lookup = new Dictionary<string, (string, string, string, string)>();

            if (!IsTableValid(rsrcTable)) return lookup;



            var unitMap = new Dictionary<string, string>();

            if (IsTableValid(umeasureTable))

            {

                var uIdx = umeasureTable.FieldIndexes;

                foreach (var row in umeasureTable.Rows)

                {

                    string id = GetFieldValue(row.Fields, uIdx, FieldNames.UnitId);

                    string name = GetFieldValue(row.Fields, uIdx, FieldNames.UnitAbbr);

                    if (string.IsNullOrEmpty(name)) name = GetFieldValue(row.Fields, uIdx, FieldNames.UnitName);

                    if (!string.IsNullOrEmpty(id)) unitMap[CreateKey(row.SourceFilename, id)] = name;

                }

            }



            var rIdx = rsrcTable.FieldIndexes;

            foreach (var row in rsrcTable.Rows)

            {

                string id = GetFieldValue(row.Fields, rIdx, FieldNames.RsrcId);

                string shortName = GetFieldValue(row.Fields, rIdx, FieldNames.RsrcShortName);

                string rName = GetFieldValue(row.Fields, rIdx, FieldNames.RsrcName);

                string type = GetFieldValue(row.Fields, rIdx, FieldNames.RsrcType);

                string unitId = GetFieldValue(row.Fields, rIdx, FieldNames.UnitId);

                string unitName = string.Empty;

                if (!string.IsNullOrEmpty(unitId))
                {
                    string unitKey = CreateKey(row.SourceFilename, unitId);
                    if (unitMap.TryGetValue(unitKey, out string? unitLookup) && !string.IsNullOrEmpty(unitLookup))
                    {
                        unitName = unitLookup;
                    }
                }

                if (!string.IsNullOrEmpty(id))

                {

                    string key = CreateKey(row.SourceFilename, id);

                    lookup[key] = (shortName, rName, type, unitName);

                }

            }

            return lookup;

        }

        // Helper Method for Generating Rows (Strict Integer Day Logic)

        private void GenerateDistributionRows(

            ConcurrentBag<DataRow> bag,

            string filename,

            IReadOnlyDictionary<string, int> indexes,

            string taskIdKey,

            string rsrcIdKey,

            string clndrIdKey,

            string projIdKey,

            DateTime startDate,

            DateTime endDate,

            double totalQty,

            bool isActual,

            string statusCode,

            string taskType,

            string taskCode,

            string[] sourceRow,

            IReadOnlyDictionary<string, int> sourceIdx,

            Dictionary<string, WorkingDayCalculator> calendars,

            Dictionary<string, decimal> calendarHoursLookup,

            Dictionary<string, string> projNameLookup,

            string rsrcName,

            string rsrcShort,

            string rsrcType,

            string unitName)

        {

            // Get the calendar calculator

            WorkingDayCalculator calculator = WorkingDayCalculator.Default;

            if (!string.IsNullOrEmpty(clndrIdKey) && calendars.TryGetValue(clndrIdKey, out var cal))

                calculator = cal;



            // Get calendar hours per day for conversions

            decimal calendarHoursPerDay = 8m;

            if (!string.IsNullOrEmpty(clndrIdKey) && calendarHoursLookup.TryGetValue(clndrIdKey, out decimal hpd))

                calendarHoursPerDay = hpd;

            if (calendarHoursPerDay <= 0) calendarHoursPerDay = 8m; // Fallback



            decimal totalWorkingHours = calculator.CountWorkingHours(startDate, endDate);



            // Fallback to calendar days if no working hours exist (all holidays)

            bool useWorkingHours = totalWorkingHours > 0;

            decimal totalCalendarDays = 0;

            if (!useWorkingHours)

            {

                totalCalendarDays = (decimal)(endDate.Date - startDate.Date).TotalDays + 1;

            }



            // Iterate through months

            DateTime currentMonthStart = new DateTime(startDate.Year, startDate.Month, 1);

            DateTime finalMonthStart = new DateTime(endDate.Year, endDate.Month, 1);



            while (currentMonthStart <= finalMonthStart)

            {

                // Get last moment of the month (includes full last day)

                DateTime currentMonthEnd = currentMonthStart.AddMonths(1).AddDays(-1).Date

                    .AddHours(23).AddMinutes(59).AddSeconds(59);



                // Determine intersection with activity period

                DateTime periodStart = (startDate > currentMonthStart) ? startDate : currentMonthStart;

                DateTime periodEnd = (endDate < currentMonthEnd) ? endDate : currentMonthEnd;



                if (periodStart <= periodEnd)

                {

                    double distributedQty = 0;

                    decimal periodWorkingHours = 0;

                    decimal periodCalendarDays = 0;



                    if (useWorkingHours)

                    {

                        // *** HOUR-BASED LOGIC: Count working hours in this month slice ***

                        // This replaces the day-counting loop with hour-accurate calculation

                        periodWorkingHours = calculator.CountWorkingHours(periodStart, periodEnd);



                        // Proportional distribution by hours (not days)

                        if (totalWorkingHours > 0)

                        {

                            distributedQty = totalQty * (double)(periodWorkingHours / totalWorkingHours);

                        }

                    }

                    else

                    {

                        // Fallback: Calendar day distribution (when no working hours exist)

                        periodCalendarDays = (decimal)(periodEnd.Date - periodStart.Date).TotalDays + 1;

                        if (totalCalendarDays > 0)

                        {

                            distributedQty = totalQty * (double)(periodCalendarDays / totalCalendarDays);

                        }

                    }



                    // Add Row if Qty > 0 OR it is Actuals (preserve data integrity)

                    if (distributedQty > 0 || isActual)

                    {

                        string[] newRow = new string[indexes.Count];



                        // Keys

                        SetTransformedField(newRow, indexes, FieldNames.TaskIdKey, taskIdKey);

                        SetTransformedField(newRow, indexes, FieldNames.RsrcIdKey, rsrcIdKey);

                        SetTransformedField(newRow, indexes, FieldNames.ClndrIdKey, clndrIdKey);

                        SetTransformedField(newRow, indexes, FieldNames.ProjIdKey, projIdKey);



                        // Distribution Data

                        SetTransformedField(newRow, indexes, FieldNames.DistributionMonth,

                            currentMonthStart.ToString("yyyy-MM-dd"));

                        SetTransformedField(newRow, indexes, FieldNames.MonthStartDate,

                            periodStart.ToString("yyyy-MM-dd HH:mm:ss"));  // Keep time precision

                        SetTransformedField(newRow, indexes, FieldNames.MonthEndDate,

                            periodEnd.ToString("yyyy-MM-dd HH:mm:ss"));    // Keep time precision

                        SetTransformedField(newRow, indexes, FieldNames.MonthlyQuantity,

                            distributedQty.ToString("F4", CultureInfo.InvariantCulture));

                        SetTransformedField(newRow, indexes, FieldNames.DistributionType,

                            useWorkingHours ? "Working Hours" : "Calendar Days");



                        // *** PRIMARY DIAGNOSTIC FIELDS (Hour-Based) ***

                        SetTransformedField(newRow, indexes, FieldNames.MonthWorkingHours,

                            periodWorkingHours.ToString("F2", CultureInfo.InvariantCulture));

                        SetTransformedField(newRow, indexes, FieldNames.TotalWorkingHours,

                            totalWorkingHours.ToString("F2", CultureInfo.InvariantCulture));

                        SetTransformedField(newRow, indexes, FieldNames.CalendarHoursPerDay,

                            calendarHoursPerDay.ToString("F2", CultureInfo.InvariantCulture));



                        // *** DERIVED FIELDS (Backward Compatibility) ***

                        // Convert hours to days using calendar's standard hours per day

                        decimal periodWorkingDays = periodWorkingHours / calendarHoursPerDay;

                        decimal totalWorkingDays = totalWorkingHours / calendarHoursPerDay;



                        SetTransformedField(newRow, indexes, FieldNames.MonthWorkingDays,

                            periodWorkingDays.ToString("F2", CultureInfo.InvariantCulture));

                        SetTransformedField(newRow, indexes, FieldNames.TotalWorkingDays,

                            totalWorkingDays.ToString("F2", CultureInfo.InvariantCulture));



                        // Calendar days (for reference)

                        if (periodCalendarDays == 0)

                            periodCalendarDays = (decimal)(periodEnd.Date - periodStart.Date).TotalDays + 1;



                        SetTransformedField(newRow, indexes, FieldNames.MonthCalendarDays,

                            periodCalendarDays.ToString("F0", CultureInfo.InvariantCulture));

                        SetTransformedField(newRow, indexes, FieldNames.TotalCalendarDays,

                            totalCalendarDays.ToString("F0", CultureInfo.InvariantCulture));



                        // Metadata (keep time precision for better accuracy)

                        SetTransformedField(newRow, indexes, FieldNames.Start,

                            startDate.ToString("yyyy-MM-dd HH:mm:ss"));

                        SetTransformedField(newRow, indexes, FieldNames.Finish,

                            endDate.ToString("yyyy-MM-dd HH:mm:ss"));

                        SetTransformedField(newRow, indexes, FieldNames.IsActual, isActual ? "1" : "0");

                        SetTransformedField(newRow, indexes, FieldNames.StatusCode, statusCode);

                        SetTransformedField(newRow, indexes, FieldNames.TaskCode, taskCode);

                        SetTransformedField(newRow, indexes, FieldNames.RsrcShortName, rsrcShort);

                        SetTransformedField(newRow, indexes, FieldNames.RsrcName, rsrcName);

                        SetTransformedField(newRow, indexes, FieldNames.RsrcType, rsrcType);



                        // Conditional Unit logic: If resource type is NOT material, use "unit/time"

                        string finalUnit = (rsrcType != "RT_Mat") ? "unit/time" : unitName;

                        SetTransformedField(newRow, indexes, FieldNames.Unit, finalUnit);



                        SetTransformedField(newRow, indexes, FieldNames.MonthUpdate,

                            ParseMonthUpdateFromFilename(filename));



                        // String intern for memory efficiency

                        for (int k = 0; k < newRow.Length; k++)

                            newRow[k] = StringInternPool.Intern(newRow[k] ?? string.Empty);



                        bag.Add(new DataRow(newRow, filename));

                    }

                }



                currentMonthStart = currentMonthStart.AddMonths(1);

            }

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
            public string? Message;
            // Fields for UI status visualization
            public string? FilePath;
            public string? FileStatus;
            public Color? StatusColor;
        }




        // UI/UX OPTIMIZATION: Added CancellationToken and detailed progress reporting
        public async Task<XerDataStore> ParseMultipleXerFilesAsync(List<string> filePaths, IProgress<DetailedProgress>? progress, CancellationToken cancellationToken)
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

            int fileIndex = 0;
            var intermediateStores = new ConcurrentBag<XerDataStore>();

            try
            {
                await Parallel.ForEachAsync(filePaths, parallelOptions, (file, ct) =>
                {
                    ct.ThrowIfCancellationRequested();

                    int currentIndex = Interlocked.Increment(ref fileIndex);
                    string shortFileName = Path.GetFileName(file);

                    // Report starting status for the file visualization
                    progress?.Report(new DetailedProgress { Percent = (currentIndex - 1) * 100 / fileCount, Message = $"Starting: {shortFileName}", FilePath = file, FileStatus = "Processing", StatusColor = Color.Blue });

                    try
                    {
                        if (!File.Exists(file))
                        {
                            progress?.Report(new DetailedProgress { Percent = currentIndex * 100 / fileCount, Message = $"Skipped (Missing): {shortFileName}", FilePath = file, FileStatus = "Skipped", StatusColor = Color.Orange });
                            return ValueTask.CompletedTask;
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
                        var singleFileStore = _parser.ParseXerFile(file, parserProgressCallback, ct);
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

                    return ValueTask.CompletedTask;
                }).ConfigureAwait(false);
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
        }


        // UI/UX OPTIMIZATION: Added CancellationToken

        public async Task<List<string>> ExportTablesAsync(XerDataStore dataStore, List<string> tablesToExport, string outputDirectory, IProgress<(int percent, string message)>? progress, CancellationToken cancellationToken)
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

            if (enhancedTablesToGenerate.Count > 0)
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

                    // Dependency Management: Pre-generate CALENDAR_DETAILED11 if needed by PREDECESSOR06
                    bool needsCalendarDetailed = enhancedTablesToGenerate.Contains(EnhancedTableNames.XerCalendarDetailed11) || enhancedTablesToGenerate.Contains(EnhancedTableNames.XerPredecessor06);
                    if (needsCalendarDetailed && !enhancedCache.ContainsKey(EnhancedTableNames.XerCalendarDetailed11))
                    {
                        // Check dependencies for CALENDAR_DETAILED11
                        if (dataStore.ContainsTable(TableNames.Calendar))
                        {
                            var calendarDetailedData = transformer.Create11XerCalendarDetailed();
                            if (calendarDetailedData != null) enhancedCache.TryAdd(EnhancedTableNames.XerCalendarDetailed11, calendarDetailedData);
                        }
                    }

                    // Generate other enhanced tables in parallel
                    await Parallel.ForEachAsync(enhancedTablesToGenerate, parallelOptions, (tableName, ct) =>
                    {
                        ct.ThrowIfCancellationRequested();

                        if (enhancedCache.ContainsKey(tableName)) return ValueTask.CompletedTask; // Already generated (e.g., TASK01)

                        var generatedTable = GenerateEnhancedTable(transformer, tableName, enhancedCache);
                        if (generatedTable != null)
                        {
                            enhancedCache.TryAdd(tableName, generatedTable);
                        }

                        return ValueTask.CompletedTask;
                    }).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    progress?.Report((0, "Enhanced table generation canceled."));
                    return exportedFiles.ToList();
                }
            }

            // If canceled during generation, stop the export process
            if (cancellationToken.IsCancellationRequested)
            {
                return exportedFiles.ToList();
            }

            progress?.Report((80, "Exporting tables to CSV..."));
            var exportProgress = new ProgressCounter(tablesToExport.Count, progress);

            try
            {
                await Parallel.ForEachAsync(tablesToExport.OrderBy(n => n), parallelOptions, (tableName, ct) =>
                {
                    // Check for cancellation before starting export of the table
                    ct.ThrowIfCancellationRequested();

                    exportProgress.UpdateStatus($"Exporting: {tableName}");

                    XerTable? tableToExport = null;
                    try
                    {
                        // Determine source of the table (raw data or enhanced cache)
                        if (dataStore.ContainsTable(tableName))
                        {
                            tableToExport = dataStore.GetTable(tableName);
                        }
                        else if (enhancedCache.TryGetValue(tableName, out XerTable? cachedTable))
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
                    return ValueTask.CompletedTask;
                }).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                progress?.Report((0, "Export canceled by user."));
            }

            return exportedFiles.ToList();
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
