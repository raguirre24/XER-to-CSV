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

namespace XerToCsvConverter
{
    #region Helper Classes/Structs/Constants (MUST BE INCLUDED)

    public struct DataRow
    {
        public string[] Fields { get; }
        public string SourceFilename { get; }
        public DataRow(string[] fields, string sourceFilename) { Fields = fields; SourceFilename = sourceFilename; }
    }

    internal static class TableNames { public const string Task = "TASK"; public const string Calendar = "CALENDAR"; public const string Project = "PROJECT"; public const string ProjWbs = "PROJWBS"; public const string TaskActv = "TASKACTV"; public const string ActvCode = "ACTVCODE"; public const string ActvType = "ACTVTYPE"; public const string TaskPred = "TASKPRED"; }
    internal static class FieldNames { public const string TaskId = "task_id"; public const string ProjectId = "proj_id"; public const string WbsId = "wbs_id"; public const string CalendarId = "clndr_id"; public const string TaskType = "task_type"; public const string StatusCode = "status_code"; public const string TaskCode = "task_code"; public const string TaskName = "task_name"; public const string RsrcId = "rsrc_id"; public const string ActStartDate = "act_start_date"; public const string ActEndDate = "act_end_date"; public const string EarlyStartDate = "early_start_date"; public const string EarlyEndDate = "early_end_date"; public const string LateEndDate = "late_end_date"; public const string LateStartDate = "late_start_date"; public const string TargetStartDate = "target_start_date"; public const string TargetEndDate = "target_end_date"; public const string CstrType = "cstr_type"; public const string CstrDate = "cstr_date"; public const string PriorityType = "priority_type"; public const string FloatPath = "float_path"; public const string FloatPathOrder = "float_path_order"; public const string DrivingPathFlag = "driving_path_flag"; public const string RemainDurationHrCnt = "remain_drtn_hr_cnt"; public const string TotalFloatHrCnt = "total_float_hr_cnt"; public const string FreeFloatHrCnt = "free_float_hr_cnt"; public const string CompletePctType = "complete_pct_type"; public const string PhysCompletePct = "phys_complete_pct"; public const string ActWorkQty = "act_work_qty"; public const string RemainWorkQty = "remain_work_qty"; public const string TargetDurationHrCnt = "target_drtn_hr_cnt"; public const string ClndrId = "clndr_id"; public const string DayHourCount = "day_hr_cnt"; public const string LastRecalcDate = "last_recalc_date"; public const string ProjectName = "proj_name"; public const string ObsId = "obs_id"; public const string SeqNum = "seq_num"; public const string EstWt = "est_wt"; public const string ProjNodeFlag = "proj_node_flag"; public const string SumDataFlag = "sum_data_flag"; public const string WbsShortName = "wbs_short_name"; public const string WbsName = "wbs_name"; public const string PhaseId = "phase_id"; public const string ParentWbsId = "parent_wbs_id"; public const string EvUserPct = "ev_user_pct"; public const string EvEtcUserValue = "ev_etc_user_value"; public const string OrigCost = "orig_cost"; public const string IndepRemainTotalCost = "indep_remain_total_cost"; public const string AnnDscntRatePct = "ann_dscnt_rate_pct"; public const string DscntPeriodType = "dscnt_period_type"; public const string IndepRemainWorkQty = "indep_remain_work_qty"; public const string AnticipStartDate = "anticip_start_date"; public const string AnticipEndDate = "anticip_end_date"; public const string EvComputeType = "ev_compute_type"; public const string EvEtcComputeType = "ev_etc_compute_type"; public const string Guid = "guid"; public const string TmplGuid = "tmpl_guid"; public const string ActvCodeTypeId = "actv_code_type_id"; public const string ActvCodeId = "actv_code_id"; public const string FileName = "FileName"; public const string Start = "Start"; public const string Finish = "Finish"; public const string IdName = "ID_Name"; public const string RemainingWorkingDays = "Remaining Working Days"; public const string OriginalDuration = "Original Duration"; public const string TotalFloat = "Total Float"; public const string FreeFloat = "Free Float"; public const string PercentComplete = "%"; public const string DataDate = "Data Date"; public const string WbsIdKey = "wbs_id_key"; public const string TaskIdKey = "task_id_key"; public const string ParentWbsIdKey = "parent_wbs_id_key"; public const string CalendarIdKey = "calendar_id_key"; public const string ProjIdKey = "proj_id_key"; public const string ActvCodeIdKey = "actv_code_id_key"; public const string ActvCodeTypeIdKey = "actv_code_type_id_key"; public const string ClndrIdKey = "clndr_id_key"; public const string PredTaskId = "pred_task_id"; public const string PredTaskIdKey = "pred_task_id_key"; public const string CalendarName = "clndr_name"; public const string CalendarData = "clndr_data"; public const string CalendarType = "clndr_type"; public const string DefaultFlag = "default_flag"; public const string BaseCalendarId = "base_clndr_id"; public const string WeekHourCount = "week_hr_cnt"; public const string MonthHourCount = "month_hr_cnt"; public const string YearHourCount = "year_hr_cnt"; public const string Date = "date"; public const string DayOfWeek = "day_of_week"; public const string WorkingDay = "working_day"; public const string WorkHours = "work_hours"; public const string ExceptionType = "exception_type"; }
    internal static class EnhancedTableNames { public const string XerTask01 = "01_XER_TASK"; public const string XerProject02 = "02_XER_PROJECT"; public const string XerProjWbs03 = "03_XER_PROJWBS"; public const string XerBaseline04 = "04_XER_BASELINE"; public const string XerPredecessor06 = "06_XER_PREDECESSOR"; public const string XerActvType07 = "07_XER_ACTVTYPE"; public const string XerActvCode08 = "08_XER_ACTVCODE"; public const string XerTaskActv09 = "09_XER_TASKACTV"; public const string XerCalendar10 = "10_XER_CALENDAR"; public const string XerCalendarDetailed11 = "11_XER_CALENDAR_DETAILED"; }

    #endregion

    #region Performance Configuration

    public static class PerformanceConfig
    {
        public static int FileReadBufferSize { get; set; } = 65536; // 64KB
        public static int ProgressReportInterval { get; set; } = 1000; // lines
        public static int BatchProcessingSize { get; set; } = 100;
        public static int MaxParallelFiles { get; set; } = Environment.ProcessorCount;
        public static bool UseMemoryMappedFiles { get; set; } = false; // Disabled by default for stability
        public static int StringBuilderInitialCapacity { get; set; } = 4096;
        public static bool EnableStringInterning { get; set; } = true;
        public static int DictionaryInitialCapacity { get; set; } = 1000;

        static PerformanceConfig()
        {
            // Configure GC for better performance with large objects
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            GCSettings.LatencyMode = GCLatencyMode.Batch;
        }
    }

    #endregion

    public partial class MainForm : Form
    {
        private System.Windows.Forms.TableLayoutPanel tableLayoutPanelMain;
        private System.Windows.Forms.GroupBox grpInput;
        private System.Windows.Forms.TableLayoutPanel tableLayoutPanelInput;
        private System.Windows.Forms.Label lblXerFiles;
        private System.Windows.Forms.ListBox lstXerFiles;
        private System.Windows.Forms.Button btnSelectXer;
        private System.Windows.Forms.Label lblOutputPath;
        private System.Windows.Forms.TextBox txtOutputPath;
        private System.Windows.Forms.Button btnSelectOutput;
        private System.Windows.Forms.Button btnParseXer;
        private System.Windows.Forms.GroupBox grpExport;
        private System.Windows.Forms.TableLayoutPanel tableLayoutPanelExport;
        private System.Windows.Forms.Label lblExtractedTables;
        private System.Windows.Forms.ListBox lstTables;
        private System.Windows.Forms.FlowLayoutPanel flowLayoutPanelPbi;
        private System.Windows.Forms.CheckBox _chkCreatePowerBiTables;
        private System.Windows.Forms.Label _lblRequirements;
        private System.Windows.Forms.FlowLayoutPanel flowLayoutPanelExportButtons;
        private System.Windows.Forms.Button btnExportAll;
        private System.Windows.Forms.Button btnExportSelected;
        private System.Windows.Forms.Label lblStatus;
        private System.Windows.Forms.ToolTip toolTip1;
        private System.Windows.Forms.Label lblDragDropHint;
        private System.Windows.Forms.Button btnClearFiles;

        private List<string> _xerFilePaths = new List<string>();
        private string _outputDirectory;
        private Dictionary<string, List<DataRow>> _extractedTables;
        private bool _canCreateTask01 = false; private bool _canCreateProjWbs03 = false; private bool _canCreateBaseline04 = false; private bool _canCreateProject02 = false; private bool _canCreatePredecessor06 = false; private bool _canCreateActvType07 = false; private bool _canCreateActvCode08 = false; private bool _canCreateTaskActv09 = false; private bool _canCreateCalendar10 = false; private bool _canCreateCalendarDetailed11 = false;

        // Performance optimization caches
        private static readonly ConcurrentDictionary<string, string> KeyCache = new ConcurrentDictionary<string, string>();
        private static readonly ConcurrentDictionary<string, DateTime?> DateCache = new ConcurrentDictionary<string, DateTime?>();

        public MainForm()
        {
            InitializeComponent();
            _extractedTables = new Dictionary<string, List<DataRow>>(StringComparer.OrdinalIgnoreCase);
            this.lstXerFiles.AllowDrop = true;
            this.lstXerFiles.DragEnter += new DragEventHandler(lstXerFiles_DragEnter);
            this.lstXerFiles.DragDrop += new DragEventHandler(lstXerFiles_DragDrop);
            UpdateStatus("Ready. Select XER file(s) and Output Directory.");
            UpdateInputButtonsState();
        }

        #region UI Event Handlers

        private void btnSelectXer_Click(object sender, EventArgs e)
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

        private void btnSelectOutput_Click(object sender, EventArgs e)
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
                }
            }
        }

        private void btnClearFiles_Click(object sender, EventArgs e)
        {
            _xerFilePaths.Clear();
            lstXerFiles.Items.Clear();
            UpdateInputButtonsState();
            UpdateStatus("File list cleared.");
        }

        private async void btnParseXer_Click(object sender, EventArgs e)
        {
            if (_xerFilePaths.Count == 0) { ShowError("Please select at least one XER file first."); return; }
            if (string.IsNullOrEmpty(_outputDirectory) || !Directory.Exists(_outputDirectory)) { ShowError("Please select a valid output directory first."); return; }

            var missingFiles = _xerFilePaths.Where(f => !File.Exists(f)).ToList();
            if (missingFiles.Any())
            {
                ShowError($"The following file(s) could not be found:\n{string.Join("\n", missingFiles)}");
                _xerFilePaths.RemoveAll(f => missingFiles.Contains(f));
                UpdateXerFileList();
                UpdateInputButtonsState();
                if (_xerFilePaths.Count == 0) return;
            }

            SetUIEnabled(false);

            var progressForm = new ProgressForm("Parsing XER File(s)...");
            var progress = new Progress<(int percent, string message)>(p =>
            {
                progressForm.UpdateProgress(p.message, p.percent);
            });

            try
            {
                progressForm.Show();

                var result = await Task.Run(() => ParseMultipleXerFilesAsync(_xerFilePaths, progress));

                _extractedTables = result;
                UpdateTableList();
                CheckEnhancedTableDependencies();
                UpdatePbiCheckboxState();
                UpdateExportButtonState();
                UpdateStatus($"Successfully extracted {_extractedTables.Count} tables from selected XER files.");
            }
            catch (Exception ex)
            {
                ShowError($"Error parsing XER file(s): {ex.Message}");
                ClearResults();
            }
            finally
            {
                progressForm.Close();
                SetUIEnabled(true);
            }
        }

        private async void btnExportAll_Click(object sender, EventArgs e)
        {
            await ExportTablesAsync(GetAllTableNamesToExport());
        }

        private async void btnExportSelected_Click(object sender, EventArgs e)
        {
            await ExportTablesAsync(GetSelectedTableNamesToExport());
        }

        private void lstTables_SelectedIndexChanged(object sender, EventArgs e)
        {
            UpdateExportButtonState();
        }

        private void ChkCreatePowerBiTables_CheckedChanged(object sender, EventArgs e)
        {
            UpdateExportButtonState();
            if (_chkCreatePowerBiTables.Checked && !AnyPbiDependencyMet())
            {
                ShowWarning(GetMissingDependenciesMessage("Cannot create any Power BI tables due to missing dependencies."));
            }
        }

        private void lstXerFiles_DragEnter(object sender, DragEventArgs e)
        {
            if (e.Data.GetDataPresent(DataFormats.FileDrop))
            {
                e.Effect = DragDropEffects.Copy;
            }
            else
            {
                e.Effect = DragDropEffects.None;
            }
        }

        private void lstXerFiles_DragDrop(object sender, DragEventArgs e)
        {
            string[] files = (string[])e.Data.GetData(DataFormats.FileDrop);

            var xerFiles = files.Where(f => Path.GetExtension(f).Equals(".xer", StringComparison.OrdinalIgnoreCase)).ToArray();
            if (xerFiles.Length > 0)
            {
                AddXerFiles(xerFiles);
            }
            else
            {
                ShowWarning("No valid .xer files found in the dropped items.");
            }
        }

        private void lstXerFiles_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Delete && lstXerFiles.SelectedItems.Count > 0)
            {
                if (MessageBox.Show($"Remove {lstXerFiles.SelectedItems.Count} selected file(s) from the list?", "Confirm Removal", MessageBoxButtons.YesNo, MessageBoxIcon.Question) == DialogResult.Yes)
                {
                    var itemsToRemove = lstXerFiles.SelectedItems.Cast<string>().ToList();
                    foreach (var item in itemsToRemove)
                    {
                        _xerFilePaths.Remove(item);
                        lstXerFiles.Items.Remove(item);
                    }
                    UpdateInputButtonsState();
                }
            }
        }

        #endregion

        #region Core Logic - Parsing & Merging (Optimized)

        private Dictionary<string, List<DataRow>> ParseMultipleXerFilesAsync(
            List<string> filePaths,
            IProgress<(int percent, string message)> progress)
        {
            var combinedTables = new Dictionary<string, List<DataRow>>(StringComparer.OrdinalIgnoreCase);
            int fileCount = filePaths.Count;

            // Process files in parallel for better performance
            var parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = Math.Min(PerformanceConfig.MaxParallelFiles, fileCount)
            };

            var fileTables = new ConcurrentBag<Dictionary<string, List<DataRow>>>();
            var fileIndex = 0;

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

                    var singleXerResult = ParseXerFile(file, (p, s) =>
                    {
                        int overallProgress = ((currentIndex - 1) * 100 + p) / fileCount;
                        progress?.Report((overallProgress, s));
                    });

                    fileTables.Add(singleXerResult);
                }
                catch (Exception ex)
                {
                    throw new Exception($"Failed to parse file '{shortFileName}': {ex.Message}", ex);
                }
            });

            // Merge results
            foreach (var fileTable in fileTables)
            {
                MergeParsedTables(combinedTables, fileTable);
            }

            progress?.Report((100, "Finished parsing all files."));
            return combinedTables;
        }

        private Dictionary<string, List<DataRow>> ParseXerFile(string xerFilePath, Action<int, string> reportProgressAction)
        {
            var tables = new Dictionary<string, List<DataRow>>(StringComparer.OrdinalIgnoreCase);
            string filename = Path.GetFileName(xerFilePath);

            int BufferSize = PerformanceConfig.FileReadBufferSize;
            int ProgressReportInterval = PerformanceConfig.ProgressReportInterval;

            string currentTable = null;
            List<DataRow> currentTableData = null;
            string[] currentHeaders = null;

            int lineCount = 0;
            long bytesRead = 0;

            try
            {
                var fileInfo = new FileInfo(xerFilePath);
                long fileSize = fileInfo.Length;
                if (fileSize == 0) return tables;

                int lastReportedProgress = -1;

                using (var fileStream = new FileStream(xerFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, BufferSize))
                using (var reader = new StreamReader(fileStream, Encoding.Default, true, BufferSize))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        bytesRead += Encoding.Default.GetByteCount(line) + Environment.NewLine.Length;
                        lineCount++;

                        // Reduce progress reporting frequency
                        if (lineCount % ProgressReportInterval == 0)
                        {
                            int progress = (int)((double)bytesRead * 100 / fileSize);
                            if (progress > lastReportedProgress)
                            {
                                reportProgressAction?.Invoke(progress, $"Parsing {filename}: {progress}%");
                                lastReportedProgress = progress;
                            }
                        }

                        if (string.IsNullOrWhiteSpace(line)) continue;

                        // Use single character comparison for better performance
                        if (line.Length < 2 || line[0] != '%') continue;

                        char typeChar = line[1];

                        switch (typeChar)
                        {
                            case 'T': // Table
                                currentTable = line.Substring(2).Trim();
                                if (!string.IsNullOrEmpty(currentTable))
                                {
                                    currentTableData = new List<DataRow>(PerformanceConfig.DictionaryInitialCapacity);
                                    tables[currentTable] = currentTableData;
                                    currentHeaders = null;
                                }
                                break;

                            case 'F': // Fields
                                if (currentTable != null)
                                {
                                    string fieldsLine = line.Substring(2);

                                    // Some XER files have a leading tab character, remove it
                                    if (fieldsLine.StartsWith("\t"))
                                    {
                                        fieldsLine = fieldsLine.Substring(1);
                                    }

                                    currentHeaders = FastSplit(fieldsLine, '\t');

                                    // Trim headers in-place
                                    for (int i = 0; i < currentHeaders.Length; i++)
                                    {
                                        currentHeaders[i] = currentHeaders[i].Trim();
                                    }

                                    if (currentTableData != null)
                                    {
                                        currentTableData.Add(new DataRow(currentHeaders, string.Empty));
                                    }
                                }
                                break;

                            case 'R': // Row
                                if (currentTable != null && currentHeaders != null && currentTableData != null)
                                {
                                    string dataLine = line.Substring(2);

                                    // Some XER files have a leading tab character, remove it
                                    if (dataLine.StartsWith("\t"))
                                    {
                                        dataLine = dataLine.Substring(1);
                                    }

                                    string[] values = FastSplit(dataLine, '\t');

                                    // Ensure correct array size without Array.Resize
                                    if (values.Length != currentHeaders.Length)
                                    {
                                        var newValues = new string[currentHeaders.Length];
                                        int copyLength = Math.Min(values.Length, currentHeaders.Length);
                                        Array.Copy(values, newValues, copyLength);

                                        // Fill remaining with empty strings
                                        for (int i = copyLength; i < currentHeaders.Length; i++)
                                        {
                                            newValues[i] = string.Empty;
                                        }
                                        values = newValues;
                                    }

                                    currentTableData.Add(new DataRow(values, filename));
                                }
                                break;
                        }
                    }
                }

                // Remove empty tables
                var emptyKeys = tables.Where(kvp => kvp.Value.Count <= 1).Select(kvp => kvp.Key).ToList();
                foreach (var key in emptyKeys)
                {
                    tables.Remove(key);
                }

                reportProgressAction?.Invoke(100, $"Finished parsing {filename}");
            }
            catch (Exception ex)
            {
                throw new Exception($"Error reading file {filename} at line {lineCount}. Bytes read: {bytesRead}.", ex);
            }

            return tables;
        }

        // High-performance string splitting
        private static string[] FastSplit(string text, char delimiter)
        {
            if (string.IsNullOrEmpty(text))
                return Array.Empty<string>();

            // Count delimiters to pre-allocate array
            int count = 1;
            for (int i = 0; i < text.Length; i++)
            {
                if (text[i] == delimiter)
                    count++;
            }

            string[] result = new string[count];
            int resultIndex = 0;
            int start = 0;

            for (int i = 0; i < text.Length; i++)
            {
                if (text[i] == delimiter)
                {
                    result[resultIndex++] = text.Substring(start, i - start);
                    start = i + 1;
                }
            }

            // Add the last segment
            result[resultIndex] = text.Substring(start);

            return result;
        }

        private void MergeParsedTables(Dictionary<string, List<DataRow>> mainTables, Dictionary<string, List<DataRow>> newTables)
        {
            foreach (var kvp in newTables)
            {
                string tableName = kvp.Key;
                List<DataRow> tableRows = kvp.Value;

                if (tableRows == null || tableRows.Count == 0) continue;

                if (!mainTables.ContainsKey(tableName))
                {
                    mainTables[tableName] = new List<DataRow>(tableRows);
                }
                else
                {
                    if (tableRows.Count > 1)
                    {
                        mainTables[tableName].AddRange(tableRows.Skip(1));
                    }
                }
            }
        }
        #endregion
        #region Core Logic - Exporting (Optimized)

        private List<string> GetAllTableNamesToExport()
        {
            var names = _extractedTables.Keys.ToList();
            if (_chkCreatePowerBiTables.Checked)
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
            }
            return names.Distinct().OrderBy(n => n).ToList();
        }

        private List<string> GetSelectedTableNamesToExport()
        {
            var names = lstTables.SelectedItems.Cast<string>().ToList();

            if (_chkCreatePowerBiTables.Checked)
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
            }
            return names.Distinct().OrderBy(n => n).ToList();
        }

        private async Task ExportTablesAsync(List<string> tablesToExport)
        {
            if (tablesToExport.Count == 0) { ShowError("No tables selected or available for export."); return; }
            if (string.IsNullOrEmpty(_outputDirectory) || !Directory.Exists(_outputDirectory)) { ShowError("Please select a valid output directory first."); return; }

            bool includePbiTables = _chkCreatePowerBiTables.Checked && tablesToExport.Any(IsEnhancedTableName);
            List<string> finalTablesToExport = new List<string>();
            List<string> skippedPbi = new List<string>();

            foreach (string tableName in tablesToExport)
            {
                if (_extractedTables.ContainsKey(tableName))
                {
                    finalTablesToExport.Add(tableName);
                }
                else if (IsEnhancedTableName(tableName))
                {
                    if (_chkCreatePowerBiTables.Checked)
                    {
                        bool canCreate = false;
                        switch (tableName)
                        {
                            case EnhancedTableNames.XerTask01: canCreate = _canCreateTask01; break;
                            case EnhancedTableNames.XerProject02: canCreate = _canCreateProject02; break;
                            case EnhancedTableNames.XerProjWbs03: canCreate = _canCreateProjWbs03; break;
                            case EnhancedTableNames.XerBaseline04: canCreate = _canCreateBaseline04; break;
                            case EnhancedTableNames.XerPredecessor06: canCreate = _canCreatePredecessor06; break;
                            case EnhancedTableNames.XerActvType07: canCreate = _canCreateActvType07; break;
                            case EnhancedTableNames.XerActvCode08: canCreate = _canCreateActvCode08; break;
                            case EnhancedTableNames.XerTaskActv09: canCreate = _canCreateTaskActv09; break;
                            case EnhancedTableNames.XerCalendar10: canCreate = _canCreateCalendar10; break;
                            case EnhancedTableNames.XerCalendarDetailed11: canCreate = _canCreateCalendarDetailed11; break;
                        }

                        if (canCreate)
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

            finalTablesToExport = finalTablesToExport.Distinct().ToList();

            if (skippedPbi.Any())
            {
                ShowWarning($"Cannot create requested Power BI tables due to missing dependencies: {string.Join(", ", skippedPbi.Distinct().OrderBy(n => n))}. Exporting available tables.");
            }

            if (finalTablesToExport.Count == 0)
            {
                ShowError("No tables available for export after checking selections and dependencies.");
                return;
            }

            SetUIEnabled(false);

            var progressForm = new ProgressForm("Exporting Tables to CSV...");
            var progress = new Progress<(int percent, string message)>(p =>
            {
                progressForm.UpdateProgress(p.message, p.percent);
            });

            try
            {
                progressForm.Show();

                var exportedFiles = await Task.Run(() => ExportTablesInternalAsync(finalTablesToExport, progress));

                UpdateStatus($"Successfully exported {exportedFiles.Count} tables.");

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
            catch (Exception ex)
            {
                ShowError($"Error exporting CSV files: {ex.Message}");
                UpdateStatus("Export failed.");
            }
            finally
            {
                progressForm.Close();
                SetUIEnabled(true);
            }
        }

        private List<string> ExportTablesInternalAsync(List<string> finalTablesToExport, IProgress<(int percent, string message)> progress)
        {
            List<string> exportedFiles = new List<string>();
            int totalTables = finalTablesToExport.Count;
            int exportedCount = 0;
            List<DataRow> task01Data = null;

            foreach (string tableName in finalTablesToExport.OrderBy(n => n))
            {
                exportedCount++;
                int progressPercent = (totalTables > 0) ? (int)((double)exportedCount / totalTables * 100) : 0;
                progress?.Report((progressPercent, $"Exporting table: {tableName} ({exportedCount}/{totalTables})"));

                string csvFilePath = Path.Combine(_outputDirectory, $"{tableName}.csv");

                try
                {
                    if (_extractedTables.ContainsKey(tableName))
                    {
                        WriteTableToCsv(_extractedTables[tableName], csvFilePath);
                        exportedFiles.Add(csvFilePath);
                    }
                    else if (IsEnhancedTableName(tableName))
                    {
                        List<DataRow> enhancedTableData = null;
                        switch (tableName)
                        {
                            case EnhancedTableNames.XerTask01:
                                enhancedTableData = Create01XerTaskTable(_extractedTables[TableNames.Task], _extractedTables[TableNames.Calendar], _extractedTables[TableNames.Project]);
                                task01Data = enhancedTableData;
                                break;
                            case EnhancedTableNames.XerProject02:
                                enhancedTableData = Create02XerProject(_extractedTables[TableNames.Project]);
                                break;
                            case EnhancedTableNames.XerProjWbs03:
                                enhancedTableData = Create03XerProjWbsTable(_extractedTables[TableNames.ProjWbs]);
                                break;
                            case EnhancedTableNames.XerBaseline04:
                                if (task01Data == null && _canCreateTask01)
                                {
                                    task01Data = Create01XerTaskTable(_extractedTables[TableNames.Task], _extractedTables[TableNames.Calendar], _extractedTables[TableNames.Project]);
                                }
                                if (task01Data != null)
                                {
                                    enhancedTableData = Create04XerBaselineTable(task01Data);
                                }
                                break;
                            case EnhancedTableNames.XerPredecessor06:
                                enhancedTableData = Create06XerPredecessor(_extractedTables[TableNames.TaskPred]);
                                break;
                            case EnhancedTableNames.XerActvType07:
                                enhancedTableData = Create07XerActvType(_extractedTables[TableNames.ActvType]);
                                break;
                            case EnhancedTableNames.XerActvCode08:
                                enhancedTableData = Create08XerActvCode(_extractedTables[TableNames.ActvCode]);
                                break;
                            case EnhancedTableNames.XerTaskActv09:
                                enhancedTableData = Create09XerTaskActv(_extractedTables[TableNames.TaskActv]);
                                break;
                            case EnhancedTableNames.XerCalendar10:
                                enhancedTableData = Create10XerCalendar(_extractedTables[TableNames.Calendar]);
                                break;
                            case EnhancedTableNames.XerCalendarDetailed11:
                                enhancedTableData = Create11XerCalendarDetailed(_extractedTables[TableNames.Calendar]);
                                break;
                        }

                        if (enhancedTableData != null && enhancedTableData.Count > 1)
                        {
                            WriteTableToCsv(enhancedTableData, csvFilePath);
                            exportedFiles.Add(csvFilePath);
                        }
                        else if (enhancedTableData == null)
                        {
                            Console.WriteLine($"Warning: Failed to create enhanced table {tableName} despite passing dependency check.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception($"Error exporting table '{tableName}': {ex.Message}", ex);
                }
            }

            return exportedFiles;
        }

        private void WriteTableToCsv(List<DataRow> tableData, string csvFilePath)
        {
            if (tableData == null || tableData.Count == 0)
                return;

            int BufferSize = PerformanceConfig.FileReadBufferSize;

            try
            {
                using (var fileStream = new FileStream(csvFilePath, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize))
                using (var writer = new StreamWriter(fileStream, Encoding.UTF8, BufferSize))
                {
                    string[] headerRow = tableData[0].Fields;
                    if (headerRow == null)
                        return;

                    // Use StringBuilder for better performance
                    var lineBuilder = new StringBuilder(PerformanceConfig.StringBuilderInitialCapacity);

                    // Write header
                    for (int i = 0; i < headerRow.Length; i++)
                    {
                        if (i > 0) lineBuilder.Append(',');
                        lineBuilder.Append(EscapeCsvFieldOptimized(headerRow[i]));
                    }
                    // Remove the extra comma here - just append the FileName header
                    if (headerRow.Length > 0) lineBuilder.Append(',');
                    lineBuilder.Append(EscapeCsvFieldOptimized(FieldNames.FileName));

                    writer.WriteLine(lineBuilder.ToString());
                    lineBuilder.Clear();

                    // Write data rows in batches
                    int BatchSize = PerformanceConfig.BatchProcessingSize;
                    var batch = new List<string>(BatchSize);

                    for (int i = 1; i < tableData.Count; i++)
                    {
                        string[] rowFields = tableData[i].Fields;
                        string filename = tableData[i].SourceFilename;

                        if (rowFields == null)
                            continue;

                        // Ensure correct field count
                        if (rowFields.Length != headerRow.Length)
                        {
                            var newFields = new string[headerRow.Length];
                            Array.Copy(rowFields, newFields, Math.Min(rowFields.Length, headerRow.Length));
                            for (int j = rowFields.Length; j < headerRow.Length; j++)
                            {
                                newFields[j] = string.Empty;
                            }
                            rowFields = newFields;
                        }

                        // Build CSV line
                        for (int j = 0; j < rowFields.Length; j++)
                        {
                            if (j > 0) lineBuilder.Append(',');
                            lineBuilder.Append(EscapeCsvFieldOptimized(rowFields[j] ?? string.Empty));
                        }
                        // Remove the extra comma here - just append the filename
                        if (rowFields.Length > 0) lineBuilder.Append(',');
                        lineBuilder.Append(EscapeCsvFieldOptimized(filename));

                        batch.Add(lineBuilder.ToString());
                        lineBuilder.Clear();

                        // Write batch
                        if (batch.Count >= BatchSize)
                        {
                            foreach (var line in batch)
                            {
                                writer.WriteLine(line);
                            }
                            batch.Clear();
                        }
                    }

                    // Write remaining lines
                    foreach (var line in batch)
                    {
                        writer.WriteLine(line);
                    }

                    writer.Flush();
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to write CSV file '{Path.GetFileName(csvFilePath)}'. {ex.Message}", ex);
            }
        }

        // Optimized CSV field escaping
        private static string EscapeCsvFieldOptimized(string field)
        {
            if (string.IsNullOrEmpty(field))
                return "";

            // Quick check for special characters
            bool needsQuotes = false;
            bool hasQuotes = false;

            for (int i = 0; i < field.Length; i++)
            {
                char c = field[i];
                if (c == ',' || c == '\r' || c == '\n')
                {
                    needsQuotes = true;
                    if (hasQuotes) break;
                }
                else if (c == '"')
                {
                    needsQuotes = true;
                    hasQuotes = true;
                }
            }

            // Check for leading/trailing spaces
            if (!needsQuotes && field.Length > 0 && (field[0] == ' ' || field[field.Length - 1] == ' '))
            {
                needsQuotes = true;
            }

            if (!needsQuotes)
                return field;

            if (hasQuotes)
            {
                var sb = new StringBuilder(field.Length + 10);
                sb.Append('"');
                foreach (char c in field)
                {
                    if (c == '"')
                        sb.Append("\"\"");
                    else
                        sb.Append(c);
                }
                sb.Append('"');
                return sb.ToString();
            }
            else
            {
                return $"\"{field}\"";
            }
        }
        #endregion

        #region Core Logic - Enhanced Tables (Optimized)

        private void CheckEnhancedTableDependencies()
        {
            bool hasTask = _extractedTables.ContainsKey(TableNames.Task);
            bool hasCalendar = _extractedTables.ContainsKey(TableNames.Calendar);
            bool hasProject = _extractedTables.ContainsKey(TableNames.Project);
            bool hasProjWbs = _extractedTables.ContainsKey(TableNames.ProjWbs);
            bool hasTaskActv = _extractedTables.ContainsKey(TableNames.TaskActv);
            bool hasActvCode = _extractedTables.ContainsKey(TableNames.ActvCode);
            bool hasActvType = _extractedTables.ContainsKey(TableNames.ActvType);
            bool hasTaskPred = _extractedTables.ContainsKey(TableNames.TaskPred);

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
        }

        private bool AnyPbiDependencyMet()
        {
            return _canCreateTask01 || _canCreateProjWbs03 || _canCreateProject02 ||
                   _canCreatePredecessor06 || _canCreateActvType07 || _canCreateActvCode08 ||
                   _canCreateTaskActv09 || _canCreateCalendar10 || _canCreateCalendarDetailed11;
        }

        private bool AllEnhancedDependenciesMet()
        {
            return _canCreateTask01 && _canCreateProjWbs03 && _canCreateProject02 &&
                   _canCreatePredecessor06 && _canCreateActvType07 && _canCreateActvCode08 &&
                   _canCreateTaskActv09 && _canCreateCalendar10 && _canCreateCalendarDetailed11;
        }

        private string GetMissingDependenciesMessage(string prefix)
        {
            var missing = new List<string>();

            if (!_canCreateTask01 && !_canCreateBaseline04)
            {
                if (!_extractedTables.ContainsKey(TableNames.Task)) missing.Add(TableNames.Task);
                if (!_extractedTables.ContainsKey(TableNames.Calendar)) missing.Add(TableNames.Calendar);
                if (!_extractedTables.ContainsKey(TableNames.Project)) missing.Add(TableNames.Project);
            }
            if (!_canCreateProjWbs03 && !_extractedTables.ContainsKey(TableNames.ProjWbs)) missing.Add(TableNames.ProjWbs);
            if (!_canCreateProject02 && !_extractedTables.ContainsKey(TableNames.Project)) missing.Add(TableNames.Project);
            if (!_canCreatePredecessor06 && !_extractedTables.ContainsKey(TableNames.TaskPred)) missing.Add(TableNames.TaskPred);
            if (!_canCreateActvType07 && !_extractedTables.ContainsKey(TableNames.ActvType)) missing.Add(TableNames.ActvType);
            if (!_canCreateActvCode08 && !_extractedTables.ContainsKey(TableNames.ActvCode)) missing.Add(TableNames.ActvCode);
            if (!_canCreateTaskActv09 && !_extractedTables.ContainsKey(TableNames.TaskActv)) missing.Add(TableNames.TaskActv);
            if (!_canCreateCalendar10 && !_canCreateCalendarDetailed11 && !_extractedTables.ContainsKey(TableNames.Calendar)) missing.Add(TableNames.Calendar);

            var distinctMissing = missing.Distinct().OrderBy(s => s).ToList();

            if (distinctMissing.Any())
            {
                return $"{prefix}\nMissing required source tables: {string.Join(", ", distinctMissing)}.";
            }

            return prefix;
        }

        private List<DataRow> Create01XerTaskTable(List<DataRow> taskTable, List<DataRow> calendarTable, List<DataRow> projectTable)
        {
            if (!IsTableValid(taskTable) || !IsTableValid(calendarTable) || !IsTableValid(projectTable))
                return null;

            try
            {
                var taskIndexes = GetFieldIndexes(taskTable[0].Fields);
                var calendarIndexes = GetFieldIndexes(calendarTable[0].Fields);
                var projectIndexes = GetFieldIndexes(projectTable[0].Fields);

                // Check if we have constraint fields
                bool hasConstraintSupport = taskIndexes.ContainsKey(FieldNames.CstrType) &&
                                           taskIndexes.ContainsKey(FieldNames.CstrDate);

                // Use dictionaries with initial capacity
                var calendarHours = new Dictionary<string, decimal>(calendarTable.Count, StringComparer.OrdinalIgnoreCase);
                var projectDataDates = new Dictionary<string, DateTime>(projectTable.Count, StringComparer.OrdinalIgnoreCase);

                // Build lookups efficiently
                BuildCalendarHoursLookupOptimized(calendarTable, calendarIndexes, calendarHours);
                BuildProjectDataDatesLookupOptimized(projectTable, projectIndexes, projectDataDates);

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

                // Pre-allocate with estimated capacity
                var finalTable = new List<DataRow>(taskTable.Count) { new DataRow(finalColumns, string.Empty) };

                // Cache string constants
                const string TK_Complete = "TK_Complete";
                const string TK_NotStart = "TK_NotStart";
                const string TK_Active = "TK_Active";

                // Process in batches to improve cache locality
                int BatchSize = PerformanceConfig.BatchProcessingSize;
                var transformedBatch = new List<DataRow>(BatchSize);

                for (int i = 1; i < taskTable.Count; i++)
                {
                    var sourceRowData = taskTable[i];
                    string[] row = sourceRowData.Fields;
                    if (row == null) continue;

                    string[] transformed = new string[finalColumns.Length];
                    string originalFilename = sourceRowData.SourceFilename;

                    // Extract frequently used values once
                    string projId = GetFieldValueOptimized(row, taskIndexes, FieldNames.ProjectId);
                    string taskId = GetFieldValueOptimized(row, taskIndexes, FieldNames.TaskId);
                    string wbsId = GetFieldValueOptimized(row, taskIndexes, FieldNames.WbsId);
                    string clndrId = GetFieldValueOptimized(row, taskIndexes, FieldNames.CalendarId);
                    string statusCode = GetFieldValueOptimized(row, taskIndexes, FieldNames.StatusCode);

                    // Parse date once
                    DateTime? actEndDate = null;
                    string actEndStr = GetFieldValueOptimized(row, taskIndexes, FieldNames.ActEndDate);
                    if (!string.IsNullOrWhiteSpace(actEndStr))
                    {
                        actEndDate = TryParseDateCached(actEndStr);
                    }

                    // Copy fields efficiently
                    CopyDirectFieldsOptimized(row, taskIndexes, transformed, finalColumns);

                    // Format dates
                    FormatDateFieldsOptimized(row, taskIndexes, transformed, finalColumns, actEndDate);

                    // Transform status code
                    SetTransformedField(transformed, finalColumns, FieldNames.StatusCode,
                        statusCode == TK_Complete ? "Complete" :
                        statusCode == TK_NotStart ? "Not Started" :
                        statusCode == TK_Active ? "In Progress" : statusCode);

                    // Calculate dates - use constraint-aware methods if constraint fields exist
                    DateTime startDate, finishDate;

                    if (hasConstraintSupport)
                    {
                        // Use NEW constraint-aware calculations
                        startDate = CalculateStartDateWithConstraints(row, taskIndexes, statusCode, originalFilename);
                        finishDate = CalculateFinishDateWithConstraints(row, taskIndexes, statusCode, originalFilename);
                    }
                    else
                    {
                        // Use original calculations if constraint fields not available
                        startDate = CalculateStartDateOptimized(row, taskIndexes, statusCode);
                        finishDate = CalculateFinishDateOptimized(row, taskIndexes, statusCode);
                    }

                    SetTransformedField(transformed, finalColumns, FieldNames.Start, FormatDateOutput(startDate));
                    SetTransformedField(transformed, finalColumns, FieldNames.Finish, FormatDateOutput(finishDate));

                    // Create ID_Name
                    string taskCode = GetFieldValueOptimized(row, taskIndexes, FieldNames.TaskCode);
                    string taskName = GetFieldValueOptimized(row, taskIndexes, FieldNames.TaskName);
                    SetTransformedField(transformed, finalColumns, FieldNames.IdName, $"{taskCode} - {taskName}");

                    // Calculate working days, original duration, and floats
                    string calendarKey = CreateKeyOptimized(originalFilename, clndrId);
                    if (!string.IsNullOrEmpty(clndrId) && calendarHours.TryGetValue(calendarKey, out decimal dayHrCnt) && dayHrCnt > 0)
                    {
                        // Calculate Remaining Working Days from remain_drtn_hr_cnt
                        SetTransformedField(transformed, finalColumns, FieldNames.RemainingWorkingDays,
                            CalculateDaysFromHoursOptimized(row, taskIndexes, FieldNames.RemainDurationHrCnt, dayHrCnt));

                        // Calculate Original Duration from target_drtn_hr_cnt
                        SetTransformedField(transformed, finalColumns, FieldNames.OriginalDuration,
                            CalculateDaysFromHoursOptimized(row, taskIndexes, FieldNames.TargetDurationHrCnt, dayHrCnt));

                        if (statusCode != TK_Complete)
                        {
                            SetTransformedField(transformed, finalColumns, FieldNames.TotalFloat,
                                CalculateDaysFromHoursOptimized(row, taskIndexes, FieldNames.TotalFloatHrCnt, dayHrCnt));
                            SetTransformedField(transformed, finalColumns, FieldNames.FreeFloat,
                                CalculateDaysFromHoursOptimized(row, taskIndexes, FieldNames.FreeFloatHrCnt, dayHrCnt));
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

                    // Calculate completion percentage
                    double pct = CalculateCompletionPercentageOptimized(row, taskIndexes, statusCode);
                    SetTransformedField(transformed, finalColumns, FieldNames.PercentComplete,
                        pct.ToString("F2", CultureInfo.InvariantCulture));

                    // Set data date
                    string projectKey = CreateKeyOptimized(originalFilename, projId);
                    if (!string.IsNullOrEmpty(projId) && projectDataDates.TryGetValue(projectKey, out DateTime dataDateValue))
                    {
                        SetTransformedField(transformed, finalColumns, FieldNames.DataDate, FormatDateOutput(dataDateValue));
                    }
                    else
                    {
                        SetTransformedField(transformed, finalColumns, FieldNames.DataDate, "");
                    }

                    // Create keys
                    SetTransformedField(transformed, finalColumns, FieldNames.WbsIdKey, CreateKeyOptimized(originalFilename, wbsId));
                    SetTransformedField(transformed, finalColumns, FieldNames.TaskIdKey, CreateKeyOptimized(originalFilename, taskId));
                    SetTransformedField(transformed, finalColumns, FieldNames.CalendarIdKey, CreateKeyOptimized(originalFilename, clndrId));
                    SetTransformedField(transformed, finalColumns, FieldNames.ProjIdKey, CreateKeyOptimized(originalFilename, projId));

                    // Ensure no nulls
                    for (int k = 0; k < transformed.Length; k++)
                    {
                        transformed[k] = transformed[k] ?? string.Empty;
                    }

                    transformedBatch.Add(new DataRow(transformed, originalFilename));

                    // Add batch to final table
                    if (transformedBatch.Count >= BatchSize)
                    {
                        finalTable.AddRange(transformedBatch);
                        transformedBatch.Clear();
                    }
                }

                // Add remaining items
                if (transformedBatch.Count > 0)
                {
                    finalTable.AddRange(transformedBatch);
                }

                return finalTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {EnhancedTableNames.XerTask01}: {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        private List<DataRow> Create04XerBaselineTable(List<DataRow> task01Table)
        {
            if (!IsTableValid(task01Table))
            {
                Console.WriteLine($"Warning: Cannot create {EnhancedTableNames.XerBaseline04} because the input 01_XER_TASK table is invalid or empty.");
                return null;
            }

            try
            {
                var baselineTable = new List<DataRow>();
                string[] headers = task01Table[0].Fields;
                if (headers == null)
                {
                    Console.WriteLine($"Warning: Cannot create {EnhancedTableNames.XerBaseline04} - input table has null headers.");
                    return null;
                }
                baselineTable.Add(new DataRow(headers, string.Empty));

                int dataDateIndex = Array.IndexOf(headers, FieldNames.DataDate);
                if (dataDateIndex == -1)
                {
                    Console.WriteLine($"Warning: Cannot create {EnhancedTableNames.XerBaseline04} - required '{FieldNames.DataDate}' column not found in input table.");
                    return null;
                }

                DateTime? minDataDate = null;
                for (int i = 1; i < task01Table.Count; i++)
                {
                    string dateStr = GetFieldValueSafe(task01Table[i].Fields, dataDateIndex);
                    if (TryParseDateOptimized(dateStr, out DateTime currentDate))
                    {
                        if (!minDataDate.HasValue || currentDate.Date < minDataDate.Value.Date)
                        {
                            minDataDate = currentDate.Date;
                        }
                    }
                }

                if (!minDataDate.HasValue)
                {
                    Console.WriteLine($"Warning: Cannot create {EnhancedTableNames.XerBaseline04} - no valid Data Dates found to determine the earliest baseline.");
                    return null;
                }

                for (int i = 1; i < task01Table.Count; i++)
                {
                    var sourceTask01Row = task01Table[i];
                    var sourceFields = sourceTask01Row.Fields;
                    if (sourceFields == null) continue;

                    string dateStr = GetFieldValueSafe(sourceFields, dataDateIndex);
                    if (TryParseDateOptimized(dateStr, out DateTime rowDate) && rowDate.Date == minDataDate.Value)
                    {
                        baselineTable.Add(new DataRow(sourceFields, sourceTask01Row.SourceFilename));
                    }
                }

                if (baselineTable.Count <= 1)
                {
                    Console.WriteLine($"Warning: No task rows found matching the earliest Data Date ({FormatDateOutput(minDataDate)}) for {EnhancedTableNames.XerBaseline04}. Resulting table will be empty.");
                }

                return baselineTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {EnhancedTableNames.XerBaseline04}: {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        private List<DataRow> Create03XerProjWbsTable(List<DataRow> projwbsTable)
        {
            if (!IsTableValid(projwbsTable))
            {
                Console.WriteLine($"Warning: Cannot create {EnhancedTableNames.XerProjWbs03} due to invalid or empty PROJWBS input table.");
                return null;
            }
            try
            {
                var sourceHeaders = projwbsTable[0].Fields;
                if (sourceHeaders == null)
                {
                    Console.WriteLine($"Warning: Cannot create {EnhancedTableNames.XerProjWbs03} - input table has null headers.");
                    return null;
                }
                var idx = GetFieldIndexes(sourceHeaders);
                var finalHeadersList = sourceHeaders.ToList();
                finalHeadersList.Add(FieldNames.WbsIdKey);
                finalHeadersList.Add(FieldNames.ParentWbsIdKey);
                string[] finalHeaders = finalHeadersList.ToArray();
                var result = new List<DataRow>(projwbsTable.Count) { new DataRow(finalHeaders, string.Empty) };

                // First pass: Create transformed rows and collect all WbsIdKeys
                var allWbsIdKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var transformedRows = new List<DataRow>(projwbsTable.Count - 1);

                for (int i = 1; i < projwbsTable.Count; i++)
                {
                    var sourceProjWbsRow = projwbsTable[i];
                    var row = sourceProjWbsRow.Fields;
                    if (row == null) continue;
                    var transformed = new string[finalHeaders.Length];
                    string originalFilename = sourceProjWbsRow.SourceFilename;
                    string projId = GetFieldValueOptimized(row, idx, FieldNames.ProjectId);
                    string wbsId = GetFieldValueOptimized(row, idx, FieldNames.WbsId);
                    string parentId = GetFieldValueOptimized(row, idx, FieldNames.ParentWbsId);
                    string wbsIdKey = CreateKeyOptimized(originalFilename, wbsId);
                    string parentWbsIdKey = CreateKeyOptimized(originalFilename, parentId);

                    // Add to our collection of valid WbsIdKeys
                    allWbsIdKeys.Add(wbsIdKey);

                    for (int k = 0; k < sourceHeaders.Length; k++)
                    {
                        transformed[k] = GetFieldValueSafe(row, k);
                    }
                    SetTransformedField(transformed, finalHeaders, FieldNames.WbsIdKey, wbsIdKey);
                    SetTransformedField(transformed, finalHeaders, FieldNames.ParentWbsIdKey, parentWbsIdKey);

                    for (int k = 0; k < transformed.Length; k++)
                    {
                        transformed[k] = transformed[k] ?? string.Empty;
                    }
                    transformedRows.Add(new DataRow(transformed, originalFilename));
                }

                // Second pass: Validate ParentWbsIdKeys and nullify invalid references
                foreach (var dataRow in transformedRows)
                {
                    var fields = dataRow.Fields;
                    int parentKeyIndex = Array.IndexOf(finalHeaders, FieldNames.ParentWbsIdKey);

                    if (parentKeyIndex >= 0)
                    {
                        string parentKey = fields[parentKeyIndex];
                        // If parent key is not empty and not found in our valid keys, set it to empty
                        if (!string.IsNullOrEmpty(parentKey) && !allWbsIdKeys.Contains(parentKey))
                        {
                            fields[parentKeyIndex] = string.Empty;
                        }
                    }

                    // Add the validated row to result
                    result.Add(dataRow);
                }

                return result;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {EnhancedTableNames.XerProjWbs03}: {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        private List<DataRow> CreateSimpleKeyedTable(List<DataRow> sourceTable, string newTableName, List<Tuple<string, string>> keyMappings)
        {
            if (!IsTableValid(sourceTable))
            {
                Console.WriteLine($"Warning: Cannot create {newTableName} - input table is invalid or empty.");
                return null;
            }
            if (keyMappings == null || !keyMappings.Any())
            {
                Console.WriteLine($"Warning: Cannot create {newTableName} - no key mappings provided.");
                return null;
            }

            try
            {
                var sourceHeaders = sourceTable[0].Fields;
                if (sourceHeaders == null)
                {
                    Console.WriteLine($"Warning: Cannot create {newTableName} - input table has null headers.");
                    return null;
                }

                var sourceIndexes = GetFieldIndexes(sourceHeaders);

                foreach (var mapping in keyMappings)
                {
                    if (!sourceIndexes.ContainsKey(mapping.Item2))
                    {
                        Console.WriteLine($"Warning: Cannot create {newTableName} because source column '{mapping.Item2}' for key '{mapping.Item1}' not found in the input table.");
                        return null;
                    }
                }

                // Define final columns explicitly
                var finalHeadersList = sourceHeaders.ToList();
                foreach (var mapping in keyMappings)
                {
                    finalHeadersList.Add(mapping.Item1);
                }
                string[] finalHeaders = finalHeadersList.ToArray();

                var result = new List<DataRow>(sourceTable.Count) { new DataRow(finalHeaders, string.Empty) };

                for (int i = 1; i < sourceTable.Count; i++)
                {
                    var sourceRow = sourceTable[i];
                    var row = sourceRow.Fields;
                    if (row == null) continue;

                    // Create a new transformed row array
                    string[] transformed = new string[finalHeaders.Length];
                    string originalFilename = sourceRow.SourceFilename;

                    // Copy all direct fields first
                    for (int k = 0; k < sourceHeaders.Length; k++)
                    {
                        transformed[k] = GetFieldValueSafe(row, k);
                    }

                    // Set key fields using the SetTransformedField method
                    foreach (var mapping in keyMappings)
                    {
                        string sourceValue = GetFieldValueOptimized(row, sourceIndexes, mapping.Item2);
                        SetTransformedField(transformed, finalHeaders, mapping.Item1, CreateKeyOptimized(originalFilename, sourceValue));
                    }

                    // Handle null values
                    for (int k = 0; k < transformed.Length; k++)
                    {
                        transformed[k] = transformed[k] ?? string.Empty;
                    }

                    result.Add(new DataRow(transformed, originalFilename));
                }

                return result;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {newTableName}: {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        private List<DataRow> Create06XerPredecessor(List<DataRow> taskPredTable)
        {
            return CreateSimpleKeyedTable(taskPredTable, EnhancedTableNames.XerPredecessor06,
                new List<Tuple<string, string>> {
            Tuple.Create(FieldNames.TaskIdKey, FieldNames.TaskId),
            Tuple.Create(FieldNames.PredTaskIdKey, FieldNames.PredTaskId)
                });
        }

        private List<DataRow> Create02XerProject(List<DataRow> projectTable)
        {
            return CreateSimpleKeyedTable(projectTable, EnhancedTableNames.XerProject02,
                new List<Tuple<string, string>> {
                    Tuple.Create(FieldNames.ProjIdKey, FieldNames.ProjectId)
                });
        }

        private List<DataRow> Create07XerActvType(List<DataRow> actvTypeTable)
        {
            return CreateSimpleKeyedTable(actvTypeTable, EnhancedTableNames.XerActvType07,
                new List<Tuple<string, string>> {
                    Tuple.Create(FieldNames.ActvCodeTypeIdKey, FieldNames.ActvCodeTypeId)
                });
        }

        private List<DataRow> Create08XerActvCode(List<DataRow> actvCodeTable)
        {
            return CreateSimpleKeyedTable(actvCodeTable, EnhancedTableNames.XerActvCode08,
                new List<Tuple<string, string>> {
                    Tuple.Create(FieldNames.ActvCodeIdKey, FieldNames.ActvCodeId),
                    Tuple.Create(FieldNames.ActvCodeTypeIdKey, FieldNames.ActvCodeTypeId)
                });
        }

        private List<DataRow> Create09XerTaskActv(List<DataRow> taskActvTable)
        {
            return CreateSimpleKeyedTable(taskActvTable, EnhancedTableNames.XerTaskActv09,
                new List<Tuple<string, string>> {
                    Tuple.Create(FieldNames.ActvCodeIdKey, FieldNames.ActvCodeId),
                    Tuple.Create(FieldNames.TaskIdKey, FieldNames.TaskId)
                });
        }

        private List<DataRow> Create10XerCalendar(List<DataRow> calendarTable)
        {
            return CreateSimpleKeyedTable(calendarTable, EnhancedTableNames.XerCalendar10,
                new List<Tuple<string, string>> {
                    Tuple.Create(FieldNames.ClndrIdKey, FieldNames.ClndrId)
                });
        }

        private List<DataRow> Create11XerCalendarDetailed(List<DataRow> calendarTable)
        {
            if (!IsTableValid(calendarTable))
            {
                Console.WriteLine($"Warning: Cannot create {EnhancedTableNames.XerCalendarDetailed11} - input CALENDAR table is invalid or empty.");
                return null;
            }

            try
            {
                var sourceHeaders = calendarTable[0].Fields;
                if (sourceHeaders == null)
                {
                    Console.WriteLine($"Warning: Cannot create {EnhancedTableNames.XerCalendarDetailed11} - input table has null headers.");
                    return null;
                }

                var sourceIndexes = GetFieldIndexes(sourceHeaders);

                // Check required fields
                if (!sourceIndexes.ContainsKey(FieldNames.ClndrId) || !sourceIndexes.ContainsKey(FieldNames.CalendarData))
                {
                    Console.WriteLine($"Warning: Cannot create {EnhancedTableNames.XerCalendarDetailed11} - missing required columns.");
                    return null;
                }

                // Define the detailed table columns
                string[] detailedColumns = {
                    FieldNames.ClndrId,
                    FieldNames.CalendarName,
                    FieldNames.CalendarType,
                    FieldNames.Date,
                    FieldNames.DayOfWeek,
                    FieldNames.WorkingDay,
                    FieldNames.WorkHours,
                    FieldNames.ExceptionType,
                    FieldNames.FileName,
                    FieldNames.ClndrIdKey
                };

                var result = new List<DataRow> { new DataRow(detailedColumns, string.Empty) };

                // Process each calendar
                for (int i = 1; i < calendarTable.Count; i++)
                {
                    var sourceRow = calendarTable[i];
                    var row = sourceRow.Fields;
                    if (row == null) continue;

                    string originalFilename = sourceRow.SourceFilename;
                    string clndrId = GetFieldValueOptimized(row, sourceIndexes, FieldNames.ClndrId);
                    string clndrName = GetFieldValueOptimized(row, sourceIndexes, FieldNames.CalendarName);
                    string clndrType = GetFieldValueOptimized(row, sourceIndexes, FieldNames.CalendarType);
                    string clndrData = GetFieldValueOptimized(row, sourceIndexes, FieldNames.CalendarData);
                    string dayHrCnt = GetFieldValueOptimized(row, sourceIndexes, FieldNames.DayHourCount);

                    string clndrIdKey = CreateKeyOptimized(originalFilename, clndrId);

                    // Parse the calendar data
                    var calendarEntries = ParseCalendarData(clndrData, dayHrCnt);

                    // Create a row for each calendar entry
                    foreach (var entry in calendarEntries)
                    {
                        string[] detailedRow = new string[detailedColumns.Length];

                        SetTransformedField(detailedRow, detailedColumns, FieldNames.ClndrId, clndrId);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.CalendarName, clndrName);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.CalendarType, clndrType);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.Date, entry.Date);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.DayOfWeek, entry.DayOfWeek);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.WorkingDay, entry.WorkingDay);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.WorkHours, entry.WorkHours);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.ExceptionType, entry.ExceptionType);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.FileName, originalFilename);
                        SetTransformedField(detailedRow, detailedColumns, FieldNames.ClndrIdKey, clndrIdKey);

                        // Ensure no nulls
                        for (int k = 0; k < detailedRow.Length; k++)
                        {
                            detailedRow[k] = detailedRow[k] ?? string.Empty;
                        }

                        result.Add(new DataRow(detailedRow, originalFilename));
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {EnhancedTableNames.XerCalendarDetailed11}: {ex.Message}\n{ex.StackTrace}");
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
            var entries = new List<CalendarEntry>();

            // If no calendar data, return default work week
            if (string.IsNullOrWhiteSpace(clndrData))
            {
                return GetDefaultWorkWeek(defaultDayHours);
            }

            try
            {
                // This is the specific P6 format from your data
                if (clndrData.Contains("CalendarData") && clndrData.Contains("DaysOfWeek"))
                {
                    entries = ParseP6CalendarFormat(clndrData, defaultDayHours);
                }
                else
                {
                    // Fallback to default if format is not recognized
                    entries = GetDefaultWorkWeek(defaultDayHours);
                }

                // If no valid entries were parsed, return default work week
                if (entries.Count == 0)
                {
                    entries = GetDefaultWorkWeek(defaultDayHours);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing calendar data: {ex.Message}. Using default work week.");
                entries = GetDefaultWorkWeek(defaultDayHours);
            }

            return entries;
        }

        private List<CalendarEntry> ParseP6CalendarFormat(string clndrData, string defaultDayHours)
        {
            var entries = new List<CalendarEntry>();

            try
            {
                // Clean Unicode data
                clndrData = CleanUnicodeData(clndrData);

                // Parse DaysOfWeek section
                int daysStart = clndrData.IndexOf("DaysOfWeek");
                if (daysStart > -1)
                {
                    int daysEnd = clndrData.IndexOf("VIEW", daysStart);
                    if (daysEnd == -1) daysEnd = clndrData.IndexOf("Exceptions", daysStart);
                    if (daysEnd > -1)
                    {
                        string daysSection = clndrData.Substring(daysStart, daysEnd - daysStart);

                        // Parse each day (1=Sunday through 7=Saturday)
                        for (int dayNum = 1; dayNum <= 7; dayNum++)
                        {
                            string dayName = GetDayName(dayNum);
                            string dayPattern = $@"\(0\|\|{dayNum}\(\)\s*\((.*?)\)\s*\)";
                            var dayMatch = System.Text.RegularExpressions.Regex.Match(daysSection, dayPattern, System.Text.RegularExpressions.RegexOptions.Singleline);

                            if (dayMatch.Success)
                            {
                                string dayContent = dayMatch.Groups[1].Value.Trim();

                                // Debug output
                                if (!string.IsNullOrWhiteSpace(dayContent))
                                {
                                    System.Diagnostics.Debug.WriteLine($"Day {dayNum} ({dayName}) content: {dayContent.Substring(0, Math.Min(50, dayContent.Length))}...");
                                }

                                decimal totalHours = ParseDayWorkHours(dayContent);

                                entries.Add(new CalendarEntry
                                {
                                    Date = "",
                                    DayOfWeek = dayName,
                                    WorkingDay = totalHours > 0 ? "Y" : "N",
                                    WorkHours = totalHours.ToString("0.##"),
                                    ExceptionType = "Standard"
                                });
                            }
                        }
                    }
                }

                // If no days found, use defaults
                if (entries.Count == 0)
                {
                    entries.AddRange(GetDefaultWorkWeek(defaultDayHours));
                }

                // Parse Exceptions section
                int excStart = clndrData.IndexOf("Exceptions");
                if (excStart > -1)
                {
                    string excSection = clndrData.Substring(excStart);
                    string excPattern = @"\(0\|\|(\d+)\(d\|(\d+)\)\s*\((.*?)\)\s*\)";
                    var excMatches = System.Text.RegularExpressions.Regex.Matches(excSection, excPattern, System.Text.RegularExpressions.RegexOptions.Singleline);

                    foreach (System.Text.RegularExpressions.Match excMatch in excMatches)
                    {
                        if (excMatch.Success)
                        {
                            string dateSerialStr = excMatch.Groups[2].Value;
                            string timeContent = excMatch.Groups[3].Value;

                            if (int.TryParse(dateSerialStr, out int dateSerial))
                            {
                                DateTime excDate = ConvertFromOleDate(dateSerial);
                                decimal hours = ParseDayWorkHours(timeContent);

                                entries.Add(new CalendarEntry
                                {
                                    Date = excDate.ToString("yyyy-MM-dd"),
                                    DayOfWeek = excDate.DayOfWeek.ToString(),
                                    WorkingDay = hours > 0 ? "Y" : "N",
                                    WorkHours = hours.ToString("0.##"),
                                    ExceptionType = hours > 0 ? "Exception - Working" : "Holiday"
                                });
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error parsing calendar: {ex.Message}");
            }

            return entries;
        }

        // Clean Unicode data - replace all control characters with spaces
        private string CleanUnicodeData(string data)
        {
            if (string.IsNullOrEmpty(data))
                return data;

            // Create a new string builder
            var cleaned = new System.Text.StringBuilder();

            foreach (char c in data)
            {
                // Keep printable ASCII characters and common symbols
                if (c >= 32 && c <= 126) // Standard ASCII printable characters
                {
                    cleaned.Append(c);
                }
                else if (c == '\r' || c == '\n' || c == '\t') // Keep standard whitespace
                {
                    cleaned.Append(' ');
                }
                else if (char.IsLetterOrDigit(c) || char.IsPunctuation(c) || char.IsSymbol(c))
                {
                    cleaned.Append(c);
                }
                else
                {
                    // Replace all other characters (including Unicode control chars) with space
                    cleaned.Append(' ');
                }
            }

            // Now normalize multiple spaces to single space where needed
            string result = cleaned.ToString();

            // But preserve the multiple spaces between ) and ( in exceptions
            // Just normalize spaces within other contexts
            result = System.Text.RegularExpressions.Regex.Replace(result, @"(\|\|)\s+", "$1");
            result = System.Text.RegularExpressions.Regex.Replace(result, @"\s+(\|\|)", "$1");

            return result;
        }

        // Parse work hours from time slot content
        private decimal ParseDayWorkHours(string content)
        {
            if (string.IsNullOrWhiteSpace(content))
                return 0;

            decimal totalHours = 0;

            // Try multiple patterns to match different formats
            var patterns = new[]
            {
        // Pattern 1: Full format with parentheses
        @"\(0\|\|\d+\s*\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)\s*\(\s*\)\s*\)",
        // Pattern 2: Without the trailing empty parentheses
        @"\(0\|\|\d+\s*\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)\s*\)",
        // Pattern 3: Just the time portion
        @"([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})"
    };

            bool foundMatch = false;

            foreach (var pattern in patterns)
            {
                var matches = System.Text.RegularExpressions.Regex.Matches(content, pattern);

                foreach (System.Text.RegularExpressions.Match match in matches)
                {
                    if (match.Groups.Count >= 5)
                    {
                        foundMatch = true;
                        string type1 = match.Groups[1].Value;
                        string time1 = match.Groups[2].Value;
                        string type2 = match.Groups[3].Value;
                        string time2 = match.Groups[4].Value;

                        // Debug output
                        System.Diagnostics.Debug.WriteLine($"Found time slot: {type1}|{time1}|{type2}|{time2}");

                        if (TimeSpan.TryParse(time1, out TimeSpan t1) && TimeSpan.TryParse(time2, out TimeSpan t2))
                        {
                            TimeSpan start, end;

                            // Determine start and end times based on 's' (start) and 'f' (finish)
                            if (type1 == "s" && type2 == "f")
                            {
                                start = t1;
                                end = t2;
                            }
                            else if (type1 == "f" && type2 == "s")
                            {
                                start = t2;
                                end = t1;
                            }
                            else
                            {
                                // If both are same type, assume first is start
                                start = t1;
                                end = t2;
                            }

                            // Special case: 00:00 to 00:00 with both 's' or both 'f' means no work
                            if (start == TimeSpan.Zero && end == TimeSpan.Zero && type1 == type2)
                            {
                                continue;
                            }

                            // Calculate hours
                            decimal hours = 0;

                            if (end > start)
                            {
                                hours = (decimal)(end - start).TotalHours;
                            }
                            else if (start > end)
                            {
                                // Overnight shift (e.g., 23:00 to 03:00)
                                hours = (decimal)(TimeSpan.FromHours(24) - start).TotalHours;
                                hours += (decimal)end.TotalHours;
                            }
                            else if (start == end && start != TimeSpan.Zero)
                            {
                                // Same non-zero time could mean 24 hours
                                hours = 24;
                            }

                            totalHours += hours;
                            System.Diagnostics.Debug.WriteLine($"Calculated hours: {hours}, Total: {totalHours}");
                        }
                    }
                }

                // If we found matches with this pattern, don't try others
                if (foundMatch) break;
            }

            // If no matches found, log the content for debugging
            if (!foundMatch && !string.IsNullOrWhiteSpace(content))
            {
                System.Diagnostics.Debug.WriteLine($"No time matches found in: {content.Substring(0, Math.Min(100, content.Length))}");
            }

            return totalHours;
        }

        // Add this debug method to test work hours parsing
        private void TestWorkHoursParsing()
        {
            // Test cases
            var testCases = new[]
            {
        // Standard work day
        "(0||0(s|08:00|f|12:00)()) (0||1(s|13:00|f|17:00)())",
        // Single slot
        "(0||0(s|07:00|f|17:00)())",
        // Night shift
        "(0||0(s|23:00|f|03:00)())",
        // Non-working
        "(0||0(s|00:00|f|00:00)())"
    };

            foreach (var testCase in testCases)
            {
                decimal hours = ParseDayWorkHours(testCase);
                System.Diagnostics.Debug.WriteLine($"Test: {testCase}");
                System.Diagnostics.Debug.WriteLine($"Result: {hours} hours");
                System.Diagnostics.Debug.WriteLine("---");
            }
        }

        private DateTime ConvertFromOleDate(int oleDate)
        {
            try
            {
                // Handle the Excel/Lotus 1-2-3 leap year bug
                if (oleDate < 1)
                {
                    return new DateTime(1900, 1, 1);
                }
                else if (oleDate < 60)
                {
                    // Before March 1, 1900
                    return new DateTime(1899, 12, 30).AddDays(oleDate);
                }
                else
                {
                    // On or after March 1, 1900 - adjust for the leap year bug
                    return new DateTime(1899, 12, 30).AddDays(oleDate - 1);
                }
            }
            catch
            {
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

        private List<CalendarEntry> GetDefaultWorkWeek(string defaultDayHours)
        {
            var entries = new List<CalendarEntry>();
            decimal defaultHours = 8;

            if (!string.IsNullOrEmpty(defaultDayHours) && decimal.TryParse(defaultDayHours, out decimal parsed))
            {
                defaultHours = parsed;
            }

            bool is24x7 = defaultHours == 12;

            var days = new[]
            {
        ("Sunday", is24x7 ? "Y" : "N", is24x7 ? "12" : "0"),
        ("Monday", "Y", defaultHours.ToString()),
        ("Tuesday", "Y", defaultHours.ToString()),
        ("Wednesday", "Y", defaultHours.ToString()),
        ("Thursday", "Y", defaultHours.ToString()),
        ("Friday", "Y", defaultHours.ToString()),
        ("Saturday", is24x7 ? "Y" : "N", is24x7 ? "12" : "0")
    };

            foreach (var (day, working, hours) in days)
            {
                entries.Add(new CalendarEntry
                {
                    Date = "",
                    DayOfWeek = day,
                    WorkingDay = working,
                    WorkHours = hours,
                    ExceptionType = "Standard"
                });
            }

            return entries;
        }

        private bool IsTableValid(List<DataRow> table) => table != null && table.Count > 1 && table[0].Fields != null;

        // Optimized key creation with string interning for common values
        private static string CreateKeyOptimized(string filename, string value)
        {
            if (string.IsNullOrEmpty(filename) || string.IsNullOrEmpty(value))
                return string.Empty;

            string key = $"{filename.Trim()}.{value.Trim()}";

            // Use string interning for frequently used keys
            if (PerformanceConfig.EnableStringInterning)
            {
                return KeyCache.GetOrAdd(key, k => string.Intern(k));
            }
            return key;
        }

        private void BuildCalendarHoursLookupOptimized(List<DataRow> calendarTable, Dictionary<string, int> indexes, Dictionary<string, decimal> lookup)
        {
            if (!IsTableValid(calendarTable)) return;

            if (!indexes.ContainsKey(FieldNames.ClndrId) || !indexes.ContainsKey(FieldNames.DayHourCount))
            {
                Console.WriteLine($"Warning: Cannot build calendar lookup - missing '{FieldNames.ClndrId}' or '{FieldNames.DayHourCount}' column.");
                return;
            }

            for (int i = 1; i < calendarTable.Count; i++)
            {
                var row = calendarTable[i].Fields;
                if (row == null) continue;

                string originalFilename = calendarTable[i].SourceFilename;
                string clndrId = GetFieldValueOptimized(row, indexes, FieldNames.ClndrId);
                string dayHrCntStr = GetFieldValueOptimized(row, indexes, FieldNames.DayHourCount);

                if (!string.IsNullOrEmpty(clndrId) &&
                    decimal.TryParse(dayHrCntStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal dayHrCnt) &&
                    dayHrCnt > 0)
                {
                    string compositeKey = CreateKeyOptimized(originalFilename, clndrId);
                    lookup[compositeKey] = dayHrCnt;
                }
            }
        }

        private void BuildProjectDataDatesLookupOptimized(List<DataRow> projectTable, Dictionary<string, int> indexes, Dictionary<string, DateTime> lookup)
        {
            if (!IsTableValid(projectTable)) return;

            if (!indexes.ContainsKey(FieldNames.ProjectId) || !indexes.ContainsKey(FieldNames.LastRecalcDate))
            {
                Console.WriteLine($"Warning: Cannot build project data date lookup - missing '{FieldNames.ProjectId}' or '{FieldNames.LastRecalcDate}' column.");
                return;
            }

            for (int i = 1; i < projectTable.Count; i++)
            {
                var row = projectTable[i].Fields;
                if (row == null) continue;

                string originalFilename = projectTable[i].SourceFilename;
                string projId = GetFieldValueOptimized(row, indexes, FieldNames.ProjectId);
                string lastRecalcDateStr = GetFieldValueOptimized(row, indexes, FieldNames.LastRecalcDate);

                if (!string.IsNullOrEmpty(projId) && TryParseDateOptimized(lastRecalcDateStr, out DateTime dataDate))
                {
                    string compositeKey = CreateKeyOptimized(originalFilename, projId);
                    lookup[compositeKey] = dataDate;
                }
            }
        }

        private Dictionary<string, int> GetFieldIndexes(string[] headers)
        {
            var dict = new Dictionary<string, int>(headers.Length * 2, StringComparer.OrdinalIgnoreCase);
            if (headers == null) return dict;

            for (int i = 0; i < headers.Length; i++)
            {
                if (!string.IsNullOrEmpty(headers[i]) && !dict.ContainsKey(headers[i]))
                {
                    dict[headers[i]] = i;
                }
                else if (dict.ContainsKey(headers[i]))
                {
                    Console.WriteLine($"Warning: Duplicate header column found: '{headers[i]}'. Using index of first occurrence.");
                }
            }
            return dict;
        }

        // Optimized field value retrieval
        private static string GetFieldValueOptimized(string[] row, Dictionary<string, int> indexes, string fieldName)
        {
            if (row == null || indexes == null) return "";

            if (indexes.TryGetValue(fieldName, out int index) && index >= 0 && index < row.Length)
            {
                return row[index] ?? "";
            }
            return "";
        }

        private string GetFieldValueSafe(string[] row, int index)
        {
            if (row == null) return "";

            if (index >= 0 && index < row.Length)
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

        // Cached date parsing
        private static DateTime? TryParseDateCached(string dateStr)
        {
            if (string.IsNullOrWhiteSpace(dateStr))
                return null;

            return DateCache.GetOrAdd(dateStr, str =>
            {
                // Define the date formats to try - PUT YOUR FORMAT FIRST
                string[] formats = {
            "d/M/yyyy",        // Handles "3/9/2025" and "15/04/2026"
            "dd/MM/yyyy",      // Handles "15/04/2026"
            "M/d/yyyy",        // US format fallback
            "MM/dd/yyyy",      // US format fallback
            "yyyy-MM-dd",      // ISO format
            "dd-MMM-yy",       // P6 format like "15-APR-26"
            "dd-MMM-yyyy"      // P6 format like "15-APR-2026"
        };

                // Try parsing with explicit formats first
                if (DateTime.TryParseExact(str, formats, CultureInfo.InvariantCulture,
                    DateTimeStyles.None, out DateTime result))
                {
                    if (result.Year > 1900)
                        return result;
                }

                // If that fails, try general parse as last resort
                if (DateTime.TryParse(str, CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
                {
                    if (result.Year > 1900)
                        return result;
                }

                return null;
            });
        }

        private static bool TryParseDateOptimized(string dateStr, out DateTime result)
        {
            result = DateTime.MinValue;
            var cached = TryParseDateCached(dateStr);
            if (cached.HasValue)
            {
                result = cached.Value;
                return true;
            }
            return false;
        }

        private string FormatDateOutput(DateTime? date)
        {
            return date.HasValue && date.Value != DateTime.MinValue ? date.Value.ToString("yyyy-MM-dd") : "";
        }

        private string FormatDateOutput(DateTime date)
        {
            return date != DateTime.MinValue ? date.ToString("yyyy-MM-dd") : "";
        }

        private void FormatDateFieldsOptimized(string[] sourceRow, Dictionary<string, int> sourceIndexes, string[] transformedRow, string[] finalColumns, DateTime? actEndDate)
        {
            // Handle simple date formatting
            string[] directFormatCols = {
        FieldNames.CstrDate,
        FieldNames.TargetStartDate,
        FieldNames.TargetEndDate,
        FieldNames.ActStartDate,
        FieldNames.EarlyStartDate,
        FieldNames.LateStartDate
    };

            foreach (string colName in directFormatCols)
            {
                int targetIndex = Array.IndexOf(finalColumns, colName);
                if (targetIndex != -1)
                {
                    string originalValue = GetFieldValueOptimized(sourceRow, sourceIndexes, colName);
                    transformedRow[targetIndex] = FormatDateOutput(TryParseDateCached(originalValue));
                }
            }

            // Handle ActEndDate (from the pre-parsed parameter)
            int actEndIdxTarget = Array.IndexOf(finalColumns, FieldNames.ActEndDate);
            if (actEndIdxTarget != -1)
            {
                transformedRow[actEndIdxTarget] = FormatDateOutput(actEndDate);
            }

            // Handle EarlyEndDate with fallback logic
            int earlyEndIdxTarget = Array.IndexOf(finalColumns, FieldNames.EarlyEndDate);
            if (earlyEndIdxTarget != -1)
            {
                string originalValue = GetFieldValueOptimized(sourceRow, sourceIndexes, FieldNames.EarlyEndDate);
                DateTime? earlyEndDate = TryParseDateCached(originalValue);

                if (earlyEndDate.HasValue)
                {
                    transformedRow[earlyEndIdxTarget] = FormatDateOutput(earlyEndDate);
                }
                else if (string.IsNullOrWhiteSpace(originalValue) && actEndDate.HasValue)
                {
                    transformedRow[earlyEndIdxTarget] = FormatDateOutput(actEndDate);
                }
                else
                {
                    transformedRow[earlyEndIdxTarget] = originalValue ?? string.Empty;
                }
            }

            // Handle LateEndDate with fallback logic
            int lateEndIdxTarget = Array.IndexOf(finalColumns, FieldNames.LateEndDate);
            if (lateEndIdxTarget != -1)
            {
                string originalValue = GetFieldValueOptimized(sourceRow, sourceIndexes, FieldNames.LateEndDate);
                DateTime? lateEndDate = TryParseDateCached(originalValue);

                if (lateEndDate.HasValue)
                {
                    transformedRow[lateEndIdxTarget] = FormatDateOutput(lateEndDate);
                }
                else if (string.IsNullOrWhiteSpace(originalValue) && actEndDate.HasValue)
                {
                    transformedRow[lateEndIdxTarget] = FormatDateOutput(actEndDate);
                }
                else
                {
                    transformedRow[lateEndIdxTarget] = originalValue ?? string.Empty;
                }
            }
        }

        private void CopyDirectFieldsOptimized(string[] sourceRow, Dictionary<string, int> sourceIndexes, string[] targetRow, string[] targetColumns)
        {
            var handledFields = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
        FieldNames.StatusCode, FieldNames.Start, FieldNames.Finish, FieldNames.IdName,
        FieldNames.RemainingWorkingDays, FieldNames.OriginalDuration, FieldNames.TotalFloat, FieldNames.FreeFloat,
        FieldNames.PercentComplete, FieldNames.DataDate,
        FieldNames.ActStartDate, FieldNames.ActEndDate, FieldNames.EarlyStartDate, FieldNames.EarlyEndDate,
        FieldNames.LateStartDate, FieldNames.LateEndDate, FieldNames.CstrDate,
        FieldNames.TargetStartDate, FieldNames.TargetEndDate, // ADD THESE
        FieldNames.WbsIdKey, FieldNames.TaskIdKey, FieldNames.ParentWbsIdKey,
        FieldNames.CalendarIdKey, FieldNames.ProjIdKey, FieldNames.ActvCodeIdKey,
        FieldNames.ActvCodeTypeIdKey, FieldNames.ClndrIdKey, FieldNames.PredTaskIdKey
    };

            for (int i = 0; i < targetColumns.Length; i++)
            {
                string colName = targetColumns[i];

                if (sourceIndexes.TryGetValue(colName, out int srcIndex) && !handledFields.Contains(colName))
                {
                    targetRow[i] = GetFieldValueSafe(sourceRow, srcIndex);
                }
            }
        }

        private DateTime CalculateStartDateOptimized(string[] row, Dictionary<string, int> indexes, string statusCode)
        {
            DateTime startDate = DateTime.MinValue;

            if (statusCode == "TK_NotStart")
            {
                if (!TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.EarlyStartDate), out startDate))
                {
                    TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.EarlyEndDate), out startDate);
                }
            }
            else
            {
                if (!TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.ActStartDate), out startDate))
                {
                    TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.ActEndDate), out startDate);
                }
            }
            return startDate;
        }

        private DateTime CalculateFinishDateOptimized(string[] row, Dictionary<string, int> indexes, string statusCode)
        {
            DateTime finishDate = DateTime.MinValue;

            if (statusCode == "TK_Complete")
            {
                TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.ActEndDate), out finishDate);
            }
            else
            {
                if (!TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.EarlyEndDate), out finishDate))
                {
                    TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.LateEndDate), out finishDate);
                }
            }
            return finishDate;
        }

        private DateTime CalculateStartDateWithConstraints(string[] row, Dictionary<string, int> indexes, string statusCode, string originalFilename)
        {
            DateTime startDate = DateTime.MinValue;
            string constraintType = GetFieldValueOptimized(row, indexes, FieldNames.CstrType);
            string constraintDateStr = GetFieldValueOptimized(row, indexes, FieldNames.CstrDate);

            // For completed or in-progress tasks, always use actuals
            if (statusCode != "TK_NotStart")
            {
                if (TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.ActStartDate), out startDate))
                    return startDate;
                if (TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.ActEndDate), out startDate))
                    return startDate;
            }

            // For not-started tasks, apply constraints
            DateTime earlyStart = DateTime.MinValue;
            DateTime lateStart = DateTime.MinValue;
            TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.EarlyStartDate), out earlyStart);

            if (indexes.ContainsKey(FieldNames.LateStartDate))
            {
                TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.LateStartDate), out lateStart);
            }

            // HANDLE ALAP FIRST (no constraint date needed)
            if (constraintType == "CS_ALAP")
            {
                return lateStart != DateTime.MinValue ? lateStart : earlyStart;
            }

            // Apply other constraint logic (these need constraint dates)
            if (!string.IsNullOrEmpty(constraintType) && TryParseDateOptimized(constraintDateStr, out DateTime constraintDate))
            {
                switch (constraintType)
                {
                    case "CS_MSO":
                    case "CS_MANDSTART":
                        return constraintDate;

                    case "CS_MSOA":
                        return earlyStart > constraintDate ? earlyStart : constraintDate;

                    case "CS_MSOB":
                        if (earlyStart != DateTime.MinValue)
                            return earlyStart < constraintDate ? earlyStart : constraintDate;
                        else
                            return constraintDate;

                    default:
                        return earlyStart;
                }
            }

            // No constraint - use early start with early end fallback
            if (earlyStart != DateTime.MinValue)
                return earlyStart;

            TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.EarlyEndDate), out startDate);
            return startDate;
        }

        private DateTime CalculateFinishDateWithConstraints(string[] row, Dictionary<string, int> indexes, string statusCode, string originalFilename)
        {
            DateTime finishDate = DateTime.MinValue;
            string constraintType = GetFieldValueOptimized(row, indexes, FieldNames.CstrType);
            string constraintDateStr = GetFieldValueOptimized(row, indexes, FieldNames.CstrDate);

            // For completed tasks, always use actual end
            if (statusCode == "TK_Complete")
            {
                if (TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.ActEndDate), out finishDate))
                    return finishDate;
            }

            // For not-started or in-progress tasks
            DateTime earlyEnd = DateTime.MinValue;
            DateTime lateEnd = DateTime.MinValue;
            TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.EarlyEndDate), out earlyEnd);
            TryParseDateOptimized(GetFieldValueOptimized(row, indexes, FieldNames.LateEndDate), out lateEnd);

            // HANDLE ALAP FIRST (no constraint date needed)
            if (constraintType == "CS_ALAP")
            {
                return lateEnd != DateTime.MinValue ? lateEnd : earlyEnd;
            }

            // Apply other constraint logic (these need constraint dates)
            if (!string.IsNullOrEmpty(constraintType) && TryParseDateOptimized(constraintDateStr, out DateTime constraintDate))
            {
                switch (constraintType)
                {
                    case "CS_MEO":
                    case "CS_MANDFIN":
                        if (statusCode != "TK_Complete")
                            return constraintDate;
                        break;

                    case "CS_MEOA":
                        return earlyEnd > constraintDate ? earlyEnd : constraintDate;

                    case "CS_MEOB":
                        if (earlyEnd != DateTime.MinValue)
                            return earlyEnd < constraintDate ? earlyEnd : constraintDate;
                        else
                            return constraintDate;
                }
            }

            // No constraint - use early end with late end fallback
            return earlyEnd != DateTime.MinValue ? earlyEnd : lateEnd;
        }

        private string CalculateDaysFromHoursOptimized(string[] row, Dictionary<string, int> indexes, string hourFieldName, decimal hoursPerDay)
        {
            if (hoursPerDay <= 0) return "";

            string hourStr = GetFieldValueOptimized(row, indexes, hourFieldName);

            if (decimal.TryParse(hourStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal hours))
            {
                int days = (int)(hours / hoursPerDay);
                return days.ToString();
            }

            return "";
        }

        private double CalculateCompletionPercentageOptimized(string[] row, Dictionary<string, int> indexes, string statusCode)
        {
            if (statusCode == "TK_Complete") return 100.0;
            if (statusCode == "TK_NotStart") return 0.0;

            try
            {
                string completePctType = GetFieldValueOptimized(row, indexes, FieldNames.CompletePctType);
                NumberStyles numStyle = NumberStyles.Any;
                IFormatProvider formatProvider = CultureInfo.InvariantCulture;

                switch (completePctType)
                {
                    case "CP_Phys":
                        string phys = GetFieldValueOptimized(row, indexes, FieldNames.PhysCompletePct);
                        if (double.TryParse(phys, numStyle, formatProvider, out double pct))
                        {
                            return Math.Max(0.0, Math.Min(100.0, pct));
                        }
                        break;

                    case "CP_Units":
                        string actStr = GetFieldValueOptimized(row, indexes, FieldNames.ActWorkQty);
                        string remStr = GetFieldValueOptimized(row, indexes, FieldNames.RemainWorkQty);
                        if (double.TryParse(actStr, numStyle, formatProvider, out double actVal) &&
                            double.TryParse(remStr, numStyle, formatProvider, out double remVal))
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
                        string targStr = GetFieldValueOptimized(row, indexes, FieldNames.TargetDurationHrCnt);
                        string remDurStr = GetFieldValueOptimized(row, indexes, FieldNames.RemainDurationHrCnt);
                        if (double.TryParse(targStr, numStyle, formatProvider, out double targVal) &&
                            double.TryParse(remDurStr, numStyle, formatProvider, out double remDurVal))
                        {
                            if (targVal > 0.0001)
                            {
                                double fraction = (targVal - remDurVal) / targVal;
                                double calculatedPct = fraction * 100.0;
                                return Math.Max(0.0, Math.Min(100.0, calculatedPct));
                            }
                        }
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error calculating percentage: {ex.Message}");
            }

            return 0.0;
        }

        private bool IsEnhancedTableName(string name)
        {
            var enhancedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
                EnhancedTableNames.XerTask01, EnhancedTableNames.XerProject02,
                EnhancedTableNames.XerProjWbs03, EnhancedTableNames.XerBaseline04,
                EnhancedTableNames.XerPredecessor06, EnhancedTableNames.XerActvType07,
                EnhancedTableNames.XerActvCode08, EnhancedTableNames.XerTaskActv09,
                EnhancedTableNames.XerCalendar10, EnhancedTableNames.XerCalendarDetailed11
            };
            return enhancedNames.Contains(name);
        }
        #endregion

        #region UI Helper Methods

        private void AddXerFiles(IEnumerable<string> filesToAdd)
        {
            bool fileAdded = false;
            foreach (var file in filesToAdd)
            {
                if (!_xerFilePaths.Any(existing => string.Equals(existing, file, StringComparison.OrdinalIgnoreCase)))
                {
                    _xerFilePaths.Add(file);
                    lstXerFiles.Items.Add(file);
                    fileAdded = true;
                }
            }
            if (fileAdded)
            {
                UpdateInputButtonsState();
            }
        }

        private void UpdateXerFileList()
        {
            lstXerFiles.Items.Clear();
            foreach (var path in _xerFilePaths)
            {
                lstXerFiles.Items.Add(path);
            }
        }

        private void ClearResults()
        {
            _extractedTables.Clear();
            lstTables.Items.Clear();
            _chkCreatePowerBiTables.Enabled = false;
            _chkCreatePowerBiTables.Checked = false;
            _lblRequirements.Text = "(Dependencies checked after parsing)";
            _canCreateTask01 = false; _canCreateProjWbs03 = false; _canCreateBaseline04 = false;
            _canCreateProject02 = false; _canCreatePredecessor06 = false; _canCreateActvType07 = false;
            _canCreateActvCode08 = false; _canCreateTaskActv09 = false; _canCreateCalendar10 = false;
            _canCreateCalendarDetailed11 = false;

            UpdateExportButtonState();
            UpdateStatus("Results cleared. Select XER file(s) and Output Directory.");
        }

        private void UpdateTableList()
        {
            lstTables.BeginUpdate();
            lstTables.Items.Clear();
            foreach (var tableName in _extractedTables.Keys.OrderBy(t => t))
            {
                lstTables.Items.Add(tableName);
            }
            lstTables.EndUpdate();
        }

        private void UpdatePbiCheckboxState()
        {
            bool dependenciesMet = AnyPbiDependencyMet();
            _chkCreatePowerBiTables.Enabled = dependenciesMet;
            if (!dependenciesMet)
            {
                _chkCreatePowerBiTables.Checked = false;
            }
            _lblRequirements.Text = GetPbiRequirementsText();
        }

        private string GetPbiRequirementsText()
        {
            if (!AnyPbiDependencyMet())
            {
                if (_extractedTables == null || _extractedTables.Count == 0)
                {
                    return "(Parse XER file(s) first)";
                }
                else
                {
                    return "Missing required source tables for Power BI.";
                }
            }

            var req = new List<string>();
            if (_canCreateTask01 || _canCreateBaseline04) req.Add($"{TableNames.Task}, {TableNames.Calendar}, {TableNames.Project}");
            if (_canCreateProjWbs03) req.Add(TableNames.ProjWbs);
            if (_canCreateProject02) req.Add(TableNames.Project);
            if (_canCreatePredecessor06) req.Add(TableNames.TaskPred);
            if (_canCreateActvType07) req.Add(TableNames.ActvType);
            if (_canCreateActvCode08) req.Add(TableNames.ActvCode);
            if (_canCreateTaskActv09) req.Add(TableNames.TaskActv);
            if (_canCreateCalendar10 || _canCreateCalendarDetailed11) req.Add(TableNames.Calendar);

            var distinctReq = req.SelectMany(s => s.Split(new[] { ", " }, StringSplitOptions.RemoveEmptyEntries))
                                     .Distinct(StringComparer.OrdinalIgnoreCase)
                                     .OrderBy(s => s);

            return $"Requires: {string.Join(", ", distinctReq)}";
        }

        private void UpdateInputButtonsState()
        {
            bool hasFiles = _xerFilePaths.Count > 0;
            bool hasValidOutput = !string.IsNullOrEmpty(_outputDirectory) && Directory.Exists(_outputDirectory);

            btnParseXer.Enabled = hasFiles && hasValidOutput;
            btnClearFiles.Enabled = hasFiles;
        }

        private void UpdateExportButtonState()
        {
            bool hasOutput = !string.IsNullOrEmpty(_outputDirectory) && Directory.Exists(_outputDirectory);
            bool hasParsedData = _extractedTables.Count > 0;
            bool pbiTablesSelectedAndPossible = _chkCreatePowerBiTables.Checked && AnyPbiDependencyMet();

            btnExportAll.Enabled = hasOutput && (hasParsedData || pbiTablesSelectedAndPossible);
            btnExportSelected.Enabled = hasOutput && (lstTables.SelectedItems.Count > 0 || pbiTablesSelectedAndPossible);
        }

        private void UpdateStatus(string message)
        {
            if (lblStatus.InvokeRequired)
            {
                lblStatus.Invoke(new Action(() => lblStatus.Text = message));
            }
            else
            {
                lblStatus.Text = message;
            }
        }

        private void SetUIEnabled(bool enabled)
        {
            btnSelectXer.Enabled = enabled;
            btnSelectOutput.Enabled = enabled;
            btnParseXer.Enabled = enabled && _xerFilePaths.Count > 0 && !string.IsNullOrEmpty(_outputDirectory);
            btnExportAll.Enabled = enabled && _extractedTables.Count > 0;
            btnExportSelected.Enabled = enabled && lstTables.SelectedItems.Count > 0;
            btnClearFiles.Enabled = enabled && _xerFilePaths.Count > 0;
            lstXerFiles.Enabled = enabled;
            lstTables.Enabled = enabled;
            _chkCreatePowerBiTables.Enabled = enabled && AnyPbiDependencyMet();
        }

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

        #region Windows Form Designer generated code
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            this.tableLayoutPanelMain = new System.Windows.Forms.TableLayoutPanel();
            this.grpInput = new System.Windows.Forms.GroupBox();
            this.tableLayoutPanelInput = new System.Windows.Forms.TableLayoutPanel();
            this.lblXerFiles = new System.Windows.Forms.Label();
            this.lstXerFiles = new System.Windows.Forms.ListBox();
            this.btnSelectXer = new System.Windows.Forms.Button();
            this.lblOutputPath = new System.Windows.Forms.Label();
            this.txtOutputPath = new System.Windows.Forms.TextBox();
            this.btnSelectOutput = new System.Windows.Forms.Button();
            this.lblDragDropHint = new System.Windows.Forms.Label();
            this.btnClearFiles = new System.Windows.Forms.Button();
            this.btnParseXer = new System.Windows.Forms.Button();
            this.grpExport = new System.Windows.Forms.GroupBox();
            this.tableLayoutPanelExport = new System.Windows.Forms.TableLayoutPanel();
            this.lblExtractedTables = new System.Windows.Forms.Label();
            this.lstTables = new System.Windows.Forms.ListBox();
            this.flowLayoutPanelPbi = new System.Windows.Forms.FlowLayoutPanel();
            this._chkCreatePowerBiTables = new System.Windows.Forms.CheckBox();
            this._lblRequirements = new System.Windows.Forms.Label();
            this.flowLayoutPanelExportButtons = new System.Windows.Forms.FlowLayoutPanel();
            this.btnExportAll = new System.Windows.Forms.Button();
            this.btnExportSelected = new System.Windows.Forms.Button();
            this.lblStatus = new System.Windows.Forms.Label();
            this.toolTip1 = new System.Windows.Forms.ToolTip(this.components);
            this.tableLayoutPanelMain.SuspendLayout();
            this.grpInput.SuspendLayout();
            this.tableLayoutPanelInput.SuspendLayout();
            this.grpExport.SuspendLayout();
            this.tableLayoutPanelExport.SuspendLayout();
            this.flowLayoutPanelPbi.SuspendLayout();
            this.flowLayoutPanelExportButtons.SuspendLayout();
            this.SuspendLayout();
            //
            // tableLayoutPanelMain
            //
            this.tableLayoutPanelMain.ColumnCount = 1;
            this.tableLayoutPanelMain.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanelMain.Controls.Add(this.grpInput, 0, 0);
            this.tableLayoutPanelMain.Controls.Add(this.btnParseXer, 0, 1);
            this.tableLayoutPanelMain.Controls.Add(this.grpExport, 0, 2);
            this.tableLayoutPanelMain.Controls.Add(this.lblStatus, 0, 3);
            this.tableLayoutPanelMain.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanelMain.Location = new System.Drawing.Point(0, 0);
            this.tableLayoutPanelMain.Name = "tableLayoutPanelMain";
            this.tableLayoutPanelMain.Padding = new System.Windows.Forms.Padding(10);
            this.tableLayoutPanelMain.RowCount = 4;
            this.tableLayoutPanelMain.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanelMain.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanelMain.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanelMain.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanelMain.Size = new System.Drawing.Size(624, 561);
            this.tableLayoutPanelMain.TabIndex = 0;
            //
            // grpInput
            //
            this.grpInput.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.grpInput.Controls.Add(this.tableLayoutPanelInput);
            this.grpInput.Location = new System.Drawing.Point(13, 13);
            this.grpInput.Name = "grpInput";
            this.grpInput.Size = new System.Drawing.Size(598, 150);
            this.grpInput.TabIndex = 0;
            this.grpInput.TabStop = false;
            this.grpInput.Text = "Input Selection";
            //
            // tableLayoutPanelInput
            //
            this.tableLayoutPanelInput.ColumnCount = 4;
            this.tableLayoutPanelInput.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle());
            this.tableLayoutPanelInput.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanelInput.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle());
            this.tableLayoutPanelInput.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle());
            this.tableLayoutPanelInput.Controls.Add(this.lblXerFiles, 0, 0);
            this.tableLayoutPanelInput.Controls.Add(this.lstXerFiles, 1, 0);
            this.tableLayoutPanelInput.Controls.Add(this.btnSelectXer, 2, 0);
            this.tableLayoutPanelInput.Controls.Add(this.lblOutputPath, 0, 2);
            this.tableLayoutPanelInput.Controls.Add(this.txtOutputPath, 1, 2);
            this.tableLayoutPanelInput.Controls.Add(this.btnSelectOutput, 2, 2);
            this.tableLayoutPanelInput.Controls.Add(this.lblDragDropHint, 1, 1);
            this.tableLayoutPanelInput.Controls.Add(this.btnClearFiles, 3, 0);
            this.tableLayoutPanelInput.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanelInput.Location = new System.Drawing.Point(3, 16);
            this.tableLayoutPanelInput.Name = "tableLayoutPanelInput";
            this.tableLayoutPanelInput.RowCount = 3;
            this.tableLayoutPanelInput.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanelInput.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanelInput.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanelInput.Size = new System.Drawing.Size(592, 131);
            this.tableLayoutPanelInput.TabIndex = 0;
            //
            // lblXerFiles
            //
            this.lblXerFiles.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.lblXerFiles.AutoSize = true;
            this.lblXerFiles.Location = new System.Drawing.Point(3, 27);
            this.lblXerFiles.Name = "lblXerFiles";
            this.lblXerFiles.Size = new System.Drawing.Size(102, 13);
            this.lblXerFiles.TabIndex = 0;
            this.lblXerFiles.Text = "Primavera P6 XER(s):";
            //
            // lstXerFiles
            //
            this.lstXerFiles.Dock = System.Windows.Forms.DockStyle.Fill;
            this.lstXerFiles.FormattingEnabled = true;
            this.lstXerFiles.HorizontalScrollbar = true;
            this.lstXerFiles.IntegralHeight = false;
            this.lstXerFiles.Location = new System.Drawing.Point(111, 3);
            this.lstXerFiles.MinimumSize = new System.Drawing.Size(4, 50);
            this.lstXerFiles.Name = "lstXerFiles";
            this.tableLayoutPanelInput.SetRowSpan(this.lstXerFiles, 1);
            this.lstXerFiles.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended;
            this.lstXerFiles.Size = new System.Drawing.Size(316, 75);
            this.lstXerFiles.TabIndex = 1;
            this.toolTip1.SetToolTip(this.lstXerFiles, "Drag and drop XER files here or use Browse.\r\nSelect files and press DEL to remov" +
        "e.");
            this.lstXerFiles.KeyDown += new System.Windows.Forms.KeyEventHandler(this.lstXerFiles_KeyDown);
            //
            // btnSelectXer
            //
            this.btnSelectXer.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.btnSelectXer.Location = new System.Drawing.Point(433, 29);
            this.btnSelectXer.Name = "btnSelectXer";
            this.btnSelectXer.Size = new System.Drawing.Size(75, 23);
            this.btnSelectXer.TabIndex = 2;
            this.btnSelectXer.Text = "Browse...";
            this.btnSelectXer.UseVisualStyleBackColor = true;
            this.btnSelectXer.Click += new System.EventHandler(this.btnSelectXer_Click);
            //
            // lblOutputPath
            //
            this.lblOutputPath.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.lblOutputPath.AutoSize = true;
            this.lblOutputPath.Location = new System.Drawing.Point(3, 109);
            this.lblOutputPath.Name = "lblOutputPath";
            this.lblOutputPath.Size = new System.Drawing.Size(87, 13);
            this.lblOutputPath.TabIndex = 3;
            this.lblOutputPath.Text = "Output Directory:";
            //
            // txtOutputPath
            //
            this.txtOutputPath.Dock = System.Windows.Forms.DockStyle.Fill;
            this.txtOutputPath.Location = new System.Drawing.Point(111, 106);
            this.txtOutputPath.Name = "txtOutputPath";
            this.txtOutputPath.ReadOnly = true;
            this.txtOutputPath.Size = new System.Drawing.Size(316, 20);
            this.txtOutputPath.TabIndex = 4;
            //
            // btnSelectOutput
            //
            this.btnSelectOutput.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.tableLayoutPanelInput.SetColumnSpan(this.btnSelectOutput, 2);
            this.btnSelectOutput.Location = new System.Drawing.Point(433, 104);
            this.btnSelectOutput.Name = "btnSelectOutput";
            this.btnSelectOutput.Size = new System.Drawing.Size(75, 23);
            this.btnSelectOutput.TabIndex = 5;
            this.btnSelectOutput.Text = "Browse...";
            this.btnSelectOutput.UseVisualStyleBackColor = true;
            this.btnSelectOutput.Click += new System.EventHandler(this.btnSelectOutput_Click);
            //
            // lblDragDropHint
            //
            this.lblDragDropHint.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)));
            this.lblDragDropHint.AutoSize = true;
            this.lblDragDropHint.ForeColor = System.Drawing.SystemColors.GrayText;
            this.lblDragDropHint.Location = new System.Drawing.Point(111, 81);
            this.lblDragDropHint.Name = "lblDragDropHint";
            this.lblDragDropHint.Padding = new System.Windows.Forms.Padding(0, 2, 0, 0);
            this.lblDragDropHint.Size = new System.Drawing.Size(157, 15);
            this.lblDragDropHint.TabIndex = 6;
            this.lblDragDropHint.Text = "(Drag && Drop XER files onto list)";
            //
            // btnClearFiles
            //
            this.btnClearFiles.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.btnClearFiles.Enabled = false;
            this.btnClearFiles.Location = new System.Drawing.Point(514, 29);
            this.btnClearFiles.Name = "btnClearFiles";
            this.btnClearFiles.Size = new System.Drawing.Size(75, 23);
            this.btnClearFiles.TabIndex = 7;
            this.btnClearFiles.Text = "Clear Files";
            this.toolTip1.SetToolTip(this.btnClearFiles, "Remove all files from the list above.");
            this.btnClearFiles.UseVisualStyleBackColor = true;
            this.btnClearFiles.Click += new System.EventHandler(this.btnClearFiles_Click);
            //
            // btnParseXer
            //
            this.btnParseXer.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.btnParseXer.Enabled = false;
            this.btnParseXer.Location = new System.Drawing.Point(13, 171);
            this.btnParseXer.Margin = new System.Windows.Forms.Padding(3, 5, 3, 5);
            this.btnParseXer.Name = "btnParseXer";
            this.btnParseXer.Size = new System.Drawing.Size(150, 23);
            this.btnParseXer.TabIndex = 1;
            this.btnParseXer.Text = "Parse XER File(s)";
            this.btnParseXer.UseVisualStyleBackColor = true;
            this.btnParseXer.Click += new System.EventHandler(this.btnParseXer_Click);
            //
            // grpExport
            //
            this.grpExport.Controls.Add(this.tableLayoutPanelExport);
            this.grpExport.Dock = System.Windows.Forms.DockStyle.Fill;
            this.grpExport.Location = new System.Drawing.Point(13, 204);
            this.grpExport.Name = "grpExport";
            this.grpExport.Size = new System.Drawing.Size(598, 311);
            this.grpExport.TabIndex = 2;
            this.grpExport.TabStop = false;
            this.grpExport.Text = "Export Options";
            //
            // tableLayoutPanelExport
            //
            this.tableLayoutPanelExport.ColumnCount = 1;
            this.tableLayoutPanelExport.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanelExport.Controls.Add(this.lblExtractedTables, 0, 0);
            this.tableLayoutPanelExport.Controls.Add(this.lstTables, 0, 1);
            this.tableLayoutPanelExport.Controls.Add(this.flowLayoutPanelPbi, 0, 2);
            this.tableLayoutPanelExport.Controls.Add(this.flowLayoutPanelExportButtons, 0, 3);
            this.tableLayoutPanelExport.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanelExport.Location = new System.Drawing.Point(3, 16);
            this.tableLayoutPanelExport.Name = "tableLayoutPanelExport";
            this.tableLayoutPanelExport.RowCount = 4;
            this.tableLayoutPanelExport.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanelExport.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanelExport.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanelExport.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanelExport.Size = new System.Drawing.Size(592, 292);
            this.tableLayoutPanelExport.TabIndex = 0;
            //
            // lblExtractedTables
            //
            this.lblExtractedTables.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.lblExtractedTables.AutoSize = true;
            this.lblExtractedTables.Location = new System.Drawing.Point(3, 0);
            this.lblExtractedTables.Margin = new System.Windows.Forms.Padding(3, 0, 3, 5);
            this.lblExtractedTables.Name = "lblExtractedTables";
            this.lblExtractedTables.Size = new System.Drawing.Size(95, 13);
            this.lblExtractedTables.TabIndex = 0;
            this.lblExtractedTables.Text = "Extracted Tables:";
            //
            // lstTables
            //
            this.lstTables.Dock = System.Windows.Forms.DockStyle.Fill;
            this.lstTables.FormattingEnabled = true;
            this.lstTables.IntegralHeight = false;
            this.lstTables.Location = new System.Drawing.Point(3, 21);
            this.lstTables.Name = "lstTables";
            this.lstTables.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended;
            this.lstTables.Size = new System.Drawing.Size(586, 198);
            this.lstTables.Sorted = true;
            this.lstTables.TabIndex = 1;
            this.lstTables.SelectedIndexChanged += new System.EventHandler(this.lstTables_SelectedIndexChanged);
            //
            // flowLayoutPanelPbi
            //
            this.flowLayoutPanelPbi.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.flowLayoutPanelPbi.AutoSize = true;
            this.flowLayoutPanelPbi.Controls.Add(this._chkCreatePowerBiTables);
            this.flowLayoutPanelPbi.Controls.Add(this._lblRequirements);
            this.flowLayoutPanelPbi.Location = new System.Drawing.Point(3, 225);
            this.flowLayoutPanelPbi.Margin = new System.Windows.Forms.Padding(3, 3, 3, 5);
            this.flowLayoutPanelPbi.Name = "flowLayoutPanelPbi";
            this.flowLayoutPanelPbi.Size = new System.Drawing.Size(586, 23);
            this.flowLayoutPanelPbi.TabIndex = 2;
            //
            // _chkCreatePowerBiTables
            //
            this._chkCreatePowerBiTables.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this._chkCreatePowerBiTables.AutoSize = true;
            this._chkCreatePowerBiTables.Enabled = false;
            this._chkCreatePowerBiTables.Location = new System.Drawing.Point(3, 3);
            this._chkCreatePowerBiTables.Name = "_chkCreatePowerBiTables";
            this._chkCreatePowerBiTables.Size = new System.Drawing.Size(135, 17);
            this._chkCreatePowerBiTables.TabIndex = 0;
            this._chkCreatePowerBiTables.Text = "Create Power BI Tables";
            this.toolTip1.SetToolTip(this._chkCreatePowerBiTables, "If checked, additional tables suitable for Power BI will be generated if the req" +
        "uired source tables are found after parsing.");
            this._chkCreatePowerBiTables.UseVisualStyleBackColor = true;
            this._chkCreatePowerBiTables.CheckedChanged += new System.EventHandler(this.ChkCreatePowerBiTables_CheckedChanged);
            //
            // _lblRequirements
            //
            this._lblRequirements.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this._lblRequirements.AutoSize = true;
            this._lblRequirements.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this._lblRequirements.ForeColor = System.Drawing.Color.Navy;
            this._lblRequirements.Location = new System.Drawing.Point(144, 5);
            this._lblRequirements.Name = "_lblRequirements";
            this._lblRequirements.Size = new System.Drawing.Size(181, 13);
            this._lblRequirements.TabIndex = 1;
            this._lblRequirements.Text = "(Dependencies checked after parsing)";
            //
            // flowLayoutPanelExportButtons
            //
            this.flowLayoutPanelExportButtons.AutoSize = true;
            this.flowLayoutPanelExportButtons.Controls.Add(this.btnExportAll);
            this.flowLayoutPanelExportButtons.Controls.Add(this.btnExportSelected);
            this.flowLayoutPanelExportButtons.Location = new System.Drawing.Point(3, 256);
            this.flowLayoutPanelExportButtons.Name = "flowLayoutPanelExportButtons";
            this.flowLayoutPanelExportButtons.Size = new System.Drawing.Size(331, 29);
            this.flowLayoutPanelExportButtons.TabIndex = 3;
            //
            // btnExportAll
            //
            this.btnExportAll.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.btnExportAll.AutoSize = true;
            this.btnExportAll.Enabled = false;
            this.btnExportAll.Location = new System.Drawing.Point(3, 3);
            this.btnExportAll.Name = "btnExportAll";
            this.btnExportAll.Size = new System.Drawing.Size(115, 23);
            this.btnExportAll.TabIndex = 0;
            this.btnExportAll.Text = "Export All Tables";
            this.btnExportAll.UseVisualStyleBackColor = true;
            this.btnExportAll.Click += new System.EventHandler(this.btnExportAll_Click);
            //
            // btnExportSelected
            //
            this.btnExportSelected.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.btnExportSelected.AutoSize = true;
            this.btnExportSelected.Enabled = false;
            this.btnExportSelected.Location = new System.Drawing.Point(124, 3);
            this.btnExportSelected.Name = "btnExportSelected";
            this.btnExportSelected.Size = new System.Drawing.Size(204, 23);
            this.btnExportSelected.TabIndex = 1;
            this.btnExportSelected.Text = "Export Selected Table(s) + Power BI";
            this.btnExportSelected.UseVisualStyleBackColor = true;
            this.btnExportSelected.Click += new System.EventHandler(this.btnExportSelected_Click);
            //
            // lblStatus
            //
            this.lblStatus.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.lblStatus.AutoSize = true;
            this.lblStatus.Location = new System.Drawing.Point(13, 538);
            this.lblStatus.Name = "lblStatus";
            this.lblStatus.Size = new System.Drawing.Size(43, 13);
            this.lblStatus.TabIndex = 3;
            this.lblStatus.Text = "Status: ";
            //
            // MainForm
            //
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(624, 561);
            this.Controls.Add(this.tableLayoutPanelMain);
            this.MinimumSize = new System.Drawing.Size(550, 500);
            this.Name = "MainForm";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Primavera P6 XER to CSV Converter";
            this.tableLayoutPanelMain.ResumeLayout(false);
            this.tableLayoutPanelMain.PerformLayout();
            this.grpInput.ResumeLayout(false);
            this.tableLayoutPanelInput.ResumeLayout(false);
            this.tableLayoutPanelInput.PerformLayout();
            this.grpExport.ResumeLayout(false);
            this.tableLayoutPanelExport.ResumeLayout(false);
            this.tableLayoutPanelExport.PerformLayout();
            this.flowLayoutPanelPbi.ResumeLayout(false);
            this.flowLayoutPanelPbi.PerformLayout();
            this.flowLayoutPanelExportButtons.ResumeLayout(false);
            this.flowLayoutPanelExportButtons.PerformLayout();
            this.ResumeLayout(false);

        }
        #endregion

        private System.ComponentModel.IContainer components = null;

        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }
    }

    #region Progress Form Class (MUST BE INCLUDED)
    public class ProgressForm : Form
    {
        private ProgressBar progressBar; private Label lblStatus;
        public ProgressForm(string title) { InitializeComponent(); this.Text = title; }
        public void UpdateProgress(string status, int percentage) { if (this.InvokeRequired) { try { this.BeginInvoke(new Action(() => UpdateProgressInternal(status, percentage))); } catch (ObjectDisposedException) { } catch (InvalidOperationException) { } } else { UpdateProgressInternal(status, percentage); } }
        private void UpdateProgressInternal(string status, int percentage) { if (this.IsDisposed || !this.IsHandleCreated) return; lblStatus.Text = status; progressBar.Value = Math.Min(progressBar.Maximum, Math.Max(progressBar.Minimum, percentage)); }
        private void InitializeComponent() { this.progressBar = new System.Windows.Forms.ProgressBar(); this.lblStatus = new System.Windows.Forms.Label(); this.SuspendLayout(); this.progressBar.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) | System.Windows.Forms.AnchorStyles.Right))); this.progressBar.Location = new System.Drawing.Point(12, 38); this.progressBar.Name = "progressBar"; this.progressBar.Size = new System.Drawing.Size(360, 23); this.progressBar.TabIndex = 0; this.lblStatus.AutoSize = true; this.lblStatus.Location = new System.Drawing.Point(12, 13); this.lblStatus.MaximumSize = new System.Drawing.Size(360, 0); this.lblStatus.Name = "lblStatus"; this.lblStatus.Size = new System.Drawing.Size(67, 13); this.lblStatus.TabIndex = 1; this.lblStatus.Text = "Processing..."; this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F); this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font; this.ClientSize = new System.Drawing.Size(384, 73); this.ControlBox = false; this.Controls.Add(this.lblStatus); this.Controls.Add(this.progressBar); this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog; this.MaximizeBox = false; this.MinimizeBox = false; this.Name = "ProgressForm"; this.ShowInTaskbar = false; this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent; this.Text = "Progress"; this.ResumeLayout(false); this.PerformLayout(); }
    }
    #endregion

    #region Program Entry Point (MUST BE INCLUDED)
    static class Program
    {
        [STAThread]
        static void Main() { Application.EnableVisualStyles(); Application.SetCompatibleTextRenderingDefault(false); Application.Run(new MainForm()); }
    }
    #endregion

}