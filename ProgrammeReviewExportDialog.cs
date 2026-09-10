using System.Globalization;
using System.Text.RegularExpressions;
using XerToCsvConverter.ProgrammeReview;

namespace XerToCsvConverter;

/// <summary>Collects the governed identity and history metadata required by the Programme Review profile.</summary>
internal sealed class ProgrammeReviewExportDialog : Form
{
    private static readonly Regex BaselineTagPattern = new(
        "^BL[0-9]{2}(?:-[A-Z])?$", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex UpdateTagPattern = new(
        "^[0-9]{4}$", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex GovernedFilenamePattern = new(
        @"^.+[-_][CT][-_](?<tag>BL[0-9]{2}(?:-[A-Z])?|[0-9]{4})_(?<date>[0-9]{8}|[0-9]{4}-[0-9]{2}-[0-9]{2})\.xer$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);
    private static readonly Regex FilenameBaselinePattern = new(
        "(?:^|[-_ ])(BL[0-9]{2}(?:-[A-Z])?)(?=[-_. ]|$)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);
    private static readonly Regex FilenameUpdatePattern = new(
        "(?:^|[-_ ])([0-9]{4})(?=[-_. ]|$)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private readonly TextBox _projectCode = new();
    private readonly TextBox _projectName = new();
    private readonly ComboBox _programmeType = new();
    private readonly TextBox _parserVersion = new();
    private readonly DataGridView _snapshots = new();
    private readonly Button _exportButton = new();
    private readonly Button _cancelButton = new();

    internal ProgrammeReviewExportDialog(
        IReadOnlyList<string> xerFilePaths,
        IReadOnlyDictionary<string, DateOnly> suggestedDataDates)
    {
        ArgumentNullException.ThrowIfNull(xerFilePaths);
        ArgumentNullException.ThrowIfNull(suggestedDataDates);

        Text = "Programme Review bundle";
        AccessibleName = "Programme Review bundle metadata";
        AccessibleDescription = "Configure governed project identity and snapshot metadata for each XER file.";
        StartPosition = FormStartPosition.CenterParent;
        AutoScaleMode = AutoScaleMode.Dpi;
        MinimumSize = new Size(760, 480);
        Size = new Size(1120, 660);
        FormBorderStyle = FormBorderStyle.Sizable;
        MaximizeBox = true;
        MinimizeBox = false;
        ShowIcon = false;

        BuildLayout();
        ConfigureInputs();
        PopulateSnapshots(xerFilePaths, suggestedDataDates);
    }

    internal ProgrammeReviewBundleRequest? BundleRequest { get; private set; }

    internal void SetIdentityForTesting(string projectCode, string projectName, string programmeType)
    {
        _projectCode.Text = projectCode;
        _projectName.Text = projectName;
        _programmeType.SelectedIndex = programmeType == "C" ? 0 : 1;
    }

    internal void SetMonthUpdateForTesting(int rowIndex, DateOnly monthUpdate) =>
        _snapshots.Rows[rowIndex].Cells["MonthUpdate"].Value = monthUpdate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    internal bool TryBuildRequestForTesting(out ProgrammeReviewBundleRequest? request) => TryBuildRequest(out request);

    internal bool HasSuggestedDataDate(string originalFilename, DateOnly dataDate)
    {
        string expected = dataDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        return _snapshots.Rows.Cast<DataGridViewRow>().Any(row =>
            string.Equals(CellText(row, "OriginalFilename"), originalFilename, StringComparison.OrdinalIgnoreCase)
            && string.Equals(CellText(row, "DataDate"), expected, StringComparison.Ordinal));
    }

    private void BuildLayout()
    {
        var root = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            AutoScroll = true,
            ColumnCount = 1,
            RowCount = 4,
            Padding = new Padding(14)
        };
        root.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100f));
        root.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        root.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        root.RowStyles.Add(new RowStyle(SizeType.Percent, 100f));
        root.RowStyles.Add(new RowStyle(SizeType.AutoSize));

        var guidance = new Label
        {
            AutoSize = true,
            Dock = DockStyle.Fill,
            MaximumSize = new Size(1040, 0),
            Margin = new Padding(0, 0, 0, 12),
            Text = "Create one full-history bundle for one governed project and programme type. " +
                   "Include at least one real baseline candidate and every applicable post-baseline update. " +
                   "Project identity is explicit; it is not inferred from legacy filenames. " +
                   "XER data dates are filled from PROJECT.last_recalc_date when available and remain editable."
        };

        var metadata = new TableLayoutPanel
        {
            Dock = DockStyle.Top,
            AutoSize = true,
            ColumnCount = 4,
            RowCount = 2,
            Margin = new Padding(0, 0, 0, 12)
        };
        metadata.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        metadata.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 40f));
        metadata.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        metadata.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 60f));
        AddField(metadata, "Project code:", _projectCode, 0, 0);
        AddField(metadata, "Project name:", _projectName, 2, 0);
        AddField(metadata, "Programme type:", _programmeType, 0, 1);
        AddField(metadata, "Parser version:", _parserVersion, 2, 1);

        ConfigureGrid();

        var buttons = new FlowLayoutPanel
        {
            AutoSize = true,
            Dock = DockStyle.Fill,
            FlowDirection = FlowDirection.RightToLeft,
            WrapContents = false,
            Margin = new Padding(0, 12, 0, 0)
        };
        _exportButton.Text = "Validate and export";
        _exportButton.AutoSize = true;
        _exportButton.MinimumSize = new Size(140, 36);
        _exportButton.AccessibleName = "Validate metadata and export Programme Review bundle";
        _exportButton.Click += ExportButton_Click;
        _cancelButton.Text = "Cancel";
        _cancelButton.AutoSize = true;
        _cancelButton.MinimumSize = new Size(90, 36);
        _cancelButton.DialogResult = DialogResult.Cancel;
        buttons.Controls.Add(_exportButton);
        buttons.Controls.Add(_cancelButton);

        root.Controls.Add(guidance, 0, 0);
        root.Controls.Add(metadata, 0, 1);
        root.Controls.Add(_snapshots, 0, 2);
        root.Controls.Add(buttons, 0, 3);
        Controls.Add(root);
        AcceptButton = _exportButton;
        CancelButton = _cancelButton;
    }

    private void ConfigureGrid()
    {
        _snapshots.Dock = DockStyle.Fill;
        _snapshots.AllowUserToAddRows = false;
        _snapshots.AllowUserToDeleteRows = false;
        _snapshots.AllowUserToOrderColumns = false;
        _snapshots.AutoGenerateColumns = false;
        _snapshots.AutoSizeRowsMode = DataGridViewAutoSizeRowsMode.AllCells;
        _snapshots.BackgroundColor = SystemColors.Window;
        _snapshots.BorderStyle = BorderStyle.FixedSingle;
        _snapshots.EditMode = DataGridViewEditMode.EditOnEnter;
        _snapshots.MultiSelect = false;
        _snapshots.RowHeadersVisible = false;
        _snapshots.SelectionMode = DataGridViewSelectionMode.CellSelect;
        _snapshots.AccessibleName = "XER snapshot metadata";
        _snapshots.AccessibleDescription = "Enter snapshot kind, tag, month update, and source data date for every XER.";

        _snapshots.Columns.Add(new DataGridViewTextBoxColumn
        {
            Name = "OriginalFilename", HeaderText = "Original XER filename", ReadOnly = true,
            AutoSizeMode = DataGridViewAutoSizeColumnMode.Fill, FillWeight = 45, MinimumWidth = 260
        });
        _snapshots.Columns.Add(new DataGridViewComboBoxColumn
        {
            Name = "SnapshotKind", HeaderText = "Kind",
            DataSource = new[] { "baseline", "update" },
            DisplayStyle = DataGridViewComboBoxDisplayStyle.DropDownButton,
            FlatStyle = FlatStyle.Flat, Width = 105
        });
        _snapshots.Columns.Add(new DataGridViewTextBoxColumn
        {
            Name = "SnapshotTag", HeaderText = "Tag",
            ToolTipText = "Updates: YYMM. Baselines: BLnn or BLnn-A.", Width = 105
        });
        _snapshots.Columns.Add(new DataGridViewTextBoxColumn
        {
            Name = "MonthUpdate", HeaderText = "Month update",
            ToolTipText = "ISO yyyy-MM-dd. Updates must use the first day from YYMM; baselines require the governed Athena anchor month.", Width = 125
        });
        _snapshots.Columns.Add(new DataGridViewTextBoxColumn
        {
            Name = "DataDate", HeaderText = "XER data date",
            ToolTipText = "ISO yyyy-MM-dd; must match PROJECT.last_recalc_date.", Width = 125
        });
    }

    private void ConfigureInputs()
    {
        _projectCode.AccessibleName = "Governed project code";
        _projectCode.PlaceholderText = "For example QAC000623-01-02 or NE Part B";
        _projectName.Text = string.Empty;
        _projectName.PlaceholderText = "Governed report project name (not the P6 short name)";
        _projectName.AccessibleName = "Project name";
        _programmeType.DropDownStyle = ComboBoxStyle.DropDownList;
        _programmeType.Items.AddRange(new object[] { "C - Contract", "T - Target" });
        _programmeType.SelectedIndex = -1;
        _programmeType.AccessibleName = "Programme type C or T";
        _parserVersion.Text = typeof(ProgrammeReviewContract).Assembly.GetName().Version?.ToString(3) ?? "1.0.0";
        _parserVersion.ReadOnly = true;
        _parserVersion.TabStop = false;
        _parserVersion.AccessibleName = "Parser version recorded in the manifest";
    }

    private void PopulateSnapshots(
        IReadOnlyList<string> xerFilePaths,
        IReadOnlyDictionary<string, DateOnly> suggestedDataDates)
    {
        foreach (string inputPath in xerFilePaths)
        {
            string fullPath = Path.GetFullPath(inputPath);
            string filename = Path.GetFileName(fullPath);
            DateOnly? dataDate = suggestedDataDates.TryGetValue(filename, out DateOnly suggested) ? suggested : null;
            (string? kind, string tag, DateOnly? monthUpdate) = InferSnapshotMetadata(filename);
            int index = _snapshots.Rows.Add(
                filename, kind, tag,
                monthUpdate?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? string.Empty,
                dataDate?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? string.Empty);
            DataGridViewRow row = _snapshots.Rows[index];
            row.Tag = fullPath;
            row.Cells["OriginalFilename"].ToolTipText = fullPath;
        }
    }

    private static (string? Kind, string Tag, DateOnly? MonthUpdate) InferSnapshotMetadata(string filename)
    {
        // Match the governed suffix before legacy hints: digits/BL tags inside a flexible
        // project code must not override the actual snapshot tag at the end of the name.
        Match governed = GovernedFilenamePattern.Match(filename);
        if (governed.Success && DateOnly.TryParseExact(
                governed.Groups["date"].Value, new[] { "yyyyMMdd", "yyyy-MM-dd" },
                CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
        {
            string tag = governed.Groups["tag"].Value.ToUpperInvariant();
            if (tag.StartsWith("BL", StringComparison.Ordinal)) return ("baseline", tag, null);
            return TryGetUpdateMonth(tag, out DateOnly governedMonth)
                ? ("update", tag, governedMonth)
                : (null, string.Empty, null);
        }
        Match baseline = FilenameBaselinePattern.Match(filename);
        if (baseline.Success)
        {
            string tag = baseline.Groups[1].Value.ToUpperInvariant();
            return ("baseline", tag, null);
        }
        Match update = FilenameUpdatePattern.Match(filename);
        if (update.Success && TryGetUpdateMonth(update.Groups[1].Value, out DateOnly updateMonth))
            return ("update", update.Groups[1].Value, updateMonth);
        return (null, string.Empty, null);
    }

    private static void AddField(TableLayoutPanel layout, string labelText, Control input, int labelColumn, int row)
    {
        var label = new Label
        {
            Text = labelText, AutoSize = true, Anchor = AnchorStyles.Left,
            Margin = new Padding(labelColumn == 0 ? 0 : 16, 6, 6, 6)
        };
        input.Dock = DockStyle.Fill;
        input.Margin = new Padding(0, 3, 0, 3);
        layout.Controls.Add(label, labelColumn, row);
        layout.Controls.Add(input, labelColumn + 1, row);
    }

    private void ExportButton_Click(object? sender, EventArgs e)
    {
        if (!TryBuildRequest(out ProgrammeReviewBundleRequest? request)) return;
        BundleRequest = request;
        DialogResult = DialogResult.OK;
        Close();
    }

    private bool TryBuildRequest(out ProgrammeReviewBundleRequest? request)
    {
        request = null;
        ClearGridErrors();
        if (string.IsNullOrWhiteSpace(_projectCode.Text))
            return Fail("Project code is required.", _projectCode);
        string projectCode = ProgrammeReviewNaming.NormalizeProjectCode(_projectCode.Text);
        string projectName = _projectName.Text.Trim();
        if (projectName.Length == 0) return Fail("Project name is required.", _projectName);
        string programmeSelection = Convert.ToString(_programmeType.SelectedItem, CultureInfo.InvariantCulture) ?? string.Empty;
        string programmeType = programmeSelection.Length > 0 ? programmeSelection[..1] : string.Empty;
        if (programmeType is not ("C" or "T")) return Fail("Select programme type C or T.", _programmeType);
        if (_snapshots.Rows.Count == 0) return Fail("Add at least one XER file before creating a Programme Review bundle.");

        var snapshots = new List<ProgrammeReviewSnapshot>(_snapshots.Rows.Count);
        var filenames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var tags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var canonicals = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        bool hasBaseline = false;

        foreach (DataGridViewRow row in _snapshots.Rows)
        {
            string filename = CellText(row, "OriginalFilename");
            string path = row.Tag as string ?? string.Empty;
            if (!File.Exists(path)) return FailRow(row, "OriginalFilename", $"XER file no longer exists: {path}");
            if (!string.Equals(Path.GetFileName(path), filename, StringComparison.Ordinal))
                return FailRow(row, "OriginalFilename", "Original filename must exactly match the selected path filename.");
            if (!filenames.Add(filename))
                return FailRow(row, "OriginalFilename", $"Duplicate original XER filename '{filename}'. Rename one source file before export.");

            string kindText = CellText(row, "SnapshotKind").ToLowerInvariant();
            ProgrammeReviewSnapshotKind kind;
            if (kindText == "baseline") { kind = ProgrammeReviewSnapshotKind.Baseline; hasBaseline = true; }
            else if (kindText == "update") kind = ProgrammeReviewSnapshotKind.Update;
            else return FailRow(row, "SnapshotKind", "Select baseline or update for every XER file.");

            string tag = CellText(row, "SnapshotTag").ToUpperInvariant();
            if (kind == ProgrammeReviewSnapshotKind.Baseline && !BaselineTagPattern.IsMatch(tag))
                return FailRow(row, "SnapshotTag", "Baseline tags must use BLnn or BLnn-A.");
            if (kind == ProgrammeReviewSnapshotKind.Update && !UpdateTagPattern.IsMatch(tag))
                return FailRow(row, "SnapshotTag", "Update tags must use YYMM.");
            if (!tags.Add(tag)) return FailRow(row, "SnapshotTag", $"Duplicate snapshot tag '{tag}'.");

            if (!TryParseDate(CellText(row, "MonthUpdate"), out DateOnly monthUpdate))
                return FailRow(row, "MonthUpdate", "Month update must use yyyy-MM-dd.");
            if (!TryParseDate(CellText(row, "DataDate"), out DateOnly dataDate))
                return FailRow(row, "DataDate", "XER data date must use yyyy-MM-dd.");
            if (monthUpdate.Year is <= 1900 or >= 2200 || dataDate.Year is <= 1900 or >= 2200)
                return FailRow(row, "DataDate", "Dates must be within the supported 1901-2199 range.");

            DateOnly? updateDate = null;
            if (kind == ProgrammeReviewSnapshotKind.Update)
            {
                if (!TryGetUpdateMonth(tag, out DateOnly tagMonth))
                    return FailRow(row, "SnapshotTag", $"Update tag '{tag}' contains an invalid month.");
                if (monthUpdate != tagMonth)
                    return FailRow(row, "MonthUpdate", $"Month update must be {tagMonth:yyyy-MM-dd}, the first day identified by tag {tag}.");
                updateDate = new DateOnly(tagMonth.Year, tagMonth.Month, DateTime.DaysInMonth(tagMonth.Year, tagMonth.Month));
            }

            string canonical = ProgrammeReviewNaming.CreateCanonicalFilename(projectCode, programmeType, tag, dataDate);
            if (!canonicals.Add(canonical))
                return FailRow(row, "SnapshotTag", $"Metadata produces duplicate canonical filename '{canonical}'.");

            row.Cells["SnapshotTag"].Value = tag;
            snapshots.Add(new ProgrammeReviewSnapshot
            {
                OriginalXerFilename = filename,
                XerFilePath = path,
                SnapshotKind = kind,
                SnapshotTag = tag,
                MonthUpdate = monthUpdate,
                UpdateDate = updateDate,
                DataDate = dataDate
            });
        }

        if (!hasBaseline)
            return Fail("A full-history Programme Review bundle must include at least one real baseline candidate. " +
                        "Do not relabel an update as a baseline for production publication.");

        _projectCode.Text = projectCode;
        request = new ProgrammeReviewBundleRequest
        {
            ProjectCode = projectCode,
            ProjectName = projectName,
            ProgrammeType = programmeType,
            ParserVersion = string.IsNullOrWhiteSpace(_parserVersion.Text) ? null : _parserVersion.Text.Trim(),
            Snapshots = snapshots
        };
        return true;
    }

    private static bool TryParseDate(string text, out DateOnly value) =>
        DateOnly.TryParseExact(text.Trim(), "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out value);

    private static bool TryGetUpdateMonth(string tag, out DateOnly month)
    {
        month = default;
        if (!UpdateTagPattern.IsMatch(tag)) return false;
        int year = 2000 + int.Parse(tag.AsSpan(0, 2), CultureInfo.InvariantCulture);
        int number = int.Parse(tag.AsSpan(2, 2), CultureInfo.InvariantCulture);
        if (number is < 1 or > 12) return false;
        month = new DateOnly(year, number, 1);
        return true;
    }

    protected override void OnShown(EventArgs e)
    {
        base.OnShown(e);
        Rectangle workingArea = Screen.FromControl(this).WorkingArea;
        MinimumSize = new Size(Math.Min(760, workingArea.Width), Math.Min(480, workingArea.Height));
        Size = new Size(Math.Min(Width, workingArea.Width), Math.Min(Height, workingArea.Height));
        CenterToParent();
    }

    private static string CellText(DataGridViewRow row, string name) =>
        Convert.ToString(row.Cells[name].Value, CultureInfo.InvariantCulture)?.Trim() ?? string.Empty;

    private void ClearGridErrors()
    {
        foreach (DataGridViewRow row in _snapshots.Rows)
        {
            row.ErrorText = string.Empty;
            foreach (DataGridViewCell cell in row.Cells) cell.ErrorText = string.Empty;
        }
    }

    private bool FailRow(DataGridViewRow row, string columnName, string message)
    {
        row.Cells[columnName].ErrorText = message;
        _snapshots.CurrentCell = row.Cells[columnName];
        return Fail(message, _snapshots);
    }

    private bool Fail(string message, Control? focus = null)
    {
        MessageBox.Show(this, message, "Programme Review metadata", MessageBoxButtons.OK, MessageBoxIcon.Warning);
        focus?.Focus();
        return false;
    }
}
