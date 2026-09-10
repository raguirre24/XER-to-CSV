using System.Globalization;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter;

/// <summary>
/// Collects the Tender Review project identity and ordered stage metadata without reusing the
/// filename-keyed Programme Review workflow.
/// </summary>
internal sealed class TenderReviewExportDialog : Form
{
    private readonly Func<DateOnly> _localStatusDateProvider;
    private readonly TextBox _projectCode = new();
    private readonly TextBox _projectName = new();
    private readonly TextBox _parserVersion = new();
    private readonly DataGridView _sources = new();
    private readonly Button _addButton = new();
    private readonly Button _removeButton = new();
    private readonly Button _moveUpButton = new();
    private readonly Button _moveDownButton = new();
    private readonly Button _exportButton = new();
    private readonly Button _cancelButton = new();
    private int _nextSourceIndex;

    internal TenderReviewExportDialog(IReadOnlyList<string> xerFilePaths)
        : this(xerFilePaths, CaptureLocalStatusDate)
    {
    }

    internal TenderReviewExportDialog(
        IReadOnlyList<string> xerFilePaths,
        Func<DateOnly> localStatusDateProvider)
    {
        ArgumentNullException.ThrowIfNull(xerFilePaths);
        ArgumentNullException.ThrowIfNull(localStatusDateProvider);

        _localStatusDateProvider = localStatusDateProvider;
        Text = "Tender Review bundle";
        AccessibleName = "Tender Review bundle metadata";
        AccessibleDescription =
            "Configure one Tender project and the editable status date for each ordered XER source.";
        StartPosition = FormStartPosition.CenterParent;
        AutoScaleMode = AutoScaleMode.Dpi;
        MinimumSize = new Size(780, 500);
        Size = new Size(1120, 680);
        FormBorderStyle = FormBorderStyle.Sizable;
        MaximizeBox = true;
        MinimizeBox = false;
        ShowIcon = false;

        BuildLayout();
        ConfigureInputs();
        AddSources(xerFilePaths);
        UpdateSourceButtons();
    }

    internal TenderReviewBundleRequest? BundleRequest { get; private set; }

    /// <summary>
    /// Captures the Windows user's calendar date without converting through UTC. The captured value
    /// is written to a source row once, when that source is added, and remains editable thereafter.
    /// </summary>
    internal static DateOnly CaptureLocalStatusDate() => DateOnly.FromDateTime(DateTime.Now);

    internal IReadOnlyList<string> SourceTokensForTesting => _sources.Rows
        .Cast<DataGridViewRow>()
        .Select(GetSourceRow)
        .Select(source => source.SourceToken)
        .ToArray();

    internal IReadOnlyList<string> SourcePathsForTesting => _sources.Rows
        .Cast<DataGridViewRow>()
        .Select(GetSourceRow)
        .Select(source => source.FullPath)
        .ToArray();

    internal IReadOnlyList<DateOnly> StatusDatesForTesting => _sources.Rows
        .Cast<DataGridViewRow>()
        .Select(row => TryParseDate(CellText(row, "StatusDate"), out DateOnly date) ? date : default)
        .ToArray();

    internal void SetIdentityForTesting(string projectCode, string projectName)
    {
        _projectCode.Text = projectCode;
        _projectName.Text = projectName;
    }

    internal void SetStatusDateForTesting(int rowIndex, DateOnly statusDate)
    {
        _sources.Rows[rowIndex].Cells["StatusDate"].Value =
            statusDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    }

    internal void MoveSourceForTesting(int rowIndex, int direction)
    {
        _sources.CurrentCell = _sources.Rows[rowIndex].Cells["StatusDate"];
        MoveCurrentSource(direction);
    }

    internal bool TryBuildRequestForTesting(out TenderReviewBundleRequest? request) => TryBuildRequest(out request);

    private void BuildLayout()
    {
        var root = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            AutoScroll = true,
            ColumnCount = 1,
            RowCount = 5,
            Padding = new Padding(14)
        };
        root.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100f));
        root.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        root.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        root.RowStyles.Add(new RowStyle(SizeType.Percent, 100f));
        root.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        root.RowStyles.Add(new RowStyle(SizeType.AutoSize));

        var guidance = new Label
        {
            AutoSize = true,
            Dock = DockStyle.Fill,
            MaximumSize = new Size(1060, 0),
            Margin = new Padding(0, 0, 0, 12),
            Text = "Create one Tender Review bundle for one project. Each XER is a distinct Tender stage, " +
                   "identified by its editable status_date. The default is captured from this computer's " +
                   "local calendar when a row is added. Repeated filenames, paths, and source content are allowed."
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
        AddField(metadata, "Parser version:", _parserVersion, 2, 1);

        ConfigureGrid();

        var sourceButtons = new FlowLayoutPanel
        {
            AutoSize = true,
            Dock = DockStyle.Fill,
            FlowDirection = FlowDirection.LeftToRight,
            WrapContents = true,
            Margin = new Padding(0, 8, 0, 0)
        };
        ConfigureSourceButton(_addButton, "&Add XER files", "Add one or more Tender stage XER files");
        ConfigureSourceButton(_removeButton, "&Remove", "Remove the selected Tender stage");
        ConfigureSourceButton(_moveUpButton, "Move &up", "Move the selected Tender stage earlier in input order");
        ConfigureSourceButton(_moveDownButton, "Move &down", "Move the selected Tender stage later in input order");
        _addButton.Click += AddButton_Click;
        _removeButton.Click += RemoveButton_Click;
        _moveUpButton.Click += (_, _) => MoveCurrentSource(-1);
        _moveDownButton.Click += (_, _) => MoveCurrentSource(1);
        sourceButtons.Controls.AddRange([_addButton, _removeButton, _moveUpButton, _moveDownButton]);

        var dialogButtons = new FlowLayoutPanel
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
        _exportButton.AccessibleName = "Validate metadata and export Tender Review bundle";
        _exportButton.Click += ExportButton_Click;
        _cancelButton.Text = "Cancel";
        _cancelButton.AutoSize = true;
        _cancelButton.MinimumSize = new Size(90, 36);
        _cancelButton.DialogResult = DialogResult.Cancel;
        dialogButtons.Controls.Add(_exportButton);
        dialogButtons.Controls.Add(_cancelButton);

        root.Controls.Add(guidance, 0, 0);
        root.Controls.Add(metadata, 0, 1);
        root.Controls.Add(_sources, 0, 2);
        root.Controls.Add(sourceButtons, 0, 3);
        root.Controls.Add(dialogButtons, 0, 4);
        Controls.Add(root);
        AcceptButton = _exportButton;
        CancelButton = _cancelButton;
    }

    private void ConfigureGrid()
    {
        _sources.Dock = DockStyle.Fill;
        _sources.AllowUserToAddRows = false;
        _sources.AllowUserToDeleteRows = false;
        _sources.AllowUserToOrderColumns = false;
        _sources.AutoGenerateColumns = false;
        _sources.AutoSizeRowsMode = DataGridViewAutoSizeRowsMode.AllCells;
        _sources.BackgroundColor = SystemColors.Window;
        _sources.BorderStyle = BorderStyle.FixedSingle;
        _sources.EditMode = DataGridViewEditMode.EditOnEnter;
        _sources.MultiSelect = false;
        _sources.RowHeadersVisible = false;
        _sources.SelectionMode = DataGridViewSelectionMode.CellSelect;
        _sources.AccessibleName = "Ordered Tender XER sources";
        _sources.AccessibleDescription =
            "Each row is one Tender stage. Edit status date in ISO yyyy-MM-dd format; repeated sources are allowed.";
        _sources.SelectionChanged += (_, _) => UpdateSourceButtons();

        _sources.Columns.Add(new DataGridViewTextBoxColumn
        {
            Name = "InputOrder", HeaderText = "Order", ReadOnly = true, Width = 65,
            SortMode = DataGridViewColumnSortMode.NotSortable
        });
        _sources.Columns.Add(new DataGridViewTextBoxColumn
        {
            Name = "OriginalFilename", HeaderText = "Original XER filename", ReadOnly = true,
            AutoSizeMode = DataGridViewAutoSizeColumnMode.Fill, FillWeight = 35, MinimumWidth = 210,
            SortMode = DataGridViewColumnSortMode.NotSortable
        });
        _sources.Columns.Add(new DataGridViewTextBoxColumn
        {
            Name = "SourcePath", HeaderText = "Source path", ReadOnly = true,
            AutoSizeMode = DataGridViewAutoSizeColumnMode.Fill, FillWeight = 65, MinimumWidth = 300,
            SortMode = DataGridViewColumnSortMode.NotSortable
        });
        _sources.Columns.Add(new DataGridViewTextBoxColumn
        {
            Name = "StatusDate", HeaderText = "status_date", Width = 125,
            ToolTipText = "Tender stage identity in ISO yyyy-MM-dd format.",
            SortMode = DataGridViewColumnSortMode.NotSortable
        });
    }

    private void ConfigureInputs()
    {
        _projectCode.AccessibleName = "Tender project code";
        _projectCode.PlaceholderText = "For example QAC000623-01-02 or NE Part B";
        _projectName.PlaceholderText = "Project name recorded in Tender output";
        _projectName.AccessibleName = "Tender project name";
        _parserVersion.Text =
            typeof(TenderReviewBundleRequest).Assembly.GetName().Version?.ToString(3) ?? "1.0.0";
        _parserVersion.ReadOnly = true;
        _parserVersion.TabStop = false;
        _parserVersion.AccessibleName = "Parser version recorded in the Tender manifest";
    }

    private void AddButton_Click(object? sender, EventArgs e)
    {
        using var picker = new OpenFileDialog
        {
            Filter = "XER files (*.xer)|*.xer|All files (*.*)|*.*",
            Title = "Add Tender Review XER stage(s)",
            Multiselect = true
        };
        if (picker.ShowDialog(this) == DialogResult.OK)
            AddSources(picker.FileNames);
    }

    private void AddSources(IEnumerable<string> xerFilePaths)
    {
        ArgumentNullException.ThrowIfNull(xerFilePaths);
        string[] paths = xerFilePaths.ToArray();
        if (paths.Length == 0) return;

        // Capture once for this add action. Later edits and export never recalculate this default.
        DateOnly capturedStatusDate = _localStatusDateProvider();
        string statusDateText = capturedStatusDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        _sources.SuspendLayout();
        try
        {
            foreach (string inputPath in paths)
            {
                string fullPath = Path.GetFullPath(inputPath);
                string token = TenderReviewNaming.CreateSourceToken(_nextSourceIndex++);
                int rowIndex = _sources.Rows.Add(
                    _sources.Rows.Count + 1,
                    Path.GetFileName(fullPath),
                    fullPath,
                    statusDateText);
                DataGridViewRow row = _sources.Rows[rowIndex];
                row.Tag = new TenderReviewUiSource(token, fullPath);
                row.Cells["OriginalFilename"].ToolTipText = fullPath;
            }
        }
        finally
        {
            _sources.ResumeLayout();
        }

        if (_sources.Rows.Count > 0)
            _sources.CurrentCell = _sources.Rows[_sources.Rows.Count - 1].Cells["StatusDate"];
        UpdateSourceButtons();
        TrySuggestProjectIdentity(paths);
    }

    private void TrySuggestProjectIdentity(IReadOnlyList<string> paths)
    {
        if (!string.IsNullOrWhiteSpace(_projectCode.Text) && !string.IsNullOrWhiteSpace(_projectName.Text))
            return;

        foreach (string path in paths)
        {
            if (!File.Exists(path)) continue;
            try
            {
                using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
                var identity = TenderReviewXerMetadataReader.ReadProjectIdentityAsync(stream).GetAwaiter().GetResult();
                if (identity is not null)
                {
                    if (string.IsNullOrWhiteSpace(_projectCode.Text) && !string.IsNullOrWhiteSpace(identity.ProjectCode))
                    {
                        try
                        {
                            _projectCode.Text = TenderReviewNaming.NormalizeProjectCode(identity.ProjectCode);
                        }
                        catch
                        {
                            // Non-blocking fallback for invalid raw codes
                        }
                    }
                    if (string.IsNullOrWhiteSpace(_projectName.Text) && !string.IsNullOrWhiteSpace(identity.ProjectName))
                    {
                        _projectName.Text = identity.ProjectName.Trim();
                    }
                    break;
                }
            }
            catch
            {
                // Non-blocking best-effort prefill
            }
        }
    }

    private void RemoveButton_Click(object? sender, EventArgs e)
    {
        int rowIndex = _sources.CurrentCell?.RowIndex ?? -1;
        if (rowIndex < 0 || rowIndex >= _sources.Rows.Count) return;
        _sources.Rows.RemoveAt(rowIndex);
        RenumberRows();
        if (_sources.Rows.Count > 0)
        {
            int next = Math.Min(rowIndex, _sources.Rows.Count - 1);
            _sources.CurrentCell = _sources.Rows[next].Cells["StatusDate"];
        }
        UpdateSourceButtons();
    }

    private void MoveCurrentSource(int direction)
    {
        int oldIndex = _sources.CurrentCell?.RowIndex ?? -1;
        int newIndex = oldIndex + direction;
        if (oldIndex < 0 || newIndex < 0 || newIndex >= _sources.Rows.Count) return;

        _sources.EndEdit();
        int columnIndex = _sources.CurrentCell?.ColumnIndex ?? _sources.Columns["StatusDate"].Index;
        DataGridViewRow row = _sources.Rows[oldIndex];
        _sources.Rows.RemoveAt(oldIndex);
        _sources.Rows.Insert(newIndex, row);
        RenumberRows();
        _sources.CurrentCell = _sources.Rows[newIndex].Cells[columnIndex];
        UpdateSourceButtons();
    }

    private void RenumberRows()
    {
        for (int index = 0; index < _sources.Rows.Count; index++)
            _sources.Rows[index].Cells["InputOrder"].Value = index + 1;
    }

    private void UpdateSourceButtons()
    {
        int rowIndex = _sources.CurrentCell?.RowIndex ?? -1;
        _removeButton.Enabled = rowIndex >= 0;
        _moveUpButton.Enabled = rowIndex > 0;
        _moveDownButton.Enabled = rowIndex >= 0 && rowIndex < _sources.Rows.Count - 1;
    }

    private void ExportButton_Click(object? sender, EventArgs e)
    {
        if (!TryBuildRequest(out TenderReviewBundleRequest? request)) return;
        BundleRequest = request;
        DialogResult = DialogResult.OK;
        Close();
    }

    private bool TryBuildRequest(out TenderReviewBundleRequest? request)
    {
        request = null;
        _sources.EndEdit();
        ClearGridErrors();

        if (string.IsNullOrWhiteSpace(_projectCode.Text))
            return Fail("Project code is required.", _projectCode);
        string projectCode = TenderReviewNaming.NormalizeProjectCode(_projectCode.Text);
        string projectName = _projectName.Text.Trim();
        if (projectName.Length == 0)
            return Fail("Project name is required.", _projectName);
        if (_sources.Rows.Count == 0)
            return Fail("Add at least one XER file before creating a Tender Review bundle.", _sources);

        var sources = new List<TenderReviewSource>(_sources.Rows.Count);
        var tokens = new HashSet<string>(StringComparer.Ordinal);
        var statusDates = new HashSet<DateOnly>();

        foreach (DataGridViewRow row in _sources.Rows)
        {
            TenderReviewUiSource source = GetSourceRow(row);
            string path = source.FullPath;
            if (!tokens.Add(source.SourceToken))
                return FailRow(row, "OriginalFilename", "Internal Tender source tokens must be unique.");
            if (!File.Exists(path))
                return FailRow(row, "SourcePath", $"XER file no longer exists: {path}");

            string filename = Path.GetFileName(path);
            if (!filename.EndsWith(".xer", StringComparison.OrdinalIgnoreCase))
                return FailRow(row, "OriginalFilename", $"Tender source '{filename}' must be an .xer file.");
            if (!string.Equals(CellText(row, "OriginalFilename"), filename, StringComparison.Ordinal))
                return FailRow(row, "OriginalFilename", "Original filename must exactly match its selected source path.");

            if (!TryParseDate(CellText(row, "StatusDate"), out DateOnly statusDate))
                return FailRow(row, "StatusDate", "status_date must use ISO yyyy-MM-dd format.");
            if (statusDate.Year is <= 1900 or >= 2200)
                return FailRow(row, "StatusDate", "status_date must be within the supported 1901-2199 range.");
            if (!statusDates.Add(statusDate))
                return FailRow(
                    row,
                    "StatusDate",
                    $"Project {projectCode} already has Tender stage status_date {statusDate:yyyy-MM-dd}. " +
                    "Each selected XER must use a unique status_date.");

            sources.Add(new TenderReviewSource
            {
                SourceToken = source.SourceToken,
                OriginalXerFilename = filename,
                XerFilePath = path,
                StatusDate = statusDate
            });
        }

        _projectCode.Text = projectCode;
        request = new TenderReviewBundleRequest
        {
            ProjectCode = projectCode,
            ProjectName = projectName,
            ParserVersion = string.IsNullOrWhiteSpace(_parserVersion.Text) ? null : _parserVersion.Text.Trim(),
            Sources = sources
        };
        return true;
    }

    private static TenderReviewUiSource GetSourceRow(DataGridViewRow row) =>
        row.Tag as TenderReviewUiSource
        ?? throw new InvalidOperationException("Tender source row is missing its stable internal source token.");

    private static bool TryParseDate(string text, out DateOnly value) =>
        DateOnly.TryParseExact(
            text.Trim(),
            "yyyy-MM-dd",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out value);

    private static string CellText(DataGridViewRow row, string name) =>
        Convert.ToString(row.Cells[name].Value, CultureInfo.InvariantCulture)?.Trim() ?? string.Empty;

    private static void AddField(
        TableLayoutPanel layout,
        string labelText,
        Control input,
        int labelColumn,
        int row)
    {
        var label = new Label
        {
            Text = labelText,
            AutoSize = true,
            Anchor = AnchorStyles.Left,
            Margin = new Padding(labelColumn == 0 ? 0 : 16, 6, 6, 6)
        };
        input.Dock = DockStyle.Fill;
        input.Margin = new Padding(0, 3, 0, 3);
        layout.Controls.Add(label, labelColumn, row);
        layout.Controls.Add(input, labelColumn + 1, row);
    }

    private static void ConfigureSourceButton(Button button, string text, string accessibleName)
    {
        button.Text = text;
        button.AutoSize = true;
        button.MinimumSize = new Size(100, 32);
        button.AccessibleName = accessibleName;
    }

    private void ClearGridErrors()
    {
        foreach (DataGridViewRow row in _sources.Rows)
        {
            row.ErrorText = string.Empty;
            foreach (DataGridViewCell cell in row.Cells)
                cell.ErrorText = string.Empty;
        }
    }

    private bool FailRow(DataGridViewRow row, string columnName, string message)
    {
        row.Cells[columnName].ErrorText = message;
        _sources.CurrentCell = row.Cells[columnName];
        return Fail(message, _sources);
    }

    private bool Fail(string message, Control? focus = null)
    {
        MessageBox.Show(this, message, "Tender Review metadata", MessageBoxButtons.OK, MessageBoxIcon.Warning);
        focus?.Focus();
        return false;
    }

    protected override void OnShown(EventArgs e)
    {
        base.OnShown(e);
        Rectangle workingArea = Screen.FromControl(this).WorkingArea;
        MinimumSize = new Size(Math.Min(780, workingArea.Width), Math.Min(500, workingArea.Height));
        Size = new Size(Math.Min(Width, workingArea.Width), Math.Min(Height, workingArea.Height));
        CenterToParent();
    }

    private sealed record TenderReviewUiSource(string SourceToken, string FullPath);
}
