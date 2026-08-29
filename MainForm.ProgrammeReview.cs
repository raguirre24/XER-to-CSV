using System.Diagnostics;
using System.Drawing;
using System.Globalization;
using System.Windows.Forms;
using XerToCsvConverter.ProgrammeReview;

namespace XerToCsvConverter;

public partial class MainForm
{
    private Button btnExportProgrammeReview = null!;
    private bool _programmeReviewOperationActive;
    private bool _closeAfterProgrammeReviewCancellation;

    private void InitializeProgrammeReviewUi()
    {
        btnExportProgrammeReview = new Button
        {
            Name = "btnExportProgrammeReview",
            Text = "Programme Review Bundle",
            AutoSize = true,
            MinimumSize = Size.Empty,
            Margin = new Padding(5, 0, 0, 0),
            AccessibleName = "Export Programme Review CSV bundle",
            AccessibleDescription = "Configure metadata and create a validated full-history Programme Review bundle."
        };
        btnExportProgrammeReview.Click += BtnExportProgrammeReview_Click;
        toolTip.SetToolTip(btnExportProgrammeReview,
            "Create the versioned ten-table Programme Review bundle with an audit manifest");
        ApplyButtonStyle(btnExportProgrammeReview, UiTheme.Success, UiTheme.Success, Color.White,
            Color.FromArgb(26, 157, 98), Color.FromArgb(21, 112, 70));
        btnExportProgrammeReview.Font = _uiFontBold;
        btnParseXer.EnabledChanged += (_, _) => UpdateProgrammeReviewButtonState();
        toolStripProgressBar.VisibleChanged += (_, _) => QueueProgrammeReviewButtonStateUpdate();
        btnAddFile.Click += (_, _) => QueueProgrammeReviewButtonStateUpdate();
        btnRemoveFile.Click += (_, _) => QueueProgrammeReviewButtonStateUpdate();
        btnClearFiles.Click += (_, _) => QueueProgrammeReviewButtonStateUpdate();
        btnSelectOutput.Click += (_, _) => QueueProgrammeReviewButtonStateUpdate();
        lvwXerFiles.DragDrop += (_, _) => QueueProgrammeReviewButtonStateUpdate();
        txtOutputPath.DragDrop += (_, _) => QueueProgrammeReviewButtonStateUpdate();
        UpdateProgrammeReviewButtonState();
    }

    private void AttachProgrammeReviewButtonToActionPanel()
    {
        FlowLayoutPanel? actions = panelExportActions.Controls
            .OfType<FlowLayoutPanel>()
            .FirstOrDefault();
        if (actions is not null && !actions.Controls.Contains(btnExportProgrammeReview))
            actions.Controls.Add(btnExportProgrammeReview);
    }

    internal void VerifyProgrammeReviewUiIntegration()
    {
        FlowLayoutPanel? actions = panelExportActions.Controls.OfType<FlowLayoutPanel>().FirstOrDefault();
        if (actions is null || !actions.Controls.Contains(btnExportProgrammeReview))
            throw new InvalidOperationException("The Programme Review action is not attached to the Windows export UI.");
        if (string.IsNullOrWhiteSpace(btnExportProgrammeReview.AccessibleName)
            || string.IsNullOrWhiteSpace(btnExportProgrammeReview.AccessibleDescription))
            throw new InvalidOperationException("The Programme Review action is missing accessible metadata.");

        using var dialog = new ProgrammeReviewExportDialog(
            Array.Empty<string>(),
            new Dictionary<string, DateOnly>(StringComparer.OrdinalIgnoreCase));
        if (string.IsNullOrWhiteSpace(dialog.AccessibleName))
            throw new InvalidOperationException("The Programme Review metadata dialog is missing an accessible name.");
    }

    private async void BtnExportProgrammeReview_Click(object? sender, EventArgs e)
    {
        if (IsOperationActive) return;
        if (_xerFilePaths.Count == 0)
        {
            ShowWarning("Add at least one XER file before creating a Programme Review bundle.");
            return;
        }
        if (string.IsNullOrWhiteSpace(_outputDirectory) || !Directory.Exists(_outputDirectory))
        {
            ShowError("Please select a valid output directory first.");
            return;
        }
        if (!HasParsedProgrammeReviewInputs())
        {
            btnExportProgrammeReview.Enabled = false;
            ShowWarning("Parse all currently selected XER files successfully before creating a Programme Review bundle.");
            return;
        }

        string[] missingFiles = _xerFilePaths.Where(path => !File.Exists(path)).ToArray();
        if (missingFiles.Length > 0)
        {
            ShowError("One or more selected XER files no longer exist:\n\n" + string.Join("\n", missingFiles));
            return;
        }

        using var dialog = new ProgrammeReviewExportDialog(
            _xerFilePaths.ToArray(),
            GetProgrammeReviewDataDateSuggestions());
        if (dialog.ShowDialog(this) != DialogResult.OK || dialog.BundleRequest is null) return;

        ProgrammeReviewBundleRequest request = dialog.BundleRequest;
        InvalidateLegacyParseStateAfterProgrammeReviewStarts();
        SetUIEnabled(enabled: false);
        UpdateStatus("Creating Programme Review bundle...");
        LogActivity($"Starting Programme Review export for {request.ProjectCode}-{request.ProgrammeType} " +
                    $"from {request.Snapshots.Count} XER file(s)...");
        toolStripProgressBar.Visible = true;
        toolStripProgressBar.Value = 0;
        _cancellationTokenSource = new CancellationTokenSource();
        CancellationToken token = _cancellationTokenSource.Token;
        _programmeReviewOperationActive = true;

        var progress = new Progress<ProcessingService.DetailedProgress>(p =>
        {
            UpdateStatus(p.Message ?? "Creating Programme Review bundle...");
            toolStripProgressBar.Value = Math.Clamp(p.Percent, toolStripProgressBar.Minimum, toolStripProgressBar.Maximum);
        });

        try
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            var service = new ProgrammeReviewBundleService();
            ProgrammeReviewBundleResult result = await Task.Run(
                () => service.BuildFromXerFilesAsync(request, _outputDirectory, progress, token), token);
            stopwatch.Stop();

            toolStripProgressBar.Value = 100;
            string summary = $"Programme Review bundle complete: {result.BundleId}. " +
                             $"{result.ManifestRows.Count} manifest rows; {stopwatch.Elapsed.TotalSeconds:F2}s.";
            UpdateStatus(summary);
            LogActivity(summary);
            LogActivity("Bundle path: " + result.BundlePath);
            ShowProgrammeReviewComplete(result);
        }
        catch (OperationCanceledException)
        {
            UpdateStatus("Programme Review export cancelled; no partial bundle was published.");
            LogActivity("Programme Review export cancelled by user; staging output was removed.");
            if (!_closeAfterProgrammeReviewCancellation)
                ShowWarning("Programme Review export was cancelled. No partial bundle was published.", "Export Cancelled");
        }
        catch (ProgrammeReviewValidationException ex)
        {
            UpdateStatus("Programme Review validation failed.");
            LogActivity("Programme Review validation failed: " + ex.Message);
            if (!_closeAfterProgrammeReviewCancellation)
                ShowError("The Programme Review bundle was not published because validation failed:\n\n" + ex.Message);
        }
        catch (Exception ex)
        {
            UpdateStatus("Programme Review export failed.");
            LogActivity("ERROR during Programme Review export: " + ex.Message);
            string details = ex.InnerException is null ? string.Empty : "\n\nDetails: " + ex.InnerException.Message;
            if (!_closeAfterProgrammeReviewCancellation)
                ShowError("Programme Review export failed. No complete bundle was published.\n\n" + ex.Message + details);
        }
        finally
        {
            _programmeReviewOperationActive = false;
            SetUIEnabled(enabled: true);
            toolStripProgressBar.Visible = false;
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;
            if (_closeAfterProgrammeReviewCancellation && !IsDisposed)
            {
                _closeAfterProgrammeReviewCancellation = false;
                BeginInvoke((Action)Close);
            }
        }
    }

    private void UpdateProgrammeReviewButtonState()
    {
        btnExportProgrammeReview.Enabled = btnParseXer.Enabled && HasParsedProgrammeReviewInputs();
    }

    private void QueueProgrammeReviewButtonStateUpdate()
    {
        if (!IsHandleCreated || IsDisposed) return;
        BeginInvoke((Action)UpdateProgrammeReviewButtonState);
    }

    private bool HasParsedProgrammeReviewInputs()
    {
        if (_xerFilePaths.Count == 0
            || string.IsNullOrWhiteSpace(_outputDirectory)
            || !Directory.Exists(_outputDirectory))
            return false;
        if (!_dataStore.ContainsTable(TableNames.Task)
            || !_dataStore.ContainsTable(TableNames.Project)
            || !_dataStore.ContainsTable(TableNames.ProjWbs)
            || !_dataStore.ContainsTable(TableNames.Calendar))
            return false;
        return lvwXerFiles.Items.Count == _xerFilePaths.Count
            && lvwXerFiles.Items.Cast<ListViewItem>()
                .All(item => string.Equals(item.SubItems[2].Text, "Success", StringComparison.Ordinal));
    }

    private IReadOnlyDictionary<string, DateOnly> GetProgrammeReviewDataDateSuggestions()
    {
        var candidates = new Dictionary<string, HashSet<DateOnly>>(StringComparer.OrdinalIgnoreCase);
        XerTable? project = _dataStore.GetTable(TableNames.Project);
        if (project?.Headers is null || !project.FieldIndexes.TryGetValue(FieldNames.LastRecalcDate, out int dateIndex))
            return new Dictionary<string, DateOnly>(StringComparer.OrdinalIgnoreCase);

        foreach (DataRow row in project.Rows)
        {
            string raw = XerTable.GetFieldValueSafe(row, dateIndex);
            if (!TryReadProgrammeReviewDate(raw, out DateOnly dataDate)) continue;
            if (!candidates.TryGetValue(row.SourceFilename, out HashSet<DateOnly>? values))
            {
                values = new HashSet<DateOnly>();
                candidates[row.SourceFilename] = values;
            }
            values.Add(dataDate);
        }

        return candidates.Where(pair => pair.Value.Count == 1)
            .ToDictionary(pair => pair.Key, pair => pair.Value.Single(), StringComparer.OrdinalIgnoreCase);
    }

    private static bool TryReadProgrammeReviewDate(string raw, out DateOnly value)
    {
        string text = raw.Trim();
        if (text.Length >= 10 && DateOnly.TryParseExact(text[..10], "yyyy-MM-dd",
                CultureInfo.InvariantCulture, DateTimeStyles.None, out value)) return true;
        if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out DateTime dateTime))
        {
            value = DateOnly.FromDateTime(dateTime);
            return true;
        }
        value = default;
        return false;
    }

    private void ShowProgrammeReviewComplete(ProgrammeReviewBundleResult result)
    {
        DialogResult open = MessageBox.Show(
            this,
            $"Programme Review bundle created successfully.\n\n" +
            $"Bundle: {result.BundleId}\n" +
            $"Folder: {result.BundlePath}\n" +
            $"Manifest rows: {result.ManifestRows.Count}\n\n" +
            "Open the completed bundle folder?",
            "Programme Review Export Complete",
            MessageBoxButtons.YesNo,
            MessageBoxIcon.Information);
        if (open != DialogResult.Yes) return;

        try
        {
            Process.Start(new ProcessStartInfo { FileName = result.BundlePath, UseShellExecute = true });
        }
        catch (Exception ex)
        {
            ShowWarning("Could not open the completed bundle folder: " + ex.Message);
        }
    }

    private void InvalidateLegacyParseStateAfterProgrammeReviewStarts()
    {
        ClearResults(forceGC: true);
        foreach (ListViewItem item in lvwXerFiles.Items)
        {
            item.SubItems[2].Text = "Ready - parse again for legacy export";
            item.ForeColor = SystemColors.ControlText;
        }
        LogActivity("Legacy parsed results were released before the authoritative Programme Review parse. " +
                    "Parse the selected XER files again before using a Standard or Enhanced export.");
    }

    protected override void OnFormClosing(FormClosingEventArgs e)
    {
        if (_programmeReviewOperationActive)
        {
            e.Cancel = true;
            if (!_closeAfterProgrammeReviewCancellation)
            {
                _closeAfterProgrammeReviewCancellation = true;
                UpdateStatus("Cancelling Programme Review export before closing...");
                LogActivity("Window close requested; waiting for Programme Review staging cleanup.");
                _cancellationTokenSource?.Cancel();
                btnCancelOperation.Enabled = false;
            }
            return;
        }

        base.OnFormClosing(e);
    }
}
