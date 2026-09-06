using System.Diagnostics;
using System.Drawing;
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
            Text = "Create Programme Review bundle",
            AutoSize = true,
            MinimumSize = Size.Empty,
            Margin = new Padding(4),
            AccessibleName = "Export Programme Review CSV bundle",
            AccessibleDescription = "Configure metadata and create a validated full-history Programme Review bundle."
        };
        btnExportProgrammeReview.Click += BtnExportProgrammeReview_Click;
        toolTip.SetToolTip(btnExportProgrammeReview,
            "Create the versioned ten-table Programme Review bundle with a data-quality companion and audit manifest");
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
        ConfigureExportActionButton(btnExportProgrammeReview, "Create Programme &Review bundle");
        btnExportProgrammeReview.TabIndex = 0;
        if (!exportActionsLayout.Controls.Contains(btnExportProgrammeReview))
            exportActionsLayout.Controls.Add(btnExportProgrammeReview, 0, 2);
    }

    internal void VerifyProgrammeReviewUiIntegration()
    {
        CreateControl();
        PerformLayout();
        exportActionsLayout.PerformLayout();

        if (!panelExportActions.Controls.Contains(exportActionsLayout)
            || !exportActionsLayout.Controls.Contains(btnExportProgrammeReview))
            throw new InvalidOperationException("The Programme Review action is not attached to the Windows export UI.");
        TableLayoutPanelCellPosition position = exportActionsLayout.GetPositionFromControl(btnExportProgrammeReview);
        if (position.Column != 0 || position.Row != 2)
            throw new InvalidOperationException("The Programme Review action is not in the primary export position.");
        if (string.IsNullOrWhiteSpace(btnExportProgrammeReview.AccessibleName)
            || string.IsNullOrWhiteSpace(btnExportProgrammeReview.AccessibleDescription))
            throw new InvalidOperationException("The Programme Review action is missing accessible metadata.");

        Control[] visibleControls =
        [
            grpInputFiles,
            grpOutput,
            btnParseXer,
            splitContainerResults,
            btnSelectOutput,
            btnPbiDetails,
            btnExportProgrammeReview,
            btnExportTenderReview,
            btnExportPowerBi,
            btnExportAll,
            btnExportSelected,
            btnCancelOperation
        ];

        Size originalSize = Size;
        try
        {
            VerifyControlsAreContained(visibleControls, "startup");
            Size = MinimumSize;
            PerformLayout();
            exportActionsLayout.PerformLayout();
            VerifyControlsAreContained(visibleControls, "minimum-size");

            int[] actionHeights =
            [
                btnExportProgrammeReview.Height,
                btnExportTenderReview.Height,
                btnExportPowerBi.Height,
                btnExportAll.Height,
                btnExportSelected.Height
            ];
            if (actionHeights.Distinct().Count() != 1)
                throw new InvalidOperationException("Export actions do not have consistent heights.");
        }
        finally
        {
            Size = originalSize;
            PerformLayout();
        }

        using var dialog = new ProgrammeReviewExportDialog(
            Array.Empty<string>(),
            new Dictionary<string, DateOnly>(StringComparer.OrdinalIgnoreCase));
        if (string.IsNullOrWhiteSpace(dialog.AccessibleName))
            throw new InvalidOperationException("The Programme Review metadata dialog is missing an accessible name.");

        VerifyProgrammeReviewIsAvailableWithoutLegacyParse();
    }

    private static void VerifyControlsAreContained(IEnumerable<Control> controls, string layoutName)
    {
        foreach (Control control in controls)
        {
            string label = string.IsNullOrWhiteSpace(control.Text) ? control.Name : control.Text;
            if (!control.Visible || control.Width <= 0 || control.Height <= 0)
                throw new InvalidOperationException($"'{label}' is not visible in the {layoutName} layout.");
            Control current = control;
            while (current.Parent is { } parent)
            {
                if (!parent.ClientRectangle.Contains(current.Bounds))
                    throw new InvalidOperationException(
                        $"'{label}' is clipped by '{parent.Name}' in the {layoutName} layout " +
                        $"(ancestor '{current.Name}', child {current.Bounds}, parent client {parent.ClientRectangle}).");
                current = parent;
                if (current is Form) break;
            }
        }
    }

    private void VerifyProgrammeReviewIsAvailableWithoutLegacyParse()
    {
        string tempDirectory = Path.Combine(Path.GetTempPath(), $"xer-ui-smoke-{Guid.NewGuid():N}");
        string xerPath = Path.Combine(tempDirectory, "2608-SMOKE-C-2608.xer");
        Directory.CreateDirectory(tempDirectory);
        try
        {
            File.WriteAllText(
                xerPath,
                "%T\tPROJECT\r\n" +
                "%F\tproj_id\tlast_recalc_date\r\n" +
                "%R\t1\t2026-08-25 17:00\r\n" +
                "%T\tTASK\r\n");
            _outputDirectory = tempDirectory;
            txtOutputPath.Text = tempDirectory;
            AddXerFiles([xerPath]);
            UpdateInputButtonsState();

            if (!btnExportProgrammeReview.Enabled)
                throw new InvalidOperationException(
                    "Programme Review is not available before the optional legacy parse.");
            if (_dataStore.TableNames.Any())
                throw new InvalidOperationException("The UI smoke test unexpectedly populated legacy parsed tables.");

            DateOnly? detected = ProgrammeReviewXerMetadataReader
                .ReadSingleProjectDataDateAsync(xerPath)
                .AsTask()
                .GetAwaiter()
                .GetResult();
            if (detected != new DateOnly(2026, 8, 25))
                throw new InvalidOperationException("Windows did not detect PROJECT.last_recalc_date directly from XER.");

            using var dialog = new ProgrammeReviewExportDialog(
                [xerPath],
                new Dictionary<string, DateOnly>(StringComparer.OrdinalIgnoreCase)
                {
                    [Path.GetFileName(xerPath)] = detected.Value
                });
            if (!dialog.HasSuggestedDataDate(Path.GetFileName(xerPath), detected.Value))
                throw new InvalidOperationException("The Windows Programme Review dialog did not prefill the detected date.");
        }
        finally
        {
            _xerFilePaths.Clear();
            lvwXerFiles.Items.Clear();
            _outputDirectory = string.Empty;
            txtOutputPath.Clear();
            UpdateInputButtonsState();
            if (Directory.Exists(tempDirectory))
                Directory.Delete(tempDirectory, recursive: true);
        }
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
        string[] missingFiles = _xerFilePaths.Where(path => !File.Exists(path)).ToArray();
        if (missingFiles.Length > 0)
        {
            ShowError("One or more selected XER files no longer exist:\n\n" + string.Join("\n", missingFiles));
            return;
        }

        IReadOnlyDictionary<string, DateOnly> suggestedDataDates;
        IReadOnlyList<string> unresolvedDataDates;
        bool closeRequested = false;
        _programmeReviewOperationActive = true;
        SetUIEnabled(enabled: false);
        UpdateStatus("Reading Programme Review data dates from XER files...");
        LogActivity($"Reading PROJECT.last_recalc_date from {_xerFilePaths.Count} selected XER file(s)...");
        toolStripProgressBar.Visible = true;
        toolStripProgressBar.Value = 0;
        _cancellationTokenSource = new CancellationTokenSource();

        try
        {
            (suggestedDataDates, unresolvedDataDates) = await ReadProgrammeReviewDataDatesAsync(
                _xerFilePaths,
                _cancellationTokenSource.Token);
            UpdateStatus($"Detected {suggestedDataDates.Count} of {_xerFilePaths.Count} XER data date(s).");
            LogActivity($"Detected {suggestedDataDates.Count} of {_xerFilePaths.Count} XER data date(s) " +
                        "from PROJECT.last_recalc_date.");
        }
        catch (OperationCanceledException)
        {
            UpdateStatus("Programme Review metadata scan cancelled.");
            LogActivity("Programme Review metadata scan cancelled by user.");
            return;
        }
        catch (Exception ex)
        {
            UpdateStatus("Could not read Programme Review XER metadata.");
            LogActivity("ERROR reading Programme Review XER metadata: " + ex.Message);
            if (!_closeAfterProgrammeReviewCancellation)
                ShowError("Could not read the selected XER file(s):\n\n" + ex.Message);
            return;
        }
        finally
        {
            _programmeReviewOperationActive = false;
            toolStripProgressBar.Visible = false;
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;
            SetUIEnabled(enabled: true);
            closeRequested = _closeAfterProgrammeReviewCancellation;
            if (closeRequested && !IsDisposed)
            {
                _closeAfterProgrammeReviewCancellation = false;
                BeginInvoke((Action)Close);
            }
        }

        if (closeRequested || IsDisposed) return;
        if (unresolvedDataDates.Count > 0)
        {
            ShowWarning(
                "The XER data date could not be detected unambiguously for:\n\n" +
                string.Join("\n", unresolvedDataDates) +
                "\n\nThose Data date cells will remain editable and blank. Enter yyyy-MM-dd manually. " +
                "A blank can mean PROJECT.last_recalc_date is missing or invalid, or the XER contains multiple projects.",
                "Data Date Needs Review");
        }

        using var dialog = new ProgrammeReviewExportDialog(
            _xerFilePaths.ToArray(),
            suggestedDataDates);
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
            string completion = result.WarningCount > 0 ? "completed with warnings" : "complete";
            string summary = $"Programme Review bundle {completion}: {result.BundleId}. " +
                             $"{result.ManifestRows.Count} manifest rows; {stopwatch.Elapsed.TotalSeconds:F2}s." +
                             (result.WarningCount > 0
                                 ? $" {result.WarningCount} data-quality issue(s); see XER_DATA_QUALITY.csv for affected tables and fields, original source values, and any unallocated actual or remaining quantities."
                                 : string.Empty);
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
            toolStripProgressBar.Visible = false;
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;
            SetUIEnabled(enabled: true);
            if (_closeAfterProgrammeReviewCancellation && !IsDisposed)
            {
                _closeAfterProgrammeReviewCancellation = false;
                BeginInvoke((Action)Close);
            }
        }
    }

    private void UpdateProgrammeReviewButtonState()
    {
        bool enabled = !IsOperationActive
            && _xerFilePaths.Count > 0
            && !string.IsNullOrWhiteSpace(_outputDirectory)
            && Directory.Exists(_outputDirectory);
        SetProgrammeReviewButtonAvailability(enabled);
    }

    private void SetProgrammeReviewButtonAvailability(bool enabled)
    {
        btnExportProgrammeReview.Enabled = enabled;
        if (enabled)
        {
            ApplyButtonStyle(btnExportProgrammeReview, UiTheme.Success, UiTheme.Success, Color.White,
                Color.FromArgb(26, 157, 98), Color.FromArgb(21, 112, 70));
        }
        else
        {
            ApplyButtonStyle(btnExportProgrammeReview, UiTheme.SurfaceAlt, UiTheme.Border, UiTheme.TextMuted,
                UiTheme.SurfaceAlt, UiTheme.SurfaceAlt);
        }
    }

    private void QueueProgrammeReviewButtonStateUpdate()
    {
        if (!IsHandleCreated || IsDisposed) return;
        BeginInvoke((Action)UpdateProgrammeReviewButtonState);
    }

    private async Task<(IReadOnlyDictionary<string, DateOnly> Suggestions, IReadOnlyList<string> Unresolved)>
        ReadProgrammeReviewDataDatesAsync(
            IReadOnlyList<string> xerFilePaths,
            CancellationToken cancellationToken)
    {
        var suggestions = new Dictionary<string, DateOnly>(StringComparer.OrdinalIgnoreCase);
        var unresolved = new List<string>();
        var readErrors = new List<string>();

        for (int index = 0; index < xerFilePaths.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string path = xerFilePaths[index];
            string filename = Path.GetFileName(path);
            int percent = xerFilePaths.Count == 0
                ? 0
                : (int)Math.Round(index * 100d / xerFilePaths.Count);
            toolStripProgressBar.Value = Math.Clamp(
                percent,
                toolStripProgressBar.Minimum,
                toolStripProgressBar.Maximum);
            UpdateStatus($"Reading XER data date {index + 1} of {xerFilePaths.Count}: {filename}");

            try
            {
                DateOnly? dataDate = await ProgrammeReviewXerMetadataReader
                    .ReadSingleProjectDataDateAsync(path, cancellationToken);
                if (dataDate.HasValue)
                    suggestions[filename] = dataDate.Value;
                else
                    unresolved.Add(filename);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                readErrors.Add($"{filename}: {ex.Message}");
            }
        }

        toolStripProgressBar.Value = toolStripProgressBar.Maximum;
        if (readErrors.Count > 0)
            throw new IOException(string.Join(Environment.NewLine, readErrors));

        return (suggestions, unresolved);
    }

    private void ShowProgrammeReviewComplete(ProgrammeReviewBundleResult result)
    {
        bool hasWarnings = result.WarningCount > 0;
        string completion = hasWarnings
            ? $"Programme Review bundle completed with warnings.\n\n" +
              $"{result.WarningCount} data-quality issue(s). See XER_DATA_QUALITY.csv for affected tables and fields, original source values, and any unallocated actual or remaining quantities.\n\n"
            : "Programme Review bundle created successfully.\n\n";
        DialogResult open = MessageBox.Show(
            this,
            completion +
            $"Bundle: {result.BundleId}\n" +
            $"Folder: {result.BundlePath}\n" +
            $"Manifest rows: {result.ManifestRows.Count}\n\n" +
            "Open the completed bundle folder?",
            hasWarnings ? "Programme Review Export Completed with Warnings" : "Programme Review Export Complete",
            MessageBoxButtons.YesNo,
            hasWarnings ? MessageBoxIcon.Warning : MessageBoxIcon.Information);
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
        bool hadLegacyResults = _dataStore.TableNames.Any() || _allTableNames.Count > 0;
        if (!hadLegacyResults) return;

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
                UpdateStatus("Cancelling Programme Review operation before closing...");
                LogActivity("Window close requested; waiting for Programme Review cancellation cleanup.");
                _cancellationTokenSource?.Cancel();
                btnCancelOperation.Enabled = false;
            }
            return;
        }

        if (_tenderReviewOperationActive)
        {
            e.Cancel = true;
            if (!_closeAfterTenderReviewCancellation)
            {
                _closeAfterTenderReviewCancellation = true;
                UpdateStatus("Cancelling Tender Review operation before closing...");
                LogActivity("Window close requested; waiting for Tender Review cancellation cleanup.");
                _cancellationTokenSource?.Cancel();
                btnCancelOperation.Enabled = false;
            }
            return;
        }

        base.OnFormClosing(e);
    }
}
