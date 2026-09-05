using System.Diagnostics;
using System.Drawing;
using System.Windows.Forms;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter;

public partial class MainForm
{
    private Button btnExportTenderReview = null!;
    private bool _tenderReviewOperationActive;
    private bool _closeAfterTenderReviewCancellation;

    private void InitializeTenderReviewUi()
    {
        btnExportTenderReview = new Button
        {
            Name = "btnExportTenderReview",
            Text = "Create Tender Review bundle",
            AutoSize = true,
            MinimumSize = Size.Empty,
            Margin = new Padding(4),
            AccessibleName = "Export Tender Review CSV bundle",
            AccessibleDescription =
                "Open the separate ordered-stage workflow and create a validated Tender Review bundle."
        };
        btnExportTenderReview.Click += BtnExportTenderReview_Click;
        toolTip.SetToolTip(
            btnExportTenderReview,
            "Create the versioned ten-table Tender Review bundle with an audit manifest");
        ApplyButtonStyle(
            btnExportTenderReview,
            UiTheme.Accent,
            UiTheme.Accent,
            Color.White,
            UiTheme.AccentHover,
            UiTheme.AccentDown);
        btnExportTenderReview.Font = _uiFontBold;
        UpdateTenderReviewButtonState();
    }

    private void AttachTenderReviewButtonToActionPanel()
    {
        ConfigureExportActionButton(btnExportTenderReview, "Create &Tender Review bundle");
        btnExportTenderReview.TabIndex = 2;
        if (!exportActionsLayout.Controls.Contains(btnExportTenderReview))
        {
            exportActionsLayout.Controls.Add(btnExportTenderReview, 0, 3);
            exportActionsLayout.SetColumnSpan(btnExportTenderReview, 2);
        }
    }

    internal void VerifyTenderReviewUiIntegration()
    {
        CreateControl();
        PerformLayout();
        exportActionsLayout.PerformLayout();

        if (!panelExportActions.Controls.Contains(exportActionsLayout)
            || !exportActionsLayout.Controls.Contains(btnExportTenderReview))
            throw new InvalidOperationException("The Tender Review action is not attached to the Windows export UI.");
        TableLayoutPanelCellPosition position = exportActionsLayout.GetPositionFromControl(btnExportTenderReview);
        if (position.Column != 0 || position.Row != 3 || exportActionsLayout.GetColumnSpan(btnExportTenderReview) != 2)
            throw new InvalidOperationException("The Tender Review action is not in its separate full-width export position.");
        if (string.IsNullOrWhiteSpace(btnExportTenderReview.AccessibleName)
            || string.IsNullOrWhiteSpace(btnExportTenderReview.AccessibleDescription))
            throw new InvalidOperationException("The Tender Review action is missing accessible metadata.");
        VerifyControlsAreContained([btnExportTenderReview], "Tender Review startup");

        int[] actionHeights =
        [
            btnExportProgrammeReview.Height,
            btnExportTenderReview.Height,
            btnExportPowerBi.Height,
            btnExportAll.Height,
            btnExportSelected.Height
        ];
        if (actionHeights.Distinct().Count() != 1)
            throw new InvalidOperationException("The Tender Review action is not sized consistently with other exports.");

        string tempDirectory = Path.Combine(Path.GetTempPath(), $"xer-tender-ui-smoke-{Guid.NewGuid():N}");
        string xerPath = Path.Combine(tempDirectory, "same-name.xer");
        Directory.CreateDirectory(tempDirectory);
        try
        {
            File.WriteAllText(xerPath, "%T\tPROJECT\r\n%F\tproj_id\r\n%R\t1\r\n%E\r\n");
            var frozenLocalDate = new DateOnly(2026, 9, 5);
            using var dialog = new TenderReviewExportDialog(
                [xerPath, xerPath],
                () => frozenLocalDate);

            if (dialog.SourcePathsForTesting.Count != 2
                || dialog.SourcePathsForTesting.Any(path => !string.Equals(path, xerPath, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException("The Windows Tender workflow did not retain repeated ordered paths.");
            if (dialog.SourceTokensForTesting.Distinct(StringComparer.Ordinal).Count() != 2)
                throw new InvalidOperationException("Repeated Windows Tender sources did not receive distinct stable tokens.");
            if (dialog.StatusDatesForTesting.Any(date => date != frozenLocalDate))
                throw new InvalidOperationException("Windows Tender status dates did not use the frozen local-date default.");

            string secondToken = dialog.SourceTokensForTesting[1];
            DateOnly editedStatusDate = frozenLocalDate.AddDays(1);
            dialog.SetStatusDateForTesting(1, editedStatusDate);
            dialog.MoveSourceForTesting(1, -1);
            dialog.SetIdentityForTesting("j1234", "Tender smoke project");
            if (!dialog.TryBuildRequestForTesting(out TenderReviewBundleRequest? request) || request is null)
                throw new InvalidOperationException("The Windows Tender dialog could not create its Core request.");
            if (!string.Equals(request.ProjectCode, "J1234", StringComparison.Ordinal)
                || request.Sources.Count != 2
                || !string.Equals(request.Sources[0].SourceToken, secondToken, StringComparison.Ordinal)
                || request.Sources[0].StatusDate != editedStatusDate
                || !string.Equals(request.Sources[0].XerFilePath, request.Sources[1].XerFilePath, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException(
                    "The Windows Tender request did not preserve edited dates, stable tokens, order, or repeated paths.");
        }
        finally
        {
            if (Directory.Exists(tempDirectory))
                Directory.Delete(tempDirectory, recursive: true);
        }
    }

    private async void BtnExportTenderReview_Click(object? sender, EventArgs e)
    {
        if (IsOperationActive) return;
        if (string.IsNullOrWhiteSpace(_outputDirectory) || !Directory.Exists(_outputDirectory))
        {
            ShowError("Please select a valid output directory first.");
            return;
        }

        using var dialog = new TenderReviewExportDialog(_xerFilePaths.ToArray());
        if (dialog.ShowDialog(this) != DialogResult.OK || dialog.BundleRequest is null) return;

        TenderReviewBundleRequest request = dialog.BundleRequest;
        SetUIEnabled(enabled: false);
        UpdateStatus("Creating Tender Review bundle...");
        LogActivity(
            $"Starting Tender Review export for {request.ProjectCode} from {request.Sources.Count} ordered XER source(s)...");
        toolStripProgressBar.Visible = true;
        toolStripProgressBar.Value = 0;
        _cancellationTokenSource = new CancellationTokenSource();
        CancellationToken token = _cancellationTokenSource.Token;
        _tenderReviewOperationActive = true;

        var progress = new Progress<ProcessingService.DetailedProgress>(p =>
        {
            UpdateStatus(p.Message ?? "Creating Tender Review bundle...");
            toolStripProgressBar.Value = Math.Clamp(
                p.Percent,
                toolStripProgressBar.Minimum,
                toolStripProgressBar.Maximum);
        });

        try
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            var service = new TenderReviewBundleService();
            TenderReviewBundleResult result = await Task.Run(
                () => service.BuildFromXerFilesAsync(request, _outputDirectory, progress, token),
                token);
            stopwatch.Stop();

            toolStripProgressBar.Value = 100;
            string summary = $"Tender Review bundle complete: {result.BundleId}. " +
                             $"{result.ManifestRows.Count} manifest rows; {stopwatch.Elapsed.TotalSeconds:F2}s.";
            UpdateStatus(summary);
            LogActivity(summary);
            LogActivity("Bundle path: " + result.BundlePath);
            ShowTenderReviewComplete(result);
        }
        catch (OperationCanceledException)
        {
            UpdateStatus("Tender Review export cancelled; no partial bundle was published.");
            LogActivity("Tender Review export cancelled by user; staging output was removed.");
            if (!_closeAfterTenderReviewCancellation)
                ShowWarning(
                    "Tender Review export was cancelled. No partial bundle was published.",
                    "Export Cancelled");
        }
        catch (TenderReviewValidationException ex)
        {
            UpdateStatus("Tender Review validation failed.");
            LogActivity("Tender Review validation failed: " + ex.Message);
            if (!_closeAfterTenderReviewCancellation)
                ShowError("The Tender Review bundle was not published because validation failed:\n\n" + ex.Message);
        }
        catch (Exception ex)
        {
            UpdateStatus("Tender Review export failed.");
            LogActivity("ERROR during Tender Review export: " + ex.Message);
            string details = ex.InnerException is null ? string.Empty : "\n\nDetails: " + ex.InnerException.Message;
            if (!_closeAfterTenderReviewCancellation)
                ShowError("Tender Review export failed. No complete bundle was published.\n\n" + ex.Message + details);
        }
        finally
        {
            _tenderReviewOperationActive = false;
            toolStripProgressBar.Visible = false;
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;
            SetUIEnabled(enabled: true);
            if (_closeAfterTenderReviewCancellation && !IsDisposed)
            {
                _closeAfterTenderReviewCancellation = false;
                BeginInvoke((Action)Close);
            }
        }
    }

    private void UpdateTenderReviewButtonState()
    {
        bool enabled = !IsOperationActive
            && !string.IsNullOrWhiteSpace(_outputDirectory)
            && Directory.Exists(_outputDirectory);
        SetTenderReviewButtonAvailability(enabled);
    }

    private void SetTenderReviewButtonAvailability(bool enabled)
    {
        btnExportTenderReview.Enabled = enabled;
        if (enabled)
        {
            ApplyButtonStyle(
                btnExportTenderReview,
                UiTheme.Accent,
                UiTheme.Accent,
                Color.White,
                UiTheme.AccentHover,
                UiTheme.AccentDown);
        }
        else
        {
            ApplyButtonStyle(
                btnExportTenderReview,
                UiTheme.SurfaceAlt,
                UiTheme.Border,
                UiTheme.TextMuted,
                UiTheme.SurfaceAlt,
                UiTheme.SurfaceAlt);
        }
    }

    private void ShowTenderReviewComplete(TenderReviewBundleResult result)
    {
        DialogResult open = MessageBox.Show(
            this,
            $"Tender Review bundle created successfully.\n\n" +
            $"Bundle: {result.BundleId}\n" +
            $"Folder: {result.BundlePath}\n" +
            $"Manifest rows: {result.ManifestRows.Count}\n\n" +
            "Open the completed bundle folder?",
            "Tender Review Export Complete",
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
}
