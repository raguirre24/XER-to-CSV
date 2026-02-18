using System;

using System.Collections.Generic;

using System.ComponentModel;

using System.Diagnostics;

using System.Drawing;

using System.Drawing.Drawing2D;

using System.IO;

using System.Linq;

using System.Runtime;

using System.Threading;

using System.Threading.Tasks;

using System.Windows.Forms;



namespace XerToCsvConverter;



public class MainForm : Form

{

	private SplitContainer splitContainerMain = null!;



	private SplitContainer splitContainerResults = null!;



	private StatusStrip statusStrip = null!;



	private ToolStripStatusLabel toolStripStatusLabel = null!;



	private ToolStripProgressBar toolStripProgressBar = null!;



	private MenuStrip menuStrip = null!;



	private ToolStripMenuItem fileToolStripMenuItem = null!;



	private ToolStripMenuItem addFilesToolStripMenuItem = null!;

	private ToolStripMenuItem exitToolStripMenuItem = null!;



	private ToolStripMenuItem helpToolStripMenuItem = null!;



	private ToolStripMenuItem aboutToolStripMenuItem = null!;



	private GroupBox grpInputFiles = null!;



	private ListView lvwXerFiles = null!;



	private ColumnHeader colFileName = null!;



	private ColumnHeader colFilePath = null!;



	private ColumnHeader colStatus = null!;



	private ToolStrip toolStripFileActions = null!;



	private ToolStripButton btnAddFile = null!;



	private ToolStripButton btnRemoveFile = null!;



	private ToolStripButton btnClearFiles = null!;



	private Label lblDragDropHint = null!;



	private GroupBox grpOutput = null!;



	private TextBox txtOutputPath = null!;



	private Button btnSelectOutput = null!;



	private Button btnParseXer = null!;



	private GroupBox grpExtractedTables = null!;



	private CheckedListBox lstTables = null!;



	private TextBox txtTableFilter = null!;



	private Label lblFilter = null!;



	private GroupBox grpActivityLog = null!;



	private TextBox txtActivityLog = null!;



	private GroupBox grpPowerBI = null!;



	private CheckBox chkCreatePowerBiTables = null!;



	private Label lblPbiStatus = null!;



	private Button btnPbiDetails = null!;



	private Panel panelExportActions = null!;



	private Button btnExportAll = null!;



	private Button btnExportSelected = null!;



	private Button btnCancelOperation = null!;

	private Button btnExportPowerBi = null!;

	private ToolTip toolTip = null!;



	private readonly List<string> _xerFilePaths = [];


	private string _outputDirectory = string.Empty;


	private List<string> _allTableNames = [];


	private XerDataStore _dataStore = new XerDataStore();


	private readonly ProcessingService _processingService;



	private CancellationTokenSource? _cancellationTokenSource;

	private UserSettings _settings = new UserSettings();

	private readonly Font _uiFont;

	private readonly Font _uiFontBold;

	private readonly Font _uiFontTitle;

	private readonly Font _monoFont;


	private static class UiTheme

	{

		public static readonly Color SurfaceStart = Color.FromArgb(250, 251, 253);

		public static readonly Color SurfaceEnd = Color.FromArgb(233, 239, 245);

		public static readonly Color Surface = Color.FromArgb(244, 247, 251);

		public static readonly Color SurfaceAlt = Color.FromArgb(236, 242, 248);

		public static readonly Color Panel = Color.FromArgb(255, 255, 255);

		public static readonly Color Border = Color.FromArgb(209, 217, 225);

		public static readonly Color Text = Color.FromArgb(24, 32, 44);

		public static readonly Color TextMuted = Color.FromArgb(86, 96, 109);

		public static readonly Color Accent = Color.FromArgb(22, 102, 170);

		public static readonly Color AccentSoft = Color.FromArgb(220, 232, 246);

		public static readonly Color AccentHover = Color.FromArgb(35, 118, 186);

		public static readonly Color AccentDown = Color.FromArgb(18, 84, 139);

		public static readonly Color Success = Color.FromArgb(28, 135, 84);

		public static readonly Color Danger = Color.FromArgb(166, 54, 54);

		public static readonly Color DangerHover = Color.FromArgb(179, 68, 68);

		public static readonly Color DangerDown = Color.FromArgb(142, 46, 46);

	}


	private const int MAX_LOG_LINES = 5000;



	private bool _canCreateTask01 = false;



	private bool _canCreateProjWbs03 = false;



	private bool _canCreateBaseline04 = false;



	private bool _canCreateProject02 = false;



	private bool _canCreatePredecessor06 = false;



	private bool _canCreateActvType07 = false;



	private bool _canCreateActvCode08 = false;



	private bool _canCreateTaskActv09 = false;



	private bool _canCreateCalendar10 = false;



	private bool _canCreateCalendarDetailed11 = false;



	private bool _canCreateRsrc12 = false;



	private bool _canCreateTaskRsrc13 = false;



	private bool _canCreateUmeasure14 = false;



	private bool _canCreateResourceDist15 = false;



	private IContainer? components;


	public MainForm()

	{

		_processingService = new ProcessingService();

		_dataStore = new XerDataStore();

		_uiFont = CreateUiFont(9f, FontStyle.Regular);

		_uiFontBold = CreateUiFont(9f, FontStyle.Bold);

		_uiFontTitle = CreateUiFont(10f, FontStyle.Bold);

		_monoFont = CreateMonoFont(9f);

		InitializeComponent();

		ApplyTheme();

		ApplyModernLayout();

		InitializeUIState();

		LoadSettings();

	}



	private void LoadSettings()
	{
		try
		{
			_settings = UserSettingsStore.Load();
			if (!string.IsNullOrEmpty(_settings.LastOutputPath) && Directory.Exists(_settings.LastOutputPath))
			{
				_outputDirectory = _settings.LastOutputPath;
				txtOutputPath.Text = _outputDirectory;
			}
			chkCreatePowerBiTables.Checked = _settings.GeneratePowerBI;
		}
		catch
		{
			// Ignore settings load failures to preserve existing behavior.
		}

		if (!string.IsNullOrEmpty(_outputDirectory))
		{
			return;
		}

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

	private void SaveSettings()
	{
		try
		{
			_settings.LastOutputPath = _outputDirectory;
			_settings.GeneratePowerBI = chkCreatePowerBiTables.Checked;
			UserSettingsStore.Save(_settings);
		}
		catch
		{
			// Ignore persistence failures to preserve existing behavior.
		}
	}



	private void InitializeUIState()

	{

		lvwXerFiles.AllowDrop = true;

		lvwXerFiles.DragEnter += LvwXerFiles_DragEnter;

		lvwXerFiles.DragDrop += LvwXerFiles_DragDrop;

		txtOutputPath.AllowDrop = true;

		txtOutputPath.DragEnter += TxtOutputPath_DragEnter;

		txtOutputPath.DragDrop += TxtOutputPath_DragDrop;

		base.Resize += MainForm_Resize;

		lstTables.ItemCheck += LstTables_ItemCheck;

		lstTables.CheckOnClick = true;

		lstTables.Sorted = false;

		// --- Accessibility Names ---
		lvwXerFiles.AccessibleName = "XER file list";
		lvwXerFiles.AccessibleDescription = "List of XER files to parse. Drag and drop files here or use Add Files.";
		txtOutputPath.AccessibleName = "Output directory path";
		txtOutputPath.AccessibleDescription = "Directory where exported CSV files will be saved.";
		txtTableFilter.AccessibleName = "Table filter";
		txtTableFilter.AccessibleDescription = "Type to filter the table list below.";
		lstTables.AccessibleName = "Tables to export";
		lstTables.AccessibleDescription = "Check the tables you want to include in the export.";
		txtActivityLog.AccessibleName = "Activity log";
		txtActivityLog.AccessibleDescription = "Shows parsing and export activity messages.";
		btnParseXer.AccessibleName = "Parse XER files";
		btnExportAll.AccessibleName = "Export all tables";
		btnExportSelected.AccessibleName = "Export selected tables";
		btnExportPowerBi.AccessibleName = "Export Power BI tables";
		btnCancelOperation.AccessibleName = "Cancel current operation";
		btnSelectOutput.AccessibleName = "Browse for output folder";
		chkCreatePowerBiTables.AccessibleName = "Include enhanced Power BI tables";

		// --- Tooltips ---
		toolTip.SetToolTip(btnParseXer, "Parse the loaded XER files and extract tables");
		toolTip.SetToolTip(btnExportAll, "Export all available tables to CSV files");
		toolTip.SetToolTip(btnExportSelected, "Export only the checked tables to CSV files");
		toolTip.SetToolTip(btnExportPowerBi, "Export Power BI enhanced tables to CSV files");
		toolTip.SetToolTip(btnCancelOperation, "Cancel the current operation");
		toolTip.SetToolTip(btnSelectOutput, "Select the folder where CSV files will be saved");
		toolTip.SetToolTip(txtOutputPath, "Drag and drop a folder here or click Browse to set the output directory");
		toolTip.SetToolTip(txtTableFilter, "Type to filter the table list");
		toolTip.SetToolTip(chkCreatePowerBiTables, "Generate enhanced tables optimized for Power BI dashboards");

		// --- Keyboard Shortcut: Ctrl+O for Add Files ---
		base.KeyPreview = true;
		base.KeyDown += MainForm_KeyDown;

		// --- Dark Terminal-Style Activity Log ---
		txtActivityLog.BackColor = Color.FromArgb(30, 30, 46);
		txtActivityLog.ForeColor = Color.FromArgb(205, 214, 244);

		UpdateStatus("Ready");

		LogActivity("Application started.");

		UpdateInputButtonsState();

		UpdateExportButtonState();

		UpdatePbiCheckboxState();

		ResizeListViewColumns();

	}

	private void MainForm_KeyDown(object? sender, KeyEventArgs e)
	{
		if (e.Control && e.KeyCode == Keys.O)
		{
			e.SuppressKeyPress = true;
			BtnAddFile_Click(sender, e);
		}
	}


	private void ApplyTheme()

	{

		SuspendLayout();

		SetStyle(ControlStyles.OptimizedDoubleBuffer | ControlStyles.AllPaintingInWmPaint, true);

		UpdateStyles();

		DoubleBuffered = true;

		BackColor = UiTheme.Surface;

		ForeColor = UiTheme.Text;

		Font = _uiFont;


		var renderer = new ToolStripProfessionalRenderer(new ModernToolStripColorTable())

		{

			RoundedEdges = false

		};


		menuStrip.RenderMode = ToolStripRenderMode.Professional;

		menuStrip.Renderer = renderer;

		menuStrip.BackColor = UiTheme.SurfaceAlt;

		menuStrip.ForeColor = UiTheme.Text;

		menuStrip.Padding = new Padding(6, 3, 6, 3);


		toolStripFileActions.RenderMode = ToolStripRenderMode.Professional;

		toolStripFileActions.Renderer = renderer;

		toolStripFileActions.BackColor = UiTheme.SurfaceAlt;

		toolStripFileActions.ForeColor = UiTheme.Text;

		toolStripFileActions.Padding = new Padding(6, 2, 6, 2);

		toolStripFileActions.GripStyle = ToolStripGripStyle.Hidden;


		ToolStripButton[] stripButtons = [btnAddFile, btnRemoveFile, btnClearFiles];

		foreach (ToolStripButton button in stripButtons)

		{

			button.DisplayStyle = ToolStripItemDisplayStyle.Text;

			button.Font = _uiFont;

			button.ForeColor = UiTheme.Text;

		}


		statusStrip.RenderMode = ToolStripRenderMode.Professional;

		statusStrip.Renderer = renderer;

		statusStrip.SizingGrip = false;

		statusStrip.BackColor = UiTheme.SurfaceAlt;

		statusStrip.ForeColor = UiTheme.TextMuted;


		toolStripStatusLabel.Font = _uiFont;

		toolStripProgressBar.Style = ProgressBarStyle.Continuous;


		splitContainerMain.BackColor = UiTheme.Border;

		splitContainerResults.BackColor = UiTheme.Border;

		splitContainerMain.Panel1.BackColor = UiTheme.Surface;

		splitContainerMain.Panel2.BackColor = UiTheme.Surface;

		splitContainerResults.Panel1.BackColor = UiTheme.Surface;

		splitContainerResults.Panel2.BackColor = UiTheme.Surface;


		ApplyGroupBoxStyle(grpInputFiles);

		ApplyGroupBoxStyle(grpOutput);

		ApplyGroupBoxStyle(grpExtractedTables);

		ApplyGroupBoxStyle(grpActivityLog);

		ApplyGroupBoxStyle(grpPowerBI);


		lblDragDropHint.ForeColor = UiTheme.TextMuted;

		lblFilter.ForeColor = UiTheme.TextMuted;


		ApplyTextBoxStyle(txtOutputPath, isReadOnly: false);

		ApplyTextBoxStyle(txtTableFilter, isReadOnly: false);

		ApplyTextBoxStyle(txtActivityLog, isReadOnly: true);

		txtActivityLog.Font = _monoFont;


		lvwXerFiles.FullRowSelect = true;

		lvwXerFiles.HideSelection = false;

		lvwXerFiles.GridLines = true;

		lvwXerFiles.BorderStyle = BorderStyle.FixedSingle;

		lvwXerFiles.BackColor = UiTheme.Panel;

		lvwXerFiles.ForeColor = UiTheme.Text;


		lstTables.BorderStyle = BorderStyle.FixedSingle;

		lstTables.BackColor = UiTheme.Panel;

		lstTables.ForeColor = UiTheme.Text;


		panelExportActions.BackColor = UiTheme.SurfaceAlt;


		ApplyButtonStyle(btnParseXer, UiTheme.Accent, UiTheme.Accent, Color.White, UiTheme.AccentHover, UiTheme.AccentDown);

		ApplyButtonStyle(btnExportAll, UiTheme.Accent, UiTheme.Accent, Color.White, UiTheme.AccentHover, UiTheme.AccentDown);

		ApplyButtonStyle(btnExportSelected, UiTheme.Panel, UiTheme.Border, UiTheme.Text, UiTheme.SurfaceAlt, UiTheme.Border);

		ApplyButtonStyle(btnSelectOutput, UiTheme.Panel, UiTheme.Border, UiTheme.Text, UiTheme.SurfaceAlt, UiTheme.Border);

		ApplyButtonStyle(btnPbiDetails, UiTheme.Panel, UiTheme.Border, UiTheme.Text, UiTheme.SurfaceAlt, UiTheme.Border);

		ApplyButtonStyle(btnExportPowerBi, UiTheme.Accent, UiTheme.Accent, Color.White, UiTheme.AccentHover, UiTheme.AccentDown);

		ApplyButtonStyle(btnCancelOperation, UiTheme.Danger, UiTheme.Danger, Color.White, UiTheme.DangerHover, UiTheme.DangerDown);


		btnParseXer.Font = _uiFontTitle;

		btnExportAll.Font = _uiFontBold;

		btnExportSelected.Font = _uiFontBold;

		btnExportPowerBi.Font = _uiFontBold;

		btnCancelOperation.Font = _uiFontBold;


		chkCreatePowerBiTables.Font = _uiFont;

		chkCreatePowerBiTables.ForeColor = UiTheme.Text;

		lblPbiStatus.Font = _uiFont;


		toolTip.BackColor = UiTheme.Panel;

		toolTip.ForeColor = UiTheme.Text;


		ResumeLayout(performLayout: true);

	}

	private void ApplyModernLayout()

	{

		SuspendLayout();

		MinimumSize = new Size(1100, 720);

		AcceptButton = btnParseXer;

		CancelButton = btnCancelOperation;


		grpInputFiles.Text = "1. Add XER files";

		grpOutput.Text = "2. Output folder";

		btnParseXer.Text = "3. Parse and load tables";

		grpExtractedTables.Text = "Tables to export";

		grpPowerBI.Text = "Enhanced tables (Power BI)";

		grpActivityLog.Text = "Activity log";

		btnExportAll.Text = "Export all";

		btnExportSelected.Text = "Export selected";

		btnExportPowerBi.Text = "Export Power BI";

		btnCancelOperation.Text = "Cancel";

		btnPbiDetails.Text = "Details";

		lblDragDropHint.Text = "Drag and drop XER files here or click Add Files";

		lblFilter.Text = "Filter:";

		chkCreatePowerBiTables.Text = "Include enhanced Power BI tables";

		txtOutputPath.PlaceholderText = "Select an output folder";

		txtTableFilter.PlaceholderText = "Type to filter tables";

		splitContainerMain.Panel1.Padding = Padding.Empty;

		splitContainerMain.Panel2.Padding = Padding.Empty;

		grpInputFiles.Padding = new Padding(0);

		grpOutput.Padding = new Padding(0);

		grpExtractedTables.Padding = new Padding(0);

		grpPowerBI.Padding = new Padding(0);

		grpActivityLog.Padding = new Padding(0);


		splitContainerMain.SplitterWidth = 6;

		splitContainerResults.SplitterWidth = 6;

		splitContainerResults.Panel1MinSize = 240;

		splitContainerResults.Panel2MinSize = 160;


		var leftLayout = new TableLayoutPanel

		{

			ColumnCount = 1,

			RowCount = 3,

			Dock = DockStyle.Fill,

			Padding = new Padding(12),

			BackColor = Color.Transparent

		};

		leftLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 100f));

		leftLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));

		leftLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));


		grpInputFiles.Dock = DockStyle.Fill;

		grpInputFiles.Margin = new Padding(0);

		grpOutput.Dock = DockStyle.Top;

		grpOutput.Margin = new Padding(0, 12, 0, 0);

		btnParseXer.Dock = DockStyle.Top;

		btnParseXer.Height = 44;

		btnParseXer.Margin = new Padding(0, 12, 0, 0);


		leftLayout.Controls.Add(grpInputFiles, 0, 0);

		leftLayout.Controls.Add(grpOutput, 0, 1);

		leftLayout.Controls.Add(btnParseXer, 0, 2);


		var inputLayout = new TableLayoutPanel

		{

			ColumnCount = 1,

			RowCount = 3,

			Dock = DockStyle.Fill,

			Padding = new Padding(8),

			BackColor = UiTheme.Panel

		};

		inputLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));

		inputLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 100f));

		inputLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));


		toolStripFileActions.Dock = DockStyle.Fill;

		lvwXerFiles.Dock = DockStyle.Fill;

		lblDragDropHint.Dock = DockStyle.Fill;

		lblDragDropHint.AutoSize = true;

		lblDragDropHint.Padding = new Padding(2, 6, 2, 0);


		inputLayout.Controls.Add(toolStripFileActions, 0, 0);

		inputLayout.Controls.Add(lvwXerFiles, 0, 1);

		inputLayout.Controls.Add(lblDragDropHint, 0, 2);


		grpInputFiles.Controls.Clear();

		grpInputFiles.Controls.Add(inputLayout);


		var outputLayout = new TableLayoutPanel

		{

			ColumnCount = 2,

			RowCount = 1,

			Dock = DockStyle.Fill,

			Padding = new Padding(8, 10, 8, 8),

			BackColor = UiTheme.Panel

		};

		outputLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100f));

		outputLayout.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));


		txtOutputPath.Dock = DockStyle.Fill;

		txtOutputPath.Margin = new Padding(0, 0, 8, 0);

		btnSelectOutput.AutoSize = true;

		btnSelectOutput.Margin = new Padding(0);


		outputLayout.Controls.Add(txtOutputPath, 0, 0);

		outputLayout.Controls.Add(btnSelectOutput, 1, 0);


		grpOutput.Controls.Clear();

		grpOutput.Controls.Add(outputLayout);


		var tablesInnerLayout = new TableLayoutPanel

		{

			ColumnCount = 2,

			RowCount = 2,

			Dock = DockStyle.Fill,

			Padding = new Padding(8),

			BackColor = UiTheme.Panel

		};

		tablesInnerLayout.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));

		tablesInnerLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100f));

		tablesInnerLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));

		tablesInnerLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 100f));


		lblFilter.Anchor = AnchorStyles.Left;

		lblFilter.AutoSize = true;

		txtTableFilter.Dock = DockStyle.Fill;

		lstTables.Dock = DockStyle.Fill;

		lstTables.IntegralHeight = false;


		tablesInnerLayout.Controls.Add(lblFilter, 0, 0);

		tablesInnerLayout.Controls.Add(txtTableFilter, 1, 0);

		tablesInnerLayout.Controls.Add(lstTables, 0, 1);

		tablesInnerLayout.SetColumnSpan(lstTables, 2);


		grpExtractedTables.Controls.Clear();

		grpExtractedTables.Controls.Add(tablesInnerLayout);


		var pbiLayout = new TableLayoutPanel

		{

			ColumnCount = 2,

			RowCount = 2,

			Dock = DockStyle.Fill,

			Padding = new Padding(8),

			BackColor = UiTheme.Panel

		};

		pbiLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100f));

		pbiLayout.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));

		pbiLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));

		pbiLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));


		chkCreatePowerBiTables.Dock = DockStyle.Fill;

		btnPbiDetails.AutoSize = true;

		btnPbiDetails.Margin = new Padding(8, 0, 0, 0);

		lblPbiStatus.Dock = DockStyle.Fill;

		lblPbiStatus.AutoSize = true;

		lblPbiStatus.Margin = new Padding(0, 6, 0, 0);


		pbiLayout.Controls.Add(chkCreatePowerBiTables, 0, 0);

		pbiLayout.Controls.Add(btnPbiDetails, 1, 0);

		pbiLayout.Controls.Add(lblPbiStatus, 0, 1);

		pbiLayout.SetColumnSpan(lblPbiStatus, 2);


		grpPowerBI.Controls.Clear();

		grpPowerBI.Controls.Add(pbiLayout);


		var tablesHost = new TableLayoutPanel

		{

			ColumnCount = 1,

			RowCount = 2,

			Dock = DockStyle.Fill,

			Padding = new Padding(0),

			BackColor = Color.Transparent

		};

		tablesHost.RowStyles.Add(new RowStyle(SizeType.AutoSize));

		tablesHost.RowStyles.Add(new RowStyle(SizeType.Percent, 100f));

		grpPowerBI.Dock = DockStyle.Top;

		grpPowerBI.Margin = new Padding(0, 0, 0, 10);

		grpExtractedTables.Dock = DockStyle.Fill;


		tablesHost.Controls.Add(grpPowerBI, 0, 0);

		tablesHost.Controls.Add(grpExtractedTables, 0, 1);


		splitContainerResults.Panel1.Controls.Clear();

		splitContainerResults.Panel1.Controls.Add(tablesHost);


		grpActivityLog.Dock = DockStyle.Fill;

		txtActivityLog.Dock = DockStyle.Fill;

		txtActivityLog.ReadOnly = true;

		txtActivityLog.ScrollBars = ScrollBars.Vertical;


		grpActivityLog.Controls.Clear();

		grpActivityLog.Controls.Add(txtActivityLog);


		splitContainerResults.Panel2.Controls.Clear();

		splitContainerResults.Panel2.Controls.Add(grpActivityLog);


		// Button Layout Redesign
		
		// 1. Cancel Button (Left)
		btnCancelOperation.AutoSize = true;
		btnCancelOperation.MinimumSize = new Size(100, 40);
		btnCancelOperation.Dock = DockStyle.Left;
		btnCancelOperation.Margin = new Padding(0);

		// 2. Action Buttons Container (Right)
		var flowActions = new FlowLayoutPanel
		{
			Dock = DockStyle.Fill, 
			WrapContents = true, 
            AutoSize = true,
			AutoSizeMode = AutoSizeMode.GrowAndShrink, 
			BackColor = Color.Transparent,
			Padding = new Padding(0)
		};

		// 3. Configure Action Buttons
		// Reduce MinimumSize completely. Let size be content-based.
		btnExportAll.AutoSize = true;
		btnExportAll.MinimumSize = Size.Empty; 
		btnExportAll.Margin = new Padding(5, 0, 0, 0); 
		btnExportAll.Text = "Export All"; // Shorter text
        btnExportAll.Dock = DockStyle.None;
        btnExportAll.Anchor = AnchorStyles.None;

		btnExportSelected.AutoSize = true;
		btnExportSelected.MinimumSize = Size.Empty; 
		btnExportSelected.Margin = new Padding(5, 0, 0, 0);
		btnExportSelected.Text = "Export Selected"; // Shorter text
        btnExportSelected.Dock = DockStyle.None;
        btnExportSelected.Anchor = AnchorStyles.None;

		btnExportPowerBi.AutoSize = true;
		btnExportPowerBi.MinimumSize = Size.Empty; 
		btnExportPowerBi.Margin = new Padding(5, 0, 0, 0);
		btnExportPowerBi.Text = "Export Power BI"; // Shorter text
        btnExportPowerBi.Dock = DockStyle.None;
        btnExportPowerBi.Anchor = AnchorStyles.None;

		// 4. Add to Flow (Order matters for RightToLeft: First added is Right-most)
		flowActions.Controls.Add(btnExportAll);       // Far Right
		flowActions.Controls.Add(btnExportSelected);  // Middle
		flowActions.Controls.Add(btnExportPowerBi);   // Left

		// 5. Container Configuration
		panelExportActions.Controls.Clear();
		panelExportActions.Padding = new Padding(12, 10, 12, 10);
		panelExportActions.AutoSize = true;
        panelExportActions.AutoSizeMode = AutoSizeMode.GrowAndShrink;
        panelExportActions.Dock = DockStyle.Bottom; // Dock to bottom so it pushes content up
		
		// Add Controls (Dock order: Filled control added last in Z-order)
        // Actually, just add them. Left dock takes left. Fill takes rest.
		panelExportActions.Controls.Add(flowActions);       // Docks Fill
		panelExportActions.Controls.Add(btnCancelOperation); // Docks Left


		var rightLayout = new TableLayoutPanel

		{

			ColumnCount = 1,

			RowCount = 2,

			Dock = DockStyle.Fill,

			Padding = new Padding(12),

			BackColor = Color.Transparent

		};

		rightLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 100f));

		rightLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 88f));


		splitContainerResults.Dock = DockStyle.Fill;

		panelExportActions.Dock = DockStyle.Fill;

		panelExportActions.Margin = new Padding(0, 10, 0, 0);


		rightLayout.Controls.Add(splitContainerResults, 0, 0);

		rightLayout.Controls.Add(panelExportActions, 0, 1);


		splitContainerMain.Panel1.Controls.Clear();

		splitContainerMain.Panel2.Controls.Clear();

		splitContainerMain.Panel1.Controls.Add(leftLayout);

		splitContainerMain.Panel2.Controls.Add(rightLayout);


		UpdateCancelButtonState(isActive: false);

		ResumeLayout(performLayout: true);

	}

	private void UpdateCancelButtonState(bool isActive)

	{

		if (isActive)

		{

			ApplyButtonStyle(btnCancelOperation, UiTheme.Danger, UiTheme.Danger, Color.White, UiTheme.DangerHover, UiTheme.DangerDown);

			btnCancelOperation.Enabled = true;

			btnCancelOperation.Visible = true;

		}

		else

		{

			ApplyButtonStyle(btnCancelOperation, UiTheme.SurfaceAlt, UiTheme.Border, UiTheme.TextMuted, UiTheme.SurfaceAlt, UiTheme.SurfaceAlt);

			btnCancelOperation.Enabled = false;

			btnCancelOperation.Visible = true;

		}

	}


	private void ApplyGroupBoxStyle(GroupBox groupBox)

	{

		groupBox.BackColor = UiTheme.Panel;

		groupBox.ForeColor = UiTheme.Text;

		groupBox.Font = _uiFontBold;

	}


	private void ApplyTextBoxStyle(TextBox textBox, bool isReadOnly)

	{

		textBox.BorderStyle = BorderStyle.FixedSingle;

		textBox.BackColor = isReadOnly ? UiTheme.SurfaceAlt : UiTheme.Panel;

		textBox.ForeColor = UiTheme.Text;

		textBox.Font = _uiFont;

	}


	private static void ApplyButtonStyle(Button button, Color backColor, Color borderColor, Color foreColor, Color hoverBackColor, Color downBackColor)

	{

		button.FlatStyle = FlatStyle.Flat;

		button.FlatAppearance.BorderSize = 1;

		button.FlatAppearance.BorderColor = borderColor;

		button.FlatAppearance.MouseOverBackColor = hoverBackColor;

		button.FlatAppearance.MouseDownBackColor = downBackColor;

		button.BackColor = backColor;

		button.ForeColor = foreColor;

		button.UseVisualStyleBackColor = false;

	}


	private static Font CreateUiFont(float size, FontStyle style)

	{

		string[] candidates = ["Bahnschrift", "Segoe UI Variable", "Segoe UI"];

		foreach (string candidate in candidates)

		{

			try

			{

				Font font = new Font(candidate, size, style);

				if (string.Equals(font.Name, candidate, StringComparison.OrdinalIgnoreCase))

				{

					return font;

				}

				font.Dispose();

			}

			catch

			{

				// Ignore font load failures and try the next candidate.

			}

		}

		Font systemFont = SystemFonts.MessageBoxFont ?? SystemFonts.DefaultFont;
		return new Font(systemFont.FontFamily, size, style);

	}


	private static Font CreateMonoFont(float size)

	{

		string[] candidates = ["Cascadia Mono", "Consolas"];

		foreach (string candidate in candidates)

		{

			try

			{

				Font font = new Font(candidate, size, FontStyle.Regular);

				if (string.Equals(font.Name, candidate, StringComparison.OrdinalIgnoreCase))

				{

					return font;

				}

				font.Dispose();

			}

			catch

			{

				// Ignore font load failures and try the next candidate.

			}

		}

		return new Font(FontFamily.GenericMonospace, size, FontStyle.Regular);

	}


	private sealed class ModernToolStripColorTable : ProfessionalColorTable

	{

		public ModernToolStripColorTable()

		{

			UseSystemColors = false;

		}


		public override Color MenuStripGradientBegin => UiTheme.SurfaceAlt;

		public override Color MenuStripGradientEnd => UiTheme.SurfaceAlt;

		public override Color ToolStripGradientBegin => UiTheme.SurfaceAlt;

		public override Color ToolStripGradientMiddle => UiTheme.SurfaceAlt;

		public override Color ToolStripGradientEnd => UiTheme.SurfaceAlt;

		public override Color ToolStripBorder => UiTheme.Border;

		public override Color ToolStripDropDownBackground => UiTheme.Panel;

		public override Color ImageMarginGradientBegin => UiTheme.Panel;

		public override Color ImageMarginGradientMiddle => UiTheme.Panel;

		public override Color ImageMarginGradientEnd => UiTheme.Panel;

		public override Color MenuItemSelected => UiTheme.AccentSoft;

		public override Color MenuItemSelectedGradientBegin => UiTheme.AccentSoft;

		public override Color MenuItemSelectedGradientEnd => UiTheme.AccentSoft;

		public override Color MenuItemBorder => UiTheme.Accent;

		public override Color MenuItemPressedGradientBegin => UiTheme.Accent;

		public override Color MenuItemPressedGradientEnd => UiTheme.Accent;

		public override Color SeparatorDark => UiTheme.Border;

		public override Color SeparatorLight => UiTheme.SurfaceAlt;

	}



	private void ExitToolStripMenuItem_Click(object? sender, EventArgs e)

	{

		Application.Exit();

	}



	private void AboutToolStripMenuItem_Click(object? sender, EventArgs e)

	{

		ShowInfo("Primavera P6 XER to CSV Converter\nVersion: 2.5\n\nSOFTWARE COPYRIGHT NOTICE\r\nCopyright © 2025-2026 Ricardo Aguirre. All Rights Reserved.\r\nUnauthorized use, copying, or sharing of this software is strictly prohibited. Written permission required for any use.\r\nTHE SOFTWARE IS PROVIDED \"AS IS\" WITHOUT WARRANTY OF ANY KIND. RICARDO AGUIRRE SHALL NOT BE LIABLE FOR ANY DAMAGES ARISING FROM USE OF THIS SOFTWARE.\r\nBy using this software, you agree to these terms.", "About");

	}



	private void BtnAddFile_Click(object? sender, EventArgs e)

	{

		using OpenFileDialog openFileDialog = new OpenFileDialog

		{

			Filter = "XER files (*.xer)|*.xer|All files (*.*)|*.*",

			Title = "Select Primavera P6 XER File(s)",

			Multiselect = true

		};

		if (openFileDialog.ShowDialog() == DialogResult.OK)

		{

			AddXerFiles(openFileDialog.FileNames);

		}

	}



	private void BtnRemoveFile_Click(object? sender, EventArgs e)

	{

		RemoveSelectedXerFiles();

	}



	private void BtnClearFiles_Click(object? sender, EventArgs e)

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



	private void LvwXerFiles_SelectedIndexChanged(object? sender, EventArgs e)

	{

		UpdateInputButtonsState();

	}



	private void BtnSelectOutput_Click(object? sender, EventArgs e)

	{

		using FolderBrowserDialog folderBrowserDialog = new FolderBrowserDialog

		{

			Description = "Select Output Directory for CSV Files",

			SelectedPath = _outputDirectory

		};

		if (folderBrowserDialog.ShowDialog() == DialogResult.OK)

		{

			_outputDirectory = folderBrowserDialog.SelectedPath;

			txtOutputPath.Text = _outputDirectory;

			UpdateInputButtonsState();

			UpdateExportButtonState();

			LogActivity("Output directory set: " + _outputDirectory);

			SaveSettings();

		}

	}



	private async void BtnParseXer_Click(object? sender, EventArgs e)

	{

		if (!ValidateInputs())

		{

			return;

		}

		ClearResults(forceGC: false);

		LogActivity("Starting XER parsing...");

		foreach (ListViewItem item in lvwXerFiles.Items)

		{

			item.SubItems[2].Text = "Pending";

			item.ForeColor = SystemColors.ControlText;

		}

		SetUIEnabled(enabled: false);

		UpdateStatus("Parsing XER File(s)...");

		toolStripProgressBar.Visible = true;

		_cancellationTokenSource = new CancellationTokenSource();

		CancellationToken token = _cancellationTokenSource.Token;

		Progress<ProcessingService.DetailedProgress> progress = new Progress<ProcessingService.DetailedProgress>(delegate(ProcessingService.DetailedProgress p)

		{

			UpdateStatus(p.Message ?? string.Empty);

			toolStripProgressBar.Value = p.Percent;

			if (!string.IsNullOrEmpty(p.FilePath) && !string.IsNullOrEmpty(p.FileStatus))

			{

				UpdateFileStatus(p.FilePath, p.FileStatus, p.StatusColor);

			}

		});

		try

		{

			Stopwatch stopwatch = Stopwatch.StartNew();

			XerDataStore result = await _processingService.ParseMultipleXerFilesAsync(_xerFilePaths, progress, token);

			stopwatch.Stop();

			_dataStore = result;

			if (token.IsCancellationRequested && _dataStore.TableCount == 0)

			{

				UpdateStatus("Parsing canceled.");

				LogActivity("Parsing canceled by user. No data extracted.");

				return;

			}

			CheckEnhancedTableDependencies();

			UpdateTableList();

			UpdatePbiCheckboxState();

			UpdateExportButtonState();

			toolStripProgressBar.Value = 100;

			string summary = $"Parsing complete. Extracted {_dataStore.TableCount} tables. Time elapsed: {stopwatch.Elapsed.TotalSeconds:F2}s.";

			if (token.IsCancellationRequested)

			{

				summary = "Parsing canceled. " + summary;

			}

			UpdateStatus(summary);

			LogActivity(summary);

		}

		catch (Exception ex)

		{

			Exception ex2 = ex;

			ShowError("Error parsing XER file(s): " + ex2.Message + "\n\nDetails: " + ex2.InnerException?.Message);

			LogActivity("ERROR during parsing: " + ex2.Message);

			ClearResults();

			UpdateStatus("Parsing failed.");

		}

		finally

		{

			SetUIEnabled(enabled: true);

			toolStripProgressBar.Visible = false;

			_cancellationTokenSource?.Dispose();

			_cancellationTokenSource = null;

		}

	}



	private async void BtnExportAll_Click(object? sender, EventArgs e)

	{

		LogActivity("Starting export of ALL tables...");

		await ExportTablesAsync(GetAllTableNamesToExport());

	}



	private async void BtnExportSelected_Click(object? sender, EventArgs e)

	{

		LogActivity("Starting export of SELECTED tables...");

		await ExportTablesAsync(GetSelectedTableNamesToExport());

	}

	private async void BtnExportPowerBi_Click(object? sender, EventArgs e)
	{
		LogActivity("Starting export of Power BI tables...");
		List<string> pbiTables = GetAvailablePbiTables();

		if (pbiTables.Count == 0)
		{
			ShowWarning("No Power BI enhanced tables can be generated with the current data.");
			return;
		}

		await ExportTablesAsync(pbiTables);
	}



	private void BtnCancelOperation_Click(object? sender, EventArgs e)

	{

		if (_cancellationTokenSource is { IsCancellationRequested: false })

		{

			LogActivity("Cancellation requested by user.");

			UpdateStatus("Cancelling operation...");

			_cancellationTokenSource.Cancel();

			btnCancelOperation.Enabled = false;

		}

	}



	private void LstTables_ItemCheck(object? sender, ItemCheckEventArgs e)

	{

		BeginInvoke((Action)delegate

		{

			UpdateExportButtonState();

		});

	}



	private void TxtTableFilter_TextChanged(object? sender, EventArgs e)

	{

		FilterTableList(txtTableFilter.Text);

	}



	private void ChkCreatePowerBiTables_CheckedChanged(object? sender, EventArgs e)

	{

		bool value = chkCreatePowerBiTables.Checked;

		lstTables.BeginUpdate();

		for (int i = 0; i < lstTables.Items.Count; i++)

		{

			string? name = lstTables.Items[i]?.ToString();
			if (string.IsNullOrEmpty(name))
			{
				continue;
			}

			if (IsEnhancedTableName(name))

			{

				lstTables.SetItemChecked(i, value);

			}

		}

		lstTables.EndUpdate();

		UpdateExportButtonState();

		SaveSettings();

	}



	private void BtnPbiDetails_Click(object? sender, EventArgs e)

	{

		string text = "Power BI Enhanced Table Generation Status:\n\n";

		text += GetPbiTableStatusDetail("01_XER_TASK", _canCreateTask01);

		text += GetPbiTableStatusDetail("02_XER_PROJECT", _canCreateProject02);

		text += GetPbiTableStatusDetail("03_XER_PROJWBS", _canCreateProjWbs03);

		text += GetPbiTableStatusDetail("04_XER_BASELINE", _canCreateBaseline04);

		text += GetPbiTableStatusDetail("06_XER_PREDECESSOR", _canCreatePredecessor06);

		text += GetPbiTableStatusDetail("07_XER_ACTVTYPE", _canCreateActvType07);

		text += GetPbiTableStatusDetail("08_XER_ACTVCODE", _canCreateActvCode08);

		text += GetPbiTableStatusDetail("09_XER_TASKACTV", _canCreateTaskActv09);

		text += GetPbiTableStatusDetail("10_XER_CALENDAR", _canCreateCalendar10);

		text += GetPbiTableStatusDetail("11_XER_CALENDAR_DETAILED", _canCreateCalendarDetailed11);

		text += GetPbiTableStatusDetail("12_XER_RSRC", _canCreateRsrc12);

		text += GetPbiTableStatusDetail("13_XER_TASKRSRC", _canCreateTaskRsrc13);

		text += GetPbiTableStatusDetail("14_XER_UMEASURE", _canCreateUmeasure14);

		text += GetPbiTableStatusDetail("15_XER_RESOURCE_DISTRIBUTION", _canCreateResourceDist15);

		text = text + "\n" + GetMissingDependenciesMessage("Summary:");

		ShowInfo(text, "Power BI Details");

	}



	private string GetPbiTableStatusDetail(string tableName, bool canCreate)

	{

		return "[" + (canCreate ? "✓" : "✗") + "] " + tableName + "\n";

	}



	private void LvwXerFiles_DragEnter(object? sender, DragEventArgs e)

	{

		if (e.Data?.GetDataPresent(DataFormats.FileDrop) == true)

		{

			e.Effect = DragDropEffects.Copy;

		}

		else

		{

			e.Effect = DragDropEffects.None;

		}

	}



	private void LvwXerFiles_DragDrop(object? sender, DragEventArgs e)

	{

		if (e.Data?.GetData(DataFormats.FileDrop) is string[] source)
		{
			string[] array = source.Where((string f) => Path.GetExtension(f).Equals(".xer", StringComparison.OrdinalIgnoreCase)).ToArray();
			if (array.Length != 0)
			{
				AddXerFiles(array);
			}
			else
			{
				ShowWarning("No valid .xer files found in the dropped items.");
			}
		}
		else
		{
			ShowWarning("No valid .xer files found in the dropped items.");
		}

	}



	private void TxtOutputPath_DragEnter(object? sender, DragEventArgs e)

	{

		if (e.Data?.GetData(DataFormats.FileDrop) is string[] array)

		{

			e.Effect = (array.Length == 1 && Directory.Exists(array[0])) ? DragDropEffects.Copy : DragDropEffects.None;

		}

		else

		{

			e.Effect = DragDropEffects.None;

		}

	}



	private void TxtOutputPath_DragDrop(object? sender, DragEventArgs e)

	{

		if (e.Data?.GetData(DataFormats.FileDrop) is string[] array)

		{

			if (array.Length == 1 && Directory.Exists(array[0]))

			{

				_outputDirectory = array[0];

				txtOutputPath.Text = _outputDirectory;

				UpdateInputButtonsState();

				UpdateExportButtonState();

				LogActivity("Output directory set via drag & drop: " + _outputDirectory);

				SaveSettings();

			}

			else

			{

				ShowWarning("Please drag and drop a single folder.");

			}

		}

		else

		{

			ShowWarning("Please drag and drop a folder.");

		}

	}



	private void LvwXerFiles_KeyDown(object? sender, KeyEventArgs e)

	{

		if (e.KeyCode == Keys.Delete && lvwXerFiles.SelectedItems.Count > 0)

		{

			RemoveSelectedXerFiles();

		}

	}



	private void MainForm_Resize(object? sender, EventArgs e)

	{

		ResizeListViewColumns();

	}



	private bool ValidateInputs()

	{

		if (_xerFilePaths.Count == 0)

		{

			ShowError("Please select at least one XER file first.");

			return false;

		}

		if (string.IsNullOrEmpty(_outputDirectory) || !Directory.Exists(_outputDirectory))

		{

			ShowError("Please select a valid output directory first.");

			return false;

		}

		List<string> list = _xerFilePaths.Where((string f) => !File.Exists(f)).ToList();

		if (list.Any())

		{

			ShowWarning("The following file(s) could not be found and were removed from the list:\n" + string.Join("\n", list));

			lvwXerFiles.BeginUpdate();

			foreach (string item in list)

			{

				_xerFilePaths.Remove(item);

				foreach (ListViewItem item2 in lvwXerFiles.Items)

				{

					if (item2.Tag is string tag && tag.Equals(item, StringComparison.OrdinalIgnoreCase))

					{

						lvwXerFiles.Items.Remove(item2);

						break;

					}

				}

			}

			lvwXerFiles.EndUpdate();

			UpdateInputButtonsState();

			if (_xerFilePaths.Count == 0)

			{

				return false;

			}

		}

		return true;

	}



	private List<string> GetAllTableNamesToExport()

	{

		return (from n in _allTableNames.Distinct()

			orderby n

			select n).ToList();

	}



	private List<string> GetSelectedTableNamesToExport()

	{

		List<string> list = new List<string>();

		foreach (object checkedItem in lstTables.CheckedItems)

		{

			string text = checkedItem?.ToString() ?? string.Empty;
			if (text.Length == 0)
			{
				continue;
			}

			int num = text.LastIndexOf(" (");

			ReadOnlySpan<char> itemSpan = (num > 0) ? text.AsSpan(0, num) : text.AsSpan();
			string item = itemSpan.ToString();

			list.Add(item);

		}

		if (list.Count == 0)

		{

			ShowWarning("Please check at least one table from the list.");

			return new List<string>();

		}

		return (from n in list.Distinct()

			orderby n

			select n).ToList();

	}



	private void AddAvailablePbiTables(List<string> names)

	{

		if (chkCreatePowerBiTables.Checked)

		{

			names.AddRange(GetAvailablePbiTables());

		}

	}
	private List<string> GetAvailablePbiTables()
	{
		List<string> names = new List<string>();

		if (_canCreateTask01) names.Add("01_XER_TASK");

		if (_canCreateProject02) names.Add("02_XER_PROJECT");

		if (_canCreateProjWbs03) names.Add("03_XER_PROJWBS");

		if (_canCreateBaseline04) names.Add("04_XER_BASELINE");

		if (_canCreatePredecessor06) names.Add("06_XER_PREDECESSOR");

		if (_canCreateActvType07) names.Add("07_XER_ACTVTYPE");

		if (_canCreateActvCode08) names.Add("08_XER_ACTVCODE");

		if (_canCreateTaskActv09) names.Add("09_XER_TASKACTV");

		if (_canCreateCalendar10) names.Add("10_XER_CALENDAR");

		if (_canCreateCalendarDetailed11) names.Add("11_XER_CALENDAR_DETAILED");

		if (_canCreateRsrc12) names.Add("12_XER_RSRC");

		if (_canCreateTaskRsrc13) names.Add("13_XER_TASKRSRC");

		if (_canCreateUmeasure14) names.Add("14_XER_UMEASURE");

		if (_canCreateResourceDist15) names.Add("15_XER_RESOURCE_DISTRIBUTION");

		return names;
	}



	private async Task ExportTablesAsync(List<string> tablesToExport)

	{

		if (tablesToExport.Count == 0)

		{

			return;

		}

		if (string.IsNullOrEmpty(_outputDirectory) || !Directory.Exists(_outputDirectory))

		{

			ShowError("Please select a valid output directory first.");

			return;

		}

		var (finalTablesToExport, skippedPbi) = DetermineFinalExportList(tablesToExport);

		if (skippedPbi.Any())

		{

			string warningMsg = "Cannot create requested Power BI tables due to missing dependencies: " + string.Join(", ", from n in skippedPbi.Distinct()

				orderby n

				select n) + ". Exporting available tables.";

			ShowWarning(warningMsg);

			LogActivity("WARNING: " + warningMsg);

		}

		if (finalTablesToExport.Count == 0)

		{

			ShowError("No tables available for export after checking selections and dependencies.");

			return;

		}

		TimeSpan estimatedTime = EstimateExportTime(finalTablesToExport);

		if (estimatedTime.TotalSeconds > 10.0)

		{

			string timeEstimate = ((estimatedTime.TotalMinutes >= 1.0) ? $"{estimatedTime.TotalMinutes:F1} minutes" : $"{estimatedTime.TotalSeconds:F0} seconds");

			LogActivity("Estimated export time: ~" + timeEstimate);

		}

		SetUIEnabled(enabled: false);

		UpdateStatus("Exporting Tables...");

		toolStripProgressBar.Visible = true;

		_cancellationTokenSource = new CancellationTokenSource();

		CancellationToken token = _cancellationTokenSource.Token;

		Progress<(int percent, string message)> progress = new Progress<(int, string)>(delegate((int percent, string message) p)

		{

			UpdateStatus(p.message);

			toolStripProgressBar.Value = p.percent;

		});

		try

		{

			Stopwatch stopwatch = Stopwatch.StartNew();

			List<string> exportedFiles = await _processingService.ExportTablesAsync(_dataStore, finalTablesToExport, _outputDirectory, progress, token);

			stopwatch.Stop();

			toolStripProgressBar.Value = 100;

			string summary = $"{exportedFiles.Count} tables exported. Time elapsed: {stopwatch.Elapsed.TotalSeconds:F2}s.";

			if (token.IsCancellationRequested)

			{

				UpdateStatus("Export canceled. " + summary);

				LogActivity("Export canceled. " + summary);

				ShowWarning($"Export operation was canceled. {exportedFiles.Count} files were written.", "Export Canceled");

			}

			else

			{

				UpdateStatus("Export complete. " + summary);

				LogActivity("Export complete. " + summary);

				ShowExportCompleteMessage(exportedFiles);

			}

		}

		catch (Exception ex)

		{

			Exception ex2 = ex;

			ShowError("Error exporting CSV files: " + ex2.Message + "\n\nDetails: " + ex2.InnerException?.Message);

			LogActivity("ERROR during export: " + ex2.Message);

			UpdateStatus("Export failed.");

		}

		finally

		{

			SetUIEnabled(enabled: true);

			toolStripProgressBar.Visible = false;

			_cancellationTokenSource?.Dispose();

			_cancellationTokenSource = null;

		}

	}



	private TimeSpan EstimateExportTime(List<string> tablesToExport)

	{

		long num = 0L;

		foreach (string item in tablesToExport)

		{

			if (_dataStore.ContainsTable(item))

			{

				if (_dataStore.GetTable(item) is { } table)
				{
					num += table.RowCount;
				}

			}

		}

		double val = (double)num / 100000.0;

		return TimeSpan.FromSeconds(Math.Max(1.0, val));

	}



	private (List<string> FinalList, List<string> Skipped) DetermineFinalExportList(List<string> requestedTables)

	{

		List<string> list = new List<string>();

		List<string> list2 = new List<string>();

		foreach (string requestedTable in requestedTables)

		{

			if (_dataStore.ContainsTable(requestedTable))

			{

				list.Add(requestedTable);

			}

			else if (IsEnhancedTableName(requestedTable))

			{

				if (CheckSpecificPbiDependency(requestedTable))

				{

					list.Add(requestedTable);

				}

				else

				{

					list2.Add(requestedTable);

				}

			}

		}

		return (FinalList: list.Distinct().ToList(), Skipped: list2);

	}



	private void CheckEnhancedTableDependencies()

	{

		bool flag = _dataStore.ContainsTable("TASK");

		bool flag2 = _dataStore.ContainsTable("CALENDAR");

		bool flag3 = _dataStore.ContainsTable("PROJECT");

		bool canCreateProjWbs = _dataStore.ContainsTable("PROJWBS");

		bool canCreateTaskActv = _dataStore.ContainsTable("TASKACTV");

		bool canCreateActvCode = _dataStore.ContainsTable("ACTVCODE");

		bool canCreateActvType = _dataStore.ContainsTable("ACTVTYPE");

		bool canCreatePredecessor = _dataStore.ContainsTable(TableNames.TaskPred)
			&& _dataStore.ContainsTable(TableNames.Task)
			&& _dataStore.ContainsTable(TableNames.Calendar);

		bool canCreateRsrc = _dataStore.ContainsTable("RSRC");

		bool flag4 = _dataStore.ContainsTable("TASKRSRC");

		bool canCreateUmeasure = _dataStore.ContainsTable("UMEASURE");

		_canCreateTask01 = flag && flag2 && flag3;

		_canCreateProjWbs03 = canCreateProjWbs;

		_canCreateBaseline04 = _canCreateTask01;

		_canCreateProject02 = flag3;

		_canCreatePredecessor06 = canCreatePredecessor;

		_canCreateActvType07 = canCreateActvType;

		_canCreateActvCode08 = canCreateActvCode;

		_canCreateTaskActv09 = canCreateTaskActv;

		_canCreateCalendar10 = flag2;

		_canCreateCalendarDetailed11 = flag2;

		_canCreateRsrc12 = canCreateRsrc;

		_canCreateTaskRsrc13 = flag4;

		_canCreateUmeasure14 = canCreateUmeasure;

		_canCreateResourceDist15 = flag4 && flag && flag3 && flag2;

	}



	private bool CheckSpecificPbiDependency(string tableName)

	{

		return tableName switch

		{

			"01_XER_TASK" => _canCreateTask01, 

			"02_XER_PROJECT" => _canCreateProject02, 

			"03_XER_PROJWBS" => _canCreateProjWbs03, 

			"04_XER_BASELINE" => _canCreateBaseline04, 

			"06_XER_PREDECESSOR" => _canCreatePredecessor06, 

			"07_XER_ACTVTYPE" => _canCreateActvType07, 

			"08_XER_ACTVCODE" => _canCreateActvCode08, 

			"09_XER_TASKACTV" => _canCreateTaskActv09, 

			"10_XER_CALENDAR" => _canCreateCalendar10, 

			"11_XER_CALENDAR_DETAILED" => _canCreateCalendarDetailed11, 

			"12_XER_RSRC" => _canCreateRsrc12, 

			"13_XER_TASKRSRC" => _canCreateTaskRsrc13, 

			"14_XER_UMEASURE" => _canCreateUmeasure14, 

			"15_XER_RESOURCE_DISTRIBUTION" => _canCreateResourceDist15, 

			_ => false, 

		};

	}



	private bool AnyPbiDependencyMet()

	{

		return _canCreateTask01 || _canCreateProjWbs03 || _canCreateProject02 || _canCreatePredecessor06 || _canCreateActvType07 || _canCreateActvCode08 || _canCreateTaskActv09 || _canCreateCalendar10 || _canCreateCalendarDetailed11 || _canCreateRsrc12 || _canCreateTaskRsrc13 || _canCreateUmeasure14 || _canCreateResourceDist15;

	}



	private string GetMissingDependenciesMessage(string prefix)

	{

		List<string> list = new List<string>();

		if (!_canCreateTask01)

		{

			if (!_dataStore.ContainsTable("TASK"))

			{

				list.Add("TASK");

			}

			if (!_dataStore.ContainsTable("CALENDAR"))

			{

				list.Add("CALENDAR");

			}

			if (!_dataStore.ContainsTable("PROJECT"))

			{

				list.Add("PROJECT");

			}

		}

		if (!_canCreateProjWbs03 && !_dataStore.ContainsTable("PROJWBS"))

		{

			list.Add("PROJWBS");

		}

		if (!_canCreatePredecessor06)

		{

			if (!_dataStore.ContainsTable(TableNames.TaskPred))
			{
				list.Add("TASKPRED");
			}

			if (!_dataStore.ContainsTable(TableNames.Task))
			{
				list.Add("TASK");
			}

			if (!_dataStore.ContainsTable(TableNames.Calendar))
			{
				list.Add("CALENDAR");
			}

		}

		if (!_canCreateActvType07 && !_dataStore.ContainsTable("ACTVTYPE"))

		{

			list.Add("ACTVTYPE");

		}

		if (!_canCreateActvCode08 && !_dataStore.ContainsTable("ACTVCODE"))

		{

			list.Add("ACTVCODE");

		}

		if (!_canCreateTaskActv09 && !_dataStore.ContainsTable("TASKACTV"))

		{

			list.Add("TASKACTV");

		}

		if (!_canCreateRsrc12 && !_dataStore.ContainsTable("RSRC"))

		{

			list.Add("RSRC");

		}

		if (!_canCreateTaskRsrc13 && !_dataStore.ContainsTable("TASKRSRC"))

		{

			list.Add("TASKRSRC");

		}

		if (!_canCreateUmeasure14 && !_dataStore.ContainsTable("UMEASURE"))

		{

			list.Add("UMEASURE");

		}

		if (!_canCreateResourceDist15)

		{

			if (!_dataStore.ContainsTable("TASKRSRC"))

			{

				list.Add("TASKRSRC");

			}

			if (!_dataStore.ContainsTable("TASK"))

			{

				list.Add("TASK");

			}

			if (!_dataStore.ContainsTable("CALENDAR"))

			{

				list.Add("CALENDAR");

			}

			if (!_dataStore.ContainsTable("PROJECT"))

			{

				list.Add("PROJECT");

			}

		}

		List<string> list2 = (from s in list.Distinct()

			orderby s

			select s).ToList();

		if (list2.Any())

		{

			return prefix + " Required source tables missing: " + string.Join(", ", list2) + ".";

		}

		return prefix + " All dependencies met.";

	}



	private bool IsEnhancedTableName(string name)

	{

		HashSet<string> hashSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase)

		{

			"01_XER_TASK", "02_XER_PROJECT", "03_XER_PROJWBS", "04_XER_BASELINE", "06_XER_PREDECESSOR", "07_XER_ACTVTYPE", "08_XER_ACTVCODE", "09_XER_TASKACTV", "10_XER_CALENDAR", "11_XER_CALENDAR_DETAILED",

			"12_XER_RSRC", "13_XER_TASKRSRC", "14_XER_UMEASURE", "15_XER_RESOURCE_DISTRIBUTION"

		};

		return hashSet.Contains(name);

	}



	private void AddXerFiles(IEnumerable<string> filesToAdd)

	{

		int num = 0;

		lvwXerFiles.BeginUpdate();

		foreach (string file in filesToAdd)

		{

			if (!_xerFilePaths.Any((string existing) => string.Equals(existing, file, StringComparison.OrdinalIgnoreCase)))

			{

				_xerFilePaths.Add(file);

				ListViewItem listViewItem = new ListViewItem(Path.GetFileName(file));

				listViewItem.SubItems.Add(Path.GetDirectoryName(file));

				listViewItem.SubItems.Add("Pending");

				listViewItem.Tag = file;

				lvwXerFiles.Items.Add(listViewItem);

				num++;

			}

		}

		lvwXerFiles.EndUpdate();

		if (num > 0)

		{

			UpdateInputButtonsState();

			ResizeListViewColumns();

			LogActivity($"Added {num} XER file(s).");

		}

	}



	private void ResizeListViewColumns()

	{

		if (lvwXerFiles.Width > 300)

		{

			colFileName.Width = 150;

			colStatus.Width = 80;

			colFilePath.Width = lvwXerFiles.ClientSize.Width - colFileName.Width - colStatus.Width - 5;

		}

	}



	private void UpdateFileStatus(string filePath, string status, string? colorName = null)

	{

		if (lvwXerFiles.InvokeRequired)

		{

			lvwXerFiles.Invoke((Action)delegate

			{

				UpdateFileStatus(filePath, status, colorName);

			});

			return;

		}

		foreach (ListViewItem item in lvwXerFiles.Items)

		{

			if (item.Tag is string tag && tag.Equals(filePath, StringComparison.OrdinalIgnoreCase))

			{

				item.SubItems[2].Text = status;

				if (!string.IsNullOrEmpty(colorName))

				{

					item.ForeColor = Color.FromName(colorName);

				}

				break;

			}

		}

	}



	private void RemoveSelectedXerFiles()

	{

		if (lvwXerFiles.SelectedItems.Count <= 0)

		{

			return;

		}

		int count = lvwXerFiles.SelectedItems.Count;

		if (MessageBox.Show($"Remove {count} selected file(s)?", "Confirm Removal", MessageBoxButtons.YesNo, MessageBoxIcon.Question) != DialogResult.Yes)

		{

			return;

		}

		lvwXerFiles.BeginUpdate();

		foreach (ListViewItem selectedItem in lvwXerFiles.SelectedItems)

		{

			string? item = selectedItem.Tag as string;
			if (string.IsNullOrEmpty(item))
			{
				continue;
			}

			_xerFilePaths.Remove(item);
			lvwXerFiles.Items.Remove(selectedItem);

		}

		lvwXerFiles.EndUpdate();

		UpdateInputButtonsState();

		LogActivity($"Removed {count} file(s).");

	}



	private void ClearResults(bool forceGC = true)

	{

		_dataStore = new XerDataStore();

		_allTableNames.Clear();

		lstTables.Items.Clear();

		txtTableFilter.Clear();

		_canCreateTask01 = false;

		_canCreateProjWbs03 = false;

		_canCreateBaseline04 = false;

		_canCreateProject02 = false;

		_canCreatePredecessor06 = false;

		_canCreateActvType07 = false;

		_canCreateActvCode08 = false;

		_canCreateTaskActv09 = false;

		_canCreateCalendar10 = false;

		_canCreateCalendarDetailed11 = false;

		_canCreateRsrc12 = false;

		_canCreateTaskRsrc13 = false;

		_canCreateUmeasure14 = false;

		UpdatePbiCheckboxState();

		UpdateExportButtonState();

		if (forceGC)

		{

			GC.Collect();

			GC.WaitForPendingFinalizers();

		}

	}



	private void UpdateTableList()

	{

		List<string> second = _dataStore.TableNames.OrderBy((string n) => n).ToList();

		List<string> list = new List<string>();

		if (_canCreateTask01)

		{

			list.Add("01_XER_TASK");

		}

		if (_canCreateProject02)

		{

			list.Add("02_XER_PROJECT");

		}

		if (_canCreateProjWbs03)

		{

			list.Add("03_XER_PROJWBS");

		}

		if (_canCreateBaseline04)

		{

			list.Add("04_XER_BASELINE");

		}

		if (_canCreatePredecessor06)

		{

			list.Add("06_XER_PREDECESSOR");

		}

		if (_canCreateActvType07)

		{

			list.Add("07_XER_ACTVTYPE");

		}

		if (_canCreateActvCode08)

		{

			list.Add("08_XER_ACTVCODE");

		}

		if (_canCreateTaskActv09)

		{

			list.Add("09_XER_TASKACTV");

		}

		if (_canCreateCalendar10)

		{

			list.Add("10_XER_CALENDAR");

		}

		if (_canCreateCalendarDetailed11)

		{

			list.Add("11_XER_CALENDAR_DETAILED");

		}

		if (_canCreateRsrc12)

		{

			list.Add("12_XER_RSRC");

		}

		if (_canCreateTaskRsrc13)

		{

			list.Add("13_XER_TASKRSRC");

		}

		if (_canCreateUmeasure14)

		{

			list.Add("14_XER_UMEASURE");

		}

		if (_canCreateResourceDist15)

		{

			list.Add("15_XER_RESOURCE_DISTRIBUTION");

		}

		list.Sort();

		_allTableNames = list.Concat(second).ToList();

		FilterTableList(txtTableFilter.Text);

		if (!chkCreatePowerBiTables.Checked)

		{

			return;

		}

		lstTables.BeginUpdate();

		for (int num = 0; num < lstTables.Items.Count; num++)

		{

			string? name = lstTables.Items[num]?.ToString();
			if (string.IsNullOrEmpty(name))
			{
				continue;
			}

			if (IsEnhancedTableName(name))

			{

				lstTables.SetItemChecked(num, value: true);

			}

		}

		lstTables.EndUpdate();

	}



	private void FilterTableList(string filter)

	{

		HashSet<string> hashSet = new HashSet<string>();

		foreach (object checkedItem in lstTables.CheckedItems)

		{

			string text = checkedItem?.ToString() ?? string.Empty;
			if (text.Length == 0)
			{
				continue;
			}

			int num = text.LastIndexOf(" (");

			ReadOnlySpan<char> itemSpan = (num > 0) ? text.AsSpan(0, num) : text.AsSpan();
			string item = itemSpan.ToString();

			hashSet.Add(item);

		}

		lstTables.BeginUpdate();

		lstTables.Items.Clear();

		IEnumerable<string> enumerable = ((!string.IsNullOrWhiteSpace(filter)) ? _allTableNames.Where((string name) => name.IndexOf(filter, StringComparison.OrdinalIgnoreCase) >= 0) : _allTableNames);

		foreach (string item3 in enumerable)

		{

			int num2 = 0;

			if (_dataStore != null && _dataStore.ContainsTable(item3))

			{

				if (_dataStore.GetTable(item3) is { } table)
				{
					num2 = table.RowCount;
				}

			}

			string item2 = ((num2 > 0) ? $"{item3} ({num2:N0} rows)" : item3);

			bool isChecked = hashSet.Contains(item3);

			lstTables.Items.Add(item2, isChecked);

		}

		lstTables.EndUpdate();

	}



	private void UpdatePbiCheckboxState()

	{

		bool flag = AnyPbiDependencyMet();

		chkCreatePowerBiTables.Enabled = flag;

		if (_dataStore == null || _dataStore.TableCount == 0)

		{

			lblPbiStatus.Text = "Status: Parse XER file(s) first.";

			lblPbiStatus.ForeColor = UiTheme.TextMuted;

			btnPbiDetails.Enabled = false;

		}

		else if (flag)

		{

			lblPbiStatus.Text = "Status: Dependencies met. Ready to generate.";

			lblPbiStatus.ForeColor = UiTheme.Success;

			btnPbiDetails.Enabled = true;

		}

		else

		{

			chkCreatePowerBiTables.Checked = false;

			lblPbiStatus.Text = "Status: Missing required source tables.";

			lblPbiStatus.ForeColor = UiTheme.Danger;

			btnPbiDetails.Enabled = true;

		}

	}



	private void UpdateInputButtonsState()

	{

		bool flag = _xerFilePaths.Count > 0;

		bool flag2 = !string.IsNullOrEmpty(_outputDirectory) && Directory.Exists(_outputDirectory);

		bool enabled = lvwXerFiles.SelectedItems.Count > 0;

		btnAddFile.Enabled = true;

		btnParseXer.Enabled = flag && flag2;

		btnClearFiles.Enabled = flag;

		btnRemoveFile.Enabled = enabled;

	}



	private void UpdateExportButtonState()

	{

		bool flag = !string.IsNullOrEmpty(_outputDirectory) && Directory.Exists(_outputDirectory);

		bool flag2 = lstTables.CheckedItems.Count > 0;

		btnExportAll.Enabled = flag && _allTableNames.Count > 0;

		btnExportSelected.Enabled = flag && flag2;

		btnExportPowerBi.Enabled = flag && AnyPbiDependencyMet();

	}



	private void UpdateStatus(string message)

	{

		if (statusStrip.InvokeRequired)

		{

			statusStrip.Invoke((Action)delegate

			{

				toolStripStatusLabel.Text = message;

			});

		}

		else

		{

			toolStripStatusLabel.Text = message;

		}

	}



	private void LogActivity(string message)

	{

		string timestampedMessage = $"{DateTime.Now:HH:mm:ss}: {message}\r\n";

		if (txtActivityLog.InvokeRequired)

		{

			txtActivityLog.Invoke((Action)delegate

			{

				AppendToActivityLog(timestampedMessage);

			});

		}

		else

		{

			AppendToActivityLog(timestampedMessage);

		}

	}



	private void AppendToActivityLog(string message)

	{

		txtActivityLog.AppendText(message);

		if (txtActivityLog.Lines.Length > 5000)

		{

			string[] lines = txtActivityLog.Lines.Skip(txtActivityLog.Lines.Length - 5000).ToArray();

			txtActivityLog.Lines = lines;

		}

		txtActivityLog.ScrollToCaret();

	}



	private void SetUIEnabled(bool enabled)

	{

		base.UseWaitCursor = !enabled;

		grpInputFiles.Enabled = enabled;

		grpOutput.Enabled = enabled;

		grpExtractedTables.Enabled = enabled;

		grpPowerBI.Enabled = enabled;

		menuStrip.Enabled = enabled;

		if (enabled)

		{

			UpdateInputButtonsState();

			UpdateExportButtonState();

			UpdatePbiCheckboxState();

			UpdateCancelButtonState(isActive: false);

			GCSettings.LatencyMode = GCLatencyMode.Interactive;

		}

		else

		{

			btnParseXer.Enabled = false;

			btnExportAll.Enabled = false;

			btnExportSelected.Enabled = false;

			btnAddFile.Enabled = false;

			btnRemoveFile.Enabled = false;

			btnClearFiles.Enabled = false;

			UpdateCancelButtonState(isActive: true);

			GCSettings.LatencyMode = GCLatencyMode.Batch;

		}

	}



	private void ShowExportCompleteMessage(List<string> exportedFiles)

	{

		List<string> source = exportedFiles
			.Select(Path.GetFileNameWithoutExtension)
			.Where(name => !string.IsNullOrEmpty(name))
			.Select(name => name!)
			.Where(IsEnhancedTableName)
			.ToList();

		string text = $"Successfully exported {exportedFiles.Count} tables to CSV files.";

		if (source.Any())

		{

			text += "\n\nEnhanced tables created:";

			foreach (string item in source.OrderBy((string n) => n))

			{

				text = text + "\n- " + item;

			}

		}

		ShowInfo(text, "Export Complete");

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

				ShowError("Could not open folder: " + ex.Message);

			}

		}

	}



	private void ShowError(string message, string title = "Error")

	{

		if (base.InvokeRequired)

		{

			Invoke((Action)delegate

			{

				MessageBox.Show(this, message, title, MessageBoxButtons.OK, MessageBoxIcon.Hand);

			});

		}

		else

		{

			MessageBox.Show(this, message, title, MessageBoxButtons.OK, MessageBoxIcon.Hand);

		}

	}



	private void ShowWarning(string message, string title = "Warning")

	{

		if (base.InvokeRequired)

		{

			Invoke((Action)delegate

			{

				MessageBox.Show(this, message, title, MessageBoxButtons.OK, MessageBoxIcon.Exclamation);

			});

		}

		else

		{

			MessageBox.Show(this, message, title, MessageBoxButtons.OK, MessageBoxIcon.Exclamation);

		}

	}



	private void ShowInfo(string message, string title = "Information")

	{

		if (base.InvokeRequired)

		{

			Invoke((Action)delegate

			{

				MessageBox.Show(this, message, title, MessageBoxButtons.OK, MessageBoxIcon.Asterisk);

			});

		}

		else

		{

			MessageBox.Show(this, message, title, MessageBoxButtons.OK, MessageBoxIcon.Asterisk);

		}

	}

	protected override void OnPaintBackground(PaintEventArgs e)

	{

		if (ClientRectangle.Width <= 0 || ClientRectangle.Height <= 0)

		{

			base.OnPaintBackground(e);

			return;

		}

		using LinearGradientBrush brush = new LinearGradientBrush(ClientRectangle, UiTheme.SurfaceStart, UiTheme.SurfaceEnd, LinearGradientMode.Vertical);

		e.Graphics.FillRectangle(brush, ClientRectangle);

	}


	protected override void Dispose(bool disposing)

	{

		if (disposing)

		{

			SaveSettings();

			_uiFont.Dispose();

			_uiFontBold.Dispose();

			_uiFontTitle.Dispose();

			_monoFont.Dispose();

			if (components != null)

			{

				components.Dispose();

			}

			if (_cancellationTokenSource != null)

			{

				_cancellationTokenSource.Dispose();

			}

		}

		base.Dispose(disposing);

	}



	private void InitializeComponent()

	{

		this.components = new System.ComponentModel.Container();

		this.toolTip = new System.Windows.Forms.ToolTip(this.components);

		this.statusStrip = new System.Windows.Forms.StatusStrip();

		this.toolStripStatusLabel = new System.Windows.Forms.ToolStripStatusLabel();

		this.toolStripProgressBar = new System.Windows.Forms.ToolStripProgressBar();

		this.menuStrip = new System.Windows.Forms.MenuStrip();

		this.fileToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();

		this.addFilesToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();

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

		this.colFileName = new System.Windows.Forms.ColumnHeader();

		this.colFilePath = new System.Windows.Forms.ColumnHeader();

		this.colStatus = new System.Windows.Forms.ColumnHeader();

		this.toolStripFileActions = new System.Windows.Forms.ToolStrip();

		this.btnAddFile = new System.Windows.Forms.ToolStripButton();

		this.btnRemoveFile = new System.Windows.Forms.ToolStripButton();

		this.btnClearFiles = new System.Windows.Forms.ToolStripButton();

		this.splitContainerResults = new System.Windows.Forms.SplitContainer();

		this.grpExtractedTables = new System.Windows.Forms.GroupBox();

		this.lblFilter = new System.Windows.Forms.Label();

		this.txtTableFilter = new System.Windows.Forms.TextBox();

		this.lstTables = new System.Windows.Forms.CheckedListBox();

		this.grpActivityLog = new System.Windows.Forms.GroupBox();

		this.txtActivityLog = new System.Windows.Forms.TextBox();

		this.panelExportActions = new System.Windows.Forms.Panel();

		this.btnCancelOperation = new System.Windows.Forms.Button();

		this.btnExportSelected = new System.Windows.Forms.Button();

		this.btnExportAll = new System.Windows.Forms.Button();

		this.btnExportPowerBi = new System.Windows.Forms.Button();

		this.grpPowerBI = new System.Windows.Forms.GroupBox();

		this.btnPbiDetails = new System.Windows.Forms.Button();

		this.lblPbiStatus = new System.Windows.Forms.Label();

		this.chkCreatePowerBiTables = new System.Windows.Forms.CheckBox();

		this.statusStrip.SuspendLayout();

		this.menuStrip.SuspendLayout();

		((System.ComponentModel.ISupportInitialize)this.splitContainerMain).BeginInit();

		this.splitContainerMain.Panel1.SuspendLayout();

		this.splitContainerMain.Panel2.SuspendLayout();

		this.splitContainerMain.SuspendLayout();

		this.grpOutput.SuspendLayout();

		this.grpInputFiles.SuspendLayout();

		this.toolStripFileActions.SuspendLayout();

		((System.ComponentModel.ISupportInitialize)this.splitContainerResults).BeginInit();

		this.splitContainerResults.Panel1.SuspendLayout();

		this.splitContainerResults.Panel2.SuspendLayout();

		this.splitContainerResults.SuspendLayout();

		this.grpExtractedTables.SuspendLayout();

		this.grpActivityLog.SuspendLayout();

		this.panelExportActions.SuspendLayout();

		this.grpPowerBI.SuspendLayout();

		base.SuspendLayout();

		this.statusStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[2] { this.toolStripStatusLabel, this.toolStripProgressBar });

		this.statusStrip.Location = new System.Drawing.Point(0, 539);

		this.statusStrip.Name = "statusStrip";

		this.statusStrip.Size = new System.Drawing.Size(884, 22);

		this.statusStrip.TabIndex = 0;

		this.statusStrip.Text = "statusStrip1";

		this.toolStripStatusLabel.Name = "toolStripStatusLabel";

		this.toolStripStatusLabel.Size = new System.Drawing.Size(869, 17);

		this.toolStripStatusLabel.Spring = true;

		this.toolStripStatusLabel.Text = "Ready";

		this.toolStripStatusLabel.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;

		this.toolStripProgressBar.Name = "toolStripProgressBar";

		this.toolStripProgressBar.Size = new System.Drawing.Size(100, 16);

		this.toolStripProgressBar.Visible = false;

		this.menuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[2] { this.fileToolStripMenuItem, this.helpToolStripMenuItem });

		this.menuStrip.Location = new System.Drawing.Point(0, 0);

		this.menuStrip.Name = "menuStrip";

		this.menuStrip.Size = new System.Drawing.Size(884, 24);

		this.menuStrip.TabIndex = 1;

		this.menuStrip.Text = "menuStrip1";

		this.fileToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[3] { this.addFilesToolStripMenuItem, new System.Windows.Forms.ToolStripSeparator(), this.exitToolStripMenuItem });

		this.fileToolStripMenuItem.Name = "fileToolStripMenuItem";

		this.fileToolStripMenuItem.Size = new System.Drawing.Size(37, 20);

		this.fileToolStripMenuItem.Text = "&File";

		this.addFilesToolStripMenuItem.Name = "addFilesToolStripMenuItem";

		this.addFilesToolStripMenuItem.Size = new System.Drawing.Size(180, 22);

		this.addFilesToolStripMenuItem.Text = "&Add Files...";

		this.addFilesToolStripMenuItem.ShortcutKeys = System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.O;

		this.addFilesToolStripMenuItem.Click += new System.EventHandler(BtnAddFile_Click);

		this.exitToolStripMenuItem.Name = "exitToolStripMenuItem";

		this.exitToolStripMenuItem.Size = new System.Drawing.Size(180, 22);

		this.exitToolStripMenuItem.Text = "E&xit";

		this.exitToolStripMenuItem.Click += new System.EventHandler(ExitToolStripMenuItem_Click);

		this.helpToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[1] { this.aboutToolStripMenuItem });

		this.helpToolStripMenuItem.Name = "helpToolStripMenuItem";

		this.helpToolStripMenuItem.Size = new System.Drawing.Size(44, 20);

		this.helpToolStripMenuItem.Text = "&Help";

		this.aboutToolStripMenuItem.Name = "aboutToolStripMenuItem";

		this.aboutToolStripMenuItem.Size = new System.Drawing.Size(107, 22);

		this.aboutToolStripMenuItem.Text = "&About";

		this.aboutToolStripMenuItem.Click += new System.EventHandler(AboutToolStripMenuItem_Click);

		this.splitContainerMain.Dock = System.Windows.Forms.DockStyle.Fill;

		this.splitContainerMain.Location = new System.Drawing.Point(0, 24);

		this.splitContainerMain.Name = "splitContainerMain";

		this.splitContainerMain.Panel1.Controls.Add(this.btnParseXer);

		this.splitContainerMain.Panel1.Controls.Add(this.grpOutput);

		this.splitContainerMain.Panel1.Controls.Add(this.grpInputFiles);

		this.splitContainerMain.Panel1.Padding = new System.Windows.Forms.Padding(5);

		this.splitContainerMain.Panel2.Controls.Add(this.splitContainerResults);

		this.splitContainerMain.Panel2.Controls.Add(this.panelExportActions);

		this.splitContainerMain.Panel2.Controls.Add(this.grpPowerBI);

		this.splitContainerMain.Panel2.Padding = new System.Windows.Forms.Padding(5);

		this.splitContainerMain.Size = new System.Drawing.Size(884, 515);

		this.splitContainerMain.SplitterDistance = 350;

		this.splitContainerMain.TabIndex = 2;

		this.btnParseXer.Anchor = System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;

		this.btnParseXer.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75f, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, 0);

		this.btnParseXer.Location = new System.Drawing.Point(8, 467);

		this.btnParseXer.Name = "btnParseXer";

		this.btnParseXer.Size = new System.Drawing.Size(404, 40);

		this.btnParseXer.TabIndex = 2;

		this.btnParseXer.Text = "Parse XER File(s)";

		this.btnParseXer.UseVisualStyleBackColor = true;

		this.btnParseXer.Click += new System.EventHandler(BtnParseXer_Click);

		this.grpOutput.Anchor = System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;

		this.grpOutput.Controls.Add(this.btnSelectOutput);

		this.grpOutput.Controls.Add(this.txtOutputPath);

		this.grpOutput.Location = new System.Drawing.Point(8, 396);

		this.grpOutput.Name = "grpOutput";

		this.grpOutput.Size = new System.Drawing.Size(404, 65);

		this.grpOutput.TabIndex = 1;

		this.grpOutput.TabStop = false;

		this.grpOutput.Text = "Output Directory";

		this.btnSelectOutput.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right;

		this.btnSelectOutput.Location = new System.Drawing.Point(323, 25);

		this.btnSelectOutput.Name = "btnSelectOutput";

		this.btnSelectOutput.Size = new System.Drawing.Size(75, 23);

		this.btnSelectOutput.TabIndex = 1;

		this.btnSelectOutput.Text = "Browse...";

		this.btnSelectOutput.UseVisualStyleBackColor = true;

		this.btnSelectOutput.Click += new System.EventHandler(BtnSelectOutput_Click);

		this.txtOutputPath.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;

		this.txtOutputPath.Location = new System.Drawing.Point(6, 27);

		this.txtOutputPath.Name = "txtOutputPath";

		this.txtOutputPath.ReadOnly = true;

		this.txtOutputPath.Size = new System.Drawing.Size(311, 20);

		this.txtOutputPath.TabIndex = 0;

		this.grpInputFiles.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;

		this.grpInputFiles.Controls.Add(this.lblDragDropHint);

		this.grpInputFiles.Controls.Add(this.lvwXerFiles);

		this.grpInputFiles.Controls.Add(this.toolStripFileActions);

		this.grpInputFiles.Location = new System.Drawing.Point(8, 8);

		this.grpInputFiles.Name = "grpInputFiles";

		this.grpInputFiles.Size = new System.Drawing.Size(404, 382);

		this.grpInputFiles.TabIndex = 0;

		this.grpInputFiles.TabStop = false;

		this.grpInputFiles.Text = "Primavera P6 XER Files";

		this.lblDragDropHint.Anchor = System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;

		this.lblDragDropHint.ForeColor = System.Drawing.SystemColors.GrayText;

		this.lblDragDropHint.Location = new System.Drawing.Point(6, 358);

		this.lblDragDropHint.Name = "lblDragDropHint";

		this.lblDragDropHint.Size = new System.Drawing.Size(392, 18);

		this.lblDragDropHint.TabIndex = 2;

		this.lblDragDropHint.Text = "Tip: Drag and drop XER files onto the list above.";

		this.lblDragDropHint.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;

		this.lvwXerFiles.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;

		this.lvwXerFiles.Columns.AddRange(new System.Windows.Forms.ColumnHeader[3] { this.colFileName, this.colFilePath, this.colStatus });

		this.lvwXerFiles.FullRowSelect = true;

		this.lvwXerFiles.HideSelection = false;

		this.lvwXerFiles.Location = new System.Drawing.Point(6, 44);

		this.lvwXerFiles.Name = "lvwXerFiles";

		this.lvwXerFiles.Size = new System.Drawing.Size(392, 311);

		this.lvwXerFiles.TabIndex = 1;

		this.lvwXerFiles.UseCompatibleStateImageBehavior = false;

		this.lvwXerFiles.View = System.Windows.Forms.View.Details;

		this.lvwXerFiles.SelectedIndexChanged += new System.EventHandler(LvwXerFiles_SelectedIndexChanged);

		this.lvwXerFiles.KeyDown += new System.Windows.Forms.KeyEventHandler(LvwXerFiles_KeyDown);

		this.colFileName.Text = "File Name";

		this.colFileName.Width = 150;

		this.colFilePath.Text = "Path";

		this.colFilePath.Width = 150;

		this.colStatus.Text = "Status";

		this.colStatus.Width = 80;

		this.toolStripFileActions.GripStyle = System.Windows.Forms.ToolStripGripStyle.Hidden;

		this.toolStripFileActions.Items.AddRange(new System.Windows.Forms.ToolStripItem[3] { this.btnAddFile, this.btnRemoveFile, this.btnClearFiles });

		this.toolStripFileActions.Location = new System.Drawing.Point(3, 16);

		this.toolStripFileActions.Name = "toolStripFileActions";

		this.toolStripFileActions.Size = new System.Drawing.Size(398, 25);

		this.toolStripFileActions.TabIndex = 0;

		this.toolStripFileActions.Text = "toolStrip1";

		this.btnAddFile.ImageTransparentColor = System.Drawing.Color.Magenta;

		this.btnAddFile.Name = "btnAddFile";

		this.btnAddFile.Size = new System.Drawing.Size(58, 22);

		this.btnAddFile.Text = "Add Files";

		this.btnAddFile.ToolTipText = "Add XER files (Ctrl+O)";

		this.btnAddFile.Click += new System.EventHandler(BtnAddFile_Click);

		this.btnRemoveFile.ImageTransparentColor = System.Drawing.Color.Magenta;

		this.btnRemoveFile.Name = "btnRemoveFile";

		this.btnRemoveFile.Size = new System.Drawing.Size(54, 22);

		this.btnRemoveFile.Text = "Remove";

		this.btnRemoveFile.ToolTipText = "Remove selected files (Del)";

		this.btnRemoveFile.Click += new System.EventHandler(BtnRemoveFile_Click);

		this.btnClearFiles.ImageTransparentColor = System.Drawing.Color.Magenta;

		this.btnClearFiles.Name = "btnClearFiles";

		this.btnClearFiles.Size = new System.Drawing.Size(57, 22);

		this.btnClearFiles.Text = "Clear All";

		this.btnClearFiles.ToolTipText = "Clear all files and results";

		this.btnClearFiles.Click += new System.EventHandler(BtnClearFiles_Click);

		this.splitContainerResults.Dock = System.Windows.Forms.DockStyle.Fill;
		//this.splitContainerResults.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;
		this.splitContainerResults.Location = new System.Drawing.Point(5, 5);

		this.splitContainerResults.Name = "splitContainerResults";

		this.splitContainerResults.Orientation = System.Windows.Forms.Orientation.Horizontal;

		this.splitContainerResults.Panel1.Controls.Add(this.grpExtractedTables);

		this.splitContainerResults.Panel2.Controls.Add(this.grpActivityLog);

		this.splitContainerResults.Size = new System.Drawing.Size(444, 362);

		this.splitContainerResults.SplitterDistance = 230;

		this.splitContainerResults.TabIndex = 3;

		this.grpExtractedTables.Controls.Add(this.lblFilter);

		this.grpExtractedTables.Controls.Add(this.txtTableFilter);

		this.grpExtractedTables.Controls.Add(this.lstTables);

		this.grpExtractedTables.Dock = System.Windows.Forms.DockStyle.Fill;

		this.grpExtractedTables.Location = new System.Drawing.Point(0, 0);

		this.grpExtractedTables.Name = "grpExtractedTables";

		this.grpExtractedTables.Size = new System.Drawing.Size(444, 230);

		this.grpExtractedTables.TabIndex = 0;

		this.grpExtractedTables.TabStop = false;

		this.grpExtractedTables.Text = "Extracted Tables (Check to select)";

		this.lblFilter.AutoSize = true;

		this.lblFilter.Location = new System.Drawing.Point(6, 22);

		this.lblFilter.Name = "lblFilter";

		this.lblFilter.Size = new System.Drawing.Size(32, 13);

		this.lblFilter.TabIndex = 2;

		this.lblFilter.Text = "Filter:";

		this.txtTableFilter.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;

		this.txtTableFilter.Location = new System.Drawing.Point(44, 19);

		this.txtTableFilter.Name = "txtTableFilter";

		this.txtTableFilter.Size = new System.Drawing.Size(394, 20);

		this.txtTableFilter.TabIndex = 1;

		this.txtTableFilter.TextChanged += new System.EventHandler(TxtTableFilter_TextChanged);

		this.lstTables.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;

		this.lstTables.FormattingEnabled = true;

		this.lstTables.IntegralHeight = false;

		this.lstTables.Location = new System.Drawing.Point(3, 45);

		this.lstTables.Name = "lstTables";

		this.lstTables.Size = new System.Drawing.Size(438, 182);

		this.lstTables.Sorted = true;

		this.lstTables.TabIndex = 0;

		this.grpActivityLog.Controls.Add(this.txtActivityLog);

		this.grpActivityLog.Dock = System.Windows.Forms.DockStyle.Fill;

		this.grpActivityLog.Location = new System.Drawing.Point(0, 0);

		this.grpActivityLog.Name = "grpActivityLog";

		this.grpActivityLog.Size = new System.Drawing.Size(444, 128);

		this.grpActivityLog.TabIndex = 0;

		this.grpActivityLog.TabStop = false;

		this.grpActivityLog.Text = "Activity Log";

		this.txtActivityLog.Dock = System.Windows.Forms.DockStyle.Fill;

		this.txtActivityLog.Location = new System.Drawing.Point(3, 16);

		this.txtActivityLog.Multiline = true;

		this.txtActivityLog.Name = "txtActivityLog";

		this.txtActivityLog.ReadOnly = true;

		this.txtActivityLog.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;

		this.txtActivityLog.Size = new System.Drawing.Size(438, 109);

		this.txtActivityLog.TabIndex = 0;

		this.panelExportActions.Dock = System.Windows.Forms.DockStyle.Bottom;
		//this.panelExportActions.Anchor = System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;
		this.panelExportActions.Location = new System.Drawing.Point(5, 470);
		this.panelExportActions.AutoSize = true;
		this.panelExportActions.AutoSizeMode = System.Windows.Forms.AutoSizeMode.GrowAndShrink;
		this.panelExportActions.MinimumSize = new System.Drawing.Size(0, 40);



		this.panelExportActions.Name = "panelExportActions";

		this.panelExportActions.Size = new System.Drawing.Size(444, 40);

		this.panelExportActions.TabIndex = 2;

		this.btnCancelOperation.Anchor = System.Windows.Forms.AnchorStyles.Top;

		this.btnCancelOperation.BackColor = System.Drawing.Color.MistyRose;

		this.btnCancelOperation.Location = new System.Drawing.Point(186, 5);

		this.btnCancelOperation.Name = "btnCancelOperation";

		this.btnCancelOperation.Size = new System.Drawing.Size(72, 30);

		this.btnCancelOperation.TabIndex = 2;

		this.btnCancelOperation.Text = "Cancel";

		this.btnCancelOperation.UseVisualStyleBackColor = false;

		this.btnCancelOperation.Visible = false;

		this.btnCancelOperation.Click += new System.EventHandler(BtnCancelOperation_Click);

		this.btnExportSelected.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right;

		this.btnExportSelected.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25f, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, 0);

		this.btnExportSelected.Location = new System.Drawing.Point(264, 0);

		this.btnExportSelected.Name = "btnExportSelected";

		this.btnExportSelected.Size = new System.Drawing.Size(180, 40);

		this.btnExportSelected.TabIndex = 1;

		this.btnExportSelected.Text = "Export Selected";

		this.btnExportSelected.UseVisualStyleBackColor = true;

		this.btnExportSelected.Click += new System.EventHandler(BtnExportSelected_Click);

		this.btnExportAll.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75f, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, 0);

		this.btnExportAll.Location = new System.Drawing.Point(0, 0);

		this.btnExportAll.Name = "btnExportAll";

		this.btnExportAll.Size = new System.Drawing.Size(180, 40);

		this.btnExportAll.TabIndex = 0;

		this.btnExportAll.Text = "Export All";

		this.btnExportAll.UseVisualStyleBackColor = true;

		this.btnExportAll.Click += new System.EventHandler(BtnExportAll_Click);

		this.btnExportPowerBi.Name = "btnExportPowerBi";

		this.btnExportPowerBi.Size = new System.Drawing.Size(150, 40);

		this.btnExportPowerBi.TabIndex = 2;

		this.btnExportPowerBi.Text = "Export Power BI";

		this.btnExportPowerBi.UseVisualStyleBackColor = true;

		this.btnExportPowerBi.Click += new System.EventHandler(BtnExportPowerBi_Click);

		this.grpPowerBI.Anchor = System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;

		this.grpPowerBI.Controls.Add(this.btnPbiDetails);

		this.grpPowerBI.Controls.Add(this.lblPbiStatus);

		this.grpPowerBI.Controls.Add(this.chkCreatePowerBiTables);

		this.grpPowerBI.Location = new System.Drawing.Point(8, 376);

		this.grpPowerBI.Name = "grpPowerBI";

		this.grpPowerBI.Size = new System.Drawing.Size(444, 85);

		this.grpPowerBI.TabIndex = 1;

		this.grpPowerBI.TabStop = false;

		this.grpPowerBI.Text = "Power BI Enhanced Tables (Included in Exports)";

		this.btnPbiDetails.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right;

		this.btnPbiDetails.Location = new System.Drawing.Point(363, 24);

		this.btnPbiDetails.Name = "btnPbiDetails";

		this.btnPbiDetails.Size = new System.Drawing.Size(75, 23);

		this.btnPbiDetails.TabIndex = 2;

		this.btnPbiDetails.Text = "Details...";

		this.btnPbiDetails.UseVisualStyleBackColor = true;

		this.btnPbiDetails.Click += new System.EventHandler(BtnPbiDetails_Click);

		this.lblPbiStatus.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;

		this.lblPbiStatus.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25f, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, 0);

		this.lblPbiStatus.Location = new System.Drawing.Point(6, 54);

		this.lblPbiStatus.Name = "lblPbiStatus";

		this.lblPbiStatus.Size = new System.Drawing.Size(432, 21);

		this.lblPbiStatus.TabIndex = 1;

		this.lblPbiStatus.Text = "Status: Unknown";

		this.chkCreatePowerBiTables.AutoSize = true;

		this.chkCreatePowerBiTables.Location = new System.Drawing.Point(9, 28);

		this.chkCreatePowerBiTables.Name = "chkCreatePowerBiTables";

		this.chkCreatePowerBiTables.Size = new System.Drawing.Size(193, 17);

		this.chkCreatePowerBiTables.TabIndex = 0;

		this.chkCreatePowerBiTables.Text = "Generate Enhanced Tables on Export";

		this.chkCreatePowerBiTables.UseVisualStyleBackColor = true;

		this.chkCreatePowerBiTables.CheckedChanged += new System.EventHandler(ChkCreatePowerBiTables_CheckedChanged);

		base.AutoScaleDimensions = new System.Drawing.SizeF(6f, 13f);

		base.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;

		base.ClientSize = new System.Drawing.Size(884, 561);

		base.Controls.Add(this.splitContainerMain);

		base.Controls.Add(this.menuStrip);

		base.Controls.Add(this.statusStrip);

		base.MainMenuStrip = this.menuStrip;

		this.MinimumSize = new System.Drawing.Size(800, 550);

		base.Name = "MainForm";

		base.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;

		this.Text = "Primavera P6 XER to CSV Converter v2.4";

		this.statusStrip.ResumeLayout(false);

		this.statusStrip.PerformLayout();

		this.menuStrip.ResumeLayout(false);

		this.menuStrip.PerformLayout();

		this.splitContainerMain.Panel1.ResumeLayout(false);

		this.splitContainerMain.Panel2.ResumeLayout(false);

		((System.ComponentModel.ISupportInitialize)this.splitContainerMain).EndInit();

		this.splitContainerMain.ResumeLayout(false);

		this.grpOutput.ResumeLayout(false);

		this.grpOutput.PerformLayout();

		this.grpInputFiles.ResumeLayout(false);

		this.grpInputFiles.PerformLayout();

		this.toolStripFileActions.ResumeLayout(false);

		this.toolStripFileActions.PerformLayout();

		this.splitContainerResults.Panel1.ResumeLayout(false);

		this.splitContainerResults.Panel2.ResumeLayout(false);

		((System.ComponentModel.ISupportInitialize)this.splitContainerResults).EndInit();

		this.splitContainerResults.ResumeLayout(false);

		this.grpExtractedTables.ResumeLayout(false);

		this.grpExtractedTables.PerformLayout();

		this.grpActivityLog.ResumeLayout(false);

		this.grpActivityLog.PerformLayout();

		this.panelExportActions.ResumeLayout(false);

		this.panelExportActions.PerformLayout();

		this.grpPowerBI.ResumeLayout(false);

		this.grpPowerBI.PerformLayout();

		base.ResumeLayout(false);

		base.PerformLayout();

	}

}

