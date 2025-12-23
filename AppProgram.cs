using System;
using System.Windows.Forms;

namespace XerToCsvConverter;

internal static class Program
{
    private static readonly bool EnableExpiration = false;
    private static readonly DateTime ExpirationDate = new DateTime(2025, 9, 1, 23, 59, 59);
    private const string ExpirationTitle = "Application Expired";
    private const string ExpirationMessage = """
This application has expired and is no longer available for use.

Please contact the software provider for a renewed version.

Thank you for using this software.
""";

    [STAThread]
    private static void Main()
    {
        if (EnableExpiration)
        {
            DateTime currentDate = DateTime.Now;
            if (currentDate > ExpirationDate)
            {
                MessageBox.Show(ExpirationMessage, ExpirationTitle, MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            TimeSpan timeUntilExpiration = ExpirationDate - currentDate;
            if (timeUntilExpiration.TotalDays <= 7 && timeUntilExpiration.TotalDays > 0)
            {
                string warningMessage = $"Warning: This application will expire in {(int)Math.Ceiling(timeUntilExpiration.TotalDays)} day(s).\n\n" +
                                        "Please contact the software provider for a renewed version.";
                MessageBox.Show(warningMessage, "Expiration Warning", MessageBoxButtons.OK, MessageBoxIcon.Warning);
            }
        }

        Application.EnableVisualStyles();
        Application.SetCompatibleTextRenderingDefault(false);
        Application.Run(new MainForm());
    }
}
