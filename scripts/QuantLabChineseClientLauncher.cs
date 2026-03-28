using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Windows.Forms;

internal static class QuantLabChineseClientLauncher
{
    [STAThread]
    private static int Main()
    {
        try
        {
            var scriptPath = ResolveScriptPath();
            if (scriptPath == null)
            {
                ShowError(
                    "\u672a\u627e\u5230 StartQuantLabChineseClient.ps1\u3002\r\n\r\n" +
                    "\u5df2\u68c0\u67e5\u7684\u4f4d\u7f6e:\r\n" +
                    string.Join("\r\n", CandidateScriptPaths()));
                return 1;
            }

            var startInfo = new ProcessStartInfo
            {
                FileName = "powershell.exe",
                Arguments = "-NoProfile -ExecutionPolicy Bypass -File \"" + scriptPath + "\"",
                WorkingDirectory = Path.GetDirectoryName(scriptPath) ?? Environment.CurrentDirectory,
                UseShellExecute = true,
            };

            var process = Process.Start(startInfo);
            if (process == null)
            {
                ShowError("\u542f\u52a8 powershell.exe \u5931\u8d25\u3002");
                return 1;
            }

            process.WaitForExit();
            return process.ExitCode;
        }
        catch (Exception ex)
        {
            ShowError("\u542f\u52a8\u4e2d\u6587\u5ba2\u6237\u7aef\u5931\u8d25:\r\n\r\n" + ex.Message);
            return 1;
        }
    }

    private static string ResolveScriptPath()
    {
        return CandidateScriptPaths().FirstOrDefault(File.Exists);
    }

    private static string[] CandidateScriptPaths()
    {
        var baseDir = AppDomain.CurrentDomain.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar);
        return new[]
        {
            Path.Combine(baseDir, "StartQuantLabChineseClient.ps1"),
            Path.Combine(baseDir, "scripts", "StartQuantLabChineseClient.ps1"),
            @"E:\quant-lab\scripts\StartQuantLabChineseClient.ps1",
        };
    }

    private static void ShowError(string message)
    {
        MessageBox.Show(
            message,
            "quant-lab \u4e2d\u6587\u542f\u52a8\u5668",
            MessageBoxButtons.OK,
            MessageBoxIcon.Error);
    }
}
