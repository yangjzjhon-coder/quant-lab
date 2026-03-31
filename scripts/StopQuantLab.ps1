$ErrorActionPreference = "Stop"

if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Start-Process PowerShell -Verb RunAs -ArgumentList "-ExecutionPolicy Bypass -File `"$PSCommandPath`""
    exit
}

$Distro = "Ubuntu-24.04"
$WslUser = "wu"
$ProjectWindowsDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$ProjectDrive = $ProjectWindowsDir.Substring(0, 1).ToLowerInvariant()
$ProjectTail = $ProjectWindowsDir.Substring(2).Replace('\', '/')
$ProjectDir = "/mnt/$ProjectDrive$ProjectTail"
$LinuxScript = "$ProjectDir/scripts/quant_lab_service.sh"
$Port = 18080
$WindowsPython = Join-Path $ProjectWindowsDir ".venv\Scripts\python.exe"
$LinuxPython = Join-Path $ProjectWindowsDir ".venv\bin\python"
$UseWindowsRuntime = (Test-Path $WindowsPython) -and (-not (Test-Path $LinuxPython))

function Stop-WindowsQuantLabRuntime {
    $patterns = @(
        "-m\s+quant_lab\s+service-api(\s|$)",
        "-m\s+quant_lab\s+demo-loop(\s|$)",
        "-m\s+quant_lab\s+demo-portfolio-loop(\s|$)"
    )

    $processes = Get-CimInstance Win32_Process | Where-Object {
        $cmd = [string]$_.CommandLine
        if (-not $cmd) {
            return $false
        }
        foreach ($pattern in $patterns) {
            if ($cmd -match $pattern) {
                return $true
            }
        }
        return $false
    }

    foreach ($process in $processes) {
        try {
            Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
            Write-Host ("Stopped Windows runtime PID {0}" -f $process.ProcessId) -ForegroundColor DarkYellow
        }
        catch {
            Write-Host ("Failed to stop PID {0}: {1}" -f $process.ProcessId, $_.Exception.Message) -ForegroundColor Red
        }
    }
}

Write-Host ""
Write-Host "=== Quant Lab Quick Stop ===" -ForegroundColor Cyan
Write-Host ""

if ($UseWindowsRuntime) {
    Write-Host "Detected Windows-local runtime; stopping local Python processes..." -ForegroundColor Yellow
    Stop-WindowsQuantLabRuntime
}
else {
    Write-Host "Detected WSL-supervised runtime; stopping WSL services..." -ForegroundColor Yellow
    wsl -d $Distro -u $WslUser -- bash -lc "cd $ProjectDir && chmod +x '$LinuxScript' && '$LinuxScript' stop"
    Start-Sleep -Seconds 2
}

netsh interface portproxy delete v4tov4 listenport=$Port listenaddress=127.0.0.1 2>$null | Out-Null

Write-Host "Quant Lab runtime stopped." -ForegroundColor Green
Write-Host ""
if ($env:QUANT_LAB_NO_PAUSE -ne "1") {
    Read-Host "Press Enter to close"
}
