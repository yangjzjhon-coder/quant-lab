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

Write-Host ""
Write-Host "=== Quant Lab Quick Stop ===" -ForegroundColor Cyan
Write-Host ""

wsl -d $Distro -u $WslUser -- bash -lc "cd $ProjectDir && chmod +x '$LinuxScript' && '$LinuxScript' stop"
Start-Sleep -Seconds 2
netsh interface portproxy delete v4tov4 listenport=$Port listenaddress=127.0.0.1 2>$null | Out-Null

Write-Host "Quant Lab runtime stopped." -ForegroundColor Green
Write-Host ""
if ($env:QUANT_LAB_NO_PAUSE -ne "1") {
    Read-Host "Press Enter to close"
}
