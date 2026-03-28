$ErrorActionPreference = "Stop"

$Distro = "Ubuntu-24.04"
$WslUser = "wu"
$ProjectWindowsDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$ProjectDrive = $ProjectWindowsDir.Substring(0, 1).ToLowerInvariant()
$ProjectTail = $ProjectWindowsDir.Substring(2).Replace('\', '/')
$ProjectDir = "/mnt/$ProjectDrive$ProjectTail"
$Command = "cd $ProjectDir && PYTHONPATH=src ./.venv/bin/python -m quant_lab demo-preflight --config config/settings.yaml --live-plan"

Write-Host ""
Write-Host "=== Quant Lab Demo Preflight ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Checking config readiness, alert channels, and live plan generation..." -ForegroundColor Yellow
Write-Host ""

wsl -d $Distro -u $WslUser -- bash -lc $Command
$exitCode = $LASTEXITCODE

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "Preflight completed." -ForegroundColor Green
}
else {
    Write-Host "Preflight found blocking issues." -ForegroundColor Red
}
Write-Host ""

if ($env:QUANT_LAB_NO_PAUSE -ne "1") {
    Read-Host "Press Enter to close"
}
exit $exitCode
