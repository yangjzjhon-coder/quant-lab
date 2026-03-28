param(
    [switch]$Submit,
    [switch]$ResetState,
    [string]$Confirm = ""
)

$ErrorActionPreference = "Stop"

$Distro = "Ubuntu-24.04"
$WslUser = "wu"
$ProjectWindowsDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$ProjectDrive = $ProjectWindowsDir.Substring(0, 1).ToLowerInvariant()
$ProjectTail = $ProjectWindowsDir.Substring(2).Replace('\', '/')
$ProjectDir = "/mnt/$ProjectDrive$ProjectTail"
$LoopCommand = @'
cd __PROJECT_DIR__ && PYTHONPATH=src ./.venv/bin/python - <<'PY'
from pathlib import Path

from quant_lab.config import configured_symbols, load_config

cfg = load_config(Path("config/settings.yaml"))
print("demo-portfolio-drill" if len(configured_symbols(cfg)) > 1 else "demo-drill")
PY
'@.Replace("__PROJECT_DIR__", $ProjectDir)

$ResolvedDrillCommand = (wsl -d $Distro -u $WslUser -- bash -lc $LoopCommand).Trim()
if (-not $ResolvedDrillCommand) {
    throw "Unable to resolve drill command from config/settings.yaml"
}

$Command = "cd $ProjectDir && PYTHONPATH=src ./.venv/bin/python -m quant_lab $ResolvedDrillCommand --config config/settings.yaml"

if ($ResetState) {
    $Command += " --reset-state"
}
if ($Submit) {
    $Command += " --submit"
    if ($Confirm) {
        $Command += " --confirm $Confirm"
    }
}

Write-Host ""
Write-Host "=== Quant Lab Demo Drill ===" -ForegroundColor Cyan
Write-Host ""
if ($Submit) {
    Write-Host "Running a submit-enabled drill against the OKX demo account..." -ForegroundColor Yellow
}
else {
    Write-Host "Running a safe plan-only drill. No orders will be sent." -ForegroundColor Yellow
}
Write-Host ""

wsl -d $Distro -u $WslUser -- bash -lc $Command
$exitCode = $LASTEXITCODE

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "Demo drill completed." -ForegroundColor Green
}
else {
    Write-Host "Demo drill failed." -ForegroundColor Red
}
Write-Host ""

if ($env:QUANT_LAB_NO_PAUSE -ne "1") {
    Read-Host "Press Enter to close"
}
exit $exitCode
