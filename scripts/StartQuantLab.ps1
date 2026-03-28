param(
    [switch]$AutoSubmit,
    [switch]$Restart,
    [string]$OpenPath = "/"
)

$ErrorActionPreference = "Stop"

if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    $relaunchArgs = @("-ExecutionPolicy Bypass", "-File `"$PSCommandPath`"")
    if ($AutoSubmit) {
        $relaunchArgs += "-AutoSubmit"
    }
    if ($Restart) {
        $relaunchArgs += "-Restart"
    }
    if ($OpenPath) {
        $relaunchArgs += "-OpenPath `"$OpenPath`""
    }
    Start-Process PowerShell -Verb RunAs -ArgumentList ($relaunchArgs -join " ")
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
$normalizedPath = if ([string]::IsNullOrWhiteSpace($OpenPath)) { "/" } elseif ($OpenPath.StartsWith("/")) { $OpenPath } else { "/$OpenPath" }
$Url = "http://127.0.0.1:$Port$normalizedPath"
$HealthUrl = "http://127.0.0.1:$Port/health"

Write-Host ""
Write-Host "=== Quant Lab Quick Start ===" -ForegroundColor Cyan
Write-Host ""

Write-Host "[1/4] Getting WSL IP..." -ForegroundColor Yellow
$wslIp = (wsl -d $Distro -u $WslUser -- hostname -I).Trim().Split(' ')[0]
if (-not $wslIp) {
    throw "Unable to resolve WSL IP."
}
Write-Host "      WSL IP: $wslIp" -ForegroundColor Green

Write-Host "[2/4] Refreshing localhost portproxy..." -ForegroundColor Yellow
netsh interface portproxy delete v4tov4 listenport=$Port listenaddress=127.0.0.1 2>$null | Out-Null
netsh interface portproxy add v4tov4 listenport=$Port listenaddress=127.0.0.1 connectport=$Port connectaddress=$wslIp | Out-Null
Write-Host "      127.0.0.1:$Port -> ${wslIp}:$Port" -ForegroundColor Green

Write-Host "[3/4] Starting runtime services..." -ForegroundColor Yellow
$action = if ($Restart) { "restart" } else { "start" }
$modeLabel = if ($AutoSubmit) { "demo auto-submit" } else { "plan-only" }
Write-Host "      mode: $modeLabel" -ForegroundColor White
$wslCommand = if ($AutoSubmit) {
    "export QUANT_LAB_ALLOW_ORDER_PLACEMENT=true; cd $ProjectDir && chmod +x '$LinuxScript' && '$LinuxScript' $action"
}
else {
    "cd $ProjectDir && chmod +x '$LinuxScript' && '$LinuxScript' $action"
}
wsl -d $Distro -u $WslUser -- bash -lc $wslCommand
if ($LASTEXITCODE -ne 0) {
    throw "quant-lab runtime failed to start."
}

Write-Host "[4/4] Waiting for health check..." -ForegroundColor Yellow
$health = $null
for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Seconds 1
    try {
        $health = & curl.exe --noproxy "*" -fsS $HealthUrl 2>$null
        if ($LASTEXITCODE -eq 0) {
            break
        }
    }
    catch {
    }
}
if (-not $health) {
    throw "Service did not become reachable from Windows."
}

$preflight = $null
try {
    $preflightRaw = & curl.exe --noproxy "*" -fsS "http://127.0.0.1:$Port/runtime/preflight" 2>$null
    if ($LASTEXITCODE -eq 0 -and $preflightRaw) {
        $preflight = $preflightRaw | ConvertFrom-Json
    }
}
catch {
}

if ($env:QUANT_LAB_SKIP_BROWSER -eq "1") {
    Write-Host "      Browser open skipped" -ForegroundColor DarkYellow
}
else {
    Start-Process $Url
}

Write-Host ""
Write-Host "Quant Lab is ready:" -ForegroundColor Green
Write-Host "  $Url" -ForegroundColor White
Write-Host "  demo-loop is supervised inside WSL" -ForegroundColor White
if ($preflight) {
    $demoMode = $preflight.demo_trading.mode
    $demoReady = [bool]$preflight.demo_trading.ready
    $readyChannels = @()
    if ($preflight.alerts.channels.telegram.ready) { $readyChannels += "telegram" }
    if ($preflight.alerts.channels.email.ready) { $readyChannels += "email" }

    Write-Host "  demo trading mode: $demoMode" -ForegroundColor White
    Write-Host "  alerts ready: $(if ($readyChannels.Count -gt 0) { $readyChannels -join ', ' } else { 'none' })" -ForegroundColor White

    if (-not $demoReady) {
        $reasons = @($preflight.demo_trading.reasons | Select-Object -First 3)
        if ($reasons.Count -gt 0) {
            Write-Host "  blocked by: $($reasons -join '; ')" -ForegroundColor DarkYellow
        }
    }
}
Write-Host ""
if ($env:QUANT_LAB_NO_PAUSE -ne "1") {
    Read-Host "Press Enter to close"
}
