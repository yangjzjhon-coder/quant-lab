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
$BaseUrl = "http://127.0.0.1:$Port$normalizedPath"
$HealthUrl = "http://127.0.0.1:$Port/health"
$WindowsPython = Join-Path $ProjectWindowsDir ".venv\Scripts\python.exe"
$LinuxPython = Join-Path $ProjectWindowsDir ".venv\bin\python"
$DataDir = Join-Path $ProjectWindowsDir "data"
$ServiceLog = Join-Path $DataDir "service-api.log"
$DemoLog = Join-Path $DataDir "demo-loop.log"
$LauncherLog = Join-Path $DataDir "service-api-launcher.log"
$UseWindowsRuntime = (Test-Path $WindowsPython) -and (-not (Test-Path $LinuxPython))

function Wait-ForHealth {
    param(
        [string]$Url,
        [int]$Retries = 20,
        [int]$DelaySeconds = 1
    )

    for ($i = 0; $i -lt $Retries; $i++) {
        Start-Sleep -Seconds $DelaySeconds
        try {
            $response = & curl.exe --noproxy "*" -fsS $Url 2>$null
            if ($LASTEXITCODE -eq 0 -and $response) {
                return $response
            }
        }
        catch {
        }
    }
    return $null
}

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
        Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Get-WindowsDemoCommand {
    Push-Location $ProjectWindowsDir
    $previousPythonPath = $env:PYTHONPATH
    try {
        $env:PYTHONPATH = "src"
        $command = @(
            "from pathlib import Path",
            "from quant_lab.config import configured_symbols, load_config",
            "cfg = load_config(Path('config/settings.yaml'))",
            "print('demo-portfolio-loop' if len(configured_symbols(cfg)) > 1 else 'demo-loop')"
        ) -join "; "
        return ((& $WindowsPython -c $command) | Out-String).Trim()
    }
    finally {
        if ($null -eq $previousPythonPath) {
            Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
        }
        else {
            $env:PYTHONPATH = $previousPythonPath
        }
        Pop-Location
    }
}

function Start-WindowsBackgroundPowerShell {
    param(
        [string]$Command
    )

    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($Command))
    Start-Process -FilePath "powershell.exe" -ArgumentList @(
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-EncodedCommand",
        $encoded
    ) -WindowStyle Hidden | Out-Null
}

function Start-WindowsQuantLabRuntime {
    New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
    if ($Restart) {
        Stop-WindowsQuantLabRuntime
        Start-Sleep -Seconds 1
    }

    netsh interface portproxy delete v4tov4 listenport=$Port listenaddress=127.0.0.1 2>$null | Out-Null

    foreach ($logPath in @($ServiceLog, $DemoLog)) {
        if (-not (Test-Path $logPath)) {
            New-Item -ItemType File -Path $logPath -Force | Out-Null
        }
        try {
            Add-Content -Path $logPath -Value "`n[$(Get-Date -Format s)] launcher start"
        }
        catch {
        }
    }
    Set-Content -Path $LauncherLog -Value "windows-runtime mode $(Get-Date -Format s)"

    $serviceCommand = @"
Set-Location '$ProjectWindowsDir'
`$env:PYTHONPATH = 'src'
`$env:PYTHONUNBUFFERED = '1'
& '$WindowsPython' -m quant_lab service-api --config 'config/settings.yaml' *>> '$ServiceLog'
"@
    Start-WindowsBackgroundPowerShell -Command $serviceCommand

    $demoCommand = Get-WindowsDemoCommand
    if (-not [string]::IsNullOrWhiteSpace($demoCommand)) {
        $autoSubmitLine = ""
        $autoSubmitArgs = ""
        if ($AutoSubmit) {
            $autoSubmitLine = "`$env:QUANT_LAB_ALLOW_ORDER_PLACEMENT = 'true'"
            $autoSubmitArgs = " --submit --confirm OKX_DEMO"
        }
        $demoLoopCommand = @"
Set-Location '$ProjectWindowsDir'
`$env:PYTHONPATH = 'src'
`$env:PYTHONUNBUFFERED = '1'
$autoSubmitLine
& '$WindowsPython' -m quant_lab $demoCommand --config 'config/settings.yaml'$autoSubmitArgs *>> '$DemoLog'
"@
        Start-WindowsBackgroundPowerShell -Command $demoLoopCommand
    }

    return (Wait-ForHealth -Url $HealthUrl -Retries 25 -DelaySeconds 1)
}

Write-Host ""
Write-Host "=== Quant Lab Quick Start ===" -ForegroundColor Cyan
Write-Host ""

if ($UseWindowsRuntime) {
    Write-Host "[1/3] Detected Windows-local virtualenv" -ForegroundColor Yellow
    Write-Host "      mode: windows-local fallback" -ForegroundColor White
    Write-Host "      python: $WindowsPython" -ForegroundColor Green

    Write-Host "[2/3] Starting runtime services..." -ForegroundColor Yellow
    $health = Start-WindowsQuantLabRuntime
    if (-not $health) {
        Write-Host "--- service log ---" -ForegroundColor DarkYellow
        Get-Content -Path $ServiceLog -Tail 80 -ErrorAction SilentlyContinue
        throw "Service did not become reachable from Windows."
    }
}
else {
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
    $health = Wait-ForHealth -Url $HealthUrl -Retries 20 -DelaySeconds 1
    if (-not $health) {
        throw "Service did not become reachable from Windows."
    }
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
    $launchNonce = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $separator = if ($BaseUrl.Contains("?")) { "&" } else { "?" }
    $Url = "${BaseUrl}${separator}_ts=$launchNonce"
    Start-Process $Url
}

if (-not $Url) {
    $Url = $BaseUrl
}

Write-Host ""
Write-Host "Quant Lab is ready:" -ForegroundColor Green
Write-Host "  $Url" -ForegroundColor White
if ($UseWindowsRuntime) {
    Write-Host "  runtime mode: windows-local fallback" -ForegroundColor White
}
else {
    Write-Host "  demo-loop is supervised inside WSL" -ForegroundColor White
}
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
