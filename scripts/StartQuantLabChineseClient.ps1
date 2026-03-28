param()

$ErrorActionPreference = "Stop"

try {
    $Host.UI.RawUI.WindowTitle = "quant-lab Chinese Client Launcher"
}
catch {
}

Write-Host ""
Write-Host "=== quant-lab Chinese Client Launcher ===" -ForegroundColor Cyan
Write-Host "Starts local services and opens /client" -ForegroundColor DarkCyan
Write-Host "URL: http://127.0.0.1:18080/client" -ForegroundColor DarkGray
Write-Host ""

& "$PSScriptRoot\StartQuantLab.ps1" -Restart -OpenPath "/client"
exit $LASTEXITCODE
