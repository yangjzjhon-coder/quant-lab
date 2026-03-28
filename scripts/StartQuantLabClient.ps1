param(
    [switch]$Restart
)

$ErrorActionPreference = "Stop"

& "$PSScriptRoot\StartQuantLab.ps1" -Restart:$Restart -OpenPath "/client"
exit $LASTEXITCODE
