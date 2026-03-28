param()

$ErrorActionPreference = "Stop"

& "$PSScriptRoot\StartQuantLab.ps1" -AutoSubmit -Restart
exit $LASTEXITCODE
