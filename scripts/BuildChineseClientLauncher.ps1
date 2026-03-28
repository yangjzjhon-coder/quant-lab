param()

$ErrorActionPreference = "Stop"

$compiler = @(
    "C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe",
    "C:\Windows\Microsoft.NET\Framework\v4.0.30319\csc.exe"
) | Where-Object { Test-Path $_ } | Select-Object -First 1

if (-not $compiler) {
    throw "csc.exe not found."
}

$sourcePath = Join-Path $PSScriptRoot "QuantLabChineseClientLauncher.cs"
$repoExePath = Join-Path $PSScriptRoot "QuantLabChineseClientLauncher.exe"
$chineseExeName = (-join @(
    [char]0x542F, [char]0x52A8, [char]0x4E2D, [char]0x6587,
    [char]0x5BA2, [char]0x6237, [char]0x7AEF, [char]0x002E,
    [char]0x0065, [char]0x0078, [char]0x0065
))
$repoChineseExePath = Join-Path $PSScriptRoot $chineseExeName
$desktopExePath = Join-Path ([Environment]::GetFolderPath("Desktop")) $chineseExeName

& $compiler `
    /nologo `
    /target:winexe `
    /platform:anycpu `
    /optimize+ `
    /out:$repoExePath `
    /reference:System.dll `
    /reference:System.Core.dll `
    /reference:System.Windows.Forms.dll `
    /reference:System.Drawing.dll `
    $sourcePath

if ($LASTEXITCODE -ne 0) {
    throw "Launcher build failed."
}

Copy-Item -LiteralPath $repoExePath -Destination $repoChineseExePath -Force
Copy-Item -LiteralPath $repoExePath -Destination $desktopExePath -Force

Write-Host "Built:" -ForegroundColor Green
Write-Host "  $repoExePath"
Write-Host "  $repoChineseExePath"
Write-Host "  $desktopExePath"
