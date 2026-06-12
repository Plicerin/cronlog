param(
    [ValidateSet("normal", "flaky", "hang", "fail", "large-output")]
    [string]$Mode = "flaky",

    [string]$Schedule = "every 1 minutes",

    [int]$TimeoutSeconds = 45,

    [string]$JobName = "content-pipeline-cron2-live"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$exe = Join-Path $root "target\debug\cron2.exe"
$db = Join-Path $root "bakeoff-cron2.db"
$outDir = Join-Path $root "bakeoff-runs"
$pipeline = Join-Path $root "examples\content_pipeline.ps1"
$powershellExe = (Get-Command powershell.exe).Source

Push-Location $root
try {
    cargo build
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with code $LASTEXITCODE"
    }

    New-Item -ItemType Directory -Force -Path $outDir | Out-Null

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $removeOutput = & $exe --db $db remove $JobName 2>&1
    $removeExitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousErrorActionPreference
    if ($removeExitCode -ne 0 -and (($removeOutput -join "`n") -notmatch "NotFound")) {
        throw "failed to remove previous Cron2 bakeoff job: $($removeOutput -join "`n")"
    }

    & $exe --db $db add --name $JobName --schedule $Schedule --timeout "${TimeoutSeconds}s" "--" $powershellExe -NoProfile -ExecutionPolicy Bypass -File $pipeline -Scheduler cron2 -OutDir $outDir -Mode $Mode
    if ($LASTEXITCODE -ne 0) {
        throw "failed to add Cron2 bakeoff job"
    }

    Write-Host "Cron2 bakeoff job registered."
    Write-Host "Run this in a foreground terminal:"
    Write-Host "  .\target\debug\cron2.exe --db .\bakeoff-cron2.db daemon"
    Write-Host ""
    Write-Host "Inspect with:"
    Write-Host "  .\target\debug\cron2.exe --db .\bakeoff-cron2.db history $JobName"
    Write-Host "  .\target\debug\cron2.exe --db .\bakeoff-cron2.db logs $JobName --last"
}
finally {
    Pop-Location
}
