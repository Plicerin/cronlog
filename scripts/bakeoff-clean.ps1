param(
    [string]$TaskName = "Cron2BakeoffContentPipeline",
    [string]$DaemonTaskName = "Cron2BakeoffDaemon"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$db = Join-Path $root "bakeoff-cron2.db"
$outDir = Join-Path $root "bakeoff-runs"

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed scheduled task: $TaskName"
}

if (Get-ScheduledTask -TaskName $DaemonTaskName -ErrorAction SilentlyContinue) {
    Stop-ScheduledTask -TaskName $DaemonTaskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $DaemonTaskName -Confirm:$false
    Write-Host "Removed scheduled task: $DaemonTaskName"
}

if (Test-Path $db) {
    Remove-Item -LiteralPath $db -Force
    Write-Host "Removed $db"
}

if (Test-Path $outDir) {
    Remove-Item -LiteralPath $outDir -Recurse -Force
    Write-Host "Removed $outDir"
}
