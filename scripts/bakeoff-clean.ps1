param(
    [string]$TaskName = "CronlogBakeoffContentPipeline",
    [string]$DaemonTaskName = "CronlogBakeoffDaemon",
    [string[]]$ExtraTaskNames = @("CronlogBakeoff-content-TaskScheduler", "CronlogBakeoff-complex-TaskScheduler")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$db = Join-Path $root "bakeoff-cronlog.db"
$outDir = Join-Path $root "bakeoff-runs"

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed scheduled task: $TaskName"
}

foreach ($extraTaskName in $ExtraTaskNames) {
    if (Get-ScheduledTask -TaskName $extraTaskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $extraTaskName -Confirm:$false
        Write-Host "Removed scheduled task: $extraTaskName"
    }
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
