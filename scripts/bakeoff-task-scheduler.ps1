param(
    [ValidateSet("normal", "flaky", "hang", "fail", "large-output")]
    [string]$Mode = "flaky",

    [int]$IntervalMinutes = 1,

    [string]$TaskName = "Cron2BakeoffContentPipeline"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$outDir = Join-Path $root "bakeoff-runs"
$pipeline = Join-Path $root "examples\content_pipeline.ps1"
$powershellExe = (Get-Command powershell.exe).Source

New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$argument = "-NoProfile -ExecutionPolicy Bypass -File `"$pipeline`" -Scheduler task-scheduler -OutDir `"$outDir`" -Mode $Mode"
$action = New-ScheduledTaskAction -Execute $powershellExe -Argument $argument -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes)
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances Parallel

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null

Write-Host "Task Scheduler bakeoff task registered: $TaskName"
Write-Host "It writes artifacts under: $outDir\task-scheduler"
Write-Host ""
Write-Host "Useful commands:"
Write-Host "  Get-ScheduledTask -TaskName $TaskName"
Write-Host "  Get-ScheduledTaskInfo -TaskName $TaskName"
Write-Host "  Unregister-ScheduledTask -TaskName $TaskName -Confirm:`$false"
