param(
    [string]$TaskName = "CronlogBakeoffDaemon",
    [string]$DbPath = $(Join-Path (Split-Path -Parent $PSScriptRoot) "bakeoff-cronlog.db")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$exe = Join-Path $root "target\debug\cronlog.exe"

if (!(Test-Path $exe)) {
    cargo build
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with code $LASTEXITCODE"
    }
}

$action = New-ScheduledTaskAction -Execute $exe -Argument "--db `"$DbPath`" daemon" -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddYears(10)
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName

Write-Host "Cronlog daemon task started: $TaskName"
Write-Host "Inspect with:"
Write-Host "  Get-ScheduledTaskInfo -TaskName $TaskName"
Write-Host "Stop with:"
Write-Host "  Stop-ScheduledTask -TaskName $TaskName"
