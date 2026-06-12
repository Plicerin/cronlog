param(
    [string]$OutDir = $(Join-Path (Split-Path -Parent $PSScriptRoot) "bakeoff-runs"),
    [string]$Cron2Db = $(Join-Path (Split-Path -Parent $PSScriptRoot) "bakeoff-cron2.db"),
    [string]$Cron2Job = "content-pipeline-cron2-live"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$exe = Join-Path $root "target\debug\cron2.exe"

function Read-JsonLines {
    param([string]$Path)

    if (!(Test-Path $Path)) {
        return @()
    }

    return Get-Content -LiteralPath $Path | Where-Object { $_.Trim().Length -gt 0 } | ForEach-Object {
        $_ | ConvertFrom-Json
    }
}

foreach ($scheduler in @("cron2", "task-scheduler")) {
    $dir = Join-Path $OutDir $scheduler
    $events = @(Read-JsonLines -Path (Join-Path $dir "events.jsonl"))
    $posts = @(Read-JsonLines -Path (Join-Path $dir "posted.jsonl"))
    $runDirs = @()
    if (Test-Path $dir) {
        $runDirs = @(Get-ChildItem -LiteralPath $dir -Directory)
    }

    $finished = @($events | Where-Object { $_.event -eq "finished" }).Count
    $failed = @($events | Where-Object { $_.event -eq "failed" }).Count
    $overlaps = @($events | Where-Object { $_.event -eq "overlap_detected" }).Count
    $postsCount = @($posts).Count

    Write-Host ""
    Write-Host "[$scheduler]"
    Write-Host "runs: $($runDirs.Count)"
    Write-Host "finished: $finished"
    Write-Host "failed: $failed"
    Write-Host "overlap_detected: $overlaps"
    Write-Host "dry_posts: $postsCount"
    if ($events.Count -gt 0) {
        Write-Host "last_event: $($events[-1] | ConvertTo-Json -Compress)"
    }
}

if ((Test-Path $exe) -and (Test-Path $Cron2Db)) {
    Write-Host ""
    Write-Host "[cron2 history]"
    & $exe --db $Cron2Db history $Cron2Job
}
