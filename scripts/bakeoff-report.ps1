param(
    [string]$OutDir = $(Join-Path (Split-Path -Parent $PSScriptRoot) "bakeoff-runs"),
    [string]$CronlogDb = $(Join-Path (Split-Path -Parent $PSScriptRoot) "bakeoff-cronlog.db"),
    [string]$CronlogJob = "complex-pipeline-Cronlog-live",

    [ValidateSet("all", "content", "complex", "production", "jet-montage")]
    [string]$Workload = "all",

    [string[]]$Schedulers = @("cronlog", "task-scheduler")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$exe = Join-Path $root "target\debug\cronlog.exe"

function Read-JsonLines {
    param([string]$Path)

    if (!(Test-Path $Path)) {
        return @()
    }

    return Get-Content -LiteralPath $Path | Where-Object { $_.Trim().Length -gt 0 } | ForEach-Object {
        $_ | ConvertFrom-Json
    }
}

function Get-Workload {
    param($Item)

    $property = $Item.PSObject.Properties["workload"]
    if ($null -eq $property) {
        return "content"
    }
    return $property.Value
}

foreach ($scheduler in $Schedulers) {
    $dir = Join-Path $OutDir $scheduler
    $events = @(Read-JsonLines -Path (Join-Path $dir "events.jsonl"))
    $metrics = @(Read-JsonLines -Path (Join-Path $dir "metrics.jsonl"))
    $posts = @(Read-JsonLines -Path (Join-Path $dir "posted.jsonl"))

    if ($Workload -eq "complex") {
        $events = @($events | Where-Object { (Get-Workload $_) -eq "complex" })
        $metrics = @($metrics | Where-Object { (Get-Workload $_) -eq "complex" })
        $posts = @($posts | Where-Object { (Get-Workload $_) -eq "complex" })
    }
    elseif ($Workload -eq "production") {
        $events = @($events | Where-Object { (Get-Workload $_) -eq "historical-production" })
        $metrics = @($metrics | Where-Object { (Get-Workload $_) -eq "historical-production" })
        $posts = @()
    }
    elseif ($Workload -eq "jet-montage") {
        $events = @($events | Where-Object { (Get-Workload $_) -eq "jet-montage" })
        $metrics = @($metrics | Where-Object { (Get-Workload $_) -eq "jet-montage" })
        $posts = @()
    }
    elseif ($Workload -eq "content") {
        $events = @($events | Where-Object { (Get-Workload $_) -eq "content" })
        $metrics = @()
        $posts = @($posts | Where-Object { (Get-Workload $_) -eq "content" })
    }

    $runDirs = @()
    if (Test-Path $dir) {
        $runDirs = @(Get-ChildItem -LiteralPath $dir -Directory)
        if ($Workload -eq "complex") {
            $runDirs = @($runDirs | Where-Object { (Test-Path (Join-Path $_.FullName "manifest.json")) -or (Test-Path (Join-Path $_.FullName "manifest.jsonl")) })
        }
        elseif ($Workload -eq "production") {
            $runDirs = @($runDirs | Where-Object { Test-Path (Join-Path $_.FullName "media-manifest.json") })
        }
        elseif ($Workload -eq "jet-montage") {
            $runDirs = @($runDirs | Where-Object { Test-Path (Join-Path $_.FullName "montage-manifest.json") })
        }
        elseif ($Workload -eq "content") {
            $runDirs = @($runDirs | Where-Object { Test-Path (Join-Path $_.FullName "metadata.json") })
        }
    }

    $finished = @($events | Where-Object { $_.event -eq "finished" }).Count
    $failed = @($events | Where-Object { $_.event -eq "failed" }).Count
    $overlaps = @($events | Where-Object { $_.event -eq "overlap_detected" }).Count
    $postsCount = @($posts).Count
    $stepCount = @($metrics | Where-Object { $_.event -eq "step" }).Count
    $failedSteps = @($metrics | Where-Object { $_.event -eq "step" -and $_.status -eq "failed" }).Count
    $maxWorkingSet = @($metrics | Where-Object { $_.event -eq "step" } | ForEach-Object { $_.working_set_mb } | Measure-Object -Maximum).Maximum
    $avgStepMs = @($metrics | Where-Object { $_.event -eq "step" } | ForEach-Object { $_.duration_ms } | Measure-Object -Average).Average

    Write-Host ""
    Write-Host "[$scheduler]"
    Write-Host "runs: $($runDirs.Count)"
    Write-Host "finished: $finished"
    Write-Host "failed: $failed"
    Write-Host "overlap_detected: $overlaps"
    Write-Host "dry_posts: $postsCount"
    Write-Host "steps: $stepCount"
    Write-Host "failed_steps: $failedSteps"
    if ($null -ne $maxWorkingSet) {
        Write-Host "max_step_working_set_mb: $([math]::Round($maxWorkingSet, 2))"
    }
    if ($null -ne $avgStepMs) {
        Write-Host "avg_step_duration_ms: $([math]::Round($avgStepMs, 2))"
    }
    if ($events.Count -gt 0) {
        Write-Host "last_event: $($events[-1] | ConvertTo-Json -Compress)"
    }
}

if ((Test-Path $exe) -and (Test-Path $CronlogDb)) {
    Write-Host ""
    Write-Host "[Cronlog history]"
    & $exe --db $CronlogDb history $CronlogJob
}
