param(
    [string]$PipelineRoot = "C:\Users\admin\Documents\historical-video-agent",
    [string]$ArticleTitle = "Statue of Liberty",
    [string]$EventText = "On June 17, 1885, the Statue of Liberty arrived in New York Harbor aboard the French ship Isere.",
    [int]$DurationSeconds = 240,
    [int]$IntervalSeconds = 120,
    [int]$TimeoutSeconds = 180,
    [switch]$NoDownload,
    [string]$JobName = "historical-production-cron2-local"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$exe = Join-Path $root "target\debug\cron2.exe"
$db = Join-Path $root "bakeoff-production-cron2.db"
$outDir = Join-Path $root "bakeoff-production-runs"
$jobScript = Join-Path $root "scripts\historical-production-job.ps1"
$powershellExe = (Get-Command powershell.exe).Source

function Stop-BakeoffJob {
    param($Job)

    if ($null -ne $Job) {
        Stop-Job $Job -ErrorAction SilentlyContinue
        Receive-Job $Job -ErrorAction SilentlyContinue | Out-Host
        Remove-Job $Job -Force -ErrorAction SilentlyContinue
    }
}

Push-Location $root
$cron2Job = $null
$baselineJob = $null

try {
    cargo build
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with code $LASTEXITCODE"
    }

    if (!(Test-Path $PipelineRoot)) {
        throw "pipeline root not found: $PipelineRoot"
    }
    if (Test-Path $db) {
        Remove-Item -LiteralPath $db -Force
    }
    if (Test-Path $outDir) {
        Remove-Item -LiteralPath $outDir -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null

    $jobArgs = @(
        "-NoProfile", "-ExecutionPolicy", "Bypass",
        "-File", $jobScript,
        "-PipelineRoot", $PipelineRoot,
        "-OutDir", $outDir,
        "-ArticleTitle", $ArticleTitle,
        "-EventText", $EventText
    )
    if ($NoDownload) {
        $jobArgs += "-NoDownload"
    }

    & $exe --db $db add --name $JobName --schedule "every $IntervalSeconds seconds" --timeout "${TimeoutSeconds}s" "--" $powershellExe @jobArgs -Scheduler cron2
    if ($LASTEXITCODE -ne 0) {
        throw "failed to add Cron2 production bakeoff job"
    }

    $cron2Job = Start-Job -Name "cron2-production-bakeoff-daemon" -ScriptBlock {
        param($Root, $Exe, $Db)
        Set-Location $Root
        & $Exe --db $Db daemon
    } -ArgumentList $root, $exe, $db

    $baselineJob = Start-Job -Name "baseline-production-bakeoff-loop" -ScriptBlock {
        param($Root, $PowerShellExe, $JobArgs, $IntervalSeconds, $EndAt)
        Set-Location $Root
        while ((Get-Date) -lt $EndAt) {
            Start-Sleep -Seconds $IntervalSeconds
            if ((Get-Date) -ge $EndAt) {
                break
            }
            & $PowerShellExe @JobArgs -Scheduler baseline-loop
        }
    } -ArgumentList $root, $powershellExe, $jobArgs, $IntervalSeconds, (Get-Date).AddSeconds($DurationSeconds)

    Write-Host "Bounded production bakeoff started."
    Write-Host "Duration: $DurationSeconds seconds"
    Write-Host "Interval: $IntervalSeconds seconds"
    Write-Host "PipelineRoot: $PipelineRoot"
    Write-Host "OutDir: $outDir"
    Write-Host "No Windows Scheduled Tasks are created."

    $samplePath = Join-Path $outDir "process-samples.jsonl"
    $deadline = (Get-Date).AddSeconds($DurationSeconds)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 10
        Get-Process cron2,python,powershell -ErrorAction SilentlyContinue | ForEach-Object {
            @{
                event = "process_sample"
                process = $_.ProcessName
                pid = $_.Id
                working_set_mb = [math]::Round($_.WorkingSet64 / 1MB, 2)
                private_mb = [math]::Round($_.PrivateMemorySize64 / 1MB, 2)
                cpu_seconds = [math]::Round($_.CPU, 3)
                at = (Get-Date).ToString("o")
            } | ConvertTo-Json -Compress | Add-Content -LiteralPath $samplePath -Encoding UTF8
        }
    }
}
finally {
    Stop-BakeoffJob $baselineJob
    Stop-BakeoffJob $cron2Job
    Get-Process cron2 -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "*cron2_mvp*" } | Stop-Process -Force -ErrorAction SilentlyContinue
    Pop-Location
}

& (Join-Path $PSScriptRoot "bakeoff-report.ps1") -OutDir $outDir -Cron2Db $db -Cron2Job $JobName -Workload production -Schedulers cron2,baseline-loop
