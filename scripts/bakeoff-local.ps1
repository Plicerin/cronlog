param(
    [ValidateSet("normal", "flaky", "hang", "fail", "large-output", "cpu", "memory", "mixed")]
    [string]$Mode = "mixed",

    [int]$DurationSeconds = 180,
    [int]$IntervalSeconds = 60,
    [int]$TimeoutSeconds = 90,
    [int]$Items = 12,
    [int]$MemoryMB = 128,
    [int]$FailurePercent = 20,
    [string]$JobName = "complex-pipeline-Cronlog-local"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$exe = Join-Path $root "target\debug\cronlog.exe"
$db = Join-Path $root "bakeoff-local-cronlog.db"
$outDir = Join-Path $root "bakeoff-local-runs"
$pipeline = Join-Path $root "examples\complex_pipeline.ps1"
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
$CronlogJob = $null
$baselineJob = $null

try {
    cargo build
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with code $LASTEXITCODE"
    }

    if (Test-Path $db) {
        Remove-Item -LiteralPath $db -Force
    }
    if (Test-Path $outDir) {
        Remove-Item -LiteralPath $outDir -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null

    & $exe --db $db add --name $JobName --schedule "every 1 minutes" --timeout "${TimeoutSeconds}s" "--" $powershellExe -NoProfile -ExecutionPolicy Bypass -File $pipeline -Scheduler cronlog -OutDir $outDir -Mode $Mode -Items $Items -MemoryMB $MemoryMB -FailurePercent $FailurePercent
    if ($LASTEXITCODE -ne 0) {
        throw "failed to add Cronlog bakeoff job"
    }

    $CronlogJob = Start-Job -Name "Cronlog-local-bakeoff-daemon" -ScriptBlock {
        param($Root, $Exe, $Db)
        Set-Location $Root
        & $Exe --db $Db daemon
    } -ArgumentList $root, $exe, $db

    $baselineJob = Start-Job -Name "baseline-local-bakeoff-loop" -ScriptBlock {
        param($Root, $PowerShellExe, $Pipeline, $OutDir, $Mode, $Items, $MemoryMB, $FailurePercent, $IntervalSeconds, $EndAt)
        Set-Location $Root
        while ((Get-Date) -lt $EndAt) {
            Start-Sleep -Seconds $IntervalSeconds
            if ((Get-Date) -ge $EndAt) {
                break
            }
            & $PowerShellExe -NoProfile -ExecutionPolicy Bypass -File $Pipeline -Scheduler baseline-loop -OutDir $OutDir -Mode $Mode -Items $Items -MemoryMB $MemoryMB -FailurePercent $FailurePercent
        }
    } -ArgumentList $root, $powershellExe, $pipeline, $outDir, $Mode, $Items, $MemoryMB, $FailurePercent, $IntervalSeconds, (Get-Date).AddSeconds($DurationSeconds)

    Write-Host "Bounded local bakeoff started."
    Write-Host "Duration: $DurationSeconds seconds"
    Write-Host "Mode: $Mode"
    Write-Host "OutDir: $outDir"
    Write-Host "No Windows Scheduled Tasks are created."

    $samplePath = Join-Path $outDir "process-samples.jsonl"
    $deadline = (Get-Date).AddSeconds($DurationSeconds)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 10
        Get-Process Cronlog -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "*Cronlog_mvp*" } | ForEach-Object {
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
    Stop-BakeoffJob $CronlogJob
    Get-Process Cronlog -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "*Cronlog_mvp*" } | Stop-Process -Force -ErrorAction SilentlyContinue
    Pop-Location
}

& (Join-Path $PSScriptRoot "bakeoff-report.ps1") -OutDir $outDir -CronlogDb $db -CronlogJob $JobName -Workload complex -Schedulers Cronlog,baseline-loop
