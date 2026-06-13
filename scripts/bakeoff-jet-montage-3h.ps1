param(
    [int]$Runs = 3,
    [int]$IntervalSeconds = 3600,
    [int]$TimeoutSeconds = 1800,
    [int]$Photos = 8,
    [double]$SecondsPerPhoto = 3.0,
    [int]$Fps = 30,
    [int]$Width = 1080,
    [int]$Height = 1920,
    [string]$JobName = "jet-montage-cron2-3h"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$exe = Join-Path $root "target\debug\cron2.exe"
$db = Join-Path $root "jet-montage-bakeoff-cron2.db"
$outDir = Join-Path $root "jet-montage-bakeoff-runs"
$jobScript = Join-Path $root "scripts\jet-fighter-montage-job.ps1"
$powershellExe = (Get-Command powershell.exe).Source

function Stop-BakeoffJob {
    param($Job)

    if ($null -ne $Job) {
        Stop-Job $Job -ErrorAction SilentlyContinue
        Receive-Job $Job -ErrorAction SilentlyContinue | Out-Host
        Remove-Job $Job -Force -ErrorAction SilentlyContinue
    }
}

function Count-FinishedRuns {
    param(
        [string]$Scheduler,
        [string]$OutDir
    )

    $eventsPath = Join-Path (Join-Path $OutDir $Scheduler) "events.jsonl"
    if (!(Test-Path $eventsPath)) {
        return 0
    }
    return @(
        Get-Content -LiteralPath $eventsPath |
            Where-Object { $_.Trim().Length -gt 0 } |
            ForEach-Object { $_ | ConvertFrom-Json } |
            Where-Object { $_.event -eq "finished" }
    ).Count
}

function Count-TerminalRuns {
    param(
        [string]$Scheduler,
        [string]$OutDir
    )

    $eventsPath = Join-Path (Join-Path $OutDir $Scheduler) "events.jsonl"
    if (!(Test-Path $eventsPath)) {
        return 0
    }
    return @(
        Get-Content -LiteralPath $eventsPath |
            Where-Object { $_.Trim().Length -gt 0 } |
            ForEach-Object { $_ | ConvertFrom-Json } |
            Where-Object { $_.event -eq "finished" -or $_.event -eq "failed" }
    ).Count
}

Push-Location $root
$cron2Job = $null
$baselineJob = $null
$initialCron2Job = $null
$initialBaselineJob = $null
$cron2Stopped = $false

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

    $commonArgs = @(
        "-NoProfile", "-ExecutionPolicy", "Bypass",
        "-File", $jobScript,
        "-OutDir", $outDir,
        "-Photos", $Photos,
        "-SecondsPerPhoto", $SecondsPerPhoto,
        "-Fps", $Fps,
        "-Width", $Width,
        "-Height", $Height
    )

    & $exe --db $db add --name $JobName --schedule "every $IntervalSeconds seconds" --timeout "${TimeoutSeconds}s" "--" $powershellExe @commonArgs -Scheduler cron2
    if ($LASTEXITCODE -ne 0) {
        throw "failed to add Cron2 montage job"
    }

    $initialCron2Job = Start-Job -Name "cron2-jet-montage-initial" -ScriptBlock {
        param($Root, $Exe, $Db, $JobName)
        Set-Location $Root
        & $Exe --db $Db run $JobName --now
    } -ArgumentList $root, $exe, $db, $JobName

    $initialBaselineJob = Start-Job -Name "baseline-jet-montage-initial" -ScriptBlock {
        param($Root, $PowerShellExe, $CommonArgs)
        Set-Location $Root
        & $PowerShellExe @CommonArgs -Scheduler baseline-loop
    } -ArgumentList $root, $powershellExe, $commonArgs

    Wait-Job $initialCron2Job | Out-Null
    Receive-Job $initialCron2Job -ErrorAction SilentlyContinue | Out-Host
    Remove-Job $initialCron2Job -Force -ErrorAction SilentlyContinue
    $initialCron2Job = $null

    $cron2Job = Start-Job -Name "cron2-jet-montage-3h-daemon" -ScriptBlock {
        param($Root, $Exe, $Db)
        Set-Location $Root
        & $Exe --db $Db daemon
    } -ArgumentList $root, $exe, $db

    $baselineJob = Start-Job -Name "baseline-jet-montage-3h-loop" -ScriptBlock {
        param($Root, $PowerShellExe, $CommonArgs, $IntervalSeconds, $TargetRuns, $OutDir)
        Set-Location $Root
        while ($true) {
            $eventsPath = Join-Path (Join-Path $OutDir "baseline-loop") "events.jsonl"
            $finished = 0
            if (Test-Path $eventsPath) {
                $finished = @(
                    Get-Content -LiteralPath $eventsPath |
                        Where-Object { $_.Trim().Length -gt 0 } |
                        ForEach-Object { $_ | ConvertFrom-Json } |
                        Where-Object { $_.event -eq "finished" }
                ).Count
            }
            if ($finished -ge $TargetRuns) {
                break
            }
            Start-Sleep -Seconds $IntervalSeconds
            & $PowerShellExe @CommonArgs -Scheduler baseline-loop
        }
    } -ArgumentList $root, $powershellExe, $commonArgs, $IntervalSeconds, $Runs, $outDir

    Write-Host "Jet montage bakeoff started."
    Write-Host "Target runs per side: $Runs"
    Write-Host "Interval: $IntervalSeconds seconds"
    Write-Host "OutDir: $outDir"
    Write-Host "No Windows Scheduled Tasks are created."

    $deadline = (Get-Date).AddSeconds(($Runs * ($IntervalSeconds + $TimeoutSeconds)) + 300)
    $pollSeconds = [Math]::Max(1, [Math]::Min(30, [Math]::Floor($IntervalSeconds / 4)))
    while ($true) {
        Start-Sleep -Seconds $pollSeconds
        $cron2Finished = Count-FinishedRuns -Scheduler "cron2" -OutDir $outDir
        $baselineFinished = Count-FinishedRuns -Scheduler "baseline-loop" -OutDir $outDir
        $cron2Terminal = Count-TerminalRuns -Scheduler "cron2" -OutDir $outDir
        $baselineTerminal = Count-TerminalRuns -Scheduler "baseline-loop" -OutDir $outDir
        Write-Host "Progress: cron2=$cron2Finished/$Runs baseline=$baselineFinished/$Runs"
        if (!$cron2Stopped -and $cron2Finished -ge $Runs) {
            Stop-BakeoffJob $cron2Job
            $cron2Job = $null
            $cron2Stopped = $true
        }
        if ($cron2Finished -ge $Runs -and $baselineFinished -ge $Runs) {
            break
        }
        if ((Get-Date) -gt $deadline) {
            throw "timed out waiting for successful runs: cron2 finished=$cron2Finished terminal=$cron2Terminal; baseline finished=$baselineFinished terminal=$baselineTerminal"
        }
    }
}
finally {
    Stop-BakeoffJob $initialBaselineJob
    Stop-BakeoffJob $initialCron2Job
    Stop-BakeoffJob $baselineJob
    Stop-BakeoffJob $cron2Job
    Get-Process cron2 -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "*cron2_mvp*" } | Stop-Process -Force -ErrorAction SilentlyContinue
    Pop-Location
}

& (Join-Path $PSScriptRoot "bakeoff-report.ps1") -OutDir $outDir -Cron2Db $db -Cron2Job $JobName -Workload jet-montage -Schedulers cron2,baseline-loop
