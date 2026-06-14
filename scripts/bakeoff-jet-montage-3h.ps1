param(
    [int]$Runs = 3,
    [int]$IntervalSeconds = 3600,
    [int]$TimeoutSeconds = 1800,
    [int]$Photos = 8,
    [double]$SecondsPerPhoto = 3.0,
    [int]$Fps = 30,
    [int]$Width = 1080,
    [int]$Height = 1920,
    [string]$JobName = "jet-montage-Cronlog-3h"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$exe = Join-Path $root "target\debug\cronlog.exe"
$db = Join-Path $root "jet-montage-bakeoff-cronlog.db"
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
$CronlogJob = $null
$baselineJob = $null
$initialCronlogJob = $null
$initialBaselineJob = $null
$CronlogStopped = $false

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

    & $exe --db $db add --name $JobName --schedule "every $IntervalSeconds seconds" --timeout "${TimeoutSeconds}s" "--" $powershellExe @commonArgs -Scheduler cronlog
    if ($LASTEXITCODE -ne 0) {
        throw "failed to add Cronlog montage job"
    }

    $initialCronlogJob = Start-Job -Name "Cronlog-jet-montage-initial" -ScriptBlock {
        param($Root, $Exe, $Db, $JobName)
        Set-Location $Root
        & $Exe --db $Db run $JobName --now
    } -ArgumentList $root, $exe, $db, $JobName

    $initialBaselineJob = Start-Job -Name "baseline-jet-montage-initial" -ScriptBlock {
        param($Root, $PowerShellExe, $CommonArgs)
        Set-Location $Root
        & $PowerShellExe @CommonArgs -Scheduler baseline-loop
    } -ArgumentList $root, $powershellExe, $commonArgs

    Wait-Job $initialCronlogJob | Out-Null
    Receive-Job $initialCronlogJob -ErrorAction SilentlyContinue | Out-Host
    Remove-Job $initialCronlogJob -Force -ErrorAction SilentlyContinue
    $initialCronlogJob = $null

    $CronlogJob = Start-Job -Name "Cronlog-jet-montage-3h-daemon" -ScriptBlock {
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
        $CronlogFinished = Count-FinishedRuns -Scheduler "cronlog" -OutDir $outDir
        $baselineFinished = Count-FinishedRuns -Scheduler "baseline-loop" -OutDir $outDir
        $CronlogTerminal = Count-TerminalRuns -Scheduler "cronlog" -OutDir $outDir
        $baselineTerminal = Count-TerminalRuns -Scheduler "baseline-loop" -OutDir $outDir
        Write-Host "Progress: Cronlog=$CronlogFinished/$Runs baseline=$baselineFinished/$Runs"
        if (!$CronlogStopped -and $CronlogFinished -ge $Runs) {
            Stop-BakeoffJob $CronlogJob
            $CronlogJob = $null
            $CronlogStopped = $true
        }
        if ($CronlogFinished -ge $Runs -and $baselineFinished -ge $Runs) {
            break
        }
        if ((Get-Date) -gt $deadline) {
            throw "timed out waiting for successful runs: Cronlog finished=$CronlogFinished terminal=$CronlogTerminal; baseline finished=$baselineFinished terminal=$baselineTerminal"
        }
    }
}
finally {
    Stop-BakeoffJob $initialBaselineJob
    Stop-BakeoffJob $initialCronlogJob
    Stop-BakeoffJob $baselineJob
    Stop-BakeoffJob $CronlogJob
    Get-Process Cronlog -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "*Cronlog_mvp*" } | Stop-Process -Force -ErrorAction SilentlyContinue
    Pop-Location
}

& (Join-Path $PSScriptRoot "bakeoff-report.ps1") -OutDir $outDir -CronlogDb $db -CronlogJob $JobName -Workload jet-montage -Schedulers Cronlog,baseline-loop
