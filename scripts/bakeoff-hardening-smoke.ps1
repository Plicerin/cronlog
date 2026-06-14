param(
    [int]$Photos = 3,
    [double]$SecondsPerPhoto = 1.0,
    [int]$Fps = 12,
    [int]$Width = 540,
    [int]$Height = 960,
    [int]$TimeoutSeconds = 300
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$exe = Join-Path $root "target\debug\cronlog.exe"
$db = Join-Path $root "jet-montage-hardening-bakeoff-cronlog.db"
$outDir = Join-Path $root "jet-montage-hardening-bakeoff-runs"
$jobScript = Join-Path $root "scripts\jet-fighter-montage-job.ps1"
$powershellExe = (Get-Command powershell.exe).Source
$jobNamePrefix = "hardening-smoke"
$script:ScenarioIndex = 0
$script:CronlogJobs = @()

function Remove-PathIfExists {
    param([string]$Path)
    if (Test-Path $Path) {
        Remove-Item -LiteralPath $Path -Recurse -Force
    }
}

function Base-Args {
    param([string]$Scheduler)
    return @(
        "-NoProfile", "-ExecutionPolicy", "Bypass",
        "-File", $jobScript,
        "-Scheduler", $Scheduler,
        "-OutDir", $outDir,
        "-Photos", $Photos.ToString(),
        "-SecondsPerPhoto", $SecondsPerPhoto.ToString([Globalization.CultureInfo]::InvariantCulture),
        "-Fps", $Fps.ToString(),
        "-Width", $Width.ToString(),
        "-Height", $Height.ToString()
    )
}

function Run-CronlogScenario {
    param(
        [string]$Name,
        [string[]]$ExtraArgs = @()
    )

    $scheduler = "Cronlog-$Name"
    $args = (Base-Args -Scheduler $scheduler) + $ExtraArgs
    $script:ScenarioIndex += 1
    $jobName = "$jobNamePrefix-$($script:ScenarioIndex)-$Name"
    $script:CronlogJobs += $jobName

    & $exe --db $db add --name $jobName --schedule "every 1 hour" --timeout "${TimeoutSeconds}s" "--" $powershellExe @args | Out-Host
    & $exe --db $db run $jobName --now | Out-Host

    $event = Last-Event -Scheduler $scheduler
    [pscustomobject]@{
        scheduler = "cronlog"
        job = $jobName
        scenario = $Name
        status = $event.status
        event = $event.event
        duration_ms = $event.duration_ms
        error = $event.error
        run_id = $event.run_id
    }
}

function Run-BaselineScenario {
    param(
        [string]$Name,
        [string[]]$ExtraArgs = @()
    )

    $scheduler = "baseline-$Name"
    $args = (Base-Args -Scheduler $scheduler) + $ExtraArgs
    & $powershellExe @args
    $event = Last-Event -Scheduler $scheduler
    [pscustomobject]@{
        scheduler = "baseline-loop"
        job = ""
        scenario = $Name
        status = $event.status
        event = $event.event
        duration_ms = $event.duration_ms
        error = $event.error
        run_id = $event.run_id
    }
}

function Run-BaselineScenarioAllowFailure {
    param(
        [string]$Name,
        [string[]]$ExtraArgs = @()
    )

    try {
        Run-BaselineScenario -Name $Name -ExtraArgs $ExtraArgs
    }
    catch {
        $event = Last-Event -Scheduler "baseline-$Name"
        [pscustomobject]@{
            scheduler = "baseline-loop"
            job = ""
            scenario = $Name
            status = $event.status
            event = $event.event
            duration_ms = $event.duration_ms
            error = $event.error
            run_id = $event.run_id
        }
    }
}

function Last-Event {
    param([string]$Scheduler)

    $eventsPath = Join-Path (Join-Path $outDir $Scheduler) "events.jsonl"
    if (!(Test-Path $eventsPath)) {
        throw "missing events for $Scheduler"
    }

    $events = @(
        Get-Content -LiteralPath $eventsPath |
            Where-Object { $_.Trim().Length -gt 0 } |
            ForEach-Object { $_ | ConvertFrom-Json } |
            Where-Object { $_.event -eq "finished" -or $_.event -eq "failed" }
    )
    if ($events.Count -eq 0) {
        throw "no terminal events for $Scheduler"
    }
    return $events[-1]
}

Push-Location $root
try {
    cargo build
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with code $LASTEXITCODE"
    }

    Remove-PathIfExists -Path $db
    Remove-PathIfExists -Path $outDir
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null

    $results = @()

    Write-Host "`n== Warm cache =="
    $results += Run-CronlogScenario -Name "warm-cache"
    $results += Run-BaselineScenario -Name "warm-cache"

    Write-Host "`n== Connectivity interrupted, cache available =="
    $results += Run-CronlogScenario -Name "warm-cache" -ExtraArgs @("-SimulateOffline")
    $results += Run-BaselineScenario -Name "warm-cache" -ExtraArgs @("-SimulateOffline")

    Write-Host "`n== Connectivity interrupted, no cache =="
    $results += Run-CronlogScenario -Name "offline-no-cache" -ExtraArgs @("-SimulateOffline")
    $results += Run-BaselineScenarioAllowFailure -Name "offline-no-cache" -ExtraArgs @("-SimulateOffline")

    Write-Host "`n== Missing sources, no cache =="
    $results += Run-CronlogScenario -Name "missing-sources" -ExtraArgs @("-SimulateMissingSources")
    $results += Run-BaselineScenarioAllowFailure -Name "missing-sources" -ExtraArgs @("-SimulateMissingSources")

    Write-Host "`n== Crash after render =="
    $results += Run-CronlogScenario -Name "crash-resume" -ExtraArgs @("-SimulateCrashAt", "after-render")
    $results += Run-BaselineScenarioAllowFailure -Name "crash-resume" -ExtraArgs @("-SimulateCrashAt", "after-render")

    Write-Host "`n== Resume after crash =="
    $results += Run-CronlogScenario -Name "crash-resume"
    $results += Run-BaselineScenario -Name "crash-resume"

    Write-Host "`n== Scenario summary =="
    $results | Format-Table -AutoSize | Out-Host

    Write-Host "`n== Cronlog history =="
    foreach ($CronlogJob in $script:CronlogJobs) {
        Write-Host "`n[$CronlogJob]"
        & $exe --db $db history $CronlogJob --limit 5 | Out-Host
    }

    Write-Host "`n== State summaries =="
    Get-ChildItem -LiteralPath $outDir -Directory |
        Where-Object { Test-Path (Join-Path $_.FullName "state\state.json") } |
        ForEach-Object {
            $state = Get-Content -LiteralPath (Join-Path $_.FullName "state\state.json") -Raw | ConvertFrom-Json
            $failureClass = $null
            if ($state.PSObject.Properties.Name -contains "failure_class") {
                $failureClass = $state.failure_class
            }
            $CronlogRunId = $null
            $previousStatus = $null
            if ($state.PSObject.Properties.Name -contains "cronlog") {
                $CronlogPropertyNames = @($state.cronlog.PSObject.Properties | ForEach-Object { $_.Name })
                if ($CronlogPropertyNames -contains "CRONLOG_RUN_ID") {
                    $CronlogRunId = $state.cronlog.CRONLOG_RUN_ID
                }
                if ($CronlogPropertyNames -contains "CRONLOG_PREVIOUS_STATUS") {
                    $previousStatus = $state.cronlog.CRONLOG_PREVIOUS_STATUS
                }
            }
            [pscustomobject]@{
                scheduler = $_.Name
                status = $state.status
                current_stage = $state.current_stage
                failure_class = $failureClass
                recovery_mode = $state.recovery_mode
                Cronlog_run_id = $CronlogRunId
                previous_status = $previousStatus
            }
        } | Format-Table -AutoSize | Out-Host

    Write-Host "`nNo Windows Scheduled Tasks were created."
}
finally {
    Get-Process Cronlog -ErrorAction SilentlyContinue |
        Where-Object { $_.Path -like "*Cronlog_mvp*" } |
        Stop-Process -Force -ErrorAction SilentlyContinue
    Pop-Location
}
