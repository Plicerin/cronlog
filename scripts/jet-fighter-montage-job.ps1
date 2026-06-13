param(
    [Parameter(Mandatory = $true)]
    [string]$Scheduler,

    [Parameter(Mandatory = $true)]
    [string]$OutDir,

    [int]$Photos = 8,
    [double]$SecondsPerPhoto = 3.0,
    [int]$Fps = 30,
    [int]$Width = 1080,
    [int]$Height = 1920,
    [switch]$SimulateOffline,
    [switch]$SimulateMissingSources,
    [ValidateSet("", "after-search", "after-download", "before-render", "after-render")]
    [string]$SimulateCrashAt = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-JsonLine {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [hashtable]$Event
    )

    $Event | ConvertTo-Json -Compress | Add-Content -LiteralPath $Path -Encoding UTF8
}

function Quote-ProcessArgument {
    param([string]$Value)

    if ($Value -notmatch '[\s"]') {
        return $Value
    }
    return '"' + ($Value -replace '\\(?=\\*")', '$0$0' -replace '"', '\"') + '"'
}

$root = Split-Path -Parent $PSScriptRoot
$pipeline = Join-Path $root "examples\jet_fighter_montage.py"
$startedAt = Get-Date
$runId = "{0:yyyyMMdd-HHmmss-fff}-{1}" -f $startedAt, ([Guid]::NewGuid().ToString("N").Substring(0, 8))
$schedulerDir = Join-Path $OutDir $Scheduler
$runDir = Join-Path $schedulerDir $runId
$eventsPath = Join-Path $schedulerDir "events.jsonl"
$metricsPath = Join-Path $schedulerDir "metrics.jsonl"
$logPath = Join-Path $runDir "pipeline.log"
$stateDir = Join-Path $schedulerDir "state"

New-Item -ItemType Directory -Force -Path $runDir | Out-Null

Write-JsonLine -Path $eventsPath -Event @{
    event = "started"
    workload = "jet-montage"
    scheduler = $Scheduler
    run_id = $runId
    at = $startedAt.ToString("o")
    pid = $PID
}

$procBefore = Get-Process -Id $PID
$status = "success"
$errorMessage = $null

try {
    $python = (Get-Command python).Source
    $arguments = @(
        $pipeline,
        "--outdir", $runDir,
        "--photos", $Photos.ToString(),
        "--seconds-per-photo", $SecondsPerPhoto.ToString([Globalization.CultureInfo]::InvariantCulture),
        "--fps", $Fps.ToString(),
        "--width", $Width.ToString(),
        "--height", $Height.ToString(),
        "--state-dir", $stateDir
    ) | ForEach-Object { Quote-ProcessArgument $_ }
    if ($SimulateOffline) {
        $arguments += "--simulate-offline"
    }
    if ($SimulateMissingSources) {
        $arguments += "--simulate-missing-sources"
    }
    if ($SimulateCrashAt.Length -gt 0) {
        $arguments += "--simulate-crash-at"
        $arguments += Quote-ProcessArgument $SimulateCrashAt
    }

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $python
    $psi.Arguments = ($arguments -join " ")
    $psi.WorkingDirectory = $root
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true

    $child = New-Object System.Diagnostics.Process
    $child.StartInfo = $psi
    [void]$child.Start()
    $stdout = $child.StandardOutput.ReadToEnd()
    $stderr = $child.StandardError.ReadToEnd()
    $child.WaitForExit()
    $exitCode = $child.ExitCode

    $outputLines = @()
    if (![string]::IsNullOrWhiteSpace($stdout)) {
        $outputLines += $stdout.TrimEnd()
    }
    if (![string]::IsNullOrWhiteSpace($stderr)) {
        $outputLines += $stderr.TrimEnd()
    }

    $outputLines | Set-Content -LiteralPath $logPath -Encoding UTF8
    if ($exitCode -ne 0) {
        $details = ($outputLines -join [Environment]::NewLine).Trim()
        if ($details.Length -gt 0) {
            throw "jet_fighter_montage.py exited with code $exitCode`: $details"
        }
        throw "jet_fighter_montage.py exited with code $exitCode"
    }

    $manifestPath = Join-Path $runDir "montage-manifest.json"
    $videoPath = Join-Path $runDir "jet-fighter-montage.mp4"
    if (!(Test-Path $manifestPath)) {
        throw "montage-manifest.json was not created"
    }
    if (!(Test-Path $videoPath)) {
        throw "jet-fighter-montage.mp4 was not created"
    }

    $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    Write-JsonLine -Path $eventsPath -Event @{
        event = "artifact_summary"
        workload = "jet-montage"
        scheduler = $Scheduler
        run_id = $runId
        downloaded_photos = $manifest.downloaded_photos
        warnings = @($manifest.warnings).Count
        video_bytes = (Get-Item -LiteralPath $videoPath).Length
        duration_estimate_seconds = $manifest.duration_estimate_seconds
        video = $videoPath
        manifest = $manifestPath
        state = $stateDir
        at = (Get-Date).ToString("o")
    }
}
catch {
    $status = "failed"
    $errorMessage = $_.Exception.Message
    if ($_.ScriptStackTrace) {
        $errorMessage = "$errorMessage`n$($_.ScriptStackTrace)"
    }
    Write-Error $errorMessage
    exit 1
}
finally {
    $finishedAt = Get-Date
    $procAfter = Get-Process -Id $PID
    Write-JsonLine -Path $metricsPath -Event @{
        event = "step"
        workload = "jet-montage"
        scheduler = $Scheduler
        run_id = $runId
        step = "render-montage"
        status = $status
        duration_ms = [int](($finishedAt - $startedAt).TotalMilliseconds)
        working_set_mb = [math]::Round($procAfter.WorkingSet64 / 1MB, 2)
        private_mb = [math]::Round($procAfter.PrivateMemorySize64 / 1MB, 2)
        cpu_delta_seconds = [math]::Round(($procAfter.CPU - $procBefore.CPU), 3)
        error = $errorMessage
        at = $finishedAt.ToString("o")
    }
    Write-JsonLine -Path $eventsPath -Event @{
        event = if ($status -eq "success") { "finished" } else { "failed" }
        workload = "jet-montage"
        scheduler = $Scheduler
        run_id = $runId
        status = $status
        duration_ms = [int](($finishedAt - $startedAt).TotalMilliseconds)
        error = $errorMessage
        at = $finishedAt.ToString("o")
    }
}

Write-Output "Jet montage run $runId finished for $Scheduler."
Write-Output "Run directory: $runDir"
