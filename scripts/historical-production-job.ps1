param(
    [Parameter(Mandatory = $true)]
    [string]$Scheduler,

    [Parameter(Mandatory = $true)]
    [string]$PipelineRoot,

    [Parameter(Mandatory = $true)]
    [string]$OutDir,

    [string]$ArticleTitle = "Statue of Liberty",
    [string]$EventText = "On June 17, 1885, the Statue of Liberty arrived in New York Harbor aboard the French ship Isere.",
    [switch]$NoDownload
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

$startedAt = Get-Date
$runId = "{0:yyyyMMdd-HHmmss-fff}-{1}" -f $startedAt, ([Guid]::NewGuid().ToString("N").Substring(0, 8))
$schedulerDir = Join-Path $OutDir $Scheduler
$runDir = Join-Path $schedulerDir $runId
$eventsPath = Join-Path $schedulerDir "events.jsonl"
$metricsPath = Join-Path $schedulerDir "metrics.jsonl"
$logPath = Join-Path $runDir "pipeline.log"
$mediaPackScript = Join-Path $PipelineRoot "scripts\wikimedia_media_pack.py"

New-Item -ItemType Directory -Force -Path $runDir | Out-Null

if (!(Test-Path $mediaPackScript)) {
    throw "missing media pack script: $mediaPackScript"
}

Write-JsonLine -Path $eventsPath -Event @{
    event = "started"
    scheduler = $Scheduler
    workload = "historical-production"
    run_id = $runId
    article_title = $ArticleTitle
    at = $startedAt.ToString("o")
    pid = $PID
}

$args = @(
    $mediaPackScript,
    "--article-title", $ArticleTitle,
    "--event-text", $EventText,
    "--outdir", $runDir
)
if ($NoDownload) {
    $args += "--no-download"
}

$procBefore = Get-Process -Id $PID
$stepStartedAt = Get-Date
$status = "success"
$errorMessage = $null

try {
    Write-JsonLine -Path $eventsPath -Event @{
        event = "step_started"
        scheduler = $Scheduler
        workload = "historical-production"
        run_id = $runId
        step = "wikimedia-media-pack"
        at = $stepStartedAt.ToString("o")
    }

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = & python @args 2>&1
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousErrorActionPreference
    $output | ForEach-Object { $_.ToString() } | Set-Content -LiteralPath $logPath -Encoding UTF8
    if ($exitCode -ne 0) {
        throw "wikimedia_media_pack.py exited with code $exitCode"
    }

    $manifestPath = Join-Path $runDir "media-manifest.json"
    if (!(Test-Path $manifestPath)) {
        throw "media-manifest.json was not created"
    }

    $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    $downloads = $manifest.PSObject.Properties["downloads"]
    $warningCount = 0
    if ($downloads -and $downloads.Value.PSObject.Properties["warnings"]) {
        $warningCount = @($downloads.Value.warnings).Count
    }

    $mediaFiles = @(Get-ChildItem -LiteralPath (Join-Path $runDir "media") -File -ErrorAction SilentlyContinue)
    Write-JsonLine -Path $eventsPath -Event @{
        event = "artifact_summary"
        scheduler = $Scheduler
        workload = "historical-production"
        run_id = $runId
        candidate_count = $manifest.candidate_count
        media_files = $mediaFiles.Count
        warnings = $warningCount
        manifest = $manifestPath
        at = (Get-Date).ToString("o")
    }
}
catch {
    $status = "failed"
    $errorMessage = $_.Exception.Message
    Write-Error $errorMessage
    exit 1
}
finally {
    $finishedAt = Get-Date
    $procAfter = Get-Process -Id $PID
    Write-JsonLine -Path $metricsPath -Event @{
        event = "step"
        scheduler = $Scheduler
        workload = "historical-production"
        run_id = $runId
        step = "wikimedia-media-pack"
        status = $status
        duration_ms = [int](($finishedAt - $stepStartedAt).TotalMilliseconds)
        working_set_mb = [math]::Round($procAfter.WorkingSet64 / 1MB, 2)
        private_mb = [math]::Round($procAfter.PrivateMemorySize64 / 1MB, 2)
        cpu_delta_seconds = [math]::Round(($procAfter.CPU - $procBefore.CPU), 3)
        error = $errorMessage
        at = $finishedAt.ToString("o")
    }
    Write-JsonLine -Path $eventsPath -Event @{
        event = if ($status -eq "success") { "finished" } else { "failed" }
        scheduler = $Scheduler
        workload = "historical-production"
        run_id = $runId
        status = $status
        duration_ms = [int](($finishedAt - $startedAt).TotalMilliseconds)
        error = $errorMessage
        at = $finishedAt.ToString("o")
    }
}

Write-Output "Historical production job $runId finished for $Scheduler."
Write-Output "Run directory: $runDir"
