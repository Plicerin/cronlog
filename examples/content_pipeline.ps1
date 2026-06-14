param(
    [Parameter(Mandatory = $true)]
    [string]$Scheduler,

    [Parameter(Mandatory = $true)]
    [string]$OutDir,

    [ValidateSet("normal", "flaky", "hang", "fail", "large-output")]
    [string]$Mode = "normal",

    [int]$MinDelaySeconds = 1,
    [int]$MaxDelaySeconds = 4,
    [int]$FailurePercent = 25,
    [int]$HangSeconds = 90
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

function Get-Hash {
    param([Parameter(Mandatory = $true)][string]$Text)

    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        return (($sha.ComputeHash($bytes) | ForEach-Object { $_.ToString("x2") }) -join "")
    }
    finally {
        $sha.Dispose()
    }
}

$startedAt = Get-Date
$runId = "{0:yyyyMMdd-HHmmss-fff}-{1}" -f $startedAt, ([Guid]::NewGuid().ToString("N").Substring(0, 8))
$schedulerDir = Join-Path $OutDir $Scheduler
$runDir = Join-Path $schedulerDir $runId
$eventsPath = Join-Path $schedulerDir "events.jsonl"
$postsPath = Join-Path $schedulerDir "posted.jsonl"
$lockPath = Join-Path $schedulerDir "pipeline.lock"
$lockAcquired = $false

New-Item -ItemType Directory -Force -Path $runDir | Out-Null

$lockStream = $null
try {
    $lockStream = [System.IO.File]::Open($lockPath, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
    $lockAcquired = $true
}
catch {
    $event = @{
        event = "overlap_detected"
        scheduler = $Scheduler
        run_id = $runId
        mode = $Mode
        at = (Get-Date).ToString("o")
        error = $_.Exception.Message
    }
    Write-JsonLine -Path $eventsPath -Event $event
    Write-Error "Another $Scheduler pipeline run is still active."
    exit 75
}

try {
    Write-JsonLine -Path $eventsPath -Event @{
        event = "started"
        scheduler = $Scheduler
        run_id = $runId
        mode = $Mode
        at = $startedAt.ToString("o")
        pid = $PID
    }

    $delay = Get-Random -Minimum $MinDelaySeconds -Maximum ($MaxDelaySeconds + 1)
    Start-Sleep -Seconds $delay

    if ($Mode -eq "hang") {
        Write-JsonLine -Path $eventsPath -Event @{
            event = "simulated_hang"
            scheduler = $Scheduler
            run_id = $runId
            at = (Get-Date).ToString("o")
            hang_seconds = $HangSeconds
        }
        Start-Sleep -Seconds $HangSeconds
    }

    if ($Mode -eq "fail") {
        throw "Simulated hard failure before content generation."
    }

    if ($Mode -eq "flaky") {
        $roll = Get-Random -Minimum 1 -Maximum 101
        if ($roll -le $FailurePercent) {
            throw "Simulated upstream API failure. roll=$roll threshold=$FailurePercent"
        }
    }

    $topics = @(
        "SQLite-backed local schedulers",
        "durable run history",
        "captured logs for batch jobs",
        "timeout handling for automation",
        "overlap protection in local workflows"
    )
    $topic = $topics[(Get-Random -Minimum 0 -Maximum $topics.Count)]
    $draft = @"
Cronlog field note: $topic.

When an automation fails, the most useful feature is being able to answer what ran, when it ran, and what it printed.

#LocalAutomation #Schedulers #Cronlog
"@

    $payloadHash = Get-Hash -Text $draft
    $draftPath = Join-Path $runDir "draft.txt"
    $metadataPath = Join-Path $runDir "metadata.json"

    $draft | Set-Content -LiteralPath $draftPath -Encoding UTF8

    $metadata = @{
        scheduler = $Scheduler
        run_id = $runId
        mode = $Mode
        topic = $topic
        payload_hash = $payloadHash
        draft_path = $draftPath
        generated_at = (Get-Date).ToString("o")
        simulated_platforms = @("x", "linkedin", "mastodon")
    }
    $metadata | ConvertTo-Json | Set-Content -LiteralPath $metadataPath -Encoding UTF8

    if ($Mode -eq "large-output") {
        for ($i = 1; $i -le 2000; $i++) {
            Write-Output ("large-output-line {0:0000} {1}" -f $i, $payloadHash)
        }
    }

    foreach ($platform in $metadata.simulated_platforms) {
        Write-JsonLine -Path $postsPath -Event @{
            event = "posted"
            scheduler = $Scheduler
            run_id = $runId
            platform = $platform
            payload_hash = $payloadHash
            at = (Get-Date).ToString("o")
            dry_run = $true
        }
    }

    $finishedAt = Get-Date
    Write-JsonLine -Path $eventsPath -Event @{
        event = "finished"
        scheduler = $Scheduler
        run_id = $runId
        mode = $Mode
        at = $finishedAt.ToString("o")
        duration_ms = [int](($finishedAt - $startedAt).TotalMilliseconds)
        status = "success"
    }

    Write-Output "Generated and dry-posted content run $runId for $Scheduler."
    Write-Output "Draft: $draftPath"
    Write-Output "Metadata: $metadataPath"
}
catch {
    Write-JsonLine -Path $eventsPath -Event @{
        event = "failed"
        scheduler = $Scheduler
        run_id = $runId
        mode = $Mode
        at = (Get-Date).ToString("o")
        error = $_.Exception.Message
    }
    Write-Error $_.Exception.Message
    exit 1
}
finally {
    if ($lockStream -ne $null) {
        $lockStream.Dispose()
    }
    if ($lockAcquired -and (Test-Path $lockPath)) {
        Remove-Item -LiteralPath $lockPath -Force
    }
}
