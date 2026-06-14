param(
    [Parameter(Mandatory = $true)]
    [string]$Scheduler,

    [Parameter(Mandatory = $true)]
    [string]$OutDir,

    [ValidateSet("normal", "flaky", "hang", "fail", "large-output", "cpu", "memory", "mixed")]
    [string]$Mode = "mixed",

    [int]$FailurePercent = 20,
    [int]$HangSeconds = 120,
    [int]$Items = 12,
    [int]$Retries = 2,
    [int]$MemoryMB = 128
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

function Invoke-Step {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,

        [Parameter(Mandatory = $true)]
        [scriptblock]$Body
    )

    $startedAt = Get-Date
    $procBefore = Get-Process -Id $PID
    $status = "success"
    $errorMessage = $null

    try {
        & $Body
    }
    catch {
        $status = "failed"
        $errorMessage = $_.Exception.Message
        throw
    }
    finally {
        $finishedAt = Get-Date
        $procAfter = Get-Process -Id $PID
        Write-JsonLine -Path $metricsPath -Event @{
            event = "step"
            scheduler = $Scheduler
            run_id = $runId
            workload = "complex"
            step = $Name
            status = $status
            duration_ms = [int](($finishedAt - $startedAt).TotalMilliseconds)
            working_set_mb = [math]::Round($procAfter.WorkingSet64 / 1MB, 2)
            private_mb = [math]::Round($procAfter.PrivateMemorySize64 / 1MB, 2)
            cpu_delta_seconds = [math]::Round(($procAfter.CPU - $procBefore.CPU), 3)
            error = $errorMessage
            at = $finishedAt.ToString("o")
        }
    }
}

function Invoke-WithRetry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,

        [Parameter(Mandatory = $true)]
        [scriptblock]$Body
    )

    $attempt = 0
    while ($true) {
        $attempt += 1
        try {
            Write-JsonLine -Path $eventsPath -Event @{
                event = "attempt"
                scheduler = $Scheduler
                run_id = $runId
                step = $Name
                attempt = $attempt
                at = (Get-Date).ToString("o")
            }
            Invoke-Step -Name "$Name-attempt-$attempt" -Body $Body
            return
        }
        catch {
            if ($attempt -gt $Retries) {
                throw
            }
            Start-Sleep -Milliseconds (250 * $attempt)
        }
    }
}

$startedAt = Get-Date
$runId = "{0:yyyyMMdd-HHmmss-fff}-{1}" -f $startedAt, ([Guid]::NewGuid().ToString("N").Substring(0, 8))
$schedulerDir = Join-Path $OutDir $Scheduler
$runDir = Join-Path $schedulerDir $runId
$eventsPath = Join-Path $schedulerDir "events.jsonl"
$metricsPath = Join-Path $schedulerDir "metrics.jsonl"
$postsPath = Join-Path $schedulerDir "posted.jsonl"
$lockPath = Join-Path $schedulerDir "complex.lock"
$manifestPath = Join-Path $runDir "manifest.jsonl"
$lockAcquired = $false
$memoryHold = $null

New-Item -ItemType Directory -Force -Path $runDir | Out-Null

$lockStream = $null
try {
    $lockStream = [System.IO.File]::Open($lockPath, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
    $lockAcquired = $true
}
catch {
    Write-JsonLine -Path $eventsPath -Event @{
        event = "overlap_detected"
        scheduler = $Scheduler
        run_id = $runId
        workload = "complex"
        mode = $Mode
        at = (Get-Date).ToString("o")
        error = $_.Exception.Message
    }
    Write-Error "Another $Scheduler complex pipeline run is still active."
    exit 75
}

try {
    Write-JsonLine -Path $eventsPath -Event @{
        event = "started"
        scheduler = $Scheduler
        run_id = $runId
        workload = "complex"
        mode = $Mode
        items = $Items
        retries = $Retries
        at = $startedAt.ToString("o")
        pid = $PID
    }

    Invoke-Step -Name "ingest" -Body {
        $source = 1..$Items | ForEach-Object {
            @{
                id = $_
                topic = "automation-topic-$_"
                priority = Get-Random -Minimum 1 -Maximum 6
                seed = [Guid]::NewGuid().ToString("N")
            }
        }
        $source | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $runDir "source.json") -Encoding UTF8
        Start-Sleep -Milliseconds 300
    }

    Invoke-WithRetry -Name "draft-fanout" -Body {
        if ($Mode -in @("flaky", "mixed")) {
            $roll = Get-Random -Minimum 1 -Maximum 101
            if ($roll -le $FailurePercent) {
                throw "Simulated model provider failure. roll=$roll threshold=$FailurePercent"
            }
        }

        $draftDir = Join-Path $runDir "drafts"
        New-Item -ItemType Directory -Force -Path $draftDir | Out-Null
        for ($i = 1; $i -le $Items; $i++) {
            $draft = @"
Post $i from $Scheduler.

This is a generated artifact for a complex local scheduler bakeoff.
It carries a stable payload hash and can be retried idempotently.
"@
            $path = Join-Path $draftDir ("draft-{0:000}.txt" -f $i)
            $draft | Set-Content -LiteralPath $path -Encoding UTF8
        }
    }

    Invoke-Step -Name "quality-gate" -Body {
        $drafts = @(Get-ChildItem -LiteralPath (Join-Path $runDir "drafts") -Filter "*.txt")
        if ($drafts.Count -ne $Items) {
            throw "Expected $Items drafts, found $($drafts.Count)."
        }
        foreach ($draft in $drafts) {
            $text = Get-Content -LiteralPath $draft.FullName -Raw
            if ($text.Length -lt 80) {
                throw "Draft too short: $($draft.Name)"
            }
        }
    }

    if ($Mode -in @("cpu", "mixed")) {
        Invoke-Step -Name "cpu-transform" -Body {
            $value = 0
            for ($i = 0; $i -lt 600000; $i++) {
                $value = ($value + (($i * 31) % 9973)) % 1000003
            }
            Set-Content -LiteralPath (Join-Path $runDir "cpu-result.txt") -Value $value -Encoding UTF8
        }
    }

    if ($Mode -in @("memory", "mixed")) {
        Invoke-Step -Name "memory-transform" -Body {
            $bytes = [Math]::Max(1, $MemoryMB) * 1MB
            $script:memoryHold = New-Object byte[] $bytes
            for ($i = 0; $i -lt $script:memoryHold.Length; $i += 4096) {
                $script:memoryHold[$i] = [byte]($i % 251)
            }
            Start-Sleep -Milliseconds 500
        }
    }

    if ($Mode -eq "hang") {
        Invoke-Step -Name "simulated-hang" -Body {
            Start-Sleep -Seconds $HangSeconds
        }
    }

    if ($Mode -eq "fail") {
        Invoke-Step -Name "simulated-hard-fail" -Body {
            throw "Simulated hard failure after draft generation."
        }
    }

    Invoke-Step -Name "package" -Body {
        $drafts = @(Get-ChildItem -LiteralPath (Join-Path $runDir "drafts") -Filter "*.txt")
        if (Test-Path $manifestPath) {
            Remove-Item -LiteralPath $manifestPath -Force
        }
        foreach ($draft in $drafts) {
            $text = Get-Content -LiteralPath $draft.FullName -Raw
            Write-JsonLine -Path $manifestPath -Event @{
                name = $draft.Name
                bytes = $draft.Length
                payload_hash = Get-Hash -Text $text
            }
        }
    }

    Invoke-WithRetry -Name "dry-post" -Body {
        if ($Mode -in @("flaky", "mixed")) {
            $roll = Get-Random -Minimum 1 -Maximum 101
            if ($roll -le [Math]::Max(1, [int]($FailurePercent / 2))) {
                throw "Simulated social API failure. roll=$roll"
            }
        }

        $manifest = @(Get-Content -LiteralPath $manifestPath | Where-Object { $_.Trim().Length -gt 0 } | ForEach-Object { $_ | ConvertFrom-Json })
        foreach ($item in $manifest) {
            foreach ($platform in @("x", "linkedin", "mastodon")) {
                Write-JsonLine -Path $postsPath -Event @{
                    event = "posted"
                    scheduler = $Scheduler
                    run_id = $runId
                    workload = "complex"
                    platform = $platform
                    artifact = $item.name
                    payload_hash = $item.payload_hash
                    at = (Get-Date).ToString("o")
                    dry_run = $true
                }
            }
        }
    }

    if ($Mode -eq "large-output") {
        Invoke-Step -Name "large-output" -Body {
            for ($i = 1; $i -le 5000; $i++) {
                Write-Output ("complex-output-line {0:00000} run={1}" -f $i, $runId)
            }
        }
    }

    $finishedAt = Get-Date
    $event = @{
        event = "finished"
        scheduler = $Scheduler
        run_id = $runId
        workload = "complex"
        mode = $Mode
        at = $finishedAt.ToString("o")
        duration_ms = [int](($finishedAt - $startedAt).TotalMilliseconds)
        status = "success"
        items = $Items
        dry_posts_expected = $Items * 3
    }
    Write-JsonLine -Path $eventsPath -Event $event

    Write-Output "Complex pipeline run $runId finished for $Scheduler."
    Write-Output "Run directory: $runDir"
    Write-Output "Manifest: $manifestPath"
}
catch {
    Write-JsonLine -Path $eventsPath -Event @{
        event = "failed"
        scheduler = $Scheduler
        run_id = $runId
        workload = "complex"
        mode = $Mode
        at = (Get-Date).ToString("o")
        error = $_.Exception.Message
    }
    Write-Error $_.Exception.Message
    exit 1
}
finally {
    $memoryHold = $null
    if ($lockStream -ne $null) {
        $lockStream.Dispose()
    }
    if ($lockAcquired -and (Test-Path $lockPath)) {
        Remove-Item -LiteralPath $lockPath -Force
    }
}
