Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$exe = Join-Path $root "target\debug\cron2.exe"
$db = Join-Path ([System.IO.Path]::GetTempPath()) ("cron2-smoke-{0}.db" -f ([Guid]::NewGuid()))

function Invoke-Cron2 {
    & $exe --db $db @args
    if ($LASTEXITCODE -ne 0) {
        throw "cron2 exited with code $LASTEXITCODE for args: $args"
    }
}

try {
    Push-Location $root

    cargo build
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with code $LASTEXITCODE"
    }

    cargo test
    if ($LASTEXITCODE -ne 0) {
        throw "cargo test failed with code $LASTEXITCODE"
    }

    Invoke-Cron2 add --name heartbeat --schedule "every 10 seconds" "--" powershell -NoProfile -Command "Write-Output alive"
    Invoke-Cron2 list
    Invoke-Cron2 run heartbeat --now
    Invoke-Cron2 history heartbeat

    $logs = & $exe --db $db logs heartbeat --last
    if ($LASTEXITCODE -ne 0) {
        throw "logs --last failed with code $LASTEXITCODE"
    }
    if (($logs -join "`n") -notmatch "alive") {
        throw "expected heartbeat logs to contain 'alive'"
    }

    Invoke-Cron2 disable heartbeat
    Invoke-Cron2 enable heartbeat
    Invoke-Cron2 remove heartbeat

    Write-Host "Smoke test passed."
}
finally {
    Pop-Location
    if (Test-Path $db) {
        Remove-Item -LiteralPath $db -Force
    }
}
