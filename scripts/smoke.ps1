Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$exe = Join-Path $root "target\debug\cronlog.exe"
$db = Join-Path ([System.IO.Path]::GetTempPath()) ("cronlog-smoke-{0}.db" -f ([Guid]::NewGuid()))

function Invoke-Cronlog {
    & $exe --db $db @args
    if ($LASTEXITCODE -ne 0) {
        throw "Cronlog exited with code $LASTEXITCODE for args: $args"
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

    Invoke-cronlog add --name heartbeat --schedule "every 10 seconds" "--" powershell -NoProfile -Command "Write-Output alive"
    Invoke-Cronlog list
    Invoke-Cronlog run heartbeat --now
    Invoke-Cronlog history heartbeat

    $logs = & $exe --db $db logs heartbeat --last
    if ($LASTEXITCODE -ne 0) {
        throw "logs --last failed with code $LASTEXITCODE"
    }
    if (($logs -join "`n") -notmatch "alive") {
        throw "expected heartbeat logs to contain 'alive'"
    }

    Invoke-Cronlog disable heartbeat
    Invoke-Cronlog enable heartbeat
    Invoke-Cronlog remove heartbeat

    Write-Host "Smoke test passed."
}
finally {
    Pop-Location
    if (Test-Path $db) {
        Remove-Item -LiteralPath $db -Force
    }
}
