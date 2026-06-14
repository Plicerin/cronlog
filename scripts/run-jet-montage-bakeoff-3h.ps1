Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $root "jet-montage-bakeoff-runs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$transcript = Join-Path $root "jet-montage-bakeoff-3h.transcript.log"
Start-Transcript -Path $transcript -Force | Out-Null
try {
    & (Join-Path $PSScriptRoot "bakeoff-jet-montage-3h.ps1") `
        -Runs 3 `
        -IntervalSeconds 3600 `
        -TimeoutSeconds 1800 `
        -Photos 8 `
        -SecondsPerPhoto 3 `
        -Fps 30 `
        -Width 1080 `
        -Height 1920
}
finally {
    Stop-Transcript | Out-Null
}
