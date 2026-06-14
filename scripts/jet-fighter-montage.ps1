param(
    [string]$OutDir = $(Join-Path (Split-Path -Parent $PSScriptRoot) "jet-fighter-montage-output"),
    [int]$Photos = 8,
    [double]$SecondsPerPhoto = 3.0,
    [int]$Fps = 30,
    [int]$Width = 1080,
    [int]$Height = 1920
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$script = Join-Path $root "examples\jet_fighter_montage.py"

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

python $script `
    --outdir $OutDir `
    --photos $Photos `
    --seconds-per-photo $SecondsPerPhoto `
    --fps $Fps `
    --width $Width `
    --height $Height
