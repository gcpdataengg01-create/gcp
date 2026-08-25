$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$PackageDir = Join-Path $RepoRoot "retail_fact"
$DistDir = Join-Path $RepoRoot "dist"
$OutputZip = Join-Path $DistDir "retail_fact.zip"

if (-not (Test-Path $DistDir)) {
    New-Item -ItemType Directory -Path $DistDir | Out-Null
}

if (Test-Path $OutputZip) {
    Remove-Item $OutputZip -Force
}

Get-ChildItem -Path $PackageDir -Recurse -Directory -Filter "__pycache__" |
    Remove-Item -Recurse -Force

Compress-Archive -Path $PackageDir -DestinationPath $OutputZip -CompressionLevel Optimal
Write-Host "Package created: $OutputZip"
