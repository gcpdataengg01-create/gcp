$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$PackageDir = Join-Path $RepoRoot "retail_dimensions"
$DistDir = Join-Path $RepoRoot "dist"
$OutputZip = Join-Path $DistDir "retail_dimensions.zip"

if (-not (Test-Path $DistDir)) {
    New-Item -ItemType Directory -Path $DistDir | Out-Null
}

if (Test-Path $OutputZip) {
    Remove-Item $OutputZip -Force
}

Get-ChildItem -Path $PackageDir -Recurse -Directory -Filter "__pycache__" |
    Remove-Item -Recurse -Force

Compress-Archive `
    -Path $PackageDir `
    -DestinationPath $OutputZip `
    -CompressionLevel Optimal

Write-Host "Module 9 package created: $OutputZip"
