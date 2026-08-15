$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path(
    Join-Path $PSScriptRoot "..\.."
)

$PackageDir = Join-Path $RepoRoot "retail_cleansing"
$DistDir = Join-Path $RepoRoot "dist"
$OutputZip = Join-Path $DistDir "retail_cleansing.zip"

Write-Host "Repository root: $RepoRoot"

if (Test-Path $DistDir) {
    Remove-Item $DistDir -Recurse -Force
}

New-Item `
    -ItemType Directory `
    -Path $DistDir `
    | Out-Null

Get-ChildItem `
    -Path $PackageDir `
    -Recurse `
    -Directory `
    -Filter "__pycache__" `
    | Remove-Item -Recurse -Force

Compress-Archive `
    -Path $PackageDir `
    -DestinationPath $OutputZip `
    -CompressionLevel Optimal

Write-Host ""
Write-Host "Package created:"
Write-Host $OutputZip