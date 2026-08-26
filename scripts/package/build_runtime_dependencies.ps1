$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$DistDir = Join-Path $RepoRoot "dist"
$TargetDir = Join-Path $DistDir "runtime_deps"
$OutputZip = Join-Path $DistDir "runtime_deps.zip"
$Requirements = Join-Path $RepoRoot "runtime\requirements.txt"

if (-not (Test-Path $DistDir)) {
    New-Item -ItemType Directory -Path $DistDir | Out-Null
}
if (Test-Path $TargetDir) {
    Remove-Item $TargetDir -Recurse -Force
}
if (Test-Path $OutputZip) {
    Remove-Item $OutputZip -Force
}
New-Item -ItemType Directory -Path $TargetDir | Out-Null

# Only vendor the pure-Python packages that are not guaranteed by Managed
# Service for Apache Spark runtime 2.3. Their transitive Google/grpc/protobuf
# dependencies are supplied by the managed runtime. --no-deps avoids packaging
# Windows-native wheels into the Linux serverless runtime artifact.
python -m pip install `
    --disable-pip-version-check `
    --no-deps `
    --requirement $Requirements `
    --target $TargetDir

Get-ChildItem -Path $TargetDir -Recurse -Directory -Filter "__pycache__" |
    Remove-Item -Recurse -Force

Compress-Archive `
    -Path (Join-Path $TargetDir "*") `
    -DestinationPath $OutputZip `
    -CompressionLevel Optimal

Remove-Item $TargetDir -Recurse -Force
Write-Host "Runtime dependency package created: $OutputZip"
