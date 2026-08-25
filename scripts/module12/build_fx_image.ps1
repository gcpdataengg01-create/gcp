param(
    [Parameter(Mandatory=$true)][string]$ProjectId,
    [Parameter(Mandatory=$true)][string]$Region,
    [Parameter(Mandatory=$true)][string]$RepositoryName,
    [string]$Tag = "module12"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $RepoRoot

$ImageUri = "$Region-docker.pkg.dev/$ProjectId/$RepositoryName/fx:$Tag"

gcloud auth configure-docker "$Region-docker.pkg.dev" --quiet

docker build -f .\jobs\fx_extract\Dockerfile -t $ImageUri .\jobs\fx_extract
docker push $ImageUri

Write-Host "FX image pushed:"
Write-Host $ImageUri
Write-Host "Use this value for Terraform variable fx_image_uri and set deploy_fx_job=true."
