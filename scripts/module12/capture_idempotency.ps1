param(
    [Parameter(Mandatory=$true)][string]$ProjectId,
    [Parameter(Mandatory=$true)][string]$BusinessDate,
    [string]$OutputFile = ".\evidence\module12\idempotency-fingerprint.txt"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $RepoRoot

$sql = (Get-Content .\sql\module12\idempotency_fingerprint.sql -Raw).Replace("PROJECT", $ProjectId)
$result = $sql | bq query --project_id=$ProjectId --use_legacy_sql=false --format=prettyjson --parameter="business_date:DATE:$BusinessDate"
$result | Tee-Object -FilePath $OutputFile
Write-Host "Capture this fingerprint before and after rerunning the same business date."
