param(
    [Parameter(Mandatory=$true)][string]$ProjectId,
    [Parameter(Mandatory=$true)][string]$Region,
    [Parameter(Mandatory=$true)][string]$ComposerEnvironment,
    [Parameter(Mandatory=$true)][string]$CodeBucket,
    [Parameter(Mandatory=$true)][string]$PostgresJdbcJarPath
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $RepoRoot

if (-not (Test-Path $PostgresJdbcJarPath)) {
    throw "PostgreSQL JDBC jar not found: $PostgresJdbcJarPath"
}

# Build shared --py-files packages. Cleansing runs first because its existing
# builder recreates dist/.
& .\scripts\package\build_cleansing_package.ps1
& .\scripts\package\build_dimensions_package.ps1
& .\scripts\package\build_fact_package.ps1

# Upload code. The code bucket is versioned; re-uploading creates a recoverable
# object generation instead of losing the previous runtime artifact.
gcloud storage cp --recursive .\jobs "gs://$CodeBucket/"
gcloud storage cp .\dist\retail_cleansing.zip "gs://$CodeBucket/packages/retail_cleansing.zip"
gcloud storage cp .\dist\retail_dimensions.zip "gs://$CodeBucket/packages/retail_dimensions.zip"
gcloud storage cp .\dist\retail_fact.zip "gs://$CodeBucket/packages/retail_fact.zip"
gcloud storage cp $PostgresJdbcJarPath "gs://$CodeBucket/deps/postgresql.jar"

# Import the SAME production DAG used by scheduled runs and backfills.
gcloud composer environments storage dags import `
  --project $ProjectId `
  --location $Region `
  --environment $ComposerEnvironment `
  --source .\dags\retail_batch_etl.py

Write-Host "Runtime code and Composer DAG deployed."
