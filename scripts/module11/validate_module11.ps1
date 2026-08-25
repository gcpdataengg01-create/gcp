$ErrorActionPreference = "Stop"

Write-Host "Module 11 static validation"

$required = @(
    ".\infra\modules\governance_policy\main.tf",
    ".\infra\modules\governance\main.tf",
    ".\docs\module11-governance-dq.md",
    ".\docs\module11-lineage.md",
    ".\sql\module11\dataplex_assertions.sql"
)

foreach ($file in $required) {
    if (-not (Test-Path $file)) {
        throw "Missing required Module 11 file: $file"
    }
}

python -m py_compile .\jobs\warehouse\load_bigquery.py

Write-Host "Python syntax checks passed."
Write-Host "Run Terraform init/validate separately after copying this patch into the existing Git tree."
