$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $RepoRoot

python -m compileall -q .
python -m pytest .\tests\predeploy .\tests\module10\test_quality.py .\tests\module11 .\tests\module12 -q

terraform fmt -check -recursive .\infra
terraform -chdir=infra\envs\dev init -reconfigure -backend-config="backend.hcl"
terraform -chdir=infra\envs\dev validate

Write-Host "Pre-deployment static validation passed. No terraform apply was run."
