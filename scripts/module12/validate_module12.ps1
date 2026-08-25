$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $RepoRoot

python -m py_compile .\dags\retail_batch_etl.py
python -m py_compile .\jobs\fx_extract\main.py
python -m py_compile .\jobs\warehouse\load_bigquery.py
python -m py_compile .\jobs\extract\extract_sales.py
python -m py_compile .\jobs\transform\spark_cleanse_c1_c4.py
python -m py_compile .\jobs\entity\spark_entity_c5_c7.py

python -m pytest .\tests\module12 -q

terraform fmt -check -recursive .\infra

Write-Host "Module 12 static validation passed. Runtime GCP tests remain for final deployment."
