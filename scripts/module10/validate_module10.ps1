$ErrorActionPreference = "Stop"

Write-Host "Checking Module 10 Python syntax..."
python -m py_compile .\retail_fact\build.py
python -m py_compile .\retail_fact\quality.py
python -m py_compile .\jobs\fact\build_fact.py
python -m py_compile .\jobs\warehouse\load_bigquery.py
python -m py_compile .\tests\module10\test_fact.py
python -m py_compile .\tests\module10\test_quality.py

Write-Host "Checking integration files touched for C9 run-state accounting..."
python -m py_compile .\jobs\extract\extract_sales.py
python -m py_compile .\jobs\transform\spark_cleanse_c1_c4.py
python -m py_compile .\jobs\entity\spark_entity_c5_c7.py
python -m py_compile .\retail_cleansing\standardise.py

Write-Host "Syntax checks passed."
Write-Host "Terraform validation should be run after final init/apply preparation:"
Write-Host "terraform -chdir=infra\envs\dev validate"
Write-Host "Spark tests can be deferred to the GCP validation cycle:"
Write-Host "python -m pytest .\tests\module10 -v"
