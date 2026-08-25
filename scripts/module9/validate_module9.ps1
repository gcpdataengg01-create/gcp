$ErrorActionPreference = "Stop"

Write-Host "Checking Module 9 Python syntax..."
python -m py_compile .\retail_dimensions\keys.py
python -m py_compile .\retail_dimensions\common.py
python -m py_compile .\retail_dimensions\customer.py
python -m py_compile .\retail_dimensions\scd2.py
python -m py_compile .\jobs\dimensions\build_date.py
python -m py_compile .\jobs\dimensions\build_customer.py
python -m py_compile .\jobs\scd\product_scd2.py
python -m py_compile .\tests\module9\test_date_dimension.py
python -m py_compile .\tests\module9\test_customer_dimension.py
python -m py_compile .\tests\module9\test_product_scd2.py

Write-Host "Syntax checks passed."
Write-Host "Run Spark tests locally when desired with:"
Write-Host "python -m pytest .\tests\module9 -v"
