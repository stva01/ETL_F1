import os
import glob
import re

dashboard_path = 'c:/Satva/Tech/ETL_F1/f1-pipeline/dashboard/src/app/**/*.tsx'
files = glob.glob(dashboard_path, recursive=True)

for file in files:
    with open(file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Skip if not using duckdb
    if 'duckdb' not in content and 'main_marts' not in content:
        continue

    # Replace import
    content = content.replace('import { queryDuckDB } from "@/lib/duckdb";', 'import { querySnowflake } from "@/lib/snowflake";')
    
    # Replace query function
    content = content.replace('queryDuckDB<', 'querySnowflake<')
    content = content.replace('queryDuckDB(', 'querySnowflake(')
    
    # Replace schema
    content = content.replace('main_marts.', 'MARTS.')
    
    # Also replace any CAST(... AS INTEGER) inside the queries because Snowflake returns BIGINT as JS Numbers normally, 
    # but the explicit casts are perfectly fine. We just leave them be.

    with open(file, 'w', encoding='utf-8') as f:
        f.write(content)

print(f"Updated {len(files)} files.")

# Remove duckdb.ts
try:
    os.remove('c:/Satva/Tech/ETL_F1/f1-pipeline/dashboard/src/lib/duckdb.ts')
    print("Deleted duckdb.ts")
except FileNotFoundError:
    pass
