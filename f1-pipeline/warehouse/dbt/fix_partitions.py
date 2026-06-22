import re

with open('c:/Satva/Tech/ETL_F1/f1-pipeline/warehouse/dbt/create_external_tables.sql', 'r') as f:
    content = f.read()

# Replace season
content = re.sub(
    r'season INTEGER AS \(VALUE:season::INTEGER\)',
    r"season INTEGER AS CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'season=(\\\\d+)', 1, 1, 'e', 1) AS INTEGER)",
    content
)

# Replace year
content = re.sub(
    r'year INTEGER AS \(VALUE:year::INTEGER\)',
    r"year INTEGER AS CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'year=(\\\\d+)', 1, 1, 'e', 1) AS INTEGER)",
    content
)

with open('c:/Satva/Tech/ETL_F1/f1-pipeline/warehouse/dbt/create_external_tables_fixed.sql', 'w') as f:
    f.write(content)
