import os
import re

STAGING_DIR = "models/staging"

# Tables that are flat (unpartitioned)
flat_kaggle = ["circuits", "constructors", "drivers", "seasons", "status"]

for root, _, files in os.walk(STAGING_DIR):
    for file in files:
        if not file.endswith(".sql"): continue
        filepath = os.path.join(root, file)
        
        with open(filepath, "r") as f:
            content = f.read()

        original_content = content
        
        # Replace now() with cloud-agnostic timestamp
        content = content.replace("now() as _dbt_loaded_at", "{{ dbt.current_timestamp() }} as _dbt_loaded_at")

        # Kaggle replacements
        def replace_kaggle(m):
            table = m.group(1)
            if table in flat_kaggle:
                path = f"kaggle/{table}/*.parquet"
            else:
                path = f"kaggle/{table}/*/*.parquet"
            return f"{{{{ s3_source('s3_kaggle', '{table}', '{path}') }}}}"
            
        content = re.sub(r"\{\{\s*source\('s3_kaggle',\s*'([^']+)'\)\s*\}\}", replace_kaggle, content)

        # Jolpica replacements
        def replace_jolpica(m):
            table = m.group(1)
            path = f"jolpica/{table}/*/*.parquet"
            return f"{{{{ s3_source('processed_jolpica', '{table}', '{path}') }}}}"
            
        content = re.sub(r"\{\{\s*source\('processed_jolpica',\s*'([^']+)'\)\s*\}\}", replace_jolpica, content)

        # OpenF1 replacements
        def replace_openf1(m):
            table = m.group(1)
            path = f"openf1/{table}/*/*.parquet"
            return f"{{{{ s3_source('processed_openf1', '{table}', '{path}') }}}}"
            
        content = re.sub(r"\{\{\s*source\('processed_openf1',\s*'([^']+)'\)\s*\}\}", replace_openf1, content)

        if content != original_content:
            with open(filepath, "w") as f:
                f.write(content)
            print(f"Updated {filepath}")
