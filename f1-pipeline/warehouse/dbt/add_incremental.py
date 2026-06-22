import os
import re

models = {
    # Kaggle
    "models/staging/kaggle/stg_kaggle_results.sql": ("r.year", ">"),
    "models/staging/kaggle/stg_kaggle_qualifying.sql": ("r.year", ">"),
    "models/staging/kaggle/stg_kaggle_lap_times.sql": ("r.year", ">"),
    "models/staging/kaggle/stg_kaggle_driver_standings.sql": ("r.year", ">"),
    "models/staging/kaggle/stg_kaggle_constructor_standings.sql": ("r.year", ">"),
    "models/staging/kaggle/stg_kaggle_pit_stops.sql": ("r.year", ">"),
    
    # Jolpica
    "models/staging/jolpica/stg_jolpica_results.sql": ("season", ">="),
    "models/staging/jolpica/stg_jolpica_qualifying.sql": ("season", ">="),
    "models/staging/jolpica/stg_jolpica_pitstops.sql": ("season", ">="),
    "models/staging/jolpica/stg_jolpica_driver_standings.sql": ("season", ">="),
    "models/staging/jolpica/stg_jolpica_constructor_standings.sql": ("season", ">="),
    "models/staging/jolpica/stg_jolpica_sprint.sql": ("season", ">="),

    # OpenF1
    "models/staging/openf1/stg_openf1_drivers.sql": ("year", ">="),
    "models/staging/openf1/stg_openf1_pit.sql": ("year", ">="),
    "models/staging/openf1/stg_openf1_stints.sql": ("year", ">="),
}

def inject_incremental(filepath, col, op):
    if not os.path.exists(filepath):
        print(f"Skipping {filepath}, does not exist")
        return
        
    with open(filepath, "r") as f:
        content = f.read()

    original_content = content
    
    # 1. Materialization
    content = re.sub(r"materialized\s*=\s*'table'", "materialized='incremental',\n    incremental_strategy='merge'", content)
    
    # 2. Inject into source CTE
    # Find the end of the source CTE: `),\n\nrenamed as (`
    source_end_idx = content.find("),\n\nrenamed as (")
    if source_end_idx != -1:
        # Check if there's already a WHERE clause in the source CTE
        source_cte = content[:source_end_idx]
        has_where = "where " in source_cte.lower().split("from ")[-1]
        
        target_col = "year" if "year" in col else "season_year"
        
        inc_block = f"""
    {{% if is_incremental() %}}
    {'and' if has_where else 'where'} {col} {op} (select max({target_col}) from {{{{ this }}}})
    {{% endif %}}"""
        
        content = content[:source_end_idx] + inc_block + "\n" + content[source_end_idx:]

    if content != original_content:
        with open(filepath, "w") as f:
            f.write(content)
        print(f"Updated {filepath}")
    else:
        print(f"No changes made to {filepath}")

for filepath, (col, op) in models.items():
    inject_incremental(filepath, col, op)
