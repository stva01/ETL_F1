import os
import re

STAGING_DIR = "models/staging/kaggle"

for file in os.listdir(STAGING_DIR):
    if not file.endswith(".sql"): continue
    filepath = os.path.join(STAGING_DIR, file)
    
    with open(filepath, "r") as f:
        content = f.read()

    original = content
    
    # Replace `nullif(column_name, '\N')` with `nullif(cast(column_name as varchar), '\N')`
    # Regex breakdown: nullif\(\s*([^,]+?)\s*,\s*'(?:\\N|\\N)'\s*\)
    # We want to be careful not to double cast if already casted
    
    def repl(m):
        col = m.group(1)
        if "cast(" in col.lower():
            return m.group(0) # don't double cast
        return f"nullif(cast({col} as varchar), '\\N')"

    content = re.sub(r"nullif\(\s*([^,]+?)\s*,\s*'\\N'\s*\)", repl, content, flags=re.IGNORECASE)

    if content != original:
        with open(filepath, "w") as f:
            f.write(content)
        print(f"Updated {filepath}")
