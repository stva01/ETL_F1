"""
Strip config() blocks and incremental filter blocks from staging models.
Staging models become plain views — materialization is controlled by dbt_project.yml.
"""
import os
import re

STAGING_DIR = "models/staging"

# Regex to match the entire {{ config(...) }} block (handles multi-line)
config_block_re = re.compile(r'\{\{[\s\S]*?config\([\s\S]*?\)\s*\}\}\s*\n?', re.DOTALL)

# Regex to strip {% if is_incremental() %} ... {% endif %} blocks from the source CTE
incremental_block_re = re.compile(
    r'\n?\s*\{%[\s\S]*?if is_incremental\(\)[\s\S]*?%\}[\s\S]*?\{%[\s\S]*?endif[\s\S]*?%\}\s*',
    re.DOTALL
)

for root, _, files in os.walk(STAGING_DIR):
    for file in files:
        if not file.endswith(".sql"):
            continue
        filepath = os.path.join(root, file)

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        original = content

        # Remove config block
        content = config_block_re.sub("", content)

        # Remove incremental filter block
        content = incremental_block_re.sub("\n", content)

        # Clean up leading blank lines
        content = content.lstrip("\n")

        if content != original:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"Stripped: {filepath}")
        else:
            print(f"Unchanged: {filepath}")
