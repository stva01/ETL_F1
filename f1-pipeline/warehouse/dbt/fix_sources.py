with open('c:/Satva/Tech/ETL_F1/f1-pipeline/warehouse/dbt/models/staging/sources.yml', 'r') as f:
    lines = f.readlines()
new_lines = []
for line in lines:
    if 'identifier: ' in line:
        indent = len(line) - len(line.lstrip())
        if indent > 8:
            continue
    new_lines.append(line)
with open('c:/Satva/Tech/ETL_F1/f1-pipeline/warehouse/dbt/models/staging/sources.yml', 'w') as f:
    f.writelines(new_lines)
