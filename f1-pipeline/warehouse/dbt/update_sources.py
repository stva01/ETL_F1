import yaml

with open('c:/Satva/Tech/ETL_F1/f1-pipeline/warehouse/dbt/models/staging/sources.yml', 'r') as f:
    lines = f.readlines()

new_lines = []
current_source = None
for line in lines:
    if line.strip().startswith('- name:'):
        if 's3_kaggle' in line:
            current_source = 'kaggle'
        elif 'processed_jolpica' in line:
            current_source = 'jolpica'
        elif 'processed_openf1' in line:
            current_source = 'openf1'
    
    if line.strip().startswith('schema:'):
        if current_source == 'kaggle':
            new_lines.append(line.replace('schema: processed_kaggle', 'schema: "{{ \'staging\' if target.type == \'snowflake\' else \'processed_kaggle\' }}"'))
            continue
        elif current_source == 'jolpica':
            new_lines.append(line.replace('schema: processed_jolpica', 'schema: "{{ \'staging\' if target.type == \'snowflake\' else \'processed_jolpica\' }}"'))
            continue
        elif current_source == 'openf1':
            new_lines.append(line.replace('schema: processed_openf1', 'schema: "{{ \'staging\' if target.type == \'snowflake\' else \'processed_openf1\' }}"'))
            continue
            
    new_lines.append(line)
    
    # Check if we are inside tables list and see '- name: xxx'
    if line.strip().startswith('- name:') and current_source:
        # Check if it's a table definition inside tables (usually indented more than source name)
        # source name is 4 spaces indented (or 2), table name is more indented
        indent = len(line) - len(line.lstrip())
        if indent > 4:
            table_name = line.strip().split(':')[1].strip()
            identifier_str = f"{line[:indent]}  identifier: \"{{{{ '{current_source}_{table_name}' if target.type == 'snowflake' else '{table_name}' }}}}\"\n"
            new_lines.append(identifier_str)

with open('c:/Satva/Tech/ETL_F1/f1-pipeline/warehouse/dbt/models/staging/sources.yml', 'w') as f:
    f.writelines(new_lines)
