import os
import subprocess
import duckdb
import glob
import json

def run_cmd(cmd, outfile=None):
    print(f"Running: {cmd}")
    try:
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        out = res.stdout + (res.stderr if 'dbt list' not in cmd else '')
        if outfile:
            with open(outfile, 'w', encoding='utf-8') as f:
                f.write(out)
        return out
    except Exception as e:
        print(f"Error running {cmd}: {e}")
        return str(e)

def format_rows(rows):
    return "\\n".join([str(r) for r in rows])

# ----------------- PHASE 1 -----------------
print("--- Phase 1 ---")
run_cmd("dbt parse")
run_cmd("dbt ls --select models/ --output json", "dbt_models_list.json")
graph_out = run_cmd("dbt ls --select models/ --graph text")
with open("dbt_graph.txt", "w", encoding="utf-8") as f:
    f.write(graph_out)

run_cmd('findstr /S /R /C:"materialized.*=.*incremental" models\\*.sql', "incremental_models.txt")
run_cmd('findstr /S /C:"unique_key" models\\*.sql', "unique_keys.txt")
run_cmd('findstr /S /C:"partition_by" models\\*.sql', "partitions.txt")

# ----------------- PHASE 2 -----------------
print("--- Phase 2 ---")
conn = duckdb.connect('f1_analytics.duckdb')

q1 = """
SELECT 'stg_kaggle_drivers' as model, COUNT(*) as row_count FROM processed_kaggle.drivers UNION ALL
SELECT 'stg_kaggle_results', COUNT(*) FROM processed_kaggle.results UNION ALL
SELECT 'stg_kaggle_qualifying', COUNT(*) FROM processed_kaggle.qualifying UNION ALL
SELECT 'stg_kaggle_lap_times', COUNT(*) FROM processed_kaggle.lap_times UNION ALL
SELECT 'stg_kaggle_pit_stops', COUNT(*) FROM processed_kaggle.pit_stops UNION ALL
SELECT 'stg_openf1_laps', COUNT(*) FROM processed_openf1.laps UNION ALL
SELECT 'stg_jolpica_results', COUNT(*) FROM processed_jolpica.results UNION ALL
SELECT 'stg_jolpica_qualifying', COUNT(*) FROM processed_jolpica.qualifying;
"""
try:
    res1 = format_rows(conn.execute(q1).fetchall())
except Exception as e:
    res1 = str(e)

q2 = """
SELECT 'int_driver_bridge' as model, COUNT(*) FROM main_intermediate.int_driver_bridge UNION ALL
SELECT 'int_results_unified', COUNT(*) FROM main_intermediate.int_results_unified UNION ALL
SELECT 'int_qualifying_unified', COUNT(*) FROM main_intermediate.int_qualifying_unified UNION ALL
SELECT 'int_pitstops_unified', COUNT(*) FROM main_intermediate.int_pitstops_unified;
"""
try:
    res2 = format_rows(conn.execute(q2).fetchall())
except Exception as e:
    res2 = str(e)

q3 = """
SELECT 'dim_driver' as model, COUNT(*) FROM main_marts.dim_driver UNION ALL
SELECT 'fct_race_result', COUNT(*) FROM main_marts.fct_race_result;
"""
try:
    res3 = format_rows(conn.execute(q3).fetchall())
except Exception as e:
    res3 = str(e)

q4 = """
SELECT 'fct_race_result NULL driver_key' as check, COUNT(*) as cnt 
FROM main_marts.fct_race_result 
WHERE driver_key IS NULL;
"""
try:
    res4 = format_rows(conn.execute(q4).fetchall())
except Exception as e:
    res4 = str(e)

q5 = """
SELECT year, driver_number, lap_number, COUNT(*) as cnt
FROM processed_openf1.laps 
GROUP BY year, driver_number, lap_number 
HAVING COUNT(*) > 1
LIMIT 10;
"""
try:
    res5 = format_rows(conn.execute(q5).fetchall())
except Exception as e:
    res5 = str(e)

q6 = """
SELECT model_name, MAX(_dbt_loaded_at) as last_loaded
FROM (
    SELECT _dbt_loaded_at, 'stg_kaggle_results' as model_name FROM processed_kaggle.results
    UNION ALL
    SELECT _dbt_loaded_at, 'stg_jolpica_results' FROM processed_jolpica.results
    UNION ALL
    SELECT _dbt_loaded_at, 'stg_openf1_laps' FROM processed_openf1.laps
)
GROUP BY model_name
ORDER BY last_loaded DESC;
"""
try:
    res6 = format_rows(conn.execute(q6).fetchall())
except Exception as e:
    res6 = str(e)

# ----------------- PHASE 3 -----------------
print("--- Phase 3 ---")
run_cmd("dbt ls --select source:* --output json", "dbt_sources.json")
try:
    with open("models/staging/sources.yml", "r", encoding="utf-8") as f:
        with open("sources_definition.txt", "w", encoding="utf-8") as out:
            out.write(f.read())
except: pass

try:
    s3_sample = format_rows(conn.execute("SELECT * FROM processed_kaggle.results LIMIT 5").fetchall())
except Exception as e:
    s3_sample = str(e)

run_cmd('dir /s /b models\\staging\\*.yml', "staging_yml_files.txt")

# ----------------- PHASE 4 -----------------
print("--- Phase 4 ---")
run_cmd("dbt test --list", "dbt_tests_list.txt")
run_cmd("dbt test --select models/staging/", "staging_tests_output.txt")
run_cmd('findstr /S /C:"unique" /C:"not_null" models\\*.yml | find /C /V ""', "test_count.txt")

# ----------------- PHASE 5 -----------------
print("--- Phase 5 ---")
q7 = """
SELECT year, driver_number, lap_number, COUNT(*) as occurrences
FROM processed_openf1.laps
GROUP BY year, driver_number, lap_number
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 20;
"""
try:
    res7 = format_rows(conn.execute(q7).fetchall())
except Exception as e:
    res7 = str(e)

# ----------------- PHASE 6 -----------------
print("--- Phase 6 ---")
run_cmd('dir /s /b *.json | findstr /i "watermark manifest"', "watermark_locations.txt")
run_cmd('findstr /A:10 /C:"incremental" dbt_project.yml', "incremental_config.txt")

# ----------------- PHASE 7 -----------------
print("--- Phase 7 ---")
prof = os.path.expanduser('~/.dbt/profiles.yml')
if os.path.exists(prof):
    run_cmd(f'findstr /A:10 /C:"bigquery" {prof}', "bigquery_profile.txt")

run_cmd('findstr /S /C:"database=" /C:"schema=" models\\*.sql', "hardcoded_refs.txt")

# ----------------- COMPILE REPORT -----------------
report = f"""# dbt Layer Investigation Report

## 1. Project Structure
**Incremental Models (`incremental_models.txt`)**
```
{open('incremental_models.txt').read() if os.path.exists('incremental_models.txt') else 'None'}
```

**Unique Keys (`unique_keys.txt`)**
```
{open('unique_keys.txt').read() if os.path.exists('unique_keys.txt') else 'None'}
```

**Partitions (`partitions.txt`)**
```
{open('partitions.txt').read() if os.path.exists('partitions.txt') else 'None'}
```

## 2. Current Data Volumes (DuckDB)
**Staging:**
```text
{res1}
```
**Intermediate:**
```text
{res2}
```
**Gold:**
```text
{res3}
```

## 3. Data Freshness
```text
{res6}
```

## 4. Duplicate Check Results
**OpenF1 Laps Unique Key Violations (Phase 2):**
```text
{res5}
```
**NULL Foreign Keys (Phase 2):**
```text
{res4}
```

## 5. Incremental Models Currently Used
See Section 1 above. 
OpenF1 Duplicates detailed check (Phase 5):
```text
{res7}
```

## 6. Unique Key Definitions
See Section 1 above.

## 7. Test Coverage
Total tests (unique/not_null in YAMLs): {open('test_count.txt').read().strip() if os.path.exists('test_count.txt') else 'N/A'}
Staging Tests Output:
```text
{open('staging_tests_output.txt').read() if os.path.exists('staging_tests_output.txt') else 'None'}
```

## 8. Known Issues
- `fct_race_result` has some NULL driver_key entries (due to 2025 Jolpica drivers not in Kaggle bridge).
- Any staging test failures will be shown above.

## 9. Questions Needing Clarification
- What is the exact expected `unique_key` for `stg_openf1_laps`? 
- Will BigQuery be handling history loads differently than DuckDB?
"""

with open("dbt_investigation_report.md", "w", encoding="utf-8") as f:
    f.write(report)
print("Done!")
