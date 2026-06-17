# Option C: Hybrid Architecture Setup Guide

## Overview

This guide explains the **Option C Hybrid** approach where:
- **Kaggle (1950-2024)**: Static historical data → Load once to DuckDB
- **Jolpica (2025+)**: Incremental current season → Stream from S3 via external tables
- **OpenF1 (2023+)**: Incremental telemetry → Stream from S3 via external tables

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ Kaggle Historical (1950-2024) - STATIC                      │
│ S3 Parquet → [load_kaggle_to_duckdb.py] → DuckDB            │
│ (Run ONCE, then cached)                                      │
└─────────────────────────────────────────────────────────────┘
         ↓
    DuckDB (In-Memory)
         ↓
    dbt sources.yml
    └─ processed_kaggle.circuits
    └─ processed_kaggle.drivers
    └─ processed_kaggle.races
    └─ processed_kaggle.results
    └─ [etc - all 14 tables]

┌─────────────────────────────────────────────────────────────┐
│ Jolpica (2025+) - INCREMENTAL                               │
│ S3 Parquet → dbt External Tables                            │
│ (Append new rounds weekly)                                   │
└─────────────────────────────────────────────────────────────┘
         ↓
    dbt sources.yml (external_location)
    └─ processed_jolpica.results
    └─ processed_jolpica.qualifying
    └─ [etc - incremental from S3]

┌─────────────────────────────────────────────────────────────┐
│ OpenF1 (2023+) - INCREMENTAL TELEMETRY                      │
│ S3 Parquet → dbt External Tables                            │
│ (Append new sessions)                                        │
└─────────────────────────────────────────────────────────────┘
         ↓
    dbt sources.yml (external_location)
    └─ processed_openf1.laps
    └─ processed_openf1.pit
    └─ [etc - incremental from S3]
```

---

## Step 1: Load Kaggle Data (Run Once)

```bash
cd f1-pipeline/scripts
python load_kaggle_to_duckdb.py
```

**What it does:**
1. ✅ Connects to S3
2. ✅ Verifies Kaggle Parquet files exist
3. ✅ Loads each table into DuckDB schema `processed_kaggle`
4. ✅ Validates row counts
5. ✅ Creates indexes for performance
6. ✅ Prints summary

**Output:**
```
🔍 Checking S3 for Kaggle tables in: f1-pipeline-processed-layer/processed/kaggle/
✅ Found 14 tables (42 files, 1250.3 MB total)
   - circuits: 1 file(s), 0.2 MB
   - drivers: 1 file(s), 0.8 MB
   - races: 1 file(s), 1.2 MB
   - results: 1 file(s), 850.0 MB
   [...]

✔️ Validating data loads...
   ✅ drivers: 856 rows (expected > 800)
   ✅ races: 1098 rows (expected > 1000)
   ✅ results: 27600 rows (expected > 20000)
   ✅ lap_times: 15800000 rows (expected > 1,000,000)

================================
KAGGLE LOAD SUMMARY
================================
✅ Loaded: 14 tables
📊 Total rows: 57,234,890
💾 DuckDB file: /path/to/taxi_rides_ny.duckdb
📦 File size: 1250.3 MB

Next steps:
  1. Run dbt models: cd warehouse/dbt && dbt run
  2. Verify sources work: dbt source freshness
```

---

## Step 2: Update dbt sources.yml for DuckDB Tables

### Before (S3 External Tables - Wrong)
```yaml
sources:
  - name: s3_kaggle
    schema: processed_kaggle
    # This doesn't work - we need actual tables, not external locations
```

### After (DuckDB Tables - Correct)
```yaml
sources:
  - name: kaggle_kaggle  # Or keep s3_kaggle, dbt doesn't care
    description: Historical F1 data from Kaggle (loaded to DuckDB)
    schema: processed_kaggle
    database: memory  # DuckDB's in-memory schema
    tables:
      - name: circuits
        description: F1 circuits/tracks
        columns:
          - name: circuitId
            tests:
              - unique
              - not_null
      # ... [rest of columns]
      - name: drivers
      - name: races
      - name: results
      # ... [all 14 tables]
```

**Key changes:**
- `database: memory` tells dbt this is DuckDB in-memory
- `schema: processed_kaggle` points to the DuckDB schema
- No `external_location` needed (we have actual tables now)

---

## Step 3: Configure Jolpica External Tables (S3)

For Jolpica data (incremental, new rounds added weekly), use DuckDB external tables in dbt:

### In dbt profiles.yml (DuckDB config)
```yaml
f1_warehouse:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ../taxi_rides_ny.duckdb
      threads: 4
      extensions:
        - httpfs  # For S3 access
```

### In dbt models/staging/sources.yml (Jolpica)
```yaml
sources:
  - name: s3_jolpica
    description: Current F1 season data from Jolpica (incremental)
    schema: processed_jolpica
    tables:
      - name: results
        description: Race results (new rounds appended to S3)
        external_location: s3://f1-pipeline-processed-layer/processed/jolpica/results/
        # DuckDB will read directly from S3 on query
```

### In staging model (stg_jolpica_results.sql)
```sql
{{ 
  config(
    materialized='view',
    description='Jolpica results, read directly from S3'
  )
}}

SELECT 
  season,
  round,
  driver_id,
  position,
  points,
  -- Transform columns as needed
FROM {{ source('s3_jolpica', 'results') }}
WHERE season >= 2025  -- Only current seasons
```

---

## Step 4: Configure OpenF1 External Tables (S3)

For OpenF1 telemetry (incremental, new sessions added):

```yaml
sources:
  - name: s3_openf1
    description: Detailed telemetry from OpenF1 API (incremental)
    schema: processed_openf1
    tables:
      - name: laps
        description: Lap-by-lap telemetry with sector times
        external_location: s3://f1-pipeline-processed-layer/processed/openf1/laps/
      - name: pit
        description: Pit stop telemetry
        external_location: s3://f1-pipeline-processed-layer/processed/openf1/pit/
```

---

## Step 5: Run dbt Models

```bash
cd f1-pipeline/warehouse/dbt

# Verify sources can be read
dbt source freshness

# Run all models
dbt run

# Test everything
dbt test
```

---

## Hybrid Benefits Summary

| Aspect | Benefit |
|--------|---------|
| **Speed** | Kaggle queries are instant (DuckDB cache) |
| **Freshness** | Jolpica/OpenF1 always current (S3 external tables) |
| **Storage** | Kaggle: +1.3GB disk. Jolpica/OpenF1: No duplication (S3 only) |
| **Cost** | Kaggle: ~$30/yr storage. Jolpica/OpenF1: S3 query costs (tiny) |
| **Maintenance** | 1 Python script + 1 dbt config update |
| **Scalability** | Future-proof for new data sources |

---

## Troubleshooting

### "Table not found" in dbt
- Run `python load_kaggle_to_duckdb.py` first
- Check DuckDB file exists at `taxi_rides_ny.duckdb`
- Run `SELECT * FROM processed_kaggle.circuits LIMIT 1` in DuckDB

### S3 reads are slow
- Normal for Jolpica/OpenF1 (they're on S3)
- Cache the results in a persistent dbt table if needed
- Use `dbt_project.yml` materialization to optimize

### S3 credentials not working
- Check `.env` file has `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- Verify IAM user has S3 read permissions
- Test with: `aws s3 ls s3://f1-pipeline-processed-layer/processed/`

---

## Next: Incremental Updates

Once Kaggle is loaded, you only need to update Jolpica/OpenF1 data weekly:

```bash
# Scheduled job (weekly, after each F1 race weekend)
# Pulls new rounds from Jolpica → S3 → dbt automatically reads them
python orchestration/dags/f1_pipeline_dag.py
```

The dbt models will automatically pick up new data from S3 on the next `dbt run`.

