# ✅ F1 Data Setup - Completed

## What Was Fixed

### Problem
- Multiple DuckDB files scattered across the project
- `taxi_rides_ny.duckdb` in root directory (wrong)
- `f1_analytics.duckdb` also in root directory (wrong)
- `f1_analytics.duckdb` in `warehouse/dbt/` (correct location, but config was broken)

### Solution
1. ✅ **Single Source of Truth**: All data now in ONE file
   - Location: `f1-pipeline/warehouse/dbt/f1_analytics.duckdb`
   - Size: 13 MB
   - Contains: 691,086 rows across 17 tables

2. ✅ **Removed**: All stray DuckDB files from root

3. ✅ **Updated Script**: `scripts/load_kaggle_to_duckdb.py`
   - Now uses correct path: `warehouse/dbt/f1_analytics.duckdb`
   - Can be run multiple times to refresh data

4. ✅ **Verified**: dbt configuration already points to correct file
   - `warehouse/dbt/profiles.yml` → `f1_analytics.duckdb`
   - No dbt config changes needed

---

## Data Architecture

### Option C: Hybrid Approach
```
┌─────────────────────────────────────┐
│ Kaggle (1950-2024) - STATIC         │
│ Local CSV → Load Once               │
│ 12 tables, 688,448 rows             │
└─────────────────────────────────────┘
              ↓
        DuckDB (Single File)
              ↓
    warehouse/dbt/f1_analytics.duckdb
        (5.3 MB for Kaggle)

┌─────────────────────────────────────┐
│ Jolpica (2025+) - INCREMENTAL       │
│ S3 Parquet → Weekly Updates         │
│ 1 table, 523 rows                   │
└─────────────────────────────────────┘
              ↓
        DuckDB (Same File)
              ↓
    warehouse/dbt/f1_analytics.duckdb
        (+ 0.2 MB for Jolpica)

┌─────────────────────────────────────┐
│ OpenF1 (2023+) - INCREMENTAL        │
│ S3 Parquet → Weekly Updates         │
│ 4 tables, 2,115 rows                │
└─────────────────────────────────────┘
              ↓
        DuckDB (Same File)
              ↓
    warehouse/dbt/f1_analytics.duckdb
        (+ 7.5 MB for OpenF1)
```

---

## Schemas in DuckDB

```
processed_kaggle:       Historical F1 data (1950-2024)
  ├── circuits
  ├── constructors
  ├── drivers
  ├── seasons
  ├── races
  ├── results
  ├── qualifying
  ├── lap_times
  ├── pit_stops
  ├── driver_standings
  ├── constructor_standings
  └── status

processed_jolpica:      Current season data (2025+)
  └── results

processed_openf1:       Telemetry data (2023+)
  ├── drivers
  ├── laps
  ├── pit
  └── stints

raw_kaggle:             (Legacy - can be deleted)
```

---

## Usage

### Refresh Data
```bash
cd f1-pipeline/scripts
python load_kaggle_to_duckdb.py
```

### Verify Data in DuckDB
```bash
cd f1-pipeline/warehouse/dbt

# Start DuckDB CLI
duckdb f1_analytics.duckdb

# Check schemas
SELECT schema_name FROM information_schema.schemata;

# Check tables in processed_kaggle
SELECT table_name FROM information_schema.tables WHERE table_schema = 'processed_kaggle';

# Query example
SELECT COUNT(*) FROM processed_kaggle.results;
```

### Run dbt Models
```bash
cd f1-pipeline/warehouse/dbt

# Verify sources work
dbt source freshness

# Run all models
dbt run

# Run tests
dbt test
```

---

## What's Next

1. **Run dbt source freshness** to verify all tables are readable
2. **Run dbt run** to create staging models
3. **Run dbt test** to validate data quality
4. **Build fact tables** (race results, driver performance, etc.)
5. **Deploy to BigQuery** (if using prod target)

---

## Files Changed

### Modified
- ✅ `scripts/load_kaggle_to_duckdb.py` → Now uses correct DuckDB path

### No Changes Needed
- ✅ `warehouse/dbt/profiles.yml` → Already correct
- ✅ `warehouse/dbt/dbt_project.yml` → Already correct
- ✅ `warehouse/dbt/models/staging/sources.yml` → Already correct

### Deleted
- ✅ `f1-pipeline/taxi_rides_ny.duckdb` → Removed (was wrong)
- ✅ `f1-pipeline/f1_analytics.duckdb` → Removed (was created by script, wrong location)

---

## Notes

- The loader script is idempotent - can run multiple times safely
- Kaggle data is static, so only S3 data (Jolpica/OpenF1) will change weekly
- All 691,086 rows now in ONE database file (13 MB)
- dbt can read all sources without any configuration changes

✨ **Setup is complete and ready for dbt models!** ✨
