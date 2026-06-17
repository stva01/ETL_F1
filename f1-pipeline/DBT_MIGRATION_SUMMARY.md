# dbt DuckDB Migration - Changes Summary

## Overview
Migrated dbt project from BigQuery to DuckDB with hybrid data architecture:
- **Kaggle (1950-2024)**: Historical data loaded to DuckDB once
- **Jolpica (2025+)**: Current season data in DuckDB processed_jolpica schema  
- **OpenF1 (2023+)**: Telemetry data in DuckDB processed_openf1 schema

## Changes Made

### 1. ✅ Data Layer Configuration
- **File**: `scripts/load_kaggle_to_duckdb.py`
- **Status**: COMPLETE
- **Changes**: 
  - Created unified loader supporting CSV (Kaggle) + Parquet (Jolpica/OpenF1)
  - Fixed path to use `warehouse/dbt/f1_analytics.duckdb`
  - Data loaded: 12 Kaggle tables (688,448 rows), 1 Jolpica table (523 rows), 4 OpenF1 tables (2,115 rows)
  - Result: 691,086 total rows across 3 schemas

### 2. ✅ dbt Project Configuration
- **Files**: `dbt_project.yml`, `profiles.yml`
- **Status**: COMPLETE
- **Changes**:
  - Fixed `dbt_project.yml` YAML syntax (removed invalid model configs)
  - Updated `profiles.yml` to point to correct DuckDB path
  - Configured DuckDB extensions: httpfs, aws
  - Set S3 region to ap-south-1

### 3. ✅ Sources Configuration
- **File**: `models/staging/sources.yml`
- **Status**: COMPLETE  
- **Changes**:
  - Changed Kaggle source schema from `raw_kaggle` to `processed_kaggle`
  - Added Jolpica source pointing to `processed_jolpica` schema
  - Added OpenF1 source pointing to `processed_openf1` schema
  - Removed complex S3 source definitions (data now in DuckDB)

### 4. ✅ SQL Function Migration
- **Status**: COMPLETE
- **Changes**: Replaced BigQuery-specific functions across all 15 staging models
  - `current_timestamp()` → `now()` (15 files)
  - Removed Jinja lap_time_udf macro (3 Kaggle files)
  - Simplified time parsing to use raw milliseconds fields

### 5. ⚠️  Source References Migration
- **Files**: 5 models (Jolpica x3, OpenF1 x2)
- **Status**: MIGRATED (with data type issues to resolve)
- **Changes**:
  - Migrated from BigQuery `{{ source() }}` references to DuckDB
  - All models now reference DuckDB schemas correctly

### 6. ⚠️  Data Type Alignment  
- **Status**: IN PROGRESS - Several casting issues discovered
- **Issues Found**:
  - Kaggle `position` column is VARCHAR (contains '\N' for nulls)
  - Kaggle `milliseconds`, `rank`, `number` columns are VARCHAR not INTEGER
  - Jolpica `position`, `q1/q2/q3` may be nullable
  - OpenF1 columns may not match model expectations
  
## Files Modified

### Core Configuration (3 files)
```
warehouse/dbt/dbt_project.yml
warehouse/dbt/profiles.yml
warehouse/dbt/models/staging/sources.yml
```

### Staging Models - Kaggle (10 files)
```
warehouse/dbt/models/staging/kaggle/stg_kaggle_circuits.sql
warehouse/dbt/models/staging/kaggle/stg_kaggle_constructors.sql
warehouse/dbt/models/staging/kaggle/stg_kaggle_driver_standings.sql
warehouse/dbt/models/staging/kaggle/stg_kaggle_drivers.sql
warehouse/dbt/models/staging/kaggle/stg_kaggle_lap_times.sql
warehouse/dbt/models/staging/kaggle/stg_kaggle_qualifying.sql
warehouse/dbt/models/staging/kaggle/stg_kaggle_races.sql
warehouse/dbt/models/staging/kaggle/stg_kaggle_results.sql
warehouse/dbt/models/staging/kaggle/stg_kaggle_seasons.sql
warehouse/dbt/models/staging/kaggle/stg_kaggle_status.sql
```

### Staging Models - Jolpica (3 files)
```
warehouse/dbt/models/staging/jolpica/stg_jolpica_results.sql
warehouse/dbt/models/staging/jolpica/stg_jolpica_qualifying.sql
warehouse/dbt/models/staging/jolpica/stg_jolpica_driver_standings.sql
```

### Staging Models - OpenF1 (2 files)
```
warehouse/dbt/models/staging/openf1/stg_openf1_drivers.sql
warehouse/dbt/models/staging/openf1/stg_openf1_laps.sql
```

## Next Steps to Fix Remaining Issues

1. **Resolve Kaggle Data Type Casting**
   - Handle NULL values ('\\N') in VARCHAR columns before casting to INTEGER
   - Use TRY_CAST() or NULLIF() for safe conversions
   - Update: position, grid, laps, milliseconds, fastestLap, rank

2. **Fix Incremental Model Logic**
   - Remove incremental filters that reference non-existent columns on first run
   - Or use `if execute and this.exists` pattern properly
   - Focus on tableonly models first

3. **Verify Column Mappings**
   - Match actual DuckDB column names to model expectations
   - Verify all casts are correct for each source table schema

4. **Test Model Execution**
   - Run `dbt parse` to check for syntax errors
   - Run `dbt run -s stg_kaggle_circuits` for individual tests
   - Run full `dbt run` suite after fixes

## Database State

### DuckDB Schemas
```
- processed_kaggle: 12 tables (Kaggle historical data)
- processed_jolpica: 1 table (Jolpica results)
- processed_openf1: 4 tables (OpenF1 telemetry)
- raw_kaggle: EMPTY (legacy, can be deleted)
- information_schema: System tables
- pg_catalog: PostgreSQL compatibility
```

### File Location
```
C:\Satva\Tech\ETL_F1\f1-pipeline\warehouse\dbt\f1_analytics.duckdb
Size: 13 MB
Rows: 691,086
```

## Status Summary

| Component | Status | Notes |
|-----------|--------|-------|
| Data Loading | ✅ Complete | All 3 sources in DuckDB |
| dbt Config | ✅ Complete | Parse succeeds, no syntax errors |
| Sources.yml | ✅ Complete | Schemas corrected, sources defined |
| SQL Functions | ✅ Complete | now() replacement done, macros removed |
| Model Casting | ⚠️ Partial | Data type issues in models |
| dbt Run | ⚠️ Failing | 7 errors, 7 successes in initial run |

## Key Learnings

1. **DuckDB is more strict** with type conversions than BigQuery
2. **String NULL handling** - DuckDB may store '\N' as literal string  
3. **Incremental models** need careful first-run handling
4. **Schema organization** - Data already split into 3 schemas by source
5. **Sources.yml** - Points to DuckDB schemas, not S3 external tables

---

**Last Updated**: Session completing migrations  
**Next Phase**: Debug and fix remaining casting/type issues
