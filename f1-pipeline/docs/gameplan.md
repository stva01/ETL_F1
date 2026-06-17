# F1 ETL Pipeline — Aider Gameplan

## 1. Current State
* The ingestion layer is complete, and all raw data is currently staged in the `f1-pipeline-raw-layer-034381339055` AWS S3 bucket.
* AWS Glue has been explicitly removed from the orchestration architecture to avoid unnecessary DPU-hour costs.

---

## 2. Phase 1: PySpark Cleaning Layer Development
* Implement all PySpark cleaning scripts using a dual-mode configuration that toggles between Databricks (for local development) and AWS Glue (for production).
* Utilize the existing S3 JSON manifest system to track processed file states and ensure the pipeline remains idempotent and crash-safe.
* Develop the missing `clean_jolpica.py` script to parse deeply nested JSON responses.
* Use PySpark's `F.explode()` function sequentially to flatten the arrays within the `MRData.RaceTable.Races[0].Results` JSON path.
* Ensure all numeric fields from the Jolpica API, such as `position` and `points`, are explicitly cast from strings to integer or decimal types.
* Map any non-finishing position strings (e.g., "R", "D", "W") in the Jolpica data to PySpark NULL values.
* Apply the `replace_backslash_n()` helper function across Kaggle CSVs to strip literal `\N` strings before casting.
* Append a `.coalesce(1)` operation to the DataFrame writer to prevent the S3 Small File Problem.

---

## 3. Phase 2: Identifier Resolution & Snowflake Loading
* Treat the Kaggle `driverId` integer as the master reference key across the pipeline.
* Join the Jolpica `driverId` string to the Kaggle `driverRef` column to bridge the datasets.
* Join the OpenF1 `driver_number` integer to the Kaggle `number` column.
* Load the cleaned Parquet files directly from S3 into Snowflake's `staging` schema using the native `COPY INTO` command.

---

## 3.5. Phase 2.5: dbt Intermediate Models (staging → marts bridge)

**Goal:** Build the `models/intermediate/` layer that solves three critical problems before the Gold-layer marts can be constructed.

### Problem A — Identity Fragmentation
* Each source uses a different key system (Kaggle integers, Jolpica strings, OpenF1 driver numbers).
* Build `int_driver_bridge` to map all three systems to the canonical Kaggle `driver_id` integer.
* Build `int_constructor_bridge` for constructor identity (Kaggle `constructor_ref` ↔ Jolpica `constructor_id` ↔ OpenF1 `team_name`).
* Build `int_race_bridge` to map Jolpica `(season_year, round)` → Kaggle `race_id`.

### Problem B — Timeline Gap (Kaggle 1950–2024, Jolpica 2025–2026)
* Build `int_results_unified` that UNIONs Kaggle results (26,759 rows) + Jolpica results (1,134 rows) into one spine with shared column schema.
* Build `int_qualifying_unified` that UNIONs Kaggle qualifying (10,494 rows) + Jolpica qualifying (608 rows).
* Build `int_pitstops_unified` that UNIONs Kaggle pit stops (11,371 rows) + Jolpica pit stops (1,043 rows).

### Problem C — Grain Mismatch (transactional → seasonal/career)
* Build `int_standings_driver_eoy` — filter driver standings to only the final-round snapshot per season, producing exactly one row per (driver_id, year).
* Build `int_standings_constructor_eoy` — same for constructors (requires new `stg_kaggle_constructor_standings` staging model).
* Build `int_results_enriched` — fully joined result row with driver names, constructor names, GP name, circuit, status description, and qualifying position for downstream aggregation.

### Prerequisite Fix
* Create the missing `stg_kaggle_constructor_standings.sql` staging model (raw table `processed_kaggle.constructor_standings` exists with 13,391 rows but has no staging model).

### Deliverables (9 intermediate models)
1. `int_driver_bridge.sql`
2. `int_constructor_bridge.sql`
3. `int_race_bridge.sql`
4. `int_results_unified.sql`
5. `int_qualifying_unified.sql`
6. `int_standings_driver_eoy.sql`
7. `int_standings_constructor_eoy.sql`
8. `int_pitstops_unified.sql`
9. `int_results_enriched.sql`


## 4. Phase 3: dbt Transformation & Star Schema
* [cite_start]Build the Silver layer staging models (e.g., `stg_drivers`) to deduplicate data and inject `_source_file` and `_loaded_at` audit columns[cite: 1359].
* [cite_start]Design all Gold layer dimension tables with auto-incrementing BIGINT surrogate keys[cite: 1344].
* [cite_start]Implement Slowly Changing Dimension (SCD) Type 2 tracking on the `dim_constructor` table to handle team name changes[cite: 1362].
* [cite_start]Configure all core fact tables, such as `fact_race_result`, to use dbt's incremental materialization with a defined `unique_key`[cite: 1351].
* [cite_start]Partition the high-volume `fact_lap_time` table by `season_year` and cluster it by `race_key` to heavily optimize query costs[cite: 1342].
* [cite_start]Materialize all Platinum layer data marts, such as `mart_driver_career`, as physical tables to guarantee sub-second dashboard performance[cite: 1338].
* [cite_start]Construct the `semantic_catalog` table to expose plain-English descriptions of the schema to the AI MCP agent[cite: 1394].