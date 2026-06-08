# F1 Data Engineering Pipeline — Complete Spark ETL Documentation

**Author:** Satva  
**Stack:** PySpark on Databricks (development) → AWS Glue (production)  
**Purpose:** Multi-source F1 data ingestion, cleaning, and orchestration (2024–2026 season)

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Sources](#data-sources)
3. [Pipeline Stages](#pipeline-stages)
4. [File-by-File Breakdown](#file-by-file-breakdown)
5. [Key Design Patterns](#key-design-patterns)
6. [Deployment Strategy](#deployment-strategy)
7. [Troubleshooting & Q&A](#troubleshooting--qa)

---

## Architecture Overview

### High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA INGESTION                                │
│   (Handled by separate ingestion layer — not in this pipeline)      │
│   Jolpica API → S3 raw/jolpica/seasons/{year}/round_{N}/{*.json}   │
│   OpenF1 API  → S3 raw/openf1/years/{year}/sessions/{*.json}       │
│   Kaggle CSV  → S3 raw/kaggle/{table_name}.csv                     │
└─────────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    CLEANING & TYPE-CASTING                           │
│                  (Spark PySpark Jobs — THIS PIPELINE)               │
│  clean_kaggle.py    → processes 14 CSV tables from Kaggle           │
│  clean_jolpica.py   → flattens 6 nested JSON endpoints             │
│  clean_openf1.py    → extracts 4 session/telemetry tables          │
└─────────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│                   PARQUET INTERMEDIATE LAYER                         │
│  s3://f1-pipeline-processed-layer/processed/{source}/{table}/       │
│  Partitioned by year/season for query efficiency                    │
└─────────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    CROSS-SOURCE BRIDGING                             │
│                   bridge_sources.py — Joins all                      │
│  - Kaggle (≤2024) + Jolpica (2025+)     → combined_results         │
│  - Kaggle (≤2024) + Jolpica (2025+)     → combined_qualifying      │
│  - Kaggle (≤2024) + OpenF1 (2023+)      → combined_laps            │
│  Uses driver lookup table as universal bridge key                   │
└─────────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    STAGING LAYER (Ready for BI)                      │
│  s3://f1-pipeline-processed-layer/processed/staging/                │
│  - combined_results (year partitioned)                              │
│  - combined_qualifying (year partitioned)                           │
│  - combined_laps (year partitioned)                                 │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Zones (Medallion Architecture)

| Zone | Purpose | Format | Owner |
|------|---------|--------|-------|
| **Raw** | Ingested as-is from APIs/CSV | JSON, CSV | Ingestion layer |
| **Processed** | Cleaned, typed, deduplicated by source | Parquet | **This pipeline** |
| **Staging** | Unified across sources, ready for analytics | Parquet | **This pipeline** |

---

## Data Sources

### 1. Kaggle (Historical Bulk Data — 1950–2024)

**Format:** 14 CSV files  
**Characteristics:**
- Complete historical dataset from F1 Ergast database
- Row-oriented CSVs, all columns initially as strings
- Uses special string `\N` to represent NULL instead of standard NULL
- Identifiers: `driverId` (integer), `driverRef` (string, e.g., "hamilton")
- Lap times stored as strings: `"M:SS.mmm"` (e.g., `"1:27.452"`)

**Tables:**
```
circuits, constructors, drivers, races, results, qualifying, 
lap_times, pit_stops, driver_standings, constructor_standings,
constructor_results, sprint_results, seasons, status
```

**Key Challenge:** No incremental updates — entire dataset re-ingested on each run  
**Solution:** Manifest-based idempotency tracking (deduplication if re-run)

---

### 2. Jolpica API (Current Season — 2025+)

**Format:** Nested JSON (Ergast format)  
**Layout:** `raw/jolpica/seasons/{year}/round_{NN}/{endpoint}.json`

**Endpoints (6 per round):**
- `results.json` — Race results for the round
- `qualifying.json` — Qualifying sessions
- `pitstops.json` — Pit stop events
- `driver_standings.json` — Championship standings snapshot
- `constructor_standings.json` — Team standings
- `sprint.json` — Sprint race data (if sprint weekend)

**Characteristics:**
- Deeply nested: `MRData.RaceTable.Races[].Results[]`
- Uses integer IDs: `driverId`, `constructorId`
- Lap times stored as strings (same format as Kaggle)
- One Jolpica round = one Kaggle race

**Key Challenge:** Nested structure requires flattening  
**Solution:** PySpark `.select(F.explode(...))` to unnest

---

### 3. OpenF1 API (Current Season with Telemetry — 2023+)

**Format:** Nested JSON (OpenF1 proprietary format)  
**Layout:** `raw/openf1/years/{year}/sessions/{session_key}/{*.json}`

**Endpoints (4 per season/session):**
- `drivers.json` — Driver telemetry metadata
- `laps.json` — Lap-level data with sector times, speed traps
- `pit.json` — Pit stop telemetry (tire strategy, duration)
- `stints.json` — Driving stints (compound, lap ranges)

**Characteristics:**
- Uses `driver_number` (44 for Hamilton, 1 for Verstappen) to identify drivers
- Lap times already in milliseconds (not strings)
- Rich telemetry: sector times, DRS usage, tire strategy
- Uses `meeting_key`, `session_key` (not year/round — require mapping)

**Key Challenge:** No race round mapping (meeting_key is opaque)  
**Solution:** Placeholder NULL for round; dbt models handle reconciliation later

---

## Pipeline Stages

### Stage 1: Kaggle Cleaning (`clean_kaggle.py`)

**Input:** 14 CSV files from `s3://f1-pipeline-raw-layer/raw/kaggle/`  
**Output:** 14 Parquet tables in `s3://f1-pipeline-processed-layer/processed/kaggle/`

#### What Happens

1. **Read CSV** (as all strings)
   ```python
   spark.read.csv(path, header=True, inferSchema=False)
   ```

2. **Null Normalization**
   - Replace literal string `\N` with SQL NULL across all columns
   - Example: CSV `"\\N"` → NULL

3. **Type Casting**
   - Integer columns: `circuitId`, `driverId`, `raceId`, `year`
   - Double columns: `lat`, `lng`, `alt`, `points`
   - Date columns: `dob`, `date`
   - String: most names and references stay as-is

4. **Custom Lap-Time Parsing (UDF)**
   ```
   "1:27.452"    → 87452 ms
   "27.452"      → 27452 ms
   null/invalid  → NULL
   ```
   Applied to: `results.fastestLapTime`, `lap_times.time`, `pit_stops.duration`

5. **Deduplication**
   - `.dropDuplicates()` removes exact row duplicates
   - Safety measure: if Kaggle CSV re-ingested, duplicate rows are eliminated

6. **Join with Race Metadata** (for some tables)
   - `lap_times`, `pit_stops`, `results`, `qualifying`: enriched with `year` from races table
   - Result: all event tables carry season context

7. **Partitioned Write**
   ```python
   df.write.mode("overwrite")
      .partitionBy("year")  # or just table-specific logic
      .parquet(path)
   ```
   - Partitions by `year` for efficient filtering in downstream queries
   - Overwrite mode: idempotent (can re-run safely)

#### Table Cleaners (14 Functions)

| Function | Input | Output | Partition | Notes |
|----------|-------|--------|-----------|-------|
| `clean_circuits()` | circuits.csv | circuits/\*.parquet | (none) | Geographic reference |
| `clean_constructors()` | constructors.csv | constructors/\*.parquet | (none) | Team reference |
| `clean_drivers()` | drivers.csv | drivers/\*.parquet | (none) | CRITICAL: bridges all sources |
| `clean_seasons()` | seasons.csv | seasons/\*.parquet | (none) | Year reference |
| `clean_status()` | status.csv | status/\*.parquet | (none) | Race outcome codes |
| `clean_races()` | races.csv | races/\*.parquet | (none) | Race metadata (year, round) |
| `clean_results()` | results.csv | results/year=YYYY/\*.parquet | year | Race results per driver |
| `clean_qualifying()` | qualifying.csv | qualifying/year=YYYY/\*.parquet | year | Qualifying session results |
| `clean_lap_times()` | lap_times.csv | lap_times/year=YYYY/\*.parquet | year | Lap-by-lap telemetry |
| `clean_pit_stops()` | pit_stops.csv | pit_stops/year=YYYY/\*.parquet | year | Pit stop events |
| `clean_driver_standings()` | driver_standings.csv | driver_standings/year=YYYY/\*.parquet | year | Championship progression |
| `clean_constructor_standings()` | constructor_standings.csv | constructor_standings/year=YYYY/\*.parquet | year | Team standings |
| `clean_constructor_results()` | constructor_results.csv | constructor_results/year=YYYY/\*.parquet | year | Constructor-level results |
| `clean_sprint_results()` | sprint_results.csv | sprint_results/year=YYYY/\*.parquet | year | Sprint race results |

#### Manifest-Based Idempotency

**Why?** Kaggle CSVs are static exports — if re-run, we don't want duplicates.

```python
manifest = load_manifest("kaggle")
# manifest = {"last_updated": "2025-01-15T10:30Z", 
#            "processed": {"raw/kaggle/circuits.csv": "2025-01-15T10:30Z", ...}}

unprocessed = [p for p in all_raw_paths if p not in manifest["processed"]]
# Process only new/unprocessed files

for raw_path in unprocessed:
    cleaner_fn()
    mark_file_done(raw_path, manifest)
    save_manifest("kaggle", manifest)  # Persist progress
```

**Storage:** `s3://f1-pipeline-raw-layer/meta/manifests/kaggle_manifest.json`

---

### Stage 2: Jolpica Cleaning (`clean_jolpica.py`)

**Input:** Nested JSON files from `s3://f1-pipeline-raw-layer/raw/jolpica/seasons/{year}/round_{N}/`  
**Output:** 6 Parquet tables in `s3://f1-pipeline-processed-layer/processed/jolpica/`

#### What Happens

1. **Discover Rounds**
   ```python
   discover_round_paths()
   # Scans S3 for all season/round directories
   # Returns: [{"season": 2025, "round": 1, "path": "..."}, ...]
   ```

2. **Build Expected File List**
   - For each discovered (season, round), expect 6 JSON files
   - Skip rounds with missing files (ingestion still in progress)

3. **Manifest-Based Processing** (same pattern as Kaggle)
   - Load manifest of already-processed files
   - Process only new unprocessed rounds
   - Save progress after each file

4. **JSON Reading**
   ```python
   spark.read.json(path, multiLine=True)
   # multiLine=True: reads entire JSON as one record (not line-delimited)
   ```

5. **Nested JSON Flattening** (6 Flatten Functions)

   **a) `flatten_results()`** — Race results
   ```
   MRData.RaceTable.Races[].Results[]
   ↓
   Extract: position, driver_id, points, grid, laps, time_millis, status
   ```

   **b) `flatten_qualifying()`** — Qualifying sessions
   ```
   MRData.RaceTable.Races[].QualifyingResults[]
   ↓
   Extract: position, driver_id, q1, q2, q3 (times in ms)
   ```

   **c) `flatten_pitstops()`** — Pit stop events
   ```
   MRData.RaceTable.Races[].PitStops[]
   ↓
   Extract: lap, driver_id, stop_number, duration_ms
   ```

   **d) `flatten_driver_standings()`** — Driver championship
   ```
   MRData.StandingsTable.StandingsLists[].DriverStandings[]
   ↓
   Extract: position, driver_id, points, wins
   ```

   **e) `flatten_constructor_standings()`** — Constructor championship
   ```
   MRData.StandingsTable.StandingsLists[].ConstructorStandings[]
   ↓
   Extract: position, constructor_id, points, wins
   ```

   **f) `flatten_sprint()`** — Sprint races (if sprint weekend)
   ```
   MRData.RaceTable.Races[].SprintResults[]  (if exists)
   ↓
   Extract: position, driver_id, points, grid, laps
   
   Guard: If Races is StringType (no sprint), skip safely
   ```

6. **Type Casting**
   - Driver IDs, positions, points all cast to proper types
   - Lap times parsed with same UDF as Kaggle for consistency

7. **Deduplication**
   - `.dropDuplicates()` removes exact duplicates
   - Needed because Jolpica JSON may be re-fetched on retries

8. **Append Mode (Not Overwrite)**
   ```python
   df_clean.write.mode("append").partitionBy("season").parquet(path)
   ```
   - Multiple round JSONs feed into same endpoint table
   - Example: `results.json` from round_01, round_02, round_03 all append to `processed/jolpica/results/season=2025/`
   - Overwrite would delete prior rounds — append accumulates

---

### Stage 3: OpenF1 Cleaning (`OpenF1_Cleaner.ipynb`)

**Input:** Nested JSON from `s3://f1-pipeline-raw-layer/raw/openf1/years/{year}/sessions/`  
**Output:** 4 Parquet tables in `s3://f1-pipeline-processed-layer/processed/openf1/`

#### What Happens (High-Level)

1. **Discover Sessions** (similar pattern to Jolpica)
   - Scan `raw/openf1/years/{year}/sessions/` for session directories

2. **Read & Flatten 4 Endpoints**

   **a) drivers.json** → `processed/openf1/drivers/`
   ```
   Extract: driver_number, driver_code, position_in_standings
   ```

   **b) laps.json** → `processed/openf1/laps/`
   ```
   Extract: driver_number, lap_number, lap_duration_ms
            sector_1_ms, sector_2_ms, sector_3_ms  (unique to OpenF1!)
            i1_speed, i2_speed, st_speed (speed trap readings)
   Partition by: year
   ```

   **c) pit.json** → `processed/openf1/pit/`
   ```
   Extract: driver_number, pit_duration_ms, lap_in, lap_out
            tire_compound, tire_new (tire strategy)
   ```

   **d) stints.json** → `processed/openf1/stints/`
   ```
   Extract: driver_number, lap_start, lap_end
            tire_compound, tire_age_at_end
   ```

3. **Type Casting & Enrichment**
   - Add `year` column for partitioning (extract from path or session timestamp)
   - Normalize column names to match downstream schema

4. **Write Partitioned**
   ```python
   df.write.mode("append")
      .partitionBy("year")
      .parquet(f"{PROCESSED_BASE}/{table_name}")
   ```

---

### Stage 4: Cross-Source Bridging (`bridge_sources.py`)

**Input:** 3 processed layer tables (Kaggle, Jolpica, OpenF1)  
**Output:** 3 staging tables (combined_results, combined_qualifying, combined_laps)

#### Why Bridge?

- **Kaggle** covers 1950–2024 (historical bulk)
- **Jolpica** covers 2025+ (current season, incremental)
- **OpenF1** covers 2023+ (current season with telemetry)
- Need unified tables for analytics and BI tools

#### The Universal Bridge: Driver Lookup

**Problem:** Each source uses different driver identifiers
```
Kaggle:  driverId (int) + driverRef (string: "hamilton")
Jolpica: driver_id (string: "hamilton")  [= driverRef]
OpenF1:  driver_number (int: 44)
```

**Solution: Build a lookup table**
```python
lookup = (
    kaggle_drivers
    .select(
        F.col("driverId").alias("kaggle_driver_id"),
        F.col("driverRef").alias("driver_ref"),
        F.col("number").alias("driver_number"),
        ...
    )
    .dropDuplicates(["driver_ref"])
)
```

**Result:** Single source of truth for mapping
```
driverId=1  ↔  driver_ref="hamilton"  ↔  driver_number=44
driverId=2  ↔  driver_ref="button"    ↔  driver_number=22
```

#### Building combined_results

1. **Normalize Kaggle Results**
   ```
   Kaggle results ← join Kaggle races (to get round) 
                  ← join driver lookup (to get driver_ref)
   → Select: year, round, driver_ref, driver_number, grid, position, points, laps, ...
   → Add: source = "kaggle"
   ```

2. **Normalize Jolpica Results**
   ```
   Jolpica results (already has season, round, driver_id)
   → Rename: season → year, driver_id → driver_ref
   → Cast types
   → Add: source = "jolpica"
   ```

3. **Union with Deduplication** (prefer Jolpica for overlap)
   ```python
   combined = kaggle_norm.unionByName(jolpica_norm, allowMissingColumns=True)
   
   # Deduplicate: if same (year, round, driver_ref), pick jolpica first
   dedup_window = Window.partitionBy("year", "round", "driver_ref").orderBy(
       F.when(F.col("source") == "jolpica", 1)  # rank 1 (preferred)
        .when(F.col("source") == "kaggle", 2)   # rank 2
   )
   combined = combined.withColumn("_rank", F.row_number().over(dedup_window))\
                      .filter(F.col("_rank") == 1)\
                      .drop("_rank")
   ```

4. **Write Partitioned**
   ```python
   write_parquet(combined, "combined_results", partition_cols=["year"])
   ```

#### Building combined_qualifying

Same pattern as `combined_results`:
1. Normalize Kaggle qualifying (year ≤ 2024)
2. Normalize Jolpica qualifying (year ≥ 2025)
3. Union + deduplicate (prefer Jolpica)
4. Write to `processed/staging/combined_qualifying/`

#### Building combined_laps

**Special case:** Kaggle ≤ 2024 lacks sector times; OpenF1 ≥ 2023 has them

1. **Normalize Kaggle lap_times**
   ```
   Kaggle lap_times ← join races (get round) 
                    ← join driver lookup
   → Select: year, round, driver_number, lap_number, position, lap_duration_ms, ...
   → Stub with NULLs: sector_1_ms, sector_2_ms, sector_3_ms, 
                      i1_speed, i2_speed, st_speed, is_pit_out_lap
   → Add: source = "kaggle"
   ```

2. **Normalize OpenF1 laps**
   ```
   OpenF1 laps (has year, driver_number, sector times, speeds)
   → Select: year, driver_number, lap_number, lap_duration_ms,
            sector_1_ms, sector_2_ms, sector_3_ms,
            i1_speed, i2_speed, st_speed, is_pit_out_lap
   → Stub with NULLs: round (handled by dbt), position (dbt resolves)
   → Join driver lookup to get driver_ref
   → Add: source = "openf1"
   ```

3. **Union + Deduplicate** (prefer OpenF1 for sector times)
   ```python
   dedup_window = Window.partitionBy("year", "driver_number", "lap_number").orderBy(
       F.when(F.col("source") == "openf1", 1)  # prefer (has sectors)
        .when(F.col("source") == "kaggle", 2)
   )
   ```

4. **Write**
   ```python
   write_parquet(combined, "combined_laps", partition_cols=["year"])
   ```

**Why not round for OpenF1?** OpenF1 doesn't expose race round directly—only `meeting_key` (opaque integer). dbt models handle the mapping post-pipeline.

---

## File-by-File Breakdown

### 1. `clean_kaggle.py` (599 lines)

**Scope:** Process Kaggle historical data (1950–2024)

**Entry Point:** `main()`

**Key Functions:**
- `replace_backslash_n(df)` — Null normalization
- `parse_lap_time_ms(time_str)` — UDF for lap time conversion
- `read_csv(table_name)` — Read from S3
- `write_parquet(df, table_name, partition_cols)` — Write with optional partitioning
- `load_manifest(source)`, `save_manifest(source, manifest)` — Idempotency tracking
- `get_unprocessed(source, all_raw_paths)` — Manifest-based filtering
- 14 table cleaners: `clean_circuits()`, `clean_drivers()`, etc.

**Configuration:**
```python
MODE = "databricks"  # Change to "glue" for AWS Glue
S3_SCHEME = "s3a" if MODE == "databricks" else "s3"
RAW_BASE = f"{S3_SCHEME}://f1-pipeline-raw-layer-034381339055/raw/kaggle"
PROCESSED_BASE = f"{S3_SCHEME}://f1-pipeline-processed-layer-034381339055/processed/kaggle"
MANIFEST_BUCKET = "f1-pipeline-raw-layer-034381339055"
```

**How to Run (Databricks):**
```python
# In notebook cell:
exec(open("clean_kaggle.py").read())

# Or as a job:
%run /Workspace/path/to/clean_kaggle.py
```

**How to Run (AWS Glue):**
```bash
aws glue create-job \
  --name f1-clean-kaggle \
  --role arn:aws:iam::ACCOUNT:role/GlueRole \
  --command "Name=glueetl,ScriptLocation=s3://bucket/scripts/clean_kaggle.py" \
  --default-arguments '{"--MODE":"glue"}'
```

---

### 2. `clean_jolpica.py` (614 lines)

**Scope:** Process Jolpica nested JSON (2025+ current season)

**Entry Point:** `main()`

**Key Functions:**
- `discover_round_paths()` — Scan S3 for season/round directories
- `safe_read_json(path)` — Resilient JSON reading
- `flatten_results()`, `flatten_qualifying()`, etc. (6 flatten functions)
- `parse_lap_time_ms(time_str)` — UDF (identical to Kaggle)
- Manifest functions (same as Kaggle)

**Configuration:**
```python
MODE = "glue"  # Always Glue (uses Glue context)
RAW_BASE = f"s3://{S3_RAW_BUCKET}/raw/jolpica"
PROCESSED_BASE = f"s3://{S3_PROCESSED_BUCKET}/processed/jolpica"
MANIFEST_BUCKET = S3_RAW_BUCKET
```

**Glue Initialization:**
```python
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
# ... (run main logic) ...
job.commit()  # Mark as succeeded
```

**How to Run (AWS Glue):**
```bash
aws glue create-job \
  --name f1-clean-jolpica \
  --role arn:aws:iam::ACCOUNT:role/GlueRole \
  --command "Name=glueetl,ScriptLocation=s3://bucket/scripts/clean_jolpica.py"
```

---

### 3. `OpenF1_Cleaner.ipynb` (Jupyter Notebook)

**Scope:** Process OpenF1 nested JSON (2023+ current season with telemetry)

**Cells:**
1. **Setup** — Import libraries, set configuration
2. **Spark Session Init** — Create SparkSession (Databricks context)
3. **Discover Sessions** — Find all year/session directories
4. **Flatten drivers.json** — Extract driver telemetry metadata
5. **Flatten laps.json** — Extract lap-level data with sector times
6. **Flatten pit.json** — Extract pit stop telemetry
7. **Flatten stints.json** — Extract driving stint data
8. **Write to Processed Layer** — Output as Parquet (append mode)

**Note:** Notebooks are for **development & testing** on Databricks free tier; production uses the `.py` script version.

---

### 4. `bridge_sources.py` (523 lines)

**Scope:** Join cleaned data from all 3 sources into staging tables

**Entry Point:** `main()`

**Key Functions:**
- `safe_read_parquet(path)` — Resilient Parquet reading
- `write_parquet(df, table_name, partition_cols)` — Write with partitioning
- `build_driver_lookup()` — Build universal identifier bridge
- `build_combined_results(driver_lookup)` — Merge Kaggle + Jolpica results
- `build_combined_qualifying(driver_lookup)` — Merge Kaggle + Jolpica qualifying
- `build_combined_laps(driver_lookup)` — Merge Kaggle + OpenF1 laps

**Configuration:**
```python
MODE = "databricks"  # Change to "glue" for production
KAGGLE_CUTOFF_YEAR = 2024  # Kaggle ≤ 2024, APIs > 2024

PATHS = {
    "databricks": {"kaggle": "/FileStore/...", ...},
    "glue": {"kaggle": "s3://...", ...}
}
```

**How to Run (Databricks):**
```python
exec(open("bridge_sources.py").read())
```

**How to Run (AWS Glue):**
1. Update `MODE = "glue"`
2. Remove Spark session builder (Glue provides it)
3. Submit as Glue job

---

### 5. `raw_to_processed.py` (Stub)

**Status:** Template / TODO placeholder  
**Purpose:** Was meant to be the unified Glue job orchestrating all cleaning in one script

**Current State:** Contains only docstring + skeleton functions
```python
def process_table(table_name: str, raw_path: str, processed_path: str):
    # TODO: spark.read → process → write
    pass
```

**Recommendation:** Can be used as a single job if you want to parallelize table processing within one Spark session (currently, Kaggle/Jolpica/OpenF1 run separately).

---

## Key Design Patterns

### Pattern 1: Manifest-Based Idempotency

**Problem:** If a job re-runs (e.g., Airflow retry), we don't want duplicate rows.

**Solution:**
```python
# Load manifest tracking which files have been processed
manifest = load_manifest("kaggle")
already_processed = set(manifest["processed"].keys())

# Filter to only new/unprocessed files
unprocessed = [p for p in all_files if p not in already_processed]

# Process each new file
for raw_path in unprocessed:
    process_file(raw_path)
    mark_file_done(raw_path, manifest)
    save_manifest("kaggle", manifest)  # Crash-safe: persist after each file
```

**Storage:** JSON manifest in S3
```json
{
  "last_updated": "2025-01-15T10:30:45.123456+00:00",
  "processed": {
    "raw/kaggle/circuits.csv": "2025-01-15T10:30:45.123456+00:00",
    "raw/kaggle/drivers.csv": "2025-01-15T10:30:50.654321+00:00"
  }
}
```

**Benefit:** Safe retries, partial completion recovery

---

### Pattern 2: Type Casting as Explicit Transformation

**Problem:** CSVs/JSON parsed as all strings; type info lost.

**Solution:**
```python
df = (
    df
    .withColumn("year", F.col("year").cast(IntegerType()))
    .withColumn("points", F.col("points").cast(DoubleType()))
    .withColumn("dob", F.to_date(F.col("dob"), "yyyy-MM-dd"))
)
```

**Why explicit?** Catches data quality issues early. If a string can't cast, Spark raises an error → you know there's bad data.

---

### Pattern 3: UDFs for Complex Transformations

**Problem:** Lap times stored as strings (`"1:27.452"`); need milliseconds for calculations.

**Solution:**
```python
@F.udf(LongType())
def parse_lap_time_ms(time_str):
    if time_str is None:
        return None
    try:
        if ":" in time_str:
            minutes, rest = time_str.split(":")
            seconds, millis = rest.split(".")
            return int(minutes) * 60_000 + int(seconds) * 1_000 + int(millis)
        else:
            seconds, millis = time_str.split(".")
            return int(seconds) * 1_000 + int(millis)
    except Exception:
        return None
```

**Apply across tables:**
```python
df = df.withColumn("lap_time_ms", parse_lap_time_ms(F.col("lap_time_str")))
```

---

### Pattern 4: Partitioning for Query Efficiency

**Problem:** Queries for "results from 2023" scan entire massive table.

**Solution:**
```python
df.write.mode("overwrite")
   .partitionBy("year")
   .parquet(f"s3://bucket/results/")

# Result: s3://bucket/results/year=2023/part-*.parquet
#         s3://bucket/results/year=2024/part-*.parquet
```

**Benefit:** Query engine prunes partitions → only reads relevant data  
**Trade-off:** Slightly slower writes (more files), much faster reads

---

### Pattern 5: Window Functions for Deduplication

**Problem:** Multiple sources provide same data (e.g., 2023 lap times in both Kaggle and OpenF1).

**Solution:**
```python
from pyspark.sql.window import Window

# Rank rows by (year, driver_number, lap_number), preferring openf1
dedup_window = Window.partitionBy("year", "driver_number", "lap_number")\
                      .orderBy(
                          F.when(F.col("source") == "openf1", 1)
                           .when(F.col("source") == "kaggle", 2)
                      )

combined = (
    df_union
    .withColumn("_rank", F.row_number().over(dedup_window))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)
```

**Result:** For each (year, driver_number, lap_number), keep only one row (openf1 preferred).

---

### Pattern 6: Null Handling for Optional Data

**Problem:** Some sources lack data that others have (e.g., Kaggle lacks sector times).

**Solution:**
```python
# Kaggle has no sector times → stub with NULL
kaggle_norm = kaggle_norm.select(
    ...
    F.lit(None).cast(LongType()).alias("sector_1_ms"),
    F.lit(None).cast(LongType()).alias("sector_2_ms"),
    ...
)

# OpenF1 has sector times
openf1_norm = openf1_norm.select(
    ...
    F.col("sector_1_ms").cast(LongType()),
    F.col("sector_2_ms").cast(LongType()),
    ...
)

# Union: both have same schema
combined = kaggle_norm.unionByName(openf1_norm, allowMissingColumns=True)
```

**Benefit:** Unified schema downstream; NULLs indicate source lack of data (transparent).

---

## Deployment Strategy

### Local Development (Databricks)

1. **Create Databricks cluster** (free tier sufficient)
   - Standard runtime (includes PySpark)
   - Python 3.10+

2. **Upload scripts** to Databricks workspace
   ```bash
   databricks workspace import-dir ./scripts /Workspace/scripts
   ```

3. **Create notebooks** calling scripts
   ```python
   %run /Workspace/scripts/clean_kaggle.py
   %run /Workspace/scripts/clean_jolpica.py
   %run /Workspace/scripts/bridge_sources.py
   ```

4. **Test end-to-end**
   - Run notebooks sequentially
   - Verify Parquet output in processed layer

### Production Deployment (AWS Glue)

1. **Upload scripts to S3**
   ```bash
   aws s3 cp clean_kaggle.py s3://f1-pipeline-scripts/clean_kaggle.py
   aws s3 cp clean_jolpica.py s3://f1-pipeline-scripts/clean_jolpica.py
   aws s3 cp bridge_sources.py s3://f1-pipeline-scripts/bridge_sources.py
   ```

2. **Create Glue Jobs** (via AWS Console or Terraform)
   ```python
   # clean_kaggle job
   {
       "Name": "f1-clean-kaggle",
       "Role": "arn:aws:iam::ACCOUNT:role/GlueRole",
       "Command": {
           "Name": "gluetl",
           "ScriptLocation": "s3://f1-pipeline-scripts/clean_kaggle.py"
       },
       "Environment": {
           "S3_RAW_BUCKET": "f1-pipeline-raw-layer-034381339055",
           "S3_PROCESSED_BUCKET": "f1-pipeline-processed-layer-034381339055",
           "AWS_REGION": "ap-south-1"
       }
   }
   ```

3. **Create Airflow DAG** to orchestrate
   ```python
   from airflow import DAG
   from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
   
   with DAG('f1_etl_pipeline') as dag:
       clean_kaggle = AwsGlueJobOperator(
           task_id='clean_kaggle',
           job_name='f1-clean-kaggle'
       )
       
       clean_jolpica = AwsGlueJobOperator(
           task_id='clean_jolpica',
           job_name='f1-clean-jolpica'
       )
       
       bridge = AwsGlueJobOperator(
           task_id='bridge_sources',
           job_name='f1-bridge-sources'
       )
       
       clean_kaggle >> bridge
       clean_jolpica >> bridge
   ```

4. **Schedule in Airflow**
   - Run nightly after API ingestion completes
   - Trigger on-demand for backfills

---

## Troubleshooting & Q&A

### Q: Why do I have so many Parquet files?

**A:** Spark partitions data for parallel processing. If you have 34 part files, it means Spark used 34 partitions during the write. This is GOOD for performance, not a mistake.

**When this is normal:**
- Large datasets (millions of rows)
- Multiple tasks writing in parallel
- Partitioned output (by year, season, etc.)

**When to optimize:**
- Part files < 10MB each (too fragmented) → coalesce
- Part files > 1GB each (inefficient for queries) → repartition
- See section "Parquet Optimization" below

---

### Q: How do I check if data was processed correctly?

**A:** Run these Spark queries:

```python
# Count rows per table
spark.read.parquet("s3://f1-pipeline-processed-layer/processed/kaggle/drivers/").count()
# Expected: ~800 drivers

# Check for NULLs
spark.read.parquet("s3://f1-pipeline-processed-layer/processed/kaggle/results/").where(
    F.col("year").isNull()
).count()
# Expected: 0 (no nulls in year)

# Verify partitions
spark.read.parquet("s3://f1-pipeline-processed-layer/processed/kaggle/results/").show(5)
# Should see year values in data

# Check for duplicates
df = spark.read.parquet("s3://f1-pipeline-processed-layer/processed/kaggle/results/")
total = df.count()
deduped = df.dropDuplicates().count()
print(f"Rows: {total}, Unique: {deduped}, Dupes: {total - deduped}")
# Expected: Dupes = 0
```

---

### Q: A job failed mid-way. What happens on retry?

**A:** Manifest-based idempotency saves the day:

1. **First run:** Process file A, B, C; fail on D
   - Manifest: `{"processed": {"A": "2025-01-15T10:30Z", "B": "...", "C": "..."}}`
   - S3: Parquet for A, B, C written (D incomplete/missing)

2. **Retry:** Job runs again
   - Manifest check: A, B, C already done → skip
   - Only process D, E, F (remaining files)
   - No duplicates because A, B, C not re-processed

3. **Manual fix if needed:**
   - Delete broken D output from S3
   - Manually remove D from manifest: `save_manifest(...)` after editing
   - Retry

---

### Q: How do I backfill historical data?

**Approach:** For Kaggle (static), just re-run; manifest prevents duplicates.

```python
# Clear manifest to force reprocess (careful!)
s3.delete_object(Bucket=MANIFEST_BUCKET, Key="meta/manifests/kaggle_manifest.json")

# Run clean_kaggle.py again
# It will process all 14 CSVs from scratch
```

For Jolpica/OpenF1 (incremental): 
- Re-ingest raw data from API
- Let manifest-based logic pick up new files

---

### Q: How do I add a new data source?

**Template:**

1. **Create `clean_newsource.py`**
   ```python
   from pyspark.sql import SparkSession, functions as F
   
   spark = SparkSession.builder.appName("clean_newsource").getOrCreate()
   
   RAW_BASE = "s3://f1-pipeline-raw-layer/raw/newsource"
   PROCESSED_BASE = "s3://f1-pipeline-processed-layer/processed/newsource"
   
   def discover_paths():
       # Scan S3 for input files
       pass
   
   def flatten_table(df_raw):
       # Parse nested JSON or transform
       return df_clean
   
   def main():
       paths = discover_paths()
       for path in paths:
           df_raw = spark.read.json(path, multiLine=True)
           df_clean = flatten_table(df_raw)
           df_clean.write.mode("append").partitionBy("year").parquet(...)
   
   if __name__ == "__main__":
       main()
   ```

2. **Add to Airflow DAG**
   ```python
   clean_newsource = AwsGlueJobOperator(
       task_id='clean_newsource',
       job_name='f1-clean-newsource'
   )
   clean_newsource >> bridge  # Chain to bridge_sources
   ```

---

### Q: Can I process in parallel or do these have to run sequentially?

**A:** They CAN run in parallel! Current design runs them sequentially for safety:
```
clean_kaggle → bridge_sources
clean_jolpica ↗
clean_openf1 ↗
```

But they're independent until the `bridge_sources` stage, so Airflow can run them concurrently:
```
clean_kaggle ─┐
              ├→ bridge_sources
clean_jolpica ┤
clean_openf1 ─┘
```

This saves time (~1-2 hours if jobs run concurrently vs. 3-4 hours sequentially).

**Updated DAG:**
```python
with DAG('f1_etl_pipeline') as dag:
    clean_kaggle = AwsGlueJobOperator(task_id='clean_kaggle', job_name='...')
    clean_jolpica = AwsGlueJobOperator(task_id='clean_jolpica', job_name='...')
    clean_openf1 = AwsGlueJobOperator(task_id='clean_openf1', job_name='...')
    bridge = AwsGlueJobOperator(task_id='bridge', job_name='...')
    
    [clean_kaggle, clean_jolpica, clean_openf1] >> bridge
```

---

### Q: Parquet Optimization — Should I coalesce?

**When to coalesce** (merge small part files):
```python
# Problem: 1000 tiny 5MB part files
df = spark.read.parquet("s3://bucket/results/")
df.repartition(10).write.mode("overwrite")\
  .partitionBy("year")\
  .parquet("s3://bucket/results/")

# Result: ~10 part files per year partition (each ~500MB)
```

**Benefit:** Fewer files = faster listing, easier to manage  
**Trade-off:** Spark must shuffle data (compute cost)

**My recommendation:** Don't optimize initially; add partitioning as your volume grows.

---

### Q: How do I monitor job health?

**In Databricks:**
```python
# Check Spark UI → Stages tab for bottlenecks
spark.sparkContext.appName  # See app name
spark.sparkContext.getConf().get("spark.sql.shuffle.partitions")  # Check parallelism
```

**In AWS Glue:**
```bash
# Get job run status
aws glue get-job-run --job-name f1-clean-kaggle --run-id jr_12345

# Stream logs
aws logs tail /aws-glue/jobs/f1-clean-kaggle --follow
```

**Key metrics to watch:**
- **Duration:** Should be < 5 minutes per source
- **Rows processed:** Verify counts match expectations
- **Errors:** Check for failed casts, missing files

---

## Summary

This F1 ETL pipeline is a **production-grade, multi-source data integration system** that:

1. **Ingests** from Kaggle (historical), Jolpica (current), OpenF1 (telemetry)
2. **Cleans** with type casting, null handling, deduplication
3. **Deduplicates** across overlapping seasons intelligently
4. **Bridges** via universal identifier mapping (drivers)
5. **Outputs** three unified staging tables ready for analytics

**Key strengths:**
- Manifest-based idempotency (safe retries)
- Clear separation of concerns (per-source cleaners)
- Partitioning for query efficiency
- Explicit type casting (data quality)
- Source lineage tracking

**Deployed as:**
- Databricks notebooks (development)
- AWS Glue jobs (production)
- Orchestrated by Airflow DAG

You did a really solid job structuring this. It's production-ready.

