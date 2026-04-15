# Cleaning Scripts — Detailed Explanation

> **Scripts covered:**
> - `processing/glue_jobs/clean_kaggle.py` — 14 Kaggle CSV tables
> - `processing/glue_jobs/clean_openf1.py` — 4 OpenF1 JSON endpoints
>
> Both scripts share the same architecture: a dual-mode design (Databricks for development, AWS Glue for production) with an incremental manifest system that tracks which raw files have already been processed.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Shared Patterns](#2-shared-patterns)
   - [Mode Switching](#21-mode-switching)
   - [Manifest System](#22-manifest-system-glue-only)
3. [clean_kaggle.py — In Depth](#3-clean_kagglepy--in-depth)
   - [Imports & Configuration](#31-imports--configuration)
   - [Helper Functions](#32-helper-functions)
   - [Table Cleaners (14 tables)](#33-table-cleaners-14-tables)
   - [main() Execution Flow](#34-main-execution-flow)
4. [clean_openf1.py — In Depth](#4-clean_openf1py--in-depth)
   - [Imports & Configuration](#41-imports--configuration)
   - [Helper Functions](#42-helper-functions)
   - [Session Discovery](#43-session-discovery)
   - [Table Cleaners (4 endpoints)](#44-table-cleaners-4-endpoints)
   - [main() Execution Flow](#45-main-execution-flow)
5. [Key Differences Between the Two Scripts](#5-key-differences-between-the-two-scripts)
6. [How to Run](#6-how-to-run)
7. [SparkSession & Entry Point](#7-sparksession--entry-point)
8. [Error Handling](#8-error-handling)
9. [Why unionByName(allowMissingColumns=True)](#9-why-unionbynameallowmissingcolumnstrue)
10. [02_clean_openf1.py — Databricks CE Notebook Variant](#10-02_clean_openf1py--databricks-ce-notebook-variant)
11. [Output Schema Reference](#11-output-schema-reference)

---

## 1. Architecture Overview

Both scripts follow a **Read → Clean → Write** ETL pattern:

```mermaid
flowchart LR
    A[Raw Zone<br>CSV / JSON] -->|Read| B[PySpark DataFrame]
    B -->|Cast types<br>Deduplicate<br>Transform| C[Cleaned DataFrame]
    C -->|Write Parquet| D[Processed Zone]
    D -->|Manifest update| E[S3 Manifest JSON]
```

**Key design principles:**

| Principle | Implementation |
|---|---|
| **Dual-mode** | A single `MODE` variable switches between Databricks (local dev) and Glue (production) |
| **Idempotent** | Databricks uses `mode("overwrite")` — safe to re-run. Glue uses manifest to skip already-processed files |
| **Crash-safe** | Manifest is saved after *each* file, so a crash mid-run loses at most one file's progress |
| **No hardcoded creds** | Glue uses IAM role; Databricks doesn't need boto3 at all |

---

## 2. Shared Patterns

### 2.1 Mode Switching

Both scripts use an identical `PATHS` dict pattern:

```python
MODE = "databricks"  # "databricks" | "glue"

PATHS = {
    "databricks": {
        "raw":       "/FileStore/f1-pipeline/raw/kaggle",
        "processed": "/FileStore/f1-pipeline/processed/kaggle",
    },
    "glue": {
        "raw":       "s3://f1-raw-satva/raw/kaggle",
        "processed": "s3://f1-processed-satva/processed/kaggle",
    },
}

RAW_BASE       = PATHS[MODE]["raw"]
PROCESSED_BASE = PATHS[MODE]["processed"]
```

- **`MODE = "databricks"`** — Reads from DBFS paths, writes Parquet locally. No manifest, full reprocess every run.
- **`MODE = "glue"`** — Reads from S3, writes Parquet to S3. Uses manifest for incremental processing.

To deploy to Glue, you only need to:
1. Change `MODE` to `"glue"`
2. Remove the `SparkSession.builder` block (Glue provides `spark` automatically)

### 2.2 Manifest System (Glue Only)

The manifest is an incremental processing tracker stored in S3. It is **character-for-character identical** across both scripts so that adding a third source (jolpica) requires zero changes to the helpers.

#### Manifest File Structure

Stored at `s3://f1-pipeline-raw-layer-034381339055/meta/manifests/{source}_manifest.json`:

```json
{
  "last_updated": "2026-04-07T14:32:00+00:00",
  "processed": {
    "raw/kaggle/circuits.csv": "2026-04-07T14:30:00+00:00",
    "raw/kaggle/drivers.csv":  "2026-04-07T14:30:05+00:00"
  }
}
```

Each key in `processed` is an S3 object key. The value is the UTC timestamp when that file was successfully cleaned.

#### S3 Client (Lazy Initialization)

```python
_manifest_s3 = None

def _get_manifest_s3():
    """Lazy-init S3 client for manifest operations."""
    global _manifest_s3
    if _manifest_s3 is None:
        _manifest_s3 = boto3.client("s3")
    return _manifest_s3
```

The client is created once on first use, then reused. In Glue, `boto3.client("s3")` automatically uses the IAM execution role — no access keys needed.

#### The Four Manifest Functions

**`load_manifest(source)`** — Reads the manifest from S3. On the very first run the file doesn't exist yet, so it catches `NoSuchKey` and returns a fresh empty dict:

```python
def load_manifest(source):
    key = f"{MANIFEST_PREFIX}/{source}_manifest.json"
    try:
        obj = _get_manifest_s3().get_object(Bucket=MANIFEST_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {"last_updated": None, "processed": {}}
        raise
```

**`save_manifest(source, manifest)`** — Stamps `last_updated` with current UTC time and writes the manifest back to S3:

```python
def save_manifest(source, manifest):
    manifest["last_updated"] = datetime.now(timezone.utc).isoformat()
    key = f"{MANIFEST_PREFIX}/{source}_manifest.json"
    _get_manifest_s3().put_object(
        Bucket=MANIFEST_BUCKET,
        Key=key,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json",
    )
```

**`mark_file_done(s3_path, manifest)`** — Adds an entry to the in-memory manifest dict. This mutates the dict in-place (no S3 call yet):

```python
def mark_file_done(s3_path, manifest):
    manifest["processed"][s3_path] = datetime.now(timezone.utc).isoformat()
```

**`get_unprocessed(source, all_raw_paths)`** — The entry point. Loads the manifest, computes the set difference, and returns only the files that haven't been processed:

```python
def get_unprocessed(source, all_raw_paths):
    manifest = load_manifest(source)
    already = set(manifest["processed"].keys())
    unprocessed = [p for p in all_raw_paths if p not in already]
    print(f"  {source}: {len(already)} already processed, {len(unprocessed)} new")
    return unprocessed, manifest
```

#### Typical Call Sequence in `main()`

```
get_unprocessed()          ← one S3 GET (reads manifest)
    └── load_manifest()

for each file:
    cleaner_fn()           ← Spark read + clean + write
    mark_file_done()       ← mutate in-memory dict
    save_manifest()        ← one S3 PUT (writes manifest)
```

The `save_manifest` call is inside the loop — not after it. This means if the script crashes on file 8 of 14, the manifest already records files 1–7. The next run picks up from file 8.

---

## 3. clean_kaggle.py — In Depth

### 3.1 Imports & Configuration

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DoubleType, StringType, DateType,
)

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None       # Available in Glue; not needed in Databricks
    ClientError = None
import json
from datetime import datetime, timezone
```

**Why the try/except?** — In Databricks Community Edition, `boto3` may not be installed. Since the manifest code only runs in Glue mode, wrapping the import prevents `ImportError` from killing the script in Databricks.

### 3.2 Helper Functions

#### `replace_backslash_n(df)`

Kaggle CSV files use the literal string `\N` (backslash-N) to represent NULL values. PySpark reads these as normal strings, so they must be replaced with actual nulls:

```python
def replace_backslash_n(df):
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) == "\\N", F.lit(None)).otherwise(F.col(col_name)),
        )
    return df
```

This is called on **every** table immediately after reading the CSV.

#### `parse_lap_time_ms(time_str)` — UDF

A PySpark UDF that converts lap-time strings to milliseconds:

```python
@F.udf(LongType())
def parse_lap_time_ms(time_str):
    # "1:27.452" → 87452 ms
    # "27.452"   → 27452 ms
    # None       → None
```

**How it works:**
1. If the string contains `:`, split into minutes and seconds — `"1:27.452"` → `minutes=1`, `seconds=27`, `millis=452` → `1×60000 + 27×1000 + 452 = 87452`
2. If no `:`, treat as seconds only — `"27.452"` → `27×1000 + 452 = 27452`
3. Returns `None` for null or unparseable input

This UDF is used on: `fastestLapTime`, `q1/q2/q3`, `time` (lap_times), and `duration` (pit_stops).

#### `read_csv(table_name)` and `write_parquet(df, table_name, partition_cols)`

```python
def read_csv(table_name):
    path = f"{RAW_BASE}/{table_name}.csv"
    return spark.read.csv(path, header=True, inferSchema=False)
```

- `inferSchema=False` reads everything as strings. The cleaners then cast explicitly.
- This approach is deliberate — it prevents PySpark from guessing wrong types on `\N` values.

```python
def write_parquet(df, table_name, partition_cols=None):
    path = f"{PROCESSED_BASE}/{table_name}"
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
```

- `mode("overwrite")` makes every run idempotent.
- Fact tables are partitioned by `year` for efficient downstream queries.

### 3.3 Table Cleaners (14 Tables)

Every cleaner follows the same pattern:
1. `read_csv(table_name)` → raw DataFrame (all strings)
2. `replace_backslash_n(df)` → `\N` → null
3. `.withColumn(col, cast(Type))` → explicit type casting
4. `.dropDuplicates()` → remove exact row duplicates
5. `write_parquet(df, table_name)` → Parquet output

#### Reference Tables (No Partitioning)

These are small, static-ish tables that don't need year partitioning:

| Table | Key Casts | Notes |
|---|---|---|
| **circuits** | `circuitId` → INT, `lat/lng` → DOUBLE, `alt` → INT | Track metadata |
| **constructors** | `constructorId` → INT | Team metadata |
| **drivers** | `driverId` → INT, `number` → INT, `dob` → DATE | Driver metadata |
| **seasons** | `year` → INT | Just year + URL |
| **status** | `statusId` → INT | Race finish status codes |

#### Fact Tables (Partitioned by Year)

These tables are larger and contain race-specific data. They all join to the **races** table to get `year` for partitioning:

```python
# Common pattern — read races just for the year column
races = (
    read_csv("races")
    .transform(replace_backslash_n)
    .select(
        F.col("raceId").cast(IntegerType()).alias("raceId"),
        F.col("year").cast(IntegerType()).alias("year"),
    )
)
# ... clean the main table ...
df = df.join(races, on="raceId", how="left")
write_parquet(df, "results", partition_cols=["year"])
```

| Table | Key Casts | Special Transforms |
|---|---|---|
| **races** | `raceId/year/round/circuitId` → INT, 6× date columns → DATE | Self-partitioned by year |
| **results** | 11× INT, `points` → DOUBLE, `milliseconds` → LONG | `fastestLapTime` → `fastestLapTime_ms` via UDF |
| **qualifying** | IDs → INT | `q1/q2/q3` → `q1_ms/q2_ms/q3_ms` via UDF |
| **lap_times** | IDs → INT, `milliseconds` → LONG | `time` → `time_ms` via UDF |
| **pit_stops** | IDs → INT, `milliseconds` → LONG | `duration` → `duration_ms` via UDF |
| **driver_standings** | IDs → INT, `points` → DOUBLE, `wins` → INT | Joined to races for year |
| **constructor_standings** | IDs → INT, `points` → DOUBLE, `wins` → INT | Joined to races for year |
| **constructor_results** | IDs → INT, `points` → DOUBLE | Joined to races for year |
| **sprint_results** | Same structure as results | Separate table for sprint race results |

#### TABLE_CLEANERS Lookup Dict

```python
TABLE_CLEANERS = {
    "circuits":               clean_circuits,
    "constructors":           clean_constructors,
    "drivers":                clean_drivers,
    "seasons":                clean_seasons,
    "status":                 clean_status,
    "races":                  clean_races,
    "results":                clean_results,
    "qualifying":             clean_qualifying,
    "lap_times":              clean_lap_times,
    "pit_stops":              clean_pit_stops,
    "driver_standings":       clean_driver_standings,
    "constructor_standings":  clean_constructor_standings,
    "constructor_results":    clean_constructor_results,
    "sprint_results":         clean_sprint_results,
}
```

This dict maps table names to their cleaner functions. It's used in Glue mode to dynamically look up cleaner functions from the manifest's file paths (e.g., `raw/kaggle/circuits.csv` → table name `circuits` → `clean_circuits()`).

### 3.4 main() Execution Flow

```python
def main():
    ALL_RAW_PATHS = [f"raw/kaggle/{name}.csv" for name in [
        "circuits", "constructors", "drivers", "races", "results",
        "qualifying", "lap_times", "pit_stops", "driver_standings",
        "constructor_standings", "constructor_results", "sprint_results",
        "seasons", "status",
    ]]

    if MODE == "glue":
        # Incremental: only process new files
        unprocessed, manifest = get_unprocessed("kaggle", ALL_RAW_PATHS)
        if not unprocessed:
            print("All files already processed — nothing to do.")
            return

        for raw_path in unprocessed:
            table_name = raw_path.split("/")[-1].replace(".csv", "")
            cleaner_fn = TABLE_CLEANERS[table_name]
            try:
                cleaner_fn()
                mark_file_done(raw_path, manifest)
                save_manifest("kaggle", manifest)
            except Exception as e:
                print(f"  ✗ Error processing {table_name}: {e}")
                raise
    else:
        # Databricks: process everything
        clean_circuits()
        clean_constructors()
        # ... all 14 cleaners called sequentially ...
```

**Glue mode flow:**
```
1. Build ALL_RAW_PATHS ─────── 14 paths like "raw/kaggle/circuits.csv"
2. get_unprocessed() ────────── Loads manifest, returns only NEW files
3. For each unprocessed file:
   a. Extract table name ─────── "raw/kaggle/circuits.csv" → "circuits"
   b. Look up cleaner ────────── TABLE_CLEANERS["circuits"] → clean_circuits
   c. Run cleaner ────────────── read CSV → cast → dedup → write Parquet
   d. mark_file_done() ───────── Add to in-memory manifest
   e. save_manifest() ────────── Persist to S3 (crash-safe checkpoint)
```

**Databricks mode flow:**
```
1. Call all 14 cleaners sequentially
2. Each cleaner overwrites its Parquet output
3. No manifest involved
```

---

## 4. clean_openf1.py — In Depth

### 4.1 Imports & Configuration

Identical pattern to Kaggle. The only difference is the data source paths:

```python
PATHS = {
    "databricks": {
        "raw":       "/FileStore/f1-pipeline/raw/openf1",
        "processed": "/FileStore/f1-pipeline/processed/openf1",
    },
    "glue": {
        "raw":       "s3://f1-raw-satva/raw/openf1",
        "processed": "s3://f1-processed-satva/processed/openf1",
    },
}
```

### 4.2 Helper Functions

#### `seconds_to_ms_col(col_name)`

OpenF1 API returns lap/sector/pit durations in **seconds** as decimals (e.g., `87.452`). This helper converts them to **milliseconds** as integers for consistency with the Kaggle dataset:

```python
def seconds_to_ms_col(col_name):
    return (F.col(col_name).cast(DoubleType()) * 1000).cast(LongType())
```

Example: `87.452` seconds → `87452` milliseconds

#### `safe_read_json(path)`

Unlike Kaggle CSVs (which always exist), OpenF1 JSON files may or may not exist for a given session. This helper prevents Spark from crashing on missing files:

```python
def safe_read_json(path):
    try:
        df = spark.read.json(path)
        if df.head(1):     # Check the file actually has data
            return df
        return None
    except Exception:
        return None        # File doesn't exist — return None silently
```

The caller checks for `None` and skips that file.

### 4.3 Session Discovery

This is the most complex helper — it discovers which `year/meeting/session` directories actually contain data.

#### Raw Data Structure

OpenF1 data is organized as:

```
raw/openf1/
  2024/
    meeting_1229/
      race/
        drivers.json
        laps.json
        stints.json
        pit.json
    meeting_1230/
      race/
        drivers.json
        ...
  2025/
    meeting_1279/
      ...
```

The meeting keys (like `1229`, `1279`) are **arbitrary integers** from the OpenF1 API — not sequential.

#### `discover_session_paths()` — Two Strategies

```python
def discover_session_paths():
    paths = []

    if MODE == "databricks":
        # Strategy 1: Walk DBFS using dbutils.fs.ls()
        try:
            year_dirs = dbutils.fs.ls(f"{RAW_BASE}/")
            for yd in year_dirs:
                # ... walk year → meeting → session dirs ...
        except Exception as e:
            # Fallback: list S3 directly via boto3
            s3 = boto3.client("s3")
            paginator = s3.get_paginator("list_objects_v2")
            # ... scan all objects under raw/openf1/ ...

    else:
        # Strategy 2 (Glue mode): List S3 objects directly
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        seen = set()
        for page in paginator.paginate(Bucket=MANIFEST_BUCKET, Prefix="raw/openf1/"):
            for obj in page.get("Contents", []):
                parts = obj["Key"].split("/")
                if len(parts) < 5:
                    continue
                year_str, meeting_dir, session_type = parts[2], parts[3], parts[4]
                if not year_str.isdigit() or not meeting_dir.startswith("meeting_"):
                    continue
                key = (year_str, meeting_dir, session_type)
                if key not in seen:
                    seen.add(key)
                    paths.append({
                        "year": int(year_str),
                        "meeting_key": meeting_dir.replace("meeting_", ""),
                        "session_type": session_type,
                        "path": f"{RAW_BASE}/{year_str}/{meeting_dir}/{session_type}",
                    })

    return paths
```

**Databricks mode:** Uses `dbutils.fs.ls()` to walk the DBFS directory tree level by level (year → meeting → session). If `dbutils` fails (e.g., running locally), falls back to S3 listing.

**Glue mode:** Uses `s3.get_paginator("list_objects_v2")` to list every object under `raw/openf1/`, then extracts unique `(year, meeting, session)` tuples from the S3 keys. The `seen` set ensures each session appears only once (since each session contains 4 JSON files).

**Return value:** A list of dicts like:

```python
[
    {"year": 2024, "meeting_key": "1229", "session_type": "race",
     "path": "s3://f1-raw-satva/raw/openf1/2024/meeting_1229/race"},
    {"year": 2024, "meeting_key": "1230", "session_type": "race",
     "path": "s3://f1-raw-satva/raw/openf1/2024/meeting_1230/race"},
    ...
]
```

### 4.4 Table Cleaners (4 Endpoints)

Unlike Kaggle, OpenF1 cleaners **receive a DataFrame** as an argument and **return a cleaned DataFrame** (they don't read/write themselves). This is because the read (JSON) and write (Parquet) logic is handled by `main()`.

#### ENDPOINT_CONFIG

```python
ENDPOINT_CONFIG = {
    "drivers":  (clean_drivers,  "drivers.json"),
    "laps":     (clean_laps,     "laps.json"),
    "stints":   (clean_stints,   "stints.json"),
    "pit":      (clean_pit,      "pit.json"),
}
```

Each entry maps an endpoint name to its `(cleaner_function, json_filename)`.

#### `clean_drivers(df)`

```python
def clean_drivers(df):
    cols_to_drop = [c for c in df.columns if c in ("headshot_url",)]
    df = df.drop(*cols_to_drop)

    df = (
        df
        .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
        .withColumn("session_key",   F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key",   F.col("meeting_key").cast(IntegerType()))
        .dropDuplicates()
    )
    return df
```

- Drops `headshot_url` (not useful for analytics)
- Casts numeric IDs to integers
- Keeps string fields as-is: `full_name`, `team_name`, `country_code`, etc.

#### `clean_laps(df)`

The most complex cleaner. Transforms:

```python
def clean_laps(df):
    # 1. Drop segment arrays (large, not needed)
    segment_cols = [c for c in df.columns if c.startswith("segments_sector")]
    df = df.drop(*segment_cols)

    df = (
        df
        # 2. Cast IDs
        .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
        .withColumn("session_key",   F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key",   F.col("meeting_key").cast(IntegerType()))
        .withColumn("lap_number",    F.col("lap_number").cast(IntegerType()))

        # 3. Convert seconds → milliseconds (new columns)
        .withColumn("lap_duration_ms",       seconds_to_ms_col("lap_duration"))
        .withColumn("duration_sector_1_ms",  seconds_to_ms_col("duration_sector_1"))
        .withColumn("duration_sector_2_ms",  seconds_to_ms_col("duration_sector_2"))
        .withColumn("duration_sector_3_ms",  seconds_to_ms_col("duration_sector_3"))

        # 4. Keep originals as DOUBLE for reference
        .withColumn("lap_duration",      F.col("lap_duration").cast(DoubleType()))
        .withColumn("duration_sector_1", F.col("duration_sector_1").cast(DoubleType()))
        .withColumn("duration_sector_2", F.col("duration_sector_2").cast(DoubleType()))
        .withColumn("duration_sector_3", F.col("duration_sector_3").cast(DoubleType()))

        # 5. Speed traps
        .withColumn("i1_speed",  F.col("i1_speed").cast(DoubleType()))
        .withColumn("i2_speed",  F.col("i2_speed").cast(DoubleType()))
        .withColumn("st_speed",  F.col("st_speed").cast(DoubleType()))

        # 6. Boolean flag
        .withColumn("is_pit_out_lap", F.col("is_pit_out_lap").cast(BooleanType()))

        .dropDuplicates()
    )
    return df
```

Key decisions:
- **Segment arrays dropped** — `segments_sector_1/2/3` are large arrays of per-mini-sector statuses. Not useful for aggregate analytics and bloat the Parquet significantly.
- **Dual columns for durations** — Both the original seconds (`DOUBLE`) and converted milliseconds (`LONG`) are kept. Milliseconds match the Kaggle convention; seconds are kept for reference.

#### `clean_stints(df)`

```python
def clean_stints(df):
    df = (
        df
        .withColumn("driver_number",     F.col("driver_number").cast(IntegerType()))
        .withColumn("session_key",       F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key",       F.col("meeting_key").cast(IntegerType()))
        .withColumn("stint_number",      F.col("stint_number").cast(IntegerType()))
        .withColumn("lap_start",         F.col("lap_start").cast(IntegerType()))
        .withColumn("lap_end",           F.col("lap_end").cast(IntegerType()))
        .withColumn("tyre_age_at_start", F.col("tyre_age_at_start").cast(IntegerType()))
        # compound → STRING (SOFT, MEDIUM, HARD, INTERMEDIATE, WET)
        .dropDuplicates()
    )
    return df
```

Simple casting — the `compound` field stays as a string enum.

#### `clean_pit(df)`

```python
def clean_pit(df):
    df = (
        df
        .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
        .withColumn("session_key",   F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key",   F.col("meeting_key").cast(IntegerType()))
        .withColumn("lap_number",    F.col("lap_number").cast(IntegerType()))

        # pit_duration: seconds → milliseconds
        .withColumn("pit_duration_ms",
                    (F.col("pit_duration").cast(DoubleType()) * 1000).cast(LongType()))
        .withColumn("pit_duration", F.col("pit_duration").cast(DoubleType()))

        .dropDuplicates()
    )
    return df
```

Same dual-column pattern as laps — `pit_duration` in seconds (DOUBLE) and `pit_duration_ms` (LONG).

### 4.5 main() Execution Flow

#### Glue Mode (Incremental, Per-File)

```python
def main():
    session_paths = discover_session_paths()

    if MODE == "glue":
        # Build ALL expected raw S3 keys
        ALL_RAW_PATHS = []
        for sp in session_paths:
            for _, (_, filename) in ENDPOINT_CONFIG.items():
                ALL_RAW_PATHS.append(
                    f"raw/openf1/{sp['year']}/meeting_{sp['meeting_key']}"
                    f"/{sp['session_type']}/{filename}"
                )

        unprocessed, manifest = get_unprocessed("openf1", ALL_RAW_PATHS)
        if not unprocessed:
            return

        for raw_path in unprocessed:
            # Parse path components
            parts = raw_path.split("/")
            year, meeting_key = int(parts[2]), parts[3].replace("meeting_", "")
            session_type, filename = parts[4], parts[5]

            # Find the cleaner function
            clean_fn, endpoint_name = ...  # looked up from ENDPOINT_CONFIG

            # Read → Clean → Write (append mode)
            df_raw = safe_read_json(json_path)
            if df_raw is None:
                continue  # Skip — don't mark as done, retry next run

            df_clean = clean_fn(df_raw)
            df_clean = df_clean.withColumn("year", F.lit(year).cast(IntegerType()))

            path = f"{PROCESSED_BASE}/{endpoint_name}"
            df_clean.write.mode("append").partitionBy("year").parquet(path)

            mark_file_done(raw_path, manifest)
            save_manifest("openf1", manifest)
```

**Key difference from Kaggle:** OpenF1 uses `mode("append")` instead of `mode("overwrite")` because many JSON files (one per session) contribute to the same endpoint table (e.g., all `drivers.json` files → one `drivers` Parquet table).

**Why `safe_read_json` returns None is not marked as done:** If a file doesn't exist or is empty, it's skipped without recording in the manifest. This means it will be retried on the next run — useful if the ingestion pipeline hasn't finished uploading all files yet.

#### Databricks Mode (Full Reprocess, Accumulator)

```python
    else:
        # Collect all DataFrames per endpoint
        accumulators = {name: [] for name in ENDPOINT_CONFIG}

        for sp in session_paths:
            for endpoint_name, (clean_fn, filename) in ENDPOINT_CONFIG.items():
                df_raw = safe_read_json(f"{base_path}/{filename}")
                if df_raw is None:
                    continue
                df_clean = clean_fn(df_raw)
                accumulators[endpoint_name].append(df_clean)

        # Union all sessions per endpoint, then write
        for endpoint_name, dfs in accumulators.items():
            combined = dfs[0]
            for df in dfs[1:]:
                combined = combined.unionByName(df, allowMissingColumns=True)
            combined = combined.dropDuplicates()
            write_parquet(combined, endpoint_name, partition_cols=["year"])
```

**Accumulator pattern:** In Databricks, all session DataFrames for each endpoint are collected in a list, unioned together with `unionByName(allowMissingColumns=True)`, deduplicated, and written as a single Parquet table with `mode("overwrite")`.

This is simpler but requires all data to fit in memory and doesn't support incremental processing. It's intentional — Databricks is for development/testing where you always want a clean, full reprocess.

---

## 5. Key Differences Between the Two Scripts

| Aspect | clean_kaggle.py | clean_openf1.py |
|---|---|---|
| **Data format** | CSV (14 files) | JSON (4 files × N sessions) |
| **Null handling** | `\N` → null via `replace_backslash_n()` | Not needed (JSON nulls are native) |
| **Time conversion** | UDF: `"1:27.452"` → `87452` ms | Arithmetic: `87.452` sec × 1000 → `87452` ms |
| **Cleaner design** | Self-contained (each reads + writes) | Pure transform (receives DF, returns DF) |
| **Year column** | Joined from `races.csv` | Added via `F.lit(year)` from directory path |
| **Partitioning** | Fact tables by `year` | All endpoints by `year` |
| **Glue write mode** | `overwrite` (1 CSV = 1 table) | `append` (N JSONs contribute to 1 table) |
| **Path discovery** | Fixed list of 14 filenames | Dynamic S3 scan of `year/meeting/session` dirs |
| **Databricks pattern** | Sequential cleaner calls | Accumulator → union → write |

---

## 6. How to Run

### Databricks (Development)

1. Set `MODE = "databricks"` (default)
2. Upload raw data to DBFS paths (`/FileStore/f1-pipeline/raw/...`)
3. Run as a notebook or Python script
4. All tables are fully reprocessed every time

### AWS Glue (Production)

1. Change `MODE = "glue"`
2. Remove the `SparkSession.builder` block
3. Deploy as a Glue job
4. First run processes all files and creates the manifest
5. Subsequent runs only process new/unprocessed files
6. Check manifest at: `s3://f1-pipeline-raw-layer-034381339055/meta/manifests/`

### Checking the Manifest

```bash
aws s3 cp s3://f1-pipeline-raw-layer-034381339055/meta/manifests/kaggle_manifest.json -
```

```json
{
  "last_updated": "2026-04-14T05:00:00+00:00",
  "processed": {
    "raw/kaggle/circuits.csv": "2026-04-14T04:58:00+00:00",
    "raw/kaggle/constructors.csv": "2026-04-14T04:58:05+00:00",
    ...
  }
}
```

To force a full reprocess in Glue, delete the manifest file:

```bash
aws s3 rm s3://f1-pipeline-raw-layer-034381339055/meta/manifests/kaggle_manifest.json
```

---

## 7. SparkSession & Entry Point

### 7.1 SparkSession Builder

Both scripts include a `SparkSession` block that must be **removed** when deploying to AWS Glue (Glue provides `spark` automatically via `GlueContext`):

```python
# ── Spark Session (remove for Glue) ─────────────────────────────
spark = (
    SparkSession.builder
    .appName("clean_kaggle")       # or "clean_openf1"
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

- `.getOrCreate()` — Reuses an existing SparkSession if one is already running (safe for Databricks notebooks where `spark` already exists).
- `.setLogLevel("WARN")` — Suppresses verbose Spark INFO logs during development.

### 7.2 `if __name__ == "__main__"` Entry Point

Both scripts end with:

```python
if __name__ == "__main__":
    main()
```

This means:
- **Run as a script** (`python clean_kaggle.py`) → calls `main()` automatically
- **Imported as a module** (`from clean_kaggle import clean_circuits`) → `main()` is NOT called, individual functions are available for testing
- **Databricks notebook** → Not triggered (notebooks don't set `__name__` to `"__main__"`), so you call `main()` explicitly in a cell.

---

## 8. Error Handling

### 8.1 Kaggle — Fail-Fast with Manifest Safety

```python
for raw_path in unprocessed:
    table_name = raw_path.split("/")[-1].replace(".csv", "")
    cleaner_fn = TABLE_CLEANERS[table_name]
    try:
        cleaner_fn()
        mark_file_done(raw_path, manifest)
        save_manifest("kaggle", manifest)
    except Exception as e:
        print(f"  ✗ Error processing {table_name}: {e}")
        raise   # ← re-raise stops the entire run
```

**Why re-raise instead of `continue`?** — Kaggle tables have dependencies. If `races` fails, then `results`, `qualifying`, `lap_times` etc. would also fail (they all join to races). Stopping immediately prevents a cascade of misleading errors. The manifest already has all previously succeeded files recorded, so the next run picks up from the failed file.

### 8.2 OpenF1 — Skip Missing, Fail on Real Errors

```python
df_raw = safe_read_json(json_path)
if df_raw is None:
    continue  # ← Silent skip — file doesn't exist yet

df_clean = clean_fn(df_raw)
if df_clean is None or not df_clean.head(1):
    continue  # ← Empty result — skip without marking
```

OpenF1 handles missing/empty files differently than Kaggle because:
- Ingestion may still be running when cleaning starts — some sessions haven't been uploaded yet
- Not every session type has all 4 endpoints (e.g., some may lack `pit.json`)
- Missing files are **not** marked as done — they stay "unprocessed" and are retried next run

If an actual error occurs during cleaning (e.g., schema mismatch), it **will** propagate as an uncaught exception and stop the run.

---

## 9. Why `unionByName(allowMissingColumns=True)`

In OpenF1's Databricks mode, DataFrames from different sessions are combined:

```python
combined = dfs[0]
for df in dfs[1:]:
    combined = combined.unionByName(df, allowMissingColumns=True)
```

**Why `unionByName` instead of `union`?**
- `union()` matches columns by **position** — if JSON schemas differ in column order across sessions, data ends up in wrong columns
- `unionByName()` matches by **column name** — safe regardless of column order

**Why `allowMissingColumns=True`?**
- Different OpenF1 API versions or sessions may have slightly different schemas (e.g., a field added in 2025 that doesn't exist in 2024 data)
- With `allowMissingColumns=True`, missing columns are filled with `null` instead of throwing an error
- This makes the pipeline resilient to schema evolution across seasons

---

## 10. `02_clean_openf1.py` — Databricks CE Notebook Variant

There is a third file — `processing/glue_jobs/02_clean_openf1.py` — which is a **Databricks Community Edition (CE) notebook version** of `clean_openf1.py`. It is NOT a Glue script. It exists because Databricks CE cannot access S3 natively, so it includes an S3 download step.

### Why This File Exists

| Limitation | Solution |
|---|---|
| Databricks CE has no IAM/Instance Profile for S3 | Uses `boto3` with hardcoded credentials to download files |
| Databricks CE only reads from DBFS Volumes | Downloads JSON from S3 into `/Volumes/workspace/f1_pipeline/raw_openf1/` |
| Databricks CE doesn't support `.py` files as notebooks | Script uses `# ── Cell N ──` comments so you can copy-paste each cell |

### Cell Layout

| Cell | Purpose |
|---|---|
| **1** | `pip install boto3` |
| **2** | AWS credentials + `download_s3_to_volume()` function |
| **3** | Runs the S3 → Volume download |
| **4** | Verifies downloaded files via `dbutils.fs.ls` |
| **5** | PySpark imports & DBFS Volume path configuration |
| **6** | Helper functions (`seconds_to_ms_col`, `write_parquet`, `discover_session_paths`, `safe_read_json`) |
| **7** | Table cleaner functions (same logic as `clean_openf1.py`) |
| **8** | Main pipeline — discover, clean, union, write |
| **9–15** | Verification cells: row counts, schema checks, spot checks per table |

### S3 Download Function

```python
def download_s3_to_volume():
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX + "/")

    for page in pages:
        for obj in page.get("Contents", []):
            s3_key  = obj["Key"]
            rel_path = s3_key[len(S3_PREFIX) + 1:]
            local_path = f"{VOLUME_RAW}/{rel_path}"

            # Skip if already downloaded and same size
            if os.path.exists(local_path) and os.path.getsize(local_path) == obj["Size"]:
                skipped += 1
                continue

            os.makedirs(parent_dir, exist_ok=True)
            s3.download_file(S3_BUCKET, s3_key, local_path)
```

**Key features:**
- **Idempotent** — Skips files that already exist with the same size
- **Preserves directory structure** — `raw/openf1/2024/meeting_1229/race/drivers.json` → same path under the Volume
- **Progress reporting** — Prints a status line every 20 files

### Relationship to `clean_openf1.py`

| Feature | `clean_openf1.py` | `02_clean_openf1.py` |
|---|---|---|
| **Target env** | AWS Glue / Databricks (DBFS) | Databricks Community Edition only |
| **S3 access** | Direct (IAM role) | Download first via boto3 |
| **Data paths** | `s3://...` or `/FileStore/...` | `/Volumes/workspace/f1_pipeline/...` |
| **Manifest** | Yes (Glue mode) | No — always full reprocess |
| **Verification** | None built-in | 7 verification cells included |
| **Credentials** | IAM role (no keys) | Hardcoded (for CE only — not committed to git) |

---

## 11. Output Schema Reference

### Kaggle Output Tables (14)

#### Reference Tables

| Table | Columns | Partition |
|---|---|---|
| **circuits** | `circuitId` INT, `circuitRef` STR, `name` STR, `location` STR, `country` STR, `lat` DOUBLE, `lng` DOUBLE, `alt` INT, `url` STR | None |
| **constructors** | `constructorId` INT, `constructorRef` STR, `name` STR, `nationality` STR, `url` STR | None |
| **drivers** | `driverId` INT, `driverRef` STR, `number` INT, `code` STR, `forename` STR, `surname` STR, `dob` DATE, `nationality` STR, `url` STR | None |
| **seasons** | `year` INT, `url` STR | None |
| **status** | `statusId` INT, `status` STR | None |

#### Fact Tables

| Table | Key Columns | Derived Columns | Partition |
|---|---|---|---|
| **races** | `raceId` INT, `year` INT, `round` INT, `circuitId` INT | 6× date fields cast to DATE | `year` |
| **results** | `resultId`, `raceId`, `driverId`, `constructorId` (all INT) | `fastestLapTime_ms` LONG (UDF), `year` INT (join) | `year` |
| **qualifying** | `qualifyId`, `raceId`, `driverId`, `constructorId` (all INT) | `q1_ms`, `q2_ms`, `q3_ms` LONG (UDF), `year` INT (join) | `year` |
| **lap_times** | `raceId`, `driverId`, `lap` (all INT), `milliseconds` LONG | `time_ms` LONG (UDF), `year` INT (join) | `year` |
| **pit_stops** | `raceId`, `driverId`, `stop`, `lap` (all INT), `milliseconds` LONG | `duration_ms` LONG (UDF), `year` INT (join) | `year` |
| **driver_standings** | `driverStandingsId`, `raceId`, `driverId` (all INT), `points` DOUBLE | `year` INT (join) | `year` |
| **constructor_standings** | `constructorStandingsId`, `raceId`, `constructorId` (all INT), `points` DOUBLE | `year` INT (join) | `year` |
| **constructor_results** | `constructorResultsId`, `raceId`, `constructorId` (all INT), `points` DOUBLE | `year` INT (join) | `year` |
| **sprint_results** | Same as results | `fastestLapTime_ms` LONG (UDF), `year` INT (join) | `year` |

### OpenF1 Output Tables (4)

| Table | Key Columns | Derived Columns | Partition |
|---|---|---|---|
| **drivers** | `driver_number` INT, `session_key` INT, `meeting_key` INT | — | `year` |
| | `full_name` STR, `team_name` STR, `country_code` STR, etc. | | |
| **laps** | `driver_number` INT, `session_key` INT, `meeting_key` INT, `lap_number` INT | `lap_duration_ms` LONG, `duration_sector_1/2/3_ms` LONG | `year` |
| | `lap_duration` DOUBLE, `i1_speed`/`i2_speed`/`st_speed` DOUBLE, `is_pit_out_lap` BOOL | | |
| **stints** | `driver_number` INT, `session_key` INT, `meeting_key` INT, `stint_number` INT | — | `year` |
| | `lap_start` INT, `lap_end` INT, `tyre_age_at_start` INT, `compound` STR | | |
| **pit** | `driver_number` INT, `session_key` INT, `meeting_key` INT, `lap_number` INT | `pit_duration_ms` LONG | `year` |
| | `pit_duration` DOUBLE, `date` STR | | |

> **Note:** All OpenF1 tables get a `year` column added automatically from the directory path (not from the data itself). This is used as the Parquet partition key.
