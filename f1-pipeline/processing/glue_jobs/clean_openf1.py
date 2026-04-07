"""
clean_openf1.py — PySpark job (Databricks / AWS Glue)

Reads flat JSON arrays from the OpenF1 API raw zone, applies:
  - Type casting (driver_number, session_key, etc.)
  - Drops segment arrays (segments_sector_1/2/3)
  - Converts seconds → milliseconds for consistency with Kaggle
  - Deduplication
  - Writes clean Parquet to the processed zone

Handles endpoints:
  - drivers   → driver metadata per session
  - laps      → per-lap telemetry (duration, sectors)
  - stints    → tyre stint information
  - pit       → pit stop timing

Databricks → Glue migration:
  1. Remove the SparkSession builder (Glue provides it)
  2. Switch MODE to "glue"
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    IntegerType, LongType, DoubleType, StringType, BooleanType,
)

# ── Configuration ────────────────────────────────────────────────
MODE = "databricks"  # "databricks" | "glue"

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

RAW_BASE       = PATHS[MODE]["raw"]
PROCESSED_BASE = PATHS[MODE]["processed"]

# ── Spark Session (remove for Glue) ─────────────────────────────
spark = (
    SparkSession.builder
    .appName("clean_openf1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def seconds_to_ms_col(col_name):
    """Convert a DOUBLE column (seconds) to LONG (milliseconds)."""
    return (F.col(col_name).cast(DoubleType()) * 1000).cast(LongType())


def write_parquet(df, table_name, partition_cols=None):
    """Write a DataFrame as Parquet (optionally partitioned)."""
    path = f"{PROCESSED_BASE}/{table_name}"
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    count = df.count()
    print(f"  ✓ Wrote {table_name} → {path}  ({count:,} rows)")


def discover_session_paths():
    """
    Discover all year/meeting/session directories that contain data.
    Returns a list of dicts: {"year": int, "meeting_key": str, "path": str}

    Tries dbutils on Databricks; falls back to known ranges.
    """
    paths = []

    if MODE == "databricks":
        try:
            import builtins
            dbutils = getattr(builtins, "dbutils", None)
            if dbutils is None:
                raise AttributeError("dbutils not available")

            year_dirs = dbutils.fs.ls(f"{RAW_BASE}/")
            for yd in year_dirs:
                year_name = yd.name.rstrip("/")
                if not year_name.isdigit():
                    continue
                year = int(year_name)
                meeting_dirs = dbutils.fs.ls(yd.path)
                for md in meeting_dirs:
                    meeting_name = md.name.rstrip("/")
                    if not meeting_name.startswith("meeting_"):
                        continue
                    # Look for /race/ subdirectory
                    try:
                        session_dirs = dbutils.fs.ls(md.path)
                        for sd in session_dirs:
                            session_name = sd.name.rstrip("/")
                            paths.append({
                                "year": year,
                                "meeting_key": meeting_name.replace("meeting_", ""),
                                "session_type": session_name,
                                "path": sd.path.rstrip("/"),
                            })
                    except Exception:
                        continue
        except Exception as e:
            print(f"  ⚠ Could not list DBFS directories: {e}")
            print("  → Using fallback: scanning years 2023–2026, meetings 1–25")
            for year in range(2023, 2027):
                for meeting in range(1, 26):
                    paths.append({
                        "year": year,
                        "meeting_key": str(meeting),
                        "session_type": "race",
                        "path": f"{RAW_BASE}/{year}/meeting_{meeting}/race",
                    })
    else:
        for year in range(2023, 2027):
            for meeting in range(1, 26):
                paths.append({
                    "year": year,
                    "meeting_key": str(meeting),
                    "session_type": "race",
                    "path": f"{RAW_BASE}/{year}/meeting_{meeting}/race",
                })

    return paths


def safe_read_json(path):
    """Try reading a JSON file. Return None if it doesn't exist."""
    try:
        df = spark.read.json(path)
        if df.head(1):
            return df
        return None
    except Exception:
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TABLE CLEANERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def clean_drivers(df):
    """
    OpenF1 drivers.json — flat array, one record per driver per session.
    Fields: driver_number, session_key, meeting_key, broadcast_name,
            full_name, name_acronym, team_name, team_colour,
            first_name, last_name, headshot_url, country_code
    """
    # Drop headshot URL — not needed for analytics
    cols_to_drop = [c for c in df.columns if c in ("headshot_url",)]
    df = df.drop(*cols_to_drop)

    df = (
        df
        .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
        .withColumn("session_key",   F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key",   F.col("meeting_key").cast(IntegerType()))
        # full_name, name_acronym, team_name, team_colour,
        # first_name, last_name, broadcast_name, country_code → STRING
        .dropDuplicates()
    )
    return df


def clean_laps(df):
    """
    OpenF1 laps.json — flat array, one record per driver per lap.
    Fields: driver_number, session_key, meeting_key, lap_number,
            lap_duration, duration_sector_1, duration_sector_2,
            duration_sector_3, segments_sector_1, segments_sector_2,
            segments_sector_3, date_start, is_pit_out_lap,
            i1_speed, i2_speed, st_speed
    """
    # Drop segment arrays — they are large arrays of per-mini-sector statuses
    segment_cols = [c for c in df.columns if c.startswith("segments_sector")]
    df = df.drop(*segment_cols)

    df = (
        df
        .withColumn("driver_number",    F.col("driver_number").cast(IntegerType()))
        .withColumn("session_key",      F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key",      F.col("meeting_key").cast(IntegerType()))
        .withColumn("lap_number",       F.col("lap_number").cast(IntegerType()))

        # Convert seconds → milliseconds for consistency with Kaggle/Jolpica
        .withColumn("lap_duration_ms",       seconds_to_ms_col("lap_duration"))
        .withColumn("duration_sector_1_ms",  seconds_to_ms_col("duration_sector_1"))
        .withColumn("duration_sector_2_ms",  seconds_to_ms_col("duration_sector_2"))
        .withColumn("duration_sector_3_ms",  seconds_to_ms_col("duration_sector_3"))

        # Keep original seconds as DOUBLE for reference
        .withColumn("lap_duration",          F.col("lap_duration").cast(DoubleType()))
        .withColumn("duration_sector_1",     F.col("duration_sector_1").cast(DoubleType()))
        .withColumn("duration_sector_2",     F.col("duration_sector_2").cast(DoubleType()))
        .withColumn("duration_sector_3",     F.col("duration_sector_3").cast(DoubleType()))

        # Speed traps
        .withColumn("i1_speed",  F.col("i1_speed").cast(DoubleType()))
        .withColumn("i2_speed",  F.col("i2_speed").cast(DoubleType()))
        .withColumn("st_speed",  F.col("st_speed").cast(DoubleType()))

        # Boolean
        .withColumn("is_pit_out_lap", F.col("is_pit_out_lap").cast(BooleanType()))

        # date_start → STRING (ISO timestamp, keep as-is for Snowflake TIMESTAMP_TZ)
        .dropDuplicates()
    )
    return df


def clean_stints(df):
    """
    OpenF1 stints.json — flat array, one record per stint.
    Fields: driver_number, session_key, meeting_key, stint_number,
            lap_start, lap_end, compound, tyre_age_at_start
    """
    df = (
        df
        .withColumn("driver_number",      F.col("driver_number").cast(IntegerType()))
        .withColumn("session_key",        F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key",        F.col("meeting_key").cast(IntegerType()))
        .withColumn("stint_number",       F.col("stint_number").cast(IntegerType()))
        .withColumn("lap_start",          F.col("lap_start").cast(IntegerType()))
        .withColumn("lap_end",            F.col("lap_end").cast(IntegerType()))
        .withColumn("tyre_age_at_start",  F.col("tyre_age_at_start").cast(IntegerType()))
        # compound → STRING (SOFT, MEDIUM, HARD, INTERMEDIATE, WET)
        .dropDuplicates()
    )
    return df


def clean_pit(df):
    """
    OpenF1 pit.json — flat array, one record per pit stop.
    Fields: driver_number, session_key, meeting_key,
            lap_number, pit_duration, date
    """
    df = (
        df
        .withColumn("driver_number",  F.col("driver_number").cast(IntegerType()))
        .withColumn("session_key",    F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key",    F.col("meeting_key").cast(IntegerType()))
        .withColumn("lap_number",     F.col("lap_number").cast(IntegerType()))

        # pit_duration is in seconds — convert to ms
        .withColumn("pit_duration_ms",
                    (F.col("pit_duration").cast(DoubleType()) * 1000).cast(LongType()))
        .withColumn("pit_duration",   F.col("pit_duration").cast(DoubleType()))

        # date → STRING (ISO timestamp)
        .dropDuplicates()
    )
    return df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MAIN PIPELINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Endpoint → (clean function, JSON filename)
ENDPOINT_CONFIG = {
    "drivers":  (clean_drivers,  "drivers.json"),
    "laps":     (clean_laps,     "laps.json"),
    "stints":   (clean_stints,   "stints.json"),
    "pit":      (clean_pit,      "pit.json"),
}


def main():
    print("=" * 60)
    print("clean_openf1.py — Starting")
    print(f"  MODE:      {MODE}")
    print(f"  RAW:       {RAW_BASE}")
    print(f"  PROCESSED: {PROCESSED_BASE}")
    print("=" * 60)

    session_paths = discover_session_paths()
    print(f"  Found {len(session_paths)} possible session paths to scan\n")

    # Accumulators: endpoint → list of DataFrames
    accumulators = {name: [] for name in ENDPOINT_CONFIG}

    for sp in session_paths:
        year        = sp["year"]
        meeting_key = sp["meeting_key"]
        base_path   = sp["path"]

        for endpoint_name, (clean_fn, filename) in ENDPOINT_CONFIG.items():
            json_path = f"{base_path}/{filename}"
            df_raw = safe_read_json(json_path)
            if df_raw is None:
                continue

            df_clean = clean_fn(df_raw)
            if df_clean is not None and df_clean.head(1):
                # Add year column for partitioning if not already present
                if "year" not in df_clean.columns:
                    df_clean = df_clean.withColumn("year", F.lit(year).cast(IntegerType()))
                accumulators[endpoint_name].append(df_clean)
                print(f"  ✓ {year}/meeting_{meeting_key}/{endpoint_name}")

    # Write accumulated results
    print("\n" + "─" * 60)
    print("Writing Parquet output\n")

    for endpoint_name, dfs in accumulators.items():
        if not dfs:
            print(f"  ⊘ {endpoint_name} — no data found, skipping")
            continue

        combined = dfs[0]
        for df in dfs[1:]:
            combined = combined.unionByName(df, allowMissingColumns=True)

        combined = combined.dropDuplicates()
        write_parquet(combined, endpoint_name, partition_cols=["year"])

    print("\n" + "=" * 60)
    print("clean_openf1.py — Complete ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
