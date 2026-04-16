"""
clean_jolpica.py — AWS Glue PySpark job

Reads nested Ergast-format JSON from the Jolpica API raw zone in S3,
flattens the deeply nested structure, casts types, deduplicates, and
writes clean Parquet to the processed zone in S3.

Uses manifest-based incremental processing — identical pattern to
clean_kaggle.py and OpenF1_Cleaner.ipynb.

Handles 6 endpoints per round:
  - results               → one row per driver-result
  - qualifying            → one row per driver-qualifying
  - pitstops              → one row per pit stop
  - driver_standings      → one row per driver-standing
  - constructor_standings → one row per constructor-standing
  - sprint                → one row per sprint result (sprint weekends only)

Raw S3 layout:
  raw/jolpica/seasons/{year}/round_{NN}/{endpoint}.json

Each JSON file contains the full Ergast wrapper:
  MRData.RaceTable.Races[].Results[]
  MRData.StandingsTable.StandingsLists[].DriverStandings[]
  etc.
"""

import os
import sys
import json
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, DoubleType, StringType,
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONFIGURATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

MODE = "glue"

# S3 bucket names — read from Glue job parameters or env defaults
S3_RAW_BUCKET       = os.environ.get("S3_RAW_BUCKET", "f1-pipeline-raw-layer-034381339055")
S3_PROCESSED_BUCKET = os.environ.get("S3_PROCESSED_BUCKET", "f1-pipeline-processed-layer-034381339055")
AWS_REGION          = os.environ.get("AWS_REGION", "ap-south-1")

RAW_BASE       = f"s3://{S3_RAW_BUCKET}/raw/jolpica"
PROCESSED_BASE = f"s3://{S3_PROCESSED_BUCKET}/processed/jolpica"

# Manifest storage — same bucket as raw, under meta/manifests/
MANIFEST_BUCKET = S3_RAW_BUCKET
MANIFEST_PREFIX = "meta/manifests"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  GLUE SESSION INIT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

sc          = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session

job = Job(glueContext)
try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job.init(args["JOB_NAME"], args)
except Exception:
    job.init("f1-jolpica-clean", {})

spark.sparkContext.setLogLevel("WARN")

print(f"MODE:      {MODE}")
print(f"RAW:       {RAW_BASE}")
print(f"PROCESSED: {PROCESSED_BASE}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MANIFEST HELPERS  (identical to clean_kaggle.py & OpenF1_Cleaner)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_manifest_s3 = None


def _get_manifest_s3():
    """Lazy-init S3 client for manifest operations."""
    global _manifest_s3
    if _manifest_s3 is None:
        _manifest_s3 = boto3.client("s3")
    return _manifest_s3


def load_manifest(source):
    """Read manifest JSON from S3. Returns empty manifest if not found."""
    key = f"{MANIFEST_PREFIX}/{source}_manifest.json"
    try:
        obj = _get_manifest_s3().get_object(Bucket=MANIFEST_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {"last_updated": None, "processed": {}}
        raise


def save_manifest(source, manifest):
    """Write manifest dict back to S3 with updated timestamp."""
    manifest["last_updated"] = datetime.now(timezone.utc).isoformat()
    key = f"{MANIFEST_PREFIX}/{source}_manifest.json"
    _get_manifest_s3().put_object(
        Bucket=MANIFEST_BUCKET, Key=key,
        Body=json.dumps(manifest, indent=2), ContentType="application/json",
    )


def mark_file_done(s3_path, manifest):
    """Record an S3 path as successfully processed in the manifest dict."""
    manifest["processed"][s3_path] = datetime.now(timezone.utc).isoformat()


def get_unprocessed(source, all_raw_paths):
    """Return (unprocessed_paths, manifest) for the given source."""
    manifest = load_manifest(source)
    already = set(manifest["processed"].keys())
    unprocessed = [p for p in all_raw_paths if p not in already]
    print(f"  {source}: {len(already)} already processed, {len(unprocessed)} new")
    return unprocessed, manifest


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@F.udf(LongType())
def parse_lap_time_ms(time_str):
    """
    Convert a lap-time string to milliseconds.
    Handles formats:
      "1:27.452"  → 87452
      "27.452"    → 27452  (no minutes)
    Returns None for null / unparseable input.

    Identical to the UDF in clean_kaggle.py — ensures
    Jolpica times are stored in the same unit as Kaggle.
    """
    if time_str is None:
        return None
    try:
        if ":" in time_str:
            parts = time_str.split(":")
            minutes = int(parts[0])
            sec_parts = parts[1].split(".")
            seconds = int(sec_parts[0])
            millis = int(sec_parts[1]) if len(sec_parts) > 1 else 0
            return minutes * 60_000 + seconds * 1_000 + millis
        else:
            sec_parts = time_str.split(".")
            seconds = int(sec_parts[0])
            millis = int(sec_parts[1]) if len(sec_parts) > 1 else 0
            return seconds * 1_000 + millis
    except Exception:
        return None


def discover_round_paths():
    """
    Discover all season/round directories in S3 that contain data.
    Returns list of dicts: {"season": int, "round": int, "path": str}

    Scans S3 objects under raw/jolpica/seasons/ and extracts unique
    (season, round) tuples — same approach as OpenF1_Cleaner's
    discover_session_paths().
    """
    paths, s3, seen = [], boto3.client("s3"), set()
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=MANIFEST_BUCKET, Prefix="raw/jolpica/seasons/"):
        for obj in page.get("Contents", []):
            # Key format: raw/jolpica/seasons/2025/round_01/results.json
            parts = obj["Key"].split("/")
            if len(parts) < 5:
                continue
            year_str, round_dir = parts[3], parts[4]
            if not year_str.isdigit() or not round_dir.startswith("round_"):
                continue
            key = (year_str, round_dir)
            if key not in seen:
                seen.add(key)
                round_num = int(round_dir.replace("round_", ""))
                paths.append({
                    "season": int(year_str),
                    "round":  round_num,
                    "path":   f"{RAW_BASE}/seasons/{year_str}/{round_dir}",
                })

    # Sort by season then round for deterministic processing order
    paths.sort(key=lambda x: (x["season"], x["round"]))
    return paths


def safe_read_json(path):
    """
    Try reading a JSON file. Return None if it doesn't exist or is empty.
    Jolpica JSON is the full Ergast wrapper — read as multiLine.
    """
    try:
        df = spark.read.json(path, multiLine=True)
        if df.head(1):
            return df
        return None
    except Exception:
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FLATTENERS — one per endpoint
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
#  Each flattener receives the raw Ergast DataFrame and the
#  season/round context, then returns a flat, typed DataFrame
#  ready for Parquet output.
#
#  Column names and types are chosen to be consistent with
#  clean_kaggle.py outputs so dbt staging models can union them.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _has_nested_field(schema, dotted_path):
    """Check if a dotted path (e.g. 'FastestLap.AverageSpeed.speed') exists in a schema."""
    fields = dotted_path.split(".")
    current = schema
    for field_name in fields:
        if not hasattr(current, "fields"):
            return False
        match = [f for f in current.fields if f.name == field_name]
        if not match:
            return False
        current = match[0].dataType
    return True


def flatten_results(df_raw, season, round_num):
    """
    Ergast results structure:
    MRData.RaceTable.Races[].Results[] →
      .Driver.{driverId, code, permanentNumber}
      .Constructor.{constructorId}
      .grid, .position, .points, .laps, .status
      .Time.{millis, time}
      .FastestLap.{rank, lap, Time.time}
      .FastestLap.AverageSpeed.speed  (MAY be absent in Jolpica)
    """
    try:
        races = df_raw.select(F.explode("MRData.RaceTable.Races").alias("race"))
        results = races.select(
            F.lit(season).alias("season"),
            F.lit(round_num).alias("round"),
            F.col("race.raceName").alias("race_name"),
            F.explode("race.Results").alias("r"),
        )

        # Check if AverageSpeed exists in the inferred schema for Results
        r_schema = results.schema["r"].dataType
        has_avg_speed = (
            _has_nested_field(r_schema, "FastestLap.AverageSpeed.speed")
        )

        select_cols = [
            F.col("season").cast(IntegerType()),
            F.col("round").cast(IntegerType()),
            F.col("race_name"),
            F.col("r.number").cast(IntegerType()).alias("driver_number"),
            F.col("r.position").cast(IntegerType()).alias("position"),
            F.col("r.positionText").alias("position_text"),
            F.col("r.points").cast(DoubleType()).alias("points"),
            F.col("r.Driver.driverId").alias("driver_id"),
            F.col("r.Driver.code").alias("driver_code"),
            F.col("r.Driver.permanentNumber").cast(IntegerType()).alias("permanent_number"),
            F.col("r.Driver.givenName").alias("forename"),
            F.col("r.Driver.familyName").alias("surname"),
            F.col("r.Constructor.constructorId").alias("constructor_id"),
            F.col("r.Constructor.name").alias("constructor_name"),
            F.col("r.grid").cast(IntegerType()).alias("grid"),
            F.col("r.laps").cast(IntegerType()).alias("laps"),
            F.col("r.status").alias("status"),
            F.col("r.Time.millis").cast(LongType()).alias("time_millis"),
            F.col("r.Time.time").alias("time_text"),
            F.col("r.FastestLap.rank").cast(IntegerType()).alias("fastest_lap_rank"),
            F.col("r.FastestLap.lap").cast(IntegerType()).alias("fastest_lap_number"),
            parse_lap_time_ms(F.col("r.FastestLap.Time.time")).alias("fastest_lap_time_ms"),
        ]

        # AverageSpeed is present in original Ergast but may be absent in Jolpica
        if has_avg_speed:
            select_cols.append(
                F.col("r.FastestLap.AverageSpeed.speed").cast(DoubleType()).alias("fastest_lap_speed")
            )
        else:
            select_cols.append(
                F.lit(None).cast(DoubleType()).alias("fastest_lap_speed")
            )

        flat = results.select(*select_cols).dropDuplicates()
        return flat
    except Exception as e:
        print(f"    ⚠ Could not flatten results for {season}/R{round_num:02d}: {e}")
        return None


def flatten_qualifying(df_raw, season, round_num):
    """
    MRData.RaceTable.Races[].QualifyingResults[] →
      .Driver.{driverId, code}
      .Constructor.{constructorId}
      .position, .Q1, .Q2, .Q3
    """
    try:
        races = df_raw.select(F.explode("MRData.RaceTable.Races").alias("race"))
        quali = races.select(
            F.lit(season).alias("season"),
            F.lit(round_num).alias("round"),
            F.explode("race.QualifyingResults").alias("q"),
        )

        flat = quali.select(
            F.col("season").cast(IntegerType()),
            F.col("round").cast(IntegerType()),
            F.col("q.number").cast(IntegerType()).alias("driver_number"),
            F.col("q.position").cast(IntegerType()).alias("position"),
            F.col("q.Driver.driverId").alias("driver_id"),
            F.col("q.Driver.code").alias("driver_code"),
            F.col("q.Constructor.constructorId").alias("constructor_id"),
            F.col("q.Q1").alias("q1"),
            F.col("q.Q2").alias("q2"),
            F.col("q.Q3").alias("q3"),
            parse_lap_time_ms(F.col("q.Q1")).alias("q1_ms"),
            parse_lap_time_ms(F.col("q.Q2")).alias("q2_ms"),
            parse_lap_time_ms(F.col("q.Q3")).alias("q3_ms"),
        ).dropDuplicates()

        return flat
    except Exception as e:
        print(f"    ⚠ Could not flatten qualifying for {season}/R{round_num:02d}: {e}")
        return None


def flatten_pitstops(df_raw, season, round_num):
    """
    MRData.RaceTable.Races[].PitStops[] →
      .driverId, .stop, .lap, .time, .duration
    """
    try:
        races = df_raw.select(F.explode("MRData.RaceTable.Races").alias("race"))
        pits = races.select(
            F.lit(season).alias("season"),
            F.lit(round_num).alias("round"),
            F.explode("race.PitStops").alias("p"),
        )

        flat = pits.select(
            F.col("season").cast(IntegerType()),
            F.col("round").cast(IntegerType()),
            F.col("p.driverId").alias("driver_id"),
            F.col("p.stop").cast(IntegerType()).alias("stop"),
            F.col("p.lap").cast(IntegerType()).alias("lap"),
            F.col("p.time").alias("time"),
            F.col("p.duration").cast(DoubleType()).alias("duration_seconds"),
            (F.col("p.duration").cast(DoubleType()) * 1000).cast(LongType()).alias("duration_ms"),
        ).dropDuplicates()

        return flat
    except Exception as e:
        print(f"    ⚠ Could not flatten pitstops for {season}/R{round_num:02d}: {e}")
        return None


def flatten_driver_standings(df_raw, season, round_num):
    """
    MRData.StandingsTable.StandingsLists[].DriverStandings[] →
      .Driver.{driverId, code}
      .Constructors[0].{constructorId}
      .position, .points, .wins
    """
    try:
        standings = df_raw.select(
            F.explode("MRData.StandingsTable.StandingsLists").alias("sl")
        )
        drivers = standings.select(
            F.lit(season).alias("season"),
            F.lit(round_num).alias("round"),
            F.explode("sl.DriverStandings").alias("ds"),
        )

        flat = drivers.select(
            F.col("season").cast(IntegerType()),
            F.col("round").cast(IntegerType()),
            F.col("ds.Driver.driverId").alias("driver_id"),
            F.col("ds.Driver.code").alias("driver_code"),
            F.col("ds.Driver.givenName").alias("forename"),
            F.col("ds.Driver.familyName").alias("surname"),
            F.col("ds.Constructors")[0]["constructorId"].alias("constructor_id"),
            F.col("ds.position").cast(IntegerType()).alias("position"),
            F.col("ds.positionText").alias("position_text"),
            F.col("ds.points").cast(DoubleType()).alias("points"),
            F.col("ds.wins").cast(IntegerType()).alias("wins"),
        ).dropDuplicates()

        return flat
    except Exception as e:
        print(f"    ⚠ Could not flatten driver_standings for {season}/R{round_num:02d}: {e}")
        return None


def flatten_constructor_standings(df_raw, season, round_num):
    """
    MRData.StandingsTable.StandingsLists[].ConstructorStandings[] →
      .Constructor.{constructorId, name}
      .position, .points, .wins
    """
    try:
        standings = df_raw.select(
            F.explode("MRData.StandingsTable.StandingsLists").alias("sl")
        )
        constructors = standings.select(
            F.lit(season).alias("season"),
            F.lit(round_num).alias("round"),
            F.explode("sl.ConstructorStandings").alias("cs"),
        )

        flat = constructors.select(
            F.col("season").cast(IntegerType()),
            F.col("round").cast(IntegerType()),
            F.col("cs.Constructor.constructorId").alias("constructor_id"),
            F.col("cs.Constructor.name").alias("constructor_name"),
            F.col("cs.Constructor.nationality").alias("nationality"),
            F.col("cs.position").cast(IntegerType()).alias("position"),
            F.col("cs.positionText").alias("position_text"),
            F.col("cs.points").cast(DoubleType()).alias("points"),
            F.col("cs.wins").cast(IntegerType()).alias("wins"),
        ).dropDuplicates()

        return flat
    except Exception as e:
        print(f"    ⚠ Could not flatten constructor_standings for {season}/R{round_num:02d}: {e}")
        return None


def flatten_sprint(df_raw, season, round_num):
    """
    Same structure as results — sprint races use identical Ergast format
    but under MRData.RaceTable.Races[].SprintResults[].

    For non-sprint rounds, the JSON may have Races as a STRING (empty)
    instead of an ARRAY. We check the schema type before exploding.
    """
    try:
        # Guard: check if Races is actually an array (sprint data exists)
        # For non-sprint rounds, Spark infers Races as StringType
        from pyspark.sql.types import ArrayType, StructType as ST

        race_table_schema = df_raw.schema
        # Navigate: MRData -> RaceTable -> Races
        try:
            mrdata_type = race_table_schema["MRData"].dataType
            racetable_type = mrdata_type["RaceTable"].dataType
            races_type = racetable_type["Races"].dataType
        except (KeyError, AttributeError):
            return None

        if not isinstance(races_type, ArrayType):
            # Races is a STRING (no sprint data) — skip silently
            return None

        races = df_raw.select(F.explode("MRData.RaceTable.Races").alias("race"))

        # Check if SprintResults field exists and is an array
        race_struct = races_type.elementType
        sprint_fields = [f for f in race_struct.fields if f.name == "SprintResults"]
        if not sprint_fields or not isinstance(sprint_fields[0].dataType, ArrayType):
            return None

        results = races.select(
            F.lit(season).alias("season"),
            F.lit(round_num).alias("round"),
            F.explode("race.SprintResults").alias("r"),
        )

        flat = results.select(
            F.col("season").cast(IntegerType()),
            F.col("round").cast(IntegerType()),
            F.col("r.number").cast(IntegerType()).alias("driver_number"),
            F.col("r.position").cast(IntegerType()).alias("position"),
            F.col("r.positionText").alias("position_text"),
            F.col("r.points").cast(DoubleType()).alias("points"),
            F.col("r.Driver.driverId").alias("driver_id"),
            F.col("r.Driver.code").alias("driver_code"),
            F.col("r.Constructor.constructorId").alias("constructor_id"),
            F.col("r.Constructor.name").alias("constructor_name"),
            F.col("r.grid").cast(IntegerType()).alias("grid"),
            F.col("r.laps").cast(IntegerType()).alias("laps"),
            F.col("r.status").alias("status"),
            F.col("r.Time.millis").cast(LongType()).alias("time_millis"),
            F.col("r.Time.time").alias("time_text"),
        ).dropDuplicates()

        return flat
    except Exception as e:
        print(f"    ⚠ Could not flatten sprint for {season}/R{round_num:02d}: {e}")
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ENDPOINT CONFIG & MAIN PIPELINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Endpoint → (flatten function, JSON filename in S3)
ENDPOINT_CONFIG = {
    "results":                 (flatten_results,                "results.json"),
    "qualifying":              (flatten_qualifying,             "qualifying.json"),
    "pitstops":                (flatten_pitstops,               "pitstops.json"),
    "driver_standings":        (flatten_driver_standings,       "driver_standings.json"),
    "constructor_standings":   (flatten_constructor_standings,  "constructor_standings.json"),
    "sprint":                  (flatten_sprint,                 "sprint.json"),
}


print("=" * 60)
print("clean_jolpica.py — Starting")
print("=" * 60)

# 1. Discover all season/round paths in S3
round_paths = discover_round_paths()
print(f"  Found {len(round_paths)} round paths to scan\n")

# 2. Build all expected raw S3 keys
#    Format: raw/jolpica/seasons/{year}/round_{NN}/{filename}
ALL_RAW_PATHS = []
for rp in round_paths:
    for _, (_, filename) in ENDPOINT_CONFIG.items():
        ALL_RAW_PATHS.append(
            f"raw/jolpica/seasons/{rp['season']}/round_{rp['round']:02d}/{filename}"
        )

# 3. Check manifest — only process new files
unprocessed, manifest = get_unprocessed("jolpica", ALL_RAW_PATHS)

if not unprocessed:
    print("All files already processed — nothing to do.")
else:
    # 4. Process each unprocessed file
    for raw_path in unprocessed:
        # Parse path components:
        #   raw/jolpica/seasons/2025/round_01/results.json
        #   [0]  [1]     [2]    [3]   [4]      [5]
        parts = raw_path.split("/")
        season    = int(parts[3])
        round_dir = parts[4]
        round_num = int(round_dir.replace("round_", ""))
        filename  = parts[5]

        # Look up the matching flatten function
        flatten_fn   = None
        endpoint_name = None
        for ep_name, (fn, fn_name) in ENDPOINT_CONFIG.items():
            if fn_name == filename:
                flatten_fn    = fn
                endpoint_name = ep_name
                break

        if flatten_fn is None:
            print(f"  ⚠ Unknown file {filename} — skipping")
            continue

        # Read the raw JSON
        json_path = f"{RAW_BASE}/seasons/{season}/{round_dir}/{filename}"
        df_raw = safe_read_json(json_path)

        if df_raw is None:
            # Don't mark as done — retry on next run
            # (file may not exist yet if ingestion hasn't finished)
            continue

        # Flatten + type cast
        df_clean = flatten_fn(df_raw, season, round_num)

        if df_clean is None or df_clean.rdd.isEmpty():
            continue

        # Write with append mode — many JSON files contribute to one
        # endpoint table (one per season/round), just like OpenF1.
        # Partitioned by season for cost-efficient downstream queries.
        path = f"{PROCESSED_BASE}/{endpoint_name}"
        df_clean.write.mode("append").partitionBy("season").parquet(path)
        print(f"  ✓ {raw_path}")

        # Crash-safe: mark done and persist manifest after each file
        mark_file_done(raw_path, manifest)
        save_manifest("jolpica", manifest)

    print("\n" + "=" * 60)
    print("clean_jolpica.py — Complete ✓")
    print("=" * 60)

# Commit the Glue job so AWS marks it as Succeeded
job.commit()
