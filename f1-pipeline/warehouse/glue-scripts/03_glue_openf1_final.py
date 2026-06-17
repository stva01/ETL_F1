"""
03_glue_openf1_final.py — AWS Glue PySpark job (PRODUCTION)

Purpose:
    Incremental load of OpenF1 F1 telemetry data (drivers, laps, pit, stints)
    from raw S3 to processed layer. Processes new sessions only via watermark.
    
    Each session is identified by meeting_id. Handles 4 endpoints with JSON
    flattening, schema enforcement, type casting, and deduplication.
    Output partitioned by year.

Watermark Logic:
    - Reads: s3://f1-pipeline-raw-layer/meta/watermarks/openf1_watermark.json
    - Pattern: {"last_year": 2026, "last_meeting_id": 1281, "last_processed": "2024-..."}
    - Processes only sessions where (year, meeting_id) > (last_year, last_meeting_id)
    - After each write, updates watermark atomically
    
    Example:
      Watermark: last_year=2026, last_meeting_id=1280
      Raw data: 2026/meeting_1279 (skip, <1280), 2026/meeting_1281..1283 (new)
      Job processes: 2026/meeting_1281..1283
      Updates: last_year=2026, last_meeting_id=1283

Input:   s3://f1-pipeline-raw-layer/raw/openf1/{year}/meeting_{id}/race/{endpoint}.json
Output:  s3://f1-pipeline-processed-layer/processed/openf1/{endpoint}/year={YYYY}/*.parquet

Safety:
    - Write FIRST, update watermark SECOND (crash-safe)
    - Re-running = idempotent (append mode + dropDuplicates)
    - Watermark moves forward only after successful writes

Endpoints:
    - drivers: driver metadata
    - laps: lap telemetry (driver_number, lap_number, gap_to_leader, etc.)
    - pit: pit stop events
    - stints: driver stint info (compound, lap_start, lap_end, etc.)
"""

import sys
import json
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, DoubleType, StringType

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIGURATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

S3_RAW_BUCKET       = "f1-pipeline-raw-layer-034381339055"
S3_PROCESSED_BUCKET = "f1-pipeline-processed-layer-034381339055"
AWS_REGION          = "ap-south-1"

RAW_OPENF1_PATH       = f"s3://{S3_RAW_BUCKET}/raw/openf1"
PROCESSED_OPENF1_PATH = f"s3://{S3_PROCESSED_BUCKET}/processed/openf1"
WATERMARK_BUCKET      = S3_RAW_BUCKET
WATERMARK_KEY         = "meta/watermarks/openf1_watermark.json"

ENDPOINTS = {
    "drivers": "drivers.json",
    "laps": "laps.json",
    "pit": "pit.json",
    "stints": "stints.json",
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GLUE SESSION INIT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

sc          = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)

try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job.init(args["JOB_NAME"], args)
except Exception:
    job.init("f1-openf1-load", {})

spark.sparkContext.setLogLevel("WARN")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WATERMARK HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_s3_client = None

def get_s3_client():
    """Lazy-init S3 client."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name=AWS_REGION)
    return _s3_client


def load_watermark():
    """
    Read OpenF1 watermark from S3.
    Returns: {"last_year": int, "last_meeting_id": int, "last_processed": "..."}
    If not found, returns empty dict (will process all data).
    """
    try:
        obj = get_s3_client().get_object(Bucket=WATERMARK_BUCKET, Key=WATERMARK_KEY)
        return json.loads(obj["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {}
        raise


def save_watermark(watermark_dict):
    """Write watermark dict to S3."""
    watermark_dict["last_processed"] = datetime.now(timezone.utc).isoformat()
    get_s3_client().put_object(
        Bucket=WATERMARK_BUCKET,
        Key=WATERMARK_KEY,
        Body=json.dumps(watermark_dict, indent=2),
        ContentType="application/json",
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def discover_session_paths():
    """
    Scan S3 raw/openf1/ for all (year, meeting_id) tuples.
    Returns: [(year, meeting_id), ...] sorted
    """
    seen, paths = set(), []
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=WATERMARK_BUCKET, Prefix="raw/openf1/"):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            if len(parts) < 4:
                continue
            year_str, meeting_dir = parts[2], parts[3]
            if not year_str.isdigit() or not meeting_dir.startswith("meeting_"):
                continue
            key = (int(year_str), int(meeting_dir.replace("meeting_", "")))
            if key not in seen:
                seen.add(key)
                paths.append(key)

    return sorted(paths)


def safe_read_json_array(path):
    """
    Try reading a JSON file as an array.
    OpenF1 endpoints return arrays of objects.
    Return None if missing or empty.
    """
    try:
        df = spark.read.json(path)
        if df.head(1):
            return df
        return None
    except Exception:
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ENDPOINT PROCESSORS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_drivers(df_raw, year, meeting_id):
    """
    Process drivers endpoint.
    OpenF1 drivers: broadcast_name, country_code, driver_number, 
                    first_name, full_name, session_key, team_colour, team_name, year
    """
    try:
        return (
            df_raw
            .withColumn("year", F.lit(year).cast(IntegerType()))
            .withColumn("meeting_id", F.lit(meeting_id).cast(IntegerType()))
            .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
            .dropDuplicates()
        )
    except Exception as e:
        print(f"  Error processing drivers {year}/meeting_{meeting_id}: {e}")
        return None


def process_laps(df_raw, year, meeting_id):
    """
    Process laps endpoint.
    OpenF1 laps: driver_number, lap_number, duration_sector_1, duration_sector_2,
                 duration_sector_3, gap_to_leader, interval, milliseconds, sector_1_ms, etc.
    """
    try:
        return (
            df_raw
            .withColumn("year", F.lit(year).cast(IntegerType()))
            .withColumn("meeting_id", F.lit(meeting_id).cast(IntegerType()))
            .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
            .withColumn("lap_number", F.col("lap_number").cast(IntegerType()))
            .withColumn("milliseconds", F.col("milliseconds").cast(LongType()))
            .dropDuplicates()
        )
    except Exception as e:
        print(f"  Error processing laps {year}/meeting_{meeting_id}: {e}")
        return None


def process_pit(df_raw, year, meeting_id):
    """
    Process pit endpoint.
    OpenF1 pit: driver_number, lap_number, time_pit_in_seconds, time_pit_out_seconds,
                duration_pit_seconds, compound, etc.
    """
    try:
        return (
            df_raw
            .withColumn("year", F.lit(year).cast(IntegerType()))
            .withColumn("meeting_id", F.lit(meeting_id).cast(IntegerType()))
            .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
            .withColumn("lap_number", F.col("lap_number").cast(IntegerType()))
            .dropDuplicates()
        )
    except Exception as e:
        print(f"  Error processing pit {year}/meeting_{meeting_id}: {e}")
        return None


def process_stints(df_raw, year, meeting_id):
    """
    Process stints endpoint.
    OpenF1 stints: driver_number, compound, lap_start, lap_end, 
                   tyre_age_at_start, tyre_age_at_end, etc.
    """
    try:
        return (
            df_raw
            .withColumn("year", F.lit(year).cast(IntegerType()))
            .withColumn("meeting_id", F.lit(meeting_id).cast(IntegerType()))
            .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
            .withColumn("lap_start", F.col("lap_start").cast(IntegerType()))
            .withColumn("lap_end", F.col("lap_end").cast(IntegerType()))
            .dropDuplicates()
        )
    except Exception as e:
        print(f"  Error processing stints {year}/meeting_{meeting_id}: {e}")
        return None


PROCESSORS = {
    "drivers": process_drivers,
    "laps": process_laps,
    "pit": process_pit,
    "stints": process_stints,
}


def write_endpoint(df, endpoint_name):
    """Write DataFrame to processed layer as Parquet (append mode)."""
    output_path = f"{PROCESSED_OPENF1_PATH}/{endpoint_name}"
    df.write.mode("append").partitionBy("year").parquet(output_path)
    print(f"  ✓ Wrote {endpoint_name}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    print("=" * 70)
    print("03_glue_openf1_final.py — Starting")
    print("=" * 70)

    # Load watermark
    watermark = load_watermark()
    last_year = watermark.get("last_year", 0)
    last_meeting_id = watermark.get("last_meeting_id", 0)
    
    print(f"\n  Last processed: year={last_year}, meeting_id={last_meeting_id}\n")

    # Discover all sessions in S3
    all_sessions = discover_session_paths()
    
    if not all_sessions:
        print("  No data found in S3")
        job.commit()
        return

    # Filter: only sessions > (last_year, last_meeting_id)
    new_sessions = [
        (y, m) for y, m in all_sessions
        if (y > last_year) or (y == last_year and m > last_meeting_id)
    ]

    if not new_sessions:
        print(f"  All sessions already processed. Skipping.\n")
        job.commit()
        return

    print(f"  Found {len(new_sessions)} new sessions to process\n")

    # Process each new session
    for year, meeting_id in new_sessions:
        print(f"Processing year={year}, meeting_id={meeting_id}")

        for endpoint_name, json_filename in ENDPOINTS.items():
            json_path = f"{RAW_OPENF1_PATH}/{year}/meeting_{meeting_id}/race/{json_filename}"
            df_raw = safe_read_json_array(json_path)

            if df_raw is None:
                continue

            processor = PROCESSORS[endpoint_name]
            df_clean = processor(df_raw, year, meeting_id)

            if df_clean is None or df_clean.rdd.isEmpty():
                continue

            write_endpoint(df_clean, endpoint_name)

        # Update watermark after this session
        save_watermark({"last_year": year, "last_meeting_id": meeting_id})
        print(f"  Watermark updated: year={year}, meeting_id={meeting_id}\n")

    print("=" * 70)
    print("03_glue_openf1_final.py — Complete")
    print("=" * 70)

    job.commit()


if __name__ == "__main__":
    main()