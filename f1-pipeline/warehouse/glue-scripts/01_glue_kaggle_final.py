"""
01_glue_kaggle_final.py — AWS Glue PySpark job (PRODUCTION)

Purpose:
    One-time load of Kaggle F1 CSVs (1950-2024) from raw S3 to processed layer.
    Uses watermark to ensure idempotency: if already loaded, script exits cleanly.
    
    Handles 13 CSV tables with schema enforcement, NULL replacement 
    type casting, and deduplication. Output is partitioned by year for
    efficient dbt staging queries.

Watermark Logic:
    - Reads: s3://f1-pipeline-raw-layer/meta/watermarks/kaggle_watermark.json
    - Pattern: {"status": "loaded_once", "load_date": "2024-03-16T..."}
    - If status == "loaded_once", script exits (already processed)
    - After successful write, updates watermark with current timestamp

Input:   s3://f1-pipeline-raw-layer/raw/kaggle/{date}/*.csv
Output:  s3://f1-pipeline-processed-layer/processed/kaggle/{table}/year={YYYY}/*.parquet

Safety:
    - Write FIRST, update watermark SECOND (crash-safe)
    - If watermark update fails, data is still in processed layer
    - Re-running with same data = idempotent (no duplicates)
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

RAW_KAGGLE_PATH       = f"s3://{S3_RAW_BUCKET}/raw/kaggle"
PROCESSED_KAGGLE_PATH = f"s3://{S3_PROCESSED_BUCKET}/processed/kaggle"
WATERMARK_BUCKET      = S3_RAW_BUCKET
WATERMARK_KEY         = "meta/watermarks/kaggle_watermark.json"

TABLES = [
    "circuits", "constructors", "drivers", "races", "results",
    "qualifying", "lap_times", "pit_stops", "driver_standings",
    "constructor_standings", "constructor_results", "sprint_results",
    "seasons", "status",
]


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
    job.init("f1-kaggle-load", {})

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
    Read Kaggle watermark from S3.
    Returns dict with status and load_date, or empty dict if not found.
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
    watermark_dict["load_date"] = datetime.now(timezone.utc).isoformat()
    get_s3_client().put_object(
        Bucket=WATERMARK_BUCKET,
        Key=WATERMARK_KEY,
        Body=json.dumps(watermark_dict, indent=2),
        ContentType="application/json",
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def replace_backslash_n(df):
    r"""Replace literal \N strings with None in all columns (Kaggle CSV quirk)."""
    for col_name in df.columns:
        df = df.withColumn(col_name, F.when(F.col(col_name) == r"\N", None).otherwise(F.col(col_name)))
    return df


@F.udf(LongType())
def parse_lap_time_ms(time_str):
    """Convert M:SS.sss or SS.sss to milliseconds."""
    if time_str is None:
        return None
    try:
        if ":" in time_str:
            parts = time_str.split(":")
            minutes = int(parts[0])
            sec_parts = parts[1].split(".")
            seconds = int(sec_parts[0])
            millis = int(sec_parts[1][:3].ljust(3, "0")) if len(sec_parts) > 1 else 0
            return minutes * 60_000 + seconds * 1_000 + millis
        else:
            sec_parts = time_str.split(".")
            seconds = int(sec_parts[0])
            millis = int(sec_parts[1][:3].ljust(3, "0")) if len(sec_parts) > 1 else 0
            return seconds * 1_000 + millis
    except Exception:
        return None


def read_csv(table_name):
    """Read a CSV file from raw Kaggle path."""
    path = f"{RAW_KAGGLE_PATH}/*/{table_name}.csv"
    return spark.read.option("header", True).option("inferSchema", False).csv(path)


def write_parquet(df, table_name, partition_cols=None):
    """Write DataFrame to processed layer as Parquet."""
    output_path = f"{PROCESSED_KAGGLE_PATH}/{table_name}"
    if partition_cols:
        df.write.mode("overwrite").partitionBy(partition_cols).parquet(output_path)
    else:
        df.write.mode("overwrite").parquet(output_path)
    print(f"  ✓ Wrote {table_name} to {output_path}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE CLEANERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def clean_circuits():
    print("[circuits]")
    df = read_csv("circuits")
    df = replace_backslash_n(df)
    df = (
        df
        .withColumn("circuitId", F.col("circuitId").cast(IntegerType()))
        .dropDuplicates()
    )
    write_parquet(df, "circuits")


def clean_constructors():
    print("[constructors]")
    df = read_csv("constructors")
    df = replace_backslash_n(df)
    df = (
        df
        .withColumn("constructorId", F.col("constructorId").cast(IntegerType()))
        .dropDuplicates()
    )
    write_parquet(df, "constructors")


def clean_drivers():
    print("[drivers]")
    df = read_csv("drivers")
    df = replace_backslash_n(df)
    df = (
        df
        .withColumn("driverId", F.col("driverId").cast(IntegerType()))
        .withColumn("number", F.col("number").cast(IntegerType()))
        .withColumn("dob", F.to_date(F.col("dob"), "yyyy-MM-dd"))
        .dropDuplicates()
    )
    write_parquet(df, "drivers")


def clean_seasons():
    print("[seasons]")
    df = read_csv("seasons")
    df = replace_backslash_n(df)
    df = (
        df
        .withColumn("year", F.col("year").cast(IntegerType()))
        .dropDuplicates()
    )
    write_parquet(df, "seasons")


def clean_status():
    print("[status]")
    df = read_csv("status")
    df = replace_backslash_n(df)
    df = (
        df
        .withColumn("statusId", F.col("statusId").cast(IntegerType()))
        .dropDuplicates()
    )
    write_parquet(df, "status")


def clean_races():
    print("[races]")
    df = read_csv("races")
    df = replace_backslash_n(df)
    df = (
        df
        .withColumn("raceId", F.col("raceId").cast(IntegerType()))
        .withColumn("year", F.col("year").cast(IntegerType()))
        .withColumn("round", F.col("round").cast(IntegerType()))
        .withColumn("circuitId", F.col("circuitId").cast(IntegerType()))
        .withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        .dropDuplicates()
    )
    write_parquet(df, "races", partition_cols=["year"])


def clean_results():
    print("[results]")
    df = read_csv("results")
    df = replace_backslash_n(df)

    races = (
        read_csv("races")
        .transform(replace_backslash_n)
        .select(
            F.col("raceId").cast(IntegerType()).alias("raceId"),
            F.col("year").cast(IntegerType()).alias("year"),
        )
        .dropDuplicates()
    )

    df = (
        df
        .withColumn("resultId", F.col("resultId").cast(IntegerType()))
        .withColumn("raceId", F.col("raceId").cast(IntegerType()))
        .withColumn("driverId", F.col("driverId").cast(IntegerType()))
        .withColumn("constructorId", F.col("constructorId").cast(IntegerType()))
        .withColumn("number", F.col("number").cast(IntegerType()))
        .withColumn("grid", F.when(F.col("grid") != r"\N", F.col("grid")).cast(IntegerType()))
        .withColumn("position", F.when(F.col("position") != r"\N", F.col("position")).cast(IntegerType()))
        .withColumn("positionOrder", F.col("positionOrder").cast(IntegerType()))
        .withColumn("points", F.col("points").cast(DoubleType()))
        .withColumn("laps", F.col("laps").cast(IntegerType()))
        .withColumn("milliseconds", F.when(F.col("milliseconds") != r"\N", F.col("milliseconds")).cast(LongType()))
        .withColumn("fastestLapTime_ms", parse_lap_time_ms(F.col("fastestLapTime")))
        .withColumn("fastestLapSpeed", F.col("fastestLapSpeed").cast(DoubleType()))
        .withColumn("statusId", F.col("statusId").cast(IntegerType()))
        .dropDuplicates()
    )

    df = df.join(races, on="raceId", how="left")
    write_parquet(df, "results", partition_cols=["year"])


def clean_qualifying():
    print("[qualifying]")
    df = read_csv("qualifying")
    df = replace_backslash_n(df)

    races = (
        read_csv("races")
        .transform(replace_backslash_n)
        .select(
            F.col("raceId").cast(IntegerType()).alias("raceId"),
            F.col("year").cast(IntegerType()).alias("year"),
        )
        .dropDuplicates()
    )

    df = (
        df
        .withColumn("qualifyId", F.col("qualifyId").cast(IntegerType()))
        .withColumn("raceId", F.col("raceId").cast(IntegerType()))
        .withColumn("driverId", F.col("driverId").cast(IntegerType()))
        .withColumn("constructorId", F.col("constructorId").cast(IntegerType()))
        .withColumn("number", F.col("number").cast(IntegerType()))
        .withColumn("position", F.col("position").cast(IntegerType()))
        .withColumn("q1_ms", parse_lap_time_ms(F.col("q1")))
        .withColumn("q2_ms", parse_lap_time_ms(F.col("q2")))
        .withColumn("q3_ms", parse_lap_time_ms(F.col("q3")))
        .dropDuplicates()
    )

    df = df.join(races, on="raceId", how="left")
    write_parquet(df, "qualifying", partition_cols=["year"])


def clean_lap_times():
    print("[lap_times]")
    df = read_csv("lap_times")
    df = replace_backslash_n(df)

    races = (
        read_csv("races")
        .transform(replace_backslash_n)
        .select(
            F.col("raceId").cast(IntegerType()).alias("raceId"),
            F.col("year").cast(IntegerType()).alias("year"),
        )
        .dropDuplicates()
    )

    df = (
        df
        .withColumn("raceId", F.col("raceId").cast(IntegerType()))
        .withColumn("driverId", F.col("driverId").cast(IntegerType()))
        .withColumn("lap", F.col("lap").cast(IntegerType()))
        .withColumn("position", F.col("position").cast(IntegerType()))
        .withColumn("milliseconds", F.col("milliseconds").cast(LongType()))
        .withColumn("time_ms", parse_lap_time_ms(F.col("time")))
        .dropDuplicates()
    )

    df = df.join(races, on="raceId", how="left")
    write_parquet(df, "lap_times", partition_cols=["year"])


def clean_pit_stops():
    print("[pit_stops]")
    df = read_csv("pit_stops")
    df = replace_backslash_n(df)

    races = (
        read_csv("races")
        .transform(replace_backslash_n)
        .select(
            F.col("raceId").cast(IntegerType()).alias("raceId"),
            F.col("year").cast(IntegerType()).alias("year"),
        )
        .dropDuplicates()
    )

    df = (
        df
        .withColumn("raceId", F.col("raceId").cast(IntegerType()))
        .withColumn("driverId", F.col("driverId").cast(IntegerType()))
        .withColumn("stop", F.col("stop").cast(IntegerType()))
        .withColumn("lap", F.col("lap").cast(IntegerType()))
        .withColumn("milliseconds", F.col("milliseconds").cast(LongType()))
        .withColumn("duration_ms", parse_lap_time_ms(F.col("duration")))
        .dropDuplicates()
    )

    df = df.join(races, on="raceId", how="left")
    write_parquet(df, "pit_stops", partition_cols=["year"])


def clean_driver_standings():
    print("[driver_standings]")
    df = read_csv("driver_standings")
    df = replace_backslash_n(df)

    races = (
        read_csv("races")
        .transform(replace_backslash_n)
        .select(
            F.col("raceId").cast(IntegerType()).alias("raceId"),
            F.col("year").cast(IntegerType()).alias("year"),
        )
        .dropDuplicates()
    )

    df = (
        df
        .withColumn("driverStandingsId", F.col("driverStandingsId").cast(IntegerType()))
        .withColumn("raceId", F.col("raceId").cast(IntegerType()))
        .withColumn("driverId", F.col("driverId").cast(IntegerType()))
        .withColumn("points", F.col("points").cast(DoubleType()))
        .withColumn("position", F.col("position").cast(IntegerType()))
        .withColumn("wins", F.col("wins").cast(IntegerType()))
        .dropDuplicates()
    )

    df = df.join(races, on="raceId", how="left")
    write_parquet(df, "driver_standings", partition_cols=["year"])


def clean_constructor_standings():
    print("[constructor_standings]")
    df = read_csv("constructor_standings")
    df = replace_backslash_n(df)

    races = (
        read_csv("races")
        .transform(replace_backslash_n)
        .select(
            F.col("raceId").cast(IntegerType()).alias("raceId"),
            F.col("year").cast(IntegerType()).alias("year"),
        )
        .dropDuplicates()
    )

    df = (
        df
        .withColumn("constructorStandingsId", F.col("constructorStandingsId").cast(IntegerType()))
        .withColumn("raceId", F.col("raceId").cast(IntegerType()))
        .withColumn("constructorId", F.col("constructorId").cast(IntegerType()))
        .withColumn("points", F.col("points").cast(DoubleType()))
        .withColumn("position", F.col("position").cast(IntegerType()))
        .withColumn("wins", F.col("wins").cast(IntegerType()))
        .dropDuplicates()
    )

    df = df.join(races, on="raceId", how="left")
    write_parquet(df, "constructor_standings", partition_cols=["year"])


def clean_constructor_results():
    print("[constructor_results]")
    df = read_csv("constructor_results")
    df = replace_backslash_n(df)

    races = (
        read_csv("races")
        .transform(replace_backslash_n)
        .select(
            F.col("raceId").cast(IntegerType()).alias("raceId"),
            F.col("year").cast(IntegerType()).alias("year"),
        )
        .dropDuplicates()
    )

    df = (
        df
        .withColumn("constructorResultsId", F.col("constructorResultsId").cast(IntegerType()))
        .withColumn("raceId", F.col("raceId").cast(IntegerType()))
        .withColumn("constructorId", F.col("constructorId").cast(IntegerType()))
        .withColumn("points", F.col("points").cast(DoubleType()))
        .dropDuplicates()
    )

    df = df.join(races, on="raceId", how="left")
    write_parquet(df, "constructor_results", partition_cols=["year"])


def clean_sprint_results():
    print("[sprint_results]")
    df = read_csv("sprint_results")
    df = replace_backslash_n(df)

    races = (
        read_csv("races")
        .transform(replace_backslash_n)
        .select(
            F.col("raceId").cast(IntegerType()).alias("raceId"),
            F.col("year").cast(IntegerType()).alias("year"),
        )
        .dropDuplicates()
    )

    df = (
        df
        .withColumn("resultId", F.col("resultId").cast(IntegerType()))
        .withColumn("raceId", F.col("raceId").cast(IntegerType()))
        .withColumn("driverId", F.col("driverId").cast(IntegerType()))
        .withColumn("constructorId", F.col("constructorId").cast(IntegerType()))
        .withColumn("number", F.col("number").cast(IntegerType()))
        .withColumn("grid", F.col("grid").cast(IntegerType()))
        .withColumn("position", F.col("position").cast(IntegerType()))
        .withColumn("positionOrder", F.col("positionOrder").cast(IntegerType()))
        .withColumn("points", F.col("points").cast(DoubleType()))
        .withColumn("laps", F.col("laps").cast(IntegerType()))
        .withColumn("milliseconds", F.col("milliseconds").cast(LongType()))
        .withColumn("fastestLapTime_ms", parse_lap_time_ms(F.col("fastestLapTime")))
        .withColumn("statusId", F.col("statusId").cast(IntegerType()))
        .dropDuplicates()
    )

    df = df.join(races, on="raceId", how="left")
    write_parquet(df, "sprint_results", partition_cols=["year"])


TABLE_CLEANERS = {
    "circuits": clean_circuits,
    "constructors": clean_constructors,
    "drivers": clean_drivers,
    "seasons": clean_seasons,
    "status": clean_status,
    "races": clean_races,
    "results": clean_results,
    "qualifying": clean_qualifying,
    "lap_times": clean_lap_times,
    "pit_stops": clean_pit_stops,
    "driver_standings": clean_driver_standings,
    "constructor_standings": clean_constructor_standings,
    "constructor_results": clean_constructor_results,
    "sprint_results": clean_sprint_results,
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    print("=" * 70)
    print("01_glue_kaggle_final.py — Starting")
    print("=" * 70)

    watermark = load_watermark()
    
    if watermark.get("status") == "loaded_once":
        print(f"\n  Kaggle already loaded at {watermark.get('load_date')}")
        print("  Skipping (idempotent).\n")
        job.commit()
        return

    print("  First-time load: processing all 14 tables\n")

    for table_name in TABLES:
        try:
            print(f"Processing {table_name}...")
            cleaner = TABLE_CLEANERS[table_name]
            cleaner()
        except Exception as e:
            print(f"  ERROR in {table_name}: {e}")
            raise

    print("\nUpdating watermark...")
    save_watermark({"status": "loaded_once"})

    print("=" * 70)
    print("01_glue_kaggle_final.py — Complete")
    print("=" * 70)

    job.commit()


if __name__ == "__main__":
    main()
