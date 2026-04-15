"""
clean_kaggle.py — PySpark job (Databricks / AWS Glue)

Reads 14 Kaggle CSVs from the raw zone in S3, applies:
  - \\N → null replacement
  - Explicit type casting per table
  - Lap-time string parsing ("M:SS.mmm" → milliseconds)
  - Deduplication
  - Writes clean Parquet to the processed zone in S3

Both environments use S3 + manifest-based incremental processing.
MODE only controls Spark session init and S3 scheme (s3a:// vs s3://).

Credentials:
  - Databricks: Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
    as environment variables (cluster config or notebook widgets).
  - Glue:       IAM execution role provides credentials automatically.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DoubleType, StringType, DateType,
)
import os
import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime, timezone

# ── Configuration ────────────────────────────────────────────────
MODE = "databricks"  # "databricks" | "glue"

# S3 bucket names — read from environment variables
S3_RAW_BUCKET       = os.environ.get("S3_RAW_BUCKET", "f1-pipeline-raw-layer-034381339055")
S3_PROCESSED_BUCKET = os.environ.get("S3_PROCESSED_BUCKET", "f1-pipeline-processed-layer-034381339055")
AWS_REGION          = os.environ.get("AWS_REGION", "ap-south-1")

# S3 scheme: Databricks uses s3a://, Glue uses s3://
S3_SCHEME = "s3a" if MODE == "databricks" else "s3"

RAW_BASE       = f"{S3_SCHEME}://{S3_RAW_BUCKET}/raw/kaggle"
PROCESSED_BASE = f"{S3_SCHEME}://{S3_PROCESSED_BUCKET}/processed/kaggle"

# Manifest storage
MANIFEST_BUCKET = S3_RAW_BUCKET
MANIFEST_PREFIX = "meta/manifests"

# ── Spark Session ───────────────────────────────────────────────
if MODE == "databricks":
    spark = (
        SparkSession.builder
        .appName("clean_kaggle")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
# In Glue: spark is provided by GlueContext — do not create it.


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def replace_backslash_n(df):
    """Replace the literal string '\\N' with null in every column."""
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) == "\\N", F.lit(None)).otherwise(F.col(col_name)),
        )
    return df


@F.udf(LongType())
def parse_lap_time_ms(time_str):
    """
    Convert a lap-time string to milliseconds.
    Handles formats:
      "1:27.452"  → 87452
      "27.452"    → 27452  (no minutes)
    Returns None for null / unparseable input.
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


def read_csv(table_name):
    """Read a raw CSV with header, all columns as string."""
    path = f"{RAW_BASE}/{table_name}.csv"
    print(f"  Reading {path}")
    return spark.read.csv(path, header=True, inferSchema=False)


def write_parquet(df, table_name, partition_cols=None):
    """Write a DataFrame as Parquet (optionally partitioned)."""
    path = f"{PROCESSED_BASE}/{table_name}"
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    count = df.count()
    print(f"  ✓ Wrote {table_name} → {path}  ({count:,} rows)")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MANIFEST HELPERS  (Glue mode only — skipped in Databricks)
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
        Bucket=MANIFEST_BUCKET,
        Key=key,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json",
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
#  TABLE CLEANERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def clean_circuits():
    print("[circuits]")
    df = read_csv("circuits")
    df = replace_backslash_n(df)
    df = (
        df
        .withColumn("circuitId",  F.col("circuitId").cast(IntegerType()))
        .withColumn("lat",        F.col("lat").cast(DoubleType()))
        .withColumn("lng",        F.col("lng").cast(DoubleType()))
        .withColumn("alt",        F.col("alt").cast(IntegerType()))
        # circuitRef, name, location, country, url → keep as STRING
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
        # constructorRef, name, nationality, url → STRING
        .dropDuplicates()
    )
    write_parquet(df, "constructors")


def clean_drivers():
    print("[drivers]")
    df = read_csv("drivers")
    df = replace_backslash_n(df)
    df = (
        df
        .withColumn("driverId",  F.col("driverId").cast(IntegerType()))
        .withColumn("number",    F.col("number").cast(IntegerType()))
        .withColumn("dob",       F.to_date(F.col("dob"), "yyyy-MM-dd"))
        # driverRef, code, forename, surname, nationality, url → STRING
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
        .withColumn("raceId",    F.col("raceId").cast(IntegerType()))
        .withColumn("year",      F.col("year").cast(IntegerType()))
        .withColumn("round",     F.col("round").cast(IntegerType()))
        .withColumn("circuitId", F.col("circuitId").cast(IntegerType()))
        .withColumn("date",      F.to_date(F.col("date"), "yyyy-MM-dd"))
        .withColumn("fp1_date",  F.to_date(F.col("fp1_date"), "yyyy-MM-dd"))
        .withColumn("fp2_date",  F.to_date(F.col("fp2_date"), "yyyy-MM-dd"))
        .withColumn("fp3_date",  F.to_date(F.col("fp3_date"), "yyyy-MM-dd"))
        .withColumn("quali_date", F.to_date(F.col("quali_date"), "yyyy-MM-dd"))
        .withColumn("sprint_date", F.to_date(F.col("sprint_date"), "yyyy-MM-dd"))
        # time, fp*_time, quali_time, sprint_time → STRING
        .dropDuplicates()
    )
    write_parquet(df, "races", partition_cols=["year"])


def clean_results():
    """
    Results table — the biggest and most important.
    Joins to races to get 'year' for partitioning.
    """
    print("[results]")
    df = read_csv("results")
    df = replace_backslash_n(df)

    # Read races to get year for partitioning
    races = (
        read_csv("races")
        .transform(replace_backslash_n)
        .select(
            F.col("raceId").cast(IntegerType()).alias("raceId"),
            F.col("year").cast(IntegerType()).alias("year"),
        )
    )

    df = (
        df
        .withColumn("resultId",       F.col("resultId").cast(IntegerType()))
        .withColumn("raceId",         F.col("raceId").cast(IntegerType()))
        .withColumn("driverId",       F.col("driverId").cast(IntegerType()))
        .withColumn("constructorId",  F.col("constructorId").cast(IntegerType()))
        .withColumn("number",         F.col("number").cast(IntegerType()))
        .withColumn("grid",           F.col("grid").cast(IntegerType()))
        .withColumn("position",       F.col("position").cast(IntegerType()))
        .withColumn("positionOrder",  F.col("positionOrder").cast(IntegerType()))
        .withColumn("points",         F.col("points").cast(DoubleType()))
        .withColumn("laps",           F.col("laps").cast(IntegerType()))
        .withColumn("milliseconds",   F.col("milliseconds").cast(LongType()))
        .withColumn("fastestLap",     F.col("fastestLap").cast(IntegerType()))
        .withColumn("rank",           F.col("rank").cast(IntegerType()))
        .withColumn("fastestLapTime_ms", parse_lap_time_ms(F.col("fastestLapTime")))
        .withColumn("fastestLapSpeed", F.col("fastestLapSpeed").cast(DoubleType()))
        .withColumn("statusId",       F.col("statusId").cast(IntegerType()))
        # positionText, time → STRING
        .dropDuplicates()
    )

    # Join to get year, then partition
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
    )

    df = (
        df
        .withColumn("qualifyId",      F.col("qualifyId").cast(IntegerType()))
        .withColumn("raceId",         F.col("raceId").cast(IntegerType()))
        .withColumn("driverId",       F.col("driverId").cast(IntegerType()))
        .withColumn("constructorId",  F.col("constructorId").cast(IntegerType()))
        .withColumn("number",         F.col("number").cast(IntegerType()))
        .withColumn("position",       F.col("position").cast(IntegerType()))
        .withColumn("q1_ms",          parse_lap_time_ms(F.col("q1")))
        .withColumn("q2_ms",          parse_lap_time_ms(F.col("q2")))
        .withColumn("q3_ms",          parse_lap_time_ms(F.col("q3")))
        # Keep original q1, q2, q3 strings too
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
    )

    df = (
        df
        .withColumn("raceId",       F.col("raceId").cast(IntegerType()))
        .withColumn("driverId",     F.col("driverId").cast(IntegerType()))
        .withColumn("lap",          F.col("lap").cast(IntegerType()))
        .withColumn("position",     F.col("position").cast(IntegerType()))
        .withColumn("milliseconds", F.col("milliseconds").cast(LongType()))
        .withColumn("time_ms",      parse_lap_time_ms(F.col("time")))
        # Keep original time string too
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
    )

    df = (
        df
        .withColumn("raceId",        F.col("raceId").cast(IntegerType()))
        .withColumn("driverId",      F.col("driverId").cast(IntegerType()))
        .withColumn("stop",          F.col("stop").cast(IntegerType()))
        .withColumn("lap",           F.col("lap").cast(IntegerType()))
        .withColumn("milliseconds",  F.col("milliseconds").cast(LongType()))
        .withColumn("duration_ms",   parse_lap_time_ms(F.col("duration")))
        # Keep original duration string for reference
        # time → STRING (clock time of pit stop)
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
    )

    df = (
        df
        .withColumn("driverStandingsId", F.col("driverStandingsId").cast(IntegerType()))
        .withColumn("raceId",            F.col("raceId").cast(IntegerType()))
        .withColumn("driverId",          F.col("driverId").cast(IntegerType()))
        .withColumn("points",            F.col("points").cast(DoubleType()))
        .withColumn("position",          F.col("position").cast(IntegerType()))
        .withColumn("wins",              F.col("wins").cast(IntegerType()))
        # positionText → STRING
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
    )

    df = (
        df
        .withColumn("constructorStandingsId", F.col("constructorStandingsId").cast(IntegerType()))
        .withColumn("raceId",                 F.col("raceId").cast(IntegerType()))
        .withColumn("constructorId",          F.col("constructorId").cast(IntegerType()))
        .withColumn("points",                 F.col("points").cast(DoubleType()))
        .withColumn("position",               F.col("position").cast(IntegerType()))
        .withColumn("wins",                   F.col("wins").cast(IntegerType()))
        # positionText → STRING
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
    )

    df = (
        df
        .withColumn("constructorResultsId", F.col("constructorResultsId").cast(IntegerType()))
        .withColumn("raceId",               F.col("raceId").cast(IntegerType()))
        .withColumn("constructorId",        F.col("constructorId").cast(IntegerType()))
        .withColumn("points",               F.col("points").cast(DoubleType()))
        # status → STRING
        .dropDuplicates()
    )

    df = df.join(races, on="raceId", how="left")
    write_parquet(df, "constructor_results", partition_cols=["year"])


def clean_sprint_results():
    """Same structure as results — separate table for sprint races."""
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
    )

    df = (
        df
        .withColumn("resultId",       F.col("resultId").cast(IntegerType()))
        .withColumn("raceId",         F.col("raceId").cast(IntegerType()))
        .withColumn("driverId",       F.col("driverId").cast(IntegerType()))
        .withColumn("constructorId",  F.col("constructorId").cast(IntegerType()))
        .withColumn("number",         F.col("number").cast(IntegerType()))
        .withColumn("grid",           F.col("grid").cast(IntegerType()))
        .withColumn("position",       F.col("position").cast(IntegerType()))
        .withColumn("positionOrder",  F.col("positionOrder").cast(IntegerType()))
        .withColumn("points",         F.col("points").cast(DoubleType()))
        .withColumn("laps",           F.col("laps").cast(IntegerType()))
        .withColumn("milliseconds",   F.col("milliseconds").cast(LongType()))
        .withColumn("fastestLap",        F.col("fastestLap").cast(IntegerType()))
        .withColumn("fastestLapTime_ms", parse_lap_time_ms(F.col("fastestLapTime")))
        .withColumn("statusId",       F.col("statusId").cast(IntegerType()))
        # positionText, time → STRING
        .dropDuplicates()
    )

    df = df.join(races, on="raceId", how="left")
    write_parquet(df, "sprint_results", partition_cols=["year"])


# ── Table cleaner lookup ─────────────────────────────────────
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    print("=" * 60)
    print("clean_kaggle.py — Starting")
    print(f"  MODE:      {MODE}")
    print(f"  RAW:       {RAW_BASE}")
    print(f"  PROCESSED: {PROCESSED_BASE}")
    print("=" * 60)

    ALL_RAW_PATHS = [f"raw/kaggle/{name}.csv" for name in [
        "circuits", "constructors", "drivers", "races", "results",
        "qualifying", "lap_times", "pit_stops", "driver_standings",
        "constructor_standings", "constructor_results", "sprint_results",
        "seasons", "status",
    ]]

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

    print("=" * 60)
    print("clean_kaggle.py — Complete ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
