"""
02_glue_jolpica_final.py — AWS Glue PySpark job (PRODUCTION)

Purpose:
    Incremental load of Jolpica F1 data (results, qualifying, pitstops, standings, sprint)
    from raw S3 to processed layer. Processes new rounds only via watermark.
    
    Handles 6 endpoints per round with nested Ergast JSON flattening, schema
    enforcement, type casting, and deduplication. Output partitioned by season.

Watermark Logic:
    - Reads: s3://f1-pipeline-raw-layer/meta/watermarks/jolpica_watermark.json
    - Pattern: {"last_season": 2025, "last_round": 24, "last_processed": "2024-..."}
    - Processes only rounds where (season, round) > (last_season, last_round)
    - After each write, updates watermark atomically
    
    Example:
      Watermark: last_season=2025, last_round=5
      Raw data: 2025/round_1..5 (already done), 2025/round_6..10 (new), 2026/round_1 (new)
      Job processes: 2025/round_6..10, then 2026/round_1
      Updates: last_season=2026, last_round=1

Input:   s3://f1-pipeline-raw-layer/raw/jolpica/seasons/{year}/round_{NN}/{endpoint}.json
Output:  s3://f1-pipeline-processed-layer/processed/jolpica/{endpoint}/season={YYYY}/*.parquet

Safety:
    - Write FIRST, update watermark SECOND (crash-safe)
    - Re-running = idempotent (processes same round twice? df.write.mode("append") + dropDuplicates)
    - Watermark moves forward only after successful writes

Endpoints:
    - results, qualifying, pitstops (one row per thing)
    - driver_standings, constructor_standings (one row per standing)
    - sprint (conditional, only for sprint weekends)
"""

import sys
import json
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, DoubleType, StringType, ArrayType, StructType

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIGURATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

S3_RAW_BUCKET       = "f1-pipeline-raw-layer-034381339055"
S3_PROCESSED_BUCKET = "f1-pipeline-processed-layer-034381339055"
AWS_REGION          = "ap-south-1"

RAW_JOLPICA_PATH       = f"s3://{S3_RAW_BUCKET}/raw/jolpica"
PROCESSED_JOLPICA_PATH = f"s3://{S3_PROCESSED_BUCKET}/processed/jolpica"
WATERMARK_BUCKET       = S3_RAW_BUCKET
WATERMARK_KEY          = "meta/watermarks/jolpica_watermark.json"

ENDPOINTS = {
    "results": "results.json",
    "qualifying": "qualifying.json",
    "pitstops": "pitstops.json",
    "driver_standings": "driver_standings.json",
    "constructor_standings": "constructor_standings.json",
    "sprint": "sprint.json",
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
    job.init("f1-jolpica-load", {})

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
    Read Jolpica watermark from S3.
    Returns: {"last_season": int, "last_round": int, "last_processed": "..."}
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

@F.udf(LongType())
def parse_lap_time_ms(time_str):
    """Convert 'M:SS.sss' or 'SS.sss' to milliseconds."""
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


def discover_round_paths():
    """
    Scan S3 raw/jolpica/seasons/ for all (season, round) tuples.
    Returns: [(season, round), ...] sorted
    """
    seen, paths = set(), []
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=WATERMARK_BUCKET, Prefix="raw/jolpica/seasons/"):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            if len(parts) < 5:
                continue
            year_str, round_dir = parts[3], parts[4]
            if not year_str.isdigit() or not round_dir.startswith("round_"):
                continue
            key = (int(year_str), int(round_dir.replace("round_", "")))
            if key not in seen:
                seen.add(key)
                paths.append(key)

    return sorted(paths)


def safe_read_json(path):
    """Try reading a JSON file. Return None if missing or empty."""
    try:
        df = spark.read.json(path, multiLine=True)
        if df.head(1):
            return df
        return None
    except Exception:
        return None


def _has_nested_field(schema, dotted_path):
    """Check if dotted path exists in schema."""
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ENDPOINT FLATTENERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def flatten_results(df_raw, season, round_num):
    """Flatten Results (MRData.RaceTable.Races[].Results[])."""
    try:
        races = df_raw.select(F.explode("MRData.RaceTable.Races").alias("race"))
        results = races.select(
            F.lit(season).alias("season"),
            F.lit(round_num).alias("round"),
            F.col("race.raceName").alias("race_name"),
            F.explode("race.Results").alias("r"),
        )

        r_schema = results.schema["r"].dataType
        has_avg_speed = _has_nested_field(r_schema, "FastestLap.AverageSpeed.speed")

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

        if has_avg_speed:
            select_cols.append(
                F.col("r.FastestLap.AverageSpeed.speed").cast(DoubleType()).alias("fastest_lap_speed")
            )
        else:
            select_cols.append(F.lit(None).cast(DoubleType()).alias("fastest_lap_speed"))

        return results.select(*select_cols).dropDuplicates()
    except Exception as e:
        print(f"  Error flattening results {season}/R{round_num:02d}: {e}")
        return None


def flatten_qualifying(df_raw, season, round_num):
    """Flatten QualifyingResults."""
    try:
        races = df_raw.select(F.explode("MRData.RaceTable.Races").alias("race"))
        quali = races.select(
            F.lit(season).alias("season"),
            F.lit(round_num).alias("round"),
            F.explode("race.QualifyingResults").alias("q"),
        )

        return quali.select(
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
    except Exception as e:
        print(f"  Error flattening qualifying {season}/R{round_num:02d}: {e}")
        return None


def flatten_pitstops(df_raw, season, round_num):
    """Flatten PitStops."""
    try:
        races = df_raw.select(F.explode("MRData.RaceTable.Races").alias("race"))
        pits = races.select(
            F.lit(season).alias("season"),
            F.lit(round_num).alias("round"),
            F.explode("race.PitStops").alias("p"),
        )

        return pits.select(
            F.col("season").cast(IntegerType()),
            F.col("round").cast(IntegerType()),
            F.col("p.driverId").alias("driver_id"),
            F.col("p.stop").cast(IntegerType()).alias("stop"),
            F.col("p.lap").cast(IntegerType()).alias("lap"),
            F.col("p.time").alias("time"),
            F.col("p.duration").cast(DoubleType()).alias("duration_seconds"),
            (F.col("p.duration").cast(DoubleType()) * 1000).cast(LongType()).alias("duration_ms"),
        ).dropDuplicates()
    except Exception as e:
        print(f"  Error flattening pitstops {season}/R{round_num:02d}: {e}")
        return None


def flatten_driver_standings(df_raw, season, round_num):
    """Flatten DriverStandings."""
    try:
        standings = df_raw.select(
            F.explode("MRData.StandingsTable.StandingsLists").alias("sl")
        )
        drivers = standings.select(
            F.lit(season).alias("season"),
            F.lit(round_num).alias("round"),
            F.explode("sl.DriverStandings").alias("ds"),
        )

        return drivers.select(
            F.col("season").cast(IntegerType()),
            F.col("round").cast(IntegerType()),
            F.col("ds.Driver.driverId").alias("driver_id"),
            F.col("ds.Driver.code").alias("driver_code"),
            F.col("ds.position").cast(IntegerType()).alias("position"),
            F.col("ds.positionText").alias("position_text"),
            F.col("ds.points").cast(DoubleType()).alias("points"),
            F.col("ds.wins").cast(IntegerType()).alias("wins"),
        ).dropDuplicates()
    except Exception as e:
        print(f"  Error flattening driver_standings {season}/R{round_num:02d}: {e}")
        return None


def flatten_constructor_standings(df_raw, season, round_num):
    """Flatten ConstructorStandings."""
    try:
        standings = df_raw.select(
            F.explode("MRData.StandingsTable.StandingsLists").alias("sl")
        )
        constructors = standings.select(
            F.lit(season).alias("season"),
            F.lit(round_num).alias("round"),
            F.explode("sl.ConstructorStandings").alias("cs"),
        )

        return constructors.select(
            F.col("season").cast(IntegerType()),
            F.col("round").cast(IntegerType()),
            F.col("cs.Constructor.constructorId").alias("constructor_id"),
            F.col("cs.Constructor.name").alias("constructor_name"),
            F.col("cs.position").cast(IntegerType()).alias("position"),
            F.col("cs.positionText").alias("position_text"),
            F.col("cs.points").cast(DoubleType()).alias("points"),
            F.col("cs.wins").cast(IntegerType()).alias("wins"),
        ).dropDuplicates()
    except Exception as e:
        print(f"  Error flattening constructor_standings {season}/R{round_num:02d}: {e}")
        return None


def flatten_sprint(df_raw, season, round_num):
    """Flatten SprintResults (conditional)."""
    try:
        race_table_schema = df_raw.schema
        try:
            mrdata_type = race_table_schema["MRData"].dataType
            racetable_type = mrdata_type["RaceTable"].dataType
            races_type = racetable_type["Races"].dataType
        except (KeyError, AttributeError):
            return None

        if not isinstance(races_type, ArrayType):
            return None

        races = df_raw.select(F.explode("MRData.RaceTable.Races").alias("race"))

        race_struct = races_type.elementType
        sprint_fields = [f for f in race_struct.fields if f.name == "SprintResults"]
        if not sprint_fields or not isinstance(sprint_fields[0].dataType, ArrayType):
            return None

        results = races.select(
            F.lit(season).alias("season"),
            F.lit(round_num).alias("round"),
            F.explode("race.SprintResults").alias("r"),
        )

        return results.select(
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
    except Exception as e:
        print(f"  Error flattening sprint {season}/R{round_num:02d}: {e}")
        return None


FLATTENERS = {
    "results": flatten_results,
    "qualifying": flatten_qualifying,
    "pitstops": flatten_pitstops,
    "driver_standings": flatten_driver_standings,
    "constructor_standings": flatten_constructor_standings,
    "sprint": flatten_sprint,
}


def write_endpoint(df, endpoint_name):
    """Write DataFrame to processed layer as Parquet (append mode)."""
    output_path = f"{PROCESSED_JOLPICA_PATH}/{endpoint_name}"
    df.write.mode("append").partitionBy("season").parquet(output_path)
    print(f"  ✓ Wrote {endpoint_name}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    print("=" * 70)
    print("02_glue_jolpica_final.py — Starting")
    print("=" * 70)

    # Load watermark
    watermark = load_watermark()
    last_season = watermark.get("last_season", 0)
    last_round = watermark.get("last_round", 0)
    
    print(f"\n  Last processed: season={last_season}, round={last_round}\n")

    # Discover all rounds in S3
    all_rounds = discover_round_paths()
    
    if not all_rounds:
        print("  No data found in S3")
        job.commit()
        return

    # Filter: only rounds > (last_season, last_round)
    new_rounds = [
        (s, r) for s, r in all_rounds
        if (s > last_season) or (s == last_season and r > last_round)
    ]

    if not new_rounds:
        print(f"  All rounds already processed. Skipping.\n")
        job.commit()
        return

    print(f"  Found {len(new_rounds)} new rounds to process\n")

    # Process each new round
    for season, round_num in new_rounds:
        print(f"Processing season={season}, round={round_num:02d}")

        for endpoint_name, json_filename in ENDPOINTS.items():
            json_path = f"{RAW_JOLPICA_PATH}/seasons/{season}/round_{round_num:02d}/{json_filename}"
            df_raw = safe_read_json(json_path)

            if df_raw is None:
                continue

            flattener = FLATTENERS[endpoint_name]
            df_clean = flattener(df_raw, season, round_num)

            if df_clean is None or df_clean.rdd.isEmpty():
                continue

            write_endpoint(df_clean, endpoint_name)

        # Update watermark after this round
        save_watermark({"last_season": season, "last_round": round_num})
        print(f"  Watermark updated: season={season}, round={round_num}\n")

    print("=" * 70)
    print("02_glue_jolpica_final.py — Complete")
    print("=" * 70)

    job.commit()


if __name__ == "__main__":
    main()