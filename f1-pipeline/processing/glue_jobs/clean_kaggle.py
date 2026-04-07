"""
clean_kaggle.py — PySpark job (Databricks / AWS Glue)

Reads 14 Kaggle CSVs from the raw zone, applies:
  - \\N → null replacement
  - Explicit type casting per table
  - Lap-time string parsing ("M:SS.mmm" → milliseconds)
  - Deduplication
  - Writes clean Parquet to the processed zone

Databricks → Glue migration:
  1. Remove the SparkSession builder (Glue provides it)
  2. Switch MODE to "glue"
  Everything else stays identical.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DoubleType, StringType, DateType,
)

# ── Configuration ────────────────────────────────────────────────
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

# ── Spark Session (remove for Glue) ─────────────────────────────
spark = (
    SparkSession.builder
    .appName("clean_kaggle")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


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

    # Reference tables (no partitioning)
    clean_circuits()
    clean_constructors()
    clean_drivers()
    clean_seasons()
    clean_status()

    # Fact / event tables (partitioned by year)
    clean_races()
    clean_results()
    clean_qualifying()
    clean_lap_times()
    clean_pit_stops()
    clean_driver_standings()
    clean_constructor_standings()
    clean_constructor_results()
    clean_sprint_results()

    print("=" * 60)
    print("clean_kaggle.py — Complete ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
