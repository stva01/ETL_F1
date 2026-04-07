"""
bridge_sources.py — PySpark job (Databricks / AWS Glue)

Joins clean Parquet from all three sources (Kaggle, Jolpica, OpenF1)
using the identifier bridge:
  - Kaggle  ↔ Jolpica  via driverRef / driver_id (e.g. "hamilton")
  - Kaggle  ↔ OpenF1   via driver number (e.g. 44)

Produces final staging Parquet with:
  - combined_results    — race results unified across Kaggle + Jolpica
  - combined_qualifying — qualifying unified across Kaggle + Jolpica
  - combined_laps       — lap data unified across Kaggle + OpenF1

Dedup strategy:
  - Kaggle covers ≤ 2024  (historical bulk)
  - APIs cover 2025+      (incremental)
  - For any overlapping data, prefer API source (more recent extraction)
  - Add a 'source' column for lineage tracking

Databricks → Glue migration:
  1. Remove the SparkSession builder (Glue provides it)
  2. Switch MODE to "glue"
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, LongType, DoubleType, StringType, BooleanType

# ── Configuration ────────────────────────────────────────────────
MODE = "databricks"  # "databricks" | "glue"

# Season boundary: Kaggle ≤ this year, APIs > this year
# For overlapping data, prefer APIs
KAGGLE_CUTOFF_YEAR = 2024

PATHS = {
    "databricks": {
        "kaggle":    "/FileStore/f1-pipeline/processed/kaggle",
        "jolpica":   "/FileStore/f1-pipeline/processed/jolpica",
        "openf1":    "/FileStore/f1-pipeline/processed/openf1",
        "staging":   "/FileStore/f1-pipeline/processed/staging",
    },
    "glue": {
        "kaggle":    "s3://f1-processed-satva/processed/kaggle",
        "jolpica":   "s3://f1-processed-satva/processed/jolpica",
        "openf1":    "s3://f1-processed-satva/processed/openf1",
        "staging":   "s3://f1-processed-satva/processed/staging",
    },
}

KAGGLE_BASE  = PATHS[MODE]["kaggle"]
JOLPICA_BASE = PATHS[MODE]["jolpica"]
OPENF1_BASE  = PATHS[MODE]["openf1"]
STAGING_BASE = PATHS[MODE]["staging"]

# ── Spark Session (remove for Glue) ─────────────────────────────
spark = (
    SparkSession.builder
    .appName("bridge_sources")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def safe_read_parquet(path):
    """Try reading Parquet. Return None if path doesn't exist."""
    try:
        df = spark.read.parquet(path)
        if df.head(1):
            return df
        return None
    except Exception:
        return None


def write_parquet(df, table_name, partition_cols=None):
    """Write a DataFrame as Parquet to the staging zone."""
    path = f"{STAGING_BASE}/{table_name}"
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    count = df.count()
    print(f"  ✓ Wrote {table_name} → {path}  ({count:,} rows)")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DRIVER LOOKUP TABLE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_driver_lookup():
    """
    Build a lookup table from Kaggle drivers that maps:
      driverId → driverRef → number (driver_number)

    This is the bridge between all three sources:
      - Kaggle uses driverId (integer PK) and driverRef (string)
      - Jolpica uses driver_id (= driverRef string)
      - OpenF1 uses driver_number (integer, same as Kaggle 'number')
    """
    drivers = safe_read_parquet(f"{KAGGLE_BASE}/drivers")
    if drivers is None:
        print("  ✗ Cannot read Kaggle drivers — bridge cannot be built")
        return None

    lookup = (
        drivers
        .select(
            F.col("driverId").alias("kaggle_driver_id"),
            F.col("driverRef").alias("driver_ref"),
            F.col("number").alias("driver_number"),
            F.col("code").alias("driver_code"),
            F.col("forename"),
            F.col("surname"),
        )
        .dropDuplicates(["driver_ref"])
    )

    count = lookup.count()
    print(f"  ✓ Driver lookup built — {count:,} unique drivers")
    return lookup


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  COMBINED RESULTS — Kaggle + Jolpica
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_combined_results(driver_lookup):
    """
    Combine Kaggle results (≤2024) with Jolpica results (2025+).
    For any overlap, prefer Jolpica (API is more recent extraction).
    """
    print("\n[combined_results]")

    # ── Kaggle results ───────────────────────────────────────────
    kaggle_results = safe_read_parquet(f"{KAGGLE_BASE}/results")
    if kaggle_results is None:
        print("  ⚠ No Kaggle results found")
        kaggle_norm = None
    else:
        # Join to get driverRef from driver lookup
        kaggle_with_ref = kaggle_results.join(
            driver_lookup.select("kaggle_driver_id", "driver_ref", "driver_number"),
            kaggle_results["driverId"] == driver_lookup["kaggle_driver_id"],
            how="left",
        )

        # Also join to races to get round
        races = safe_read_parquet(f"{KAGGLE_BASE}/races")
        if races is not None:
            kaggle_with_ref = kaggle_with_ref.join(
                races.select(
                    F.col("raceId").alias("_raceId"),
                    F.col("round").alias("race_round"),
                ),
                kaggle_with_ref["raceId"] == F.col("_raceId"),
                how="left",
            ).drop("_raceId")
        else:
            kaggle_with_ref = kaggle_with_ref.withColumn("race_round", F.lit(None).cast(IntegerType()))

        kaggle_norm = (
            kaggle_with_ref
            .select(
                F.col("year").cast(IntegerType()),
                F.col("race_round").alias("round").cast(IntegerType()),
                F.col("driver_ref").alias("driver_ref"),
                F.col("driver_number").cast(IntegerType()),
                F.col("grid").cast(IntegerType()),
                F.col("position").cast(IntegerType()),
                F.col("positionText").alias("position_text"),
                F.col("points").cast(DoubleType()),
                F.col("laps").cast(IntegerType()),
                F.col("milliseconds").cast(LongType()).alias("time_millis"),
                F.col("time").alias("time_text"),
                F.col("fastestLapTime_ms").cast(LongType()).alias("fastest_lap_time_ms"),
                F.col("fastestLapSpeed").cast(DoubleType()).alias("fastest_lap_speed"),
                F.col("statusId").cast(IntegerType()).alias("status_id"),
                F.lit("kaggle").alias("source"),
            )
        )
        print(f"  Kaggle results: {kaggle_norm.count():,} rows")

    # ── Jolpica results ──────────────────────────────────────────
    jolpica_results = safe_read_parquet(f"{JOLPICA_BASE}/results")
    if jolpica_results is None:
        print("  ⚠ No Jolpica results found")
        jolpica_norm = None
    else:
        jolpica_norm = (
            jolpica_results
            .select(
                F.col("season").alias("year").cast(IntegerType()),
                F.col("round").cast(IntegerType()),
                F.col("driver_id").alias("driver_ref"),
                F.col("driver_number").cast(IntegerType()),
                F.col("grid").cast(IntegerType()),
                F.col("position").cast(IntegerType()),
                F.col("position_text"),
                F.col("points").cast(DoubleType()),
                F.col("laps").cast(IntegerType()),
                F.col("time_millis").cast(LongType()),
                F.col("time_text"),
                F.col("fastest_lap_time_ms").cast(LongType()),
                F.col("fastest_lap_speed").cast(DoubleType()),
                F.lit(None).cast(IntegerType()).alias("status_id"),  # Jolpica has status text, not ID
                F.lit("jolpica").alias("source"),
            )
        )
        print(f"  Jolpica results: {jolpica_norm.count():,} rows")

    # ── Combine with dedup ───────────────────────────────────────
    if kaggle_norm is None and jolpica_norm is None:
        print("  ✗ No results from either source — skipping")
        return

    if kaggle_norm is not None and jolpica_norm is not None:
        # Union both, then dedup: prefer API for overlapping rows
        combined = kaggle_norm.unionByName(jolpica_norm, allowMissingColumns=True)

        # For dedup: partition by (year, round, driver_ref),
        # rank by source (jolpica first), keep rank 1
        from pyspark.sql.window import Window

        dedup_window = Window.partitionBy("year", "round", "driver_ref").orderBy(
            F.when(F.col("source") == "jolpica", 1)
             .when(F.col("source") == "kaggle", 2)
             .otherwise(3)
        )
        combined = (
            combined
            .withColumn("_rank", F.row_number().over(dedup_window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )
    elif kaggle_norm is not None:
        combined = kaggle_norm
    else:
        combined = jolpica_norm

    write_parquet(combined, "combined_results", partition_cols=["year"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  COMBINED QUALIFYING — Kaggle + Jolpica
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_combined_qualifying(driver_lookup):
    """
    Combine Kaggle qualifying (≤2024) with Jolpica qualifying (2025+).
    """
    print("\n[combined_qualifying]")

    # ── Kaggle qualifying ────────────────────────────────────────
    kaggle_quali = safe_read_parquet(f"{KAGGLE_BASE}/qualifying")
    if kaggle_quali is None:
        print("  ⚠ No Kaggle qualifying found")
        kaggle_norm = None
    else:
        kaggle_with_ref = kaggle_quali.join(
            driver_lookup.select("kaggle_driver_id", "driver_ref", "driver_number"),
            kaggle_quali["driverId"] == driver_lookup["kaggle_driver_id"],
            how="left",
        )

        # Get round from races
        races = safe_read_parquet(f"{KAGGLE_BASE}/races")
        if races is not None:
            kaggle_with_ref = kaggle_with_ref.join(
                races.select(
                    F.col("raceId").alias("_raceId"),
                    F.col("round").alias("race_round"),
                ),
                kaggle_with_ref["raceId"] == F.col("_raceId"),
                how="left",
            ).drop("_raceId")
        else:
            kaggle_with_ref = kaggle_with_ref.withColumn("race_round", F.lit(None).cast(IntegerType()))

        kaggle_norm = (
            kaggle_with_ref
            .select(
                F.col("year").cast(IntegerType()),
                F.col("race_round").alias("round").cast(IntegerType()),
                F.col("driver_ref"),
                F.col("driver_number").cast(IntegerType()),
                F.col("position").cast(IntegerType()),
                F.col("q1_ms").cast(LongType()),
                F.col("q2_ms").cast(LongType()),
                F.col("q3_ms").cast(LongType()),
                F.col("q1"),
                F.col("q2"),
                F.col("q3"),
                F.lit("kaggle").alias("source"),
            )
        )
        print(f"  Kaggle qualifying: {kaggle_norm.count():,} rows")

    # ── Jolpica qualifying ───────────────────────────────────────
    jolpica_quali = safe_read_parquet(f"{JOLPICA_BASE}/qualifying")
    if jolpica_quali is None:
        print("  ⚠ No Jolpica qualifying found")
        jolpica_norm = None
    else:
        jolpica_norm = (
            jolpica_quali
            .select(
                F.col("season").alias("year").cast(IntegerType()),
                F.col("round").cast(IntegerType()),
                F.col("driver_id").alias("driver_ref"),
                F.col("driver_number").cast(IntegerType()),
                F.col("position").cast(IntegerType()),
                F.col("q1_ms").cast(LongType()),
                F.col("q2_ms").cast(LongType()),
                F.col("q3_ms").cast(LongType()),
                F.col("q1"),
                F.col("q2"),
                F.col("q3"),
                F.lit("jolpica").alias("source"),
            )
        )
        print(f"  Jolpica qualifying: {jolpica_norm.count():,} rows")

    # ── Combine with dedup ───────────────────────────────────────
    if kaggle_norm is None and jolpica_norm is None:
        print("  ✗ No qualifying from either source — skipping")
        return

    if kaggle_norm is not None and jolpica_norm is not None:
        from pyspark.sql.window import Window

        combined = kaggle_norm.unionByName(jolpica_norm, allowMissingColumns=True)
        dedup_window = Window.partitionBy("year", "round", "driver_ref").orderBy(
            F.when(F.col("source") == "jolpica", 1)
             .when(F.col("source") == "kaggle", 2)
             .otherwise(3)
        )
        combined = (
            combined
            .withColumn("_rank", F.row_number().over(dedup_window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )
    elif kaggle_norm is not None:
        combined = kaggle_norm
    else:
        combined = jolpica_norm

    write_parquet(combined, "combined_qualifying", partition_cols=["year"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  COMBINED LAPS — Kaggle + OpenF1
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_combined_laps(driver_lookup):
    """
    Combine Kaggle lap_times (≤2024) with OpenF1 laps (2023+).
    OpenF1 has richer telemetry (sector times, speed traps) that Kaggle lacks.
    For 2023–2024 overlap, prefer OpenF1 (has sector data).
    """
    print("\n[combined_laps]")

    # ── Kaggle lap_times ─────────────────────────────────────────
    kaggle_laps = safe_read_parquet(f"{KAGGLE_BASE}/lap_times")
    if kaggle_laps is None:
        print("  ⚠ No Kaggle lap_times found")
        kaggle_norm = None
    else:
        # Join to get driver_number and driverRef
        kaggle_with_ref = kaggle_laps.join(
            driver_lookup.select("kaggle_driver_id", "driver_ref", "driver_number"),
            kaggle_laps["driverId"] == driver_lookup["kaggle_driver_id"],
            how="left",
        )

        # Get round from races
        races = safe_read_parquet(f"{KAGGLE_BASE}/races")
        if races is not None:
            kaggle_with_ref = kaggle_with_ref.join(
                races.select(
                    F.col("raceId").alias("_raceId"),
                    F.col("round").alias("race_round"),
                ),
                kaggle_with_ref["raceId"] == F.col("_raceId"),
                how="left",
            ).drop("_raceId")
        else:
            kaggle_with_ref = kaggle_with_ref.withColumn("race_round", F.lit(None).cast(IntegerType()))

        kaggle_norm = (
            kaggle_with_ref
            .select(
                F.col("year").cast(IntegerType()),
                F.col("race_round").alias("round").cast(IntegerType()),
                F.col("driver_ref"),
                F.col("driver_number").cast(IntegerType()),
                F.col("lap").alias("lap_number").cast(IntegerType()),
                F.col("position").cast(IntegerType()),
                F.col("milliseconds").cast(LongType()).alias("lap_duration_ms"),
                F.col("time").alias("lap_time_text"),
                # Kaggle doesn't have sector times or speed traps
                F.lit(None).cast(LongType()).alias("duration_sector_1_ms"),
                F.lit(None).cast(LongType()).alias("duration_sector_2_ms"),
                F.lit(None).cast(LongType()).alias("duration_sector_3_ms"),
                F.lit(None).cast(DoubleType()).alias("i1_speed"),
                F.lit(None).cast(DoubleType()).alias("i2_speed"),
                F.lit(None).cast(DoubleType()).alias("st_speed"),
                F.lit(None).cast(BooleanType()).alias("is_pit_out_lap"),
                F.lit("kaggle").alias("source"),
            )
        )
        print(f"  Kaggle laps: {kaggle_norm.count():,} rows")

    # ── OpenF1 laps ──────────────────────────────────────────────
    openf1_laps = safe_read_parquet(f"{OPENF1_BASE}/laps")
    if openf1_laps is None:
        print("  ⚠ No OpenF1 laps found")
        openf1_norm = None
    else:
        # OpenF1 uses meeting_key/session_key, not year/round
        # We need to map these to year. The 'year' partition column was added by clean_openf1.
        # For round, we don't have a direct mapping — use meeting_key as a proxy
        # (dbt will handle the proper round<->meeting mapping later)
        openf1_norm = (
            openf1_laps
            .select(
                F.col("year").cast(IntegerType()),
                F.lit(None).cast(IntegerType()).alias("round"),  # Not in OpenF1 — dbt resolves
                F.lit(None).alias("driver_ref"),  # Not in OpenF1 — join via driver_number
                F.col("driver_number").cast(IntegerType()),
                F.col("lap_number").cast(IntegerType()),
                F.lit(None).cast(IntegerType()).alias("position"),  # OpenF1 laps don't have race position
                F.col("lap_duration_ms").cast(LongType()),
                F.lit(None).alias("lap_time_text"),  # OpenF1 already has ms
                F.col("duration_sector_1_ms").cast(LongType()),
                F.col("duration_sector_2_ms").cast(LongType()),
                F.col("duration_sector_3_ms").cast(LongType()),
                F.col("i1_speed").cast(DoubleType()),
                F.col("i2_speed").cast(DoubleType()),
                F.col("st_speed").cast(DoubleType()),
                F.col("is_pit_out_lap"),
                F.lit("openf1").alias("source"),
            )
        )

        # Enrich with driver_ref from lookup (via driver_number)
        openf1_norm = openf1_norm.join(
            driver_lookup.select("driver_number", "driver_ref").alias("lookup"),
            on="driver_number",
            how="left",
        )

        print(f"  OpenF1 laps: {openf1_norm.count():,} rows")

    # ── Combine with dedup ───────────────────────────────────────
    if kaggle_norm is None and openf1_norm is None:
        print("  ✗ No laps from either source — skipping")
        return


    if kaggle_norm is not None and openf1_norm is not None:
        from pyspark.sql.window import Window

        combined = kaggle_norm.unionByName(openf1_norm, allowMissingColumns=True)

        # Dedup: prefer OpenF1 (richer data) for overlapping seasons
        dedup_window = Window.partitionBy("year", "driver_number", "lap_number").orderBy(
            F.when(F.col("source") == "openf1", 1)
             .when(F.col("source") == "kaggle", 2)
             .otherwise(3)
        )
        combined = (
            combined
            .withColumn("_rank", F.row_number().over(dedup_window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )
    elif kaggle_norm is not None:
        combined = kaggle_norm
    else:
        combined = openf1_norm

    write_parquet(combined, "combined_laps", partition_cols=["year"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    print("=" * 60)
    print("bridge_sources.py — Starting")
    print(f"  MODE:          {MODE}")
    print(f"  KAGGLE:        {KAGGLE_BASE}")
    print(f"  JOLPICA:       {JOLPICA_BASE}")
    print(f"  OPENF1:        {OPENF1_BASE}")
    print(f"  STAGING:       {STAGING_BASE}")
    print(f"  CUTOFF:        Kaggle ≤ {KAGGLE_CUTOFF_YEAR}, APIs > {KAGGLE_CUTOFF_YEAR}")
    print("=" * 60)

    # Build the driver bridge
    print("\n[driver_lookup]")
    driver_lookup = build_driver_lookup()
    if driver_lookup is None:
        print("FATAL: Cannot build driver lookup — aborting")
        return

    # Build combined tables
    build_combined_results(driver_lookup)
    build_combined_qualifying(driver_lookup)
    build_combined_laps(driver_lookup)

    print("\n" + "=" * 60)
    print("bridge_sources.py — Complete ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
