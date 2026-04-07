"""
clean_jolpica.py — PySpark job (Databricks / AWS Glue)

Reads nested Ergast-format JSON from the Jolpica API raw zone,
flattens the deeply nested structure, casts types, and writes
clean Parquet to the processed zone.

Handles endpoints:
  - results       → one row per driver-result
  - qualifying    → one row per driver-qualifying
  - pitstops      → one row per pit stop
  - driver_standings → one row per driver-standing
  - constructor_standings → one row per constructor-standing
  - sprint        → one row per sprint result

Databricks → Glue migration:
  1. Remove the SparkSession builder (Glue provides it)
  2. Switch MODE to "glue"
"""

from pyspark.sql import SparkSession, functions as F, Row
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DoubleType, StringType,
)
import json

# ── Configuration ────────────────────────────────────────────────
MODE = "databricks"  # "databricks" | "glue"

PATHS = {
    "databricks": {
        "raw":       "/FileStore/f1-pipeline/raw/jolpica",
        "processed": "/FileStore/f1-pipeline/processed/jolpica",
    },
    "glue": {
        "raw":       "s3://f1-raw-satva/raw/jolpica",
        "processed": "s3://f1-processed-satva/processed/jolpica",
    },
}

RAW_BASE       = PATHS[MODE]["raw"]
PROCESSED_BASE = PATHS[MODE]["processed"]

# ── Spark Session (remove for Glue) ─────────────────────────────
spark = (
    SparkSession.builder
    .appName("clean_jolpica")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@F.udf(LongType())
def parse_lap_time_ms(time_str):
    """Convert "M:SS.mmm" or "SS.mmm" → milliseconds."""
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


def write_parquet(df, table_name, partition_cols=None):
    """Write a DataFrame as Parquet (optionally partitioned)."""
    path = f"{PROCESSED_BASE}/{table_name}"
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    count = df.count()
    print(f"  ✓ Wrote {table_name} → {path}  ({count:,} rows)")


def discover_round_paths():
    """
    Discover all season/round directories that contain data.
    Returns a list of dicts: {"season": int, "round": int, "path": str}

    In Databricks, uses dbutils. In Glue, uses boto3.
    For simplicity, we build paths from known season/round ranges.
    Override this if you have a different layout.
    """
    paths = []

    if MODE == "databricks":
        # On Databricks, try to list directories using dbutils
        # Fallback: hardcode known seasons/rounds for testing
        try:
            # dbutils is available in Databricks notebooks
            import builtins
            dbutils = getattr(builtins, "dbutils", None)
            if dbutils is None:
                # Running as script — use fallback
                raise AttributeError("dbutils not available")

            season_dirs = dbutils.fs.ls(f"{RAW_BASE}/seasons/")
            for sd in season_dirs:
                season_name = sd.name.rstrip("/")
                if not season_name.isdigit():
                    continue
                season = int(season_name)
                round_dirs = dbutils.fs.ls(sd.path)
                for rd in round_dirs:
                    round_name = rd.name.rstrip("/")
                    if not round_name.startswith("round_"):
                        continue
                    round_num = int(round_name.replace("round_", ""))
                    paths.append({
                        "season": season,
                        "round": round_num,
                        "path": rd.path.rstrip("/"),
                    })
        except Exception as e:
            print(f"  ⚠ Could not list DBFS directories: {e}")
            print("  → Using fallback: scanning seasons 2025 rounds 1–24")
            for season in range(2025, 2027):
                for round_num in range(1, 25):
                    paths.append({
                        "season": season,
                        "round": round_num,
                        "path": f"{RAW_BASE}/seasons/{season}/round_{round_num:02d}",
                    })
    else:
        # Glue — scan S3 prefixes (handled by Spark glob)
        for season in range(2025, 2027):
            for round_num in range(1, 25):
                paths.append({
                    "season": season,
                    "round": round_num,
                    "path": f"{RAW_BASE}/seasons/{season}/round_{round_num:02d}",
                })

    return paths


def safe_read_json(path):
    """
    Try reading a JSON file. Return None if it doesn't exist.
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

def flatten_results(df_raw, season, round_num):
    """
    Ergast results structure:
    MRData.RaceTable.Races[].Results[] →
      .Driver.{driverId, code, permanentNumber}
      .Constructor.{constructorId}
      .grid, .position, .points, .laps, .status
      .Time.{millis, time}
      .FastestLap.{rank, lap, Time.time, AverageSpeed.speed}
    """
    try:
        races = df_raw.select(F.explode("MRData.RaceTable.Races").alias("race"))
        results = races.select(
            F.lit(season).alias("season"),
            F.lit(round_num).alias("round"),
            F.col("race.raceName").alias("race_name"),
            F.explode("race.Results").alias("r"),
        )

        flat = results.select(
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
            F.col("r.FastestLap.AverageSpeed.speed").cast(DoubleType()).alias("fastest_lap_speed"),
        )
        return flat
    except Exception as e:
        print(f"    ⚠ Could not flatten results for {season}/{round_num}: {e}")
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
        )
        return flat
    except Exception as e:
        print(f"    ⚠ Could not flatten qualifying for {season}/{round_num}: {e}")
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
        )
        return flat
    except Exception as e:
        print(f"    ⚠ Could not flatten pitstops for {season}/{round_num}: {e}")
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
        )
        return flat
    except Exception as e:
        print(f"    ⚠ Could not flatten driver_standings for {season}/{round_num}: {e}")
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
        )
        return flat
    except Exception as e:
        print(f"    ⚠ Could not flatten constructor_standings for {season}/{round_num}: {e}")
        return None


def flatten_sprint(df_raw, season, round_num):
    """Same structure as results — sprint races use identical Ergast format."""
    try:
        races = df_raw.select(F.explode("MRData.RaceTable.Races").alias("race"))
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
        )
        return flat
    except Exception as e:
        print(f"    ⚠ Could not flatten sprint for {season}/{round_num}: {e}")
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MAIN PIPELINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Endpoint → (flatten function, Ergast JSON filename)
ENDPOINT_CONFIG = {
    "results":                 (flatten_results,                "results.json"),
    "qualifying":              (flatten_qualifying,             "qualifying.json"),
    "pitstops":                (flatten_pitstops,               "pitstops.json"),
    "driver_standings":        (flatten_driver_standings,        "driver_standings.json"),
    "constructor_standings":   (flatten_constructor_standings,   "constructor_standings.json"),
    "sprint":                  (flatten_sprint,                  "sprint.json"),
}


def main():
    print("=" * 60)
    print("clean_jolpica.py — Starting")
    print(f"  MODE:      {MODE}")
    print(f"  RAW:       {RAW_BASE}")
    print(f"  PROCESSED: {PROCESSED_BASE}")
    print("=" * 60)

    round_paths = discover_round_paths()
    print(f"  Found {len(round_paths)} possible round paths to scan\n")

    # Accumulators: endpoint → list of DataFrames
    accumulators = {name: [] for name in ENDPOINT_CONFIG}

    for rp in round_paths:
        season    = rp["season"]
        round_num = rp["round"]
        base_path = rp["path"]

        for endpoint_name, (flatten_fn, filename) in ENDPOINT_CONFIG.items():
            json_path = f"{base_path}/{filename}"
            df_raw = safe_read_json(json_path)
            if df_raw is None:
                continue

            df_flat = flatten_fn(df_raw, season, round_num)
            if df_flat is not None and df_flat.head(1):
                accumulators[endpoint_name].append(df_flat)
                print(f"  ✓ {season}/R{round_num:02d}/{endpoint_name}")

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
        write_parquet(combined, endpoint_name, partition_cols=["season"])

    print("\n" + "=" * 60)
    print("clean_jolpica.py — Complete ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
