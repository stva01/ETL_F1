# F1 Data Pipeline — Ingestion Layer Handoff Report

**Prepared for:** Middleware AI Agent (PySpark / Cleaning Layer)
**Date:** 2026-03-23
**Status:** Ingestion layer complete — ready for transformation

---

## 1. Project Overview

This is a Formula 1 data engineering portfolio project. The goal is to build an end-to-end pipeline from raw data sources through to a Snowflake warehouse with dbt models, served via Grafana dashboards.

The ingestion layer (this report's scope) is complete. All raw data is now landing in AWS S3. The next stage is PySpark-based cleaning and loading into Snowflake raw tables, followed by dbt transformations.

### Full Stack

| Layer | Technology | Status |
|---|---|---|
| Ingestion | Python scripts | ✅ Complete |
| Raw storage | AWS S3 | ✅ Complete |
| Orchestration | Apache Airflow on EC2 t2.micro | 🔜 Not started |
| Processing | PySpark (no Glue — direct to Snowflake) | 🔜 Your scope |
| Warehouse | Snowflake + dbt Core | 🔜 Not started |
| Visualisation | Grafana | 🔜 Not started |
| Infrastructure | Terraform | ✅ S3 + IAM done |

---

## 2. Architecture Decisions

### 2.1 No AWS Glue
AWS Glue was explicitly removed from the architecture. The decision was made to load directly from S3 into Snowflake using Snowflake's native `COPY INTO` command. PySpark is used only for validation and lightweight transformation before loading — not as a managed EMR/Glue service.

**Rationale:** Startup-grade architecture. Glue charges per DPU-hour even on small jobs. For this dataset size (thousands of rows per race, not gigabytes), direct S3 → Snowflake is cheaper and simpler. PySpark knowledge is still demonstrated but without the Glue overhead.

### 2.2 Medallion Architecture (Simplified)

```
Bronze (S3 raw)     → Silver (Snowflake staging via COPY INTO + PySpark)
                    → Gold (Snowflake core — star schema via dbt)
                    → Platinum (Snowflake marts — pre-aggregated via dbt)
```

### 2.3 Two Ingestion Patterns

| Pattern | Source | Trigger | Frequency |
|---|---|---|---|
| One-time historical backfill | Kaggle | Manual, run once | Never again |
| Incremental post-race | Jolpica + OpenF1 APIs | Airflow DAG | After each race |

### 2.4 Watermark-Based Incremental Loading
Both API scripts use watermark files stored in S3 to track last ingested position. This enables:
- Automatic catch-up if the project is paused for weeks/months
- Idempotent reruns — S3 overwrites on same key, no duplicates
- Safe interruption — watermark only advances after successful upload
- Season rollover detection — automatic year advancement in March

---

## 3. S3 Bucket Structure

**Bucket:** `f1-pipeline-raw-layer-034381339055`
**Region:** `ap-south-1` (Mumbai)

```
f1-pipeline-raw-layer-034381339055/
│
├── raw/
│   ├── kaggle/
│   │   └── 2026-03-16/                    ← datestamped on run date
│   │       ├── circuits.csv
│   │       ├── constructors.csv
│   │       ├── constructor_results.csv
│   │       ├── constructor_standings.csv
│   │       ├── drivers.csv
│   │       ├── lap_times.csv
│   │       ├── pit_stops.csv
│   │       ├── qualifying.csv
│   │       ├── races.csv
│   │       ├── results.csv
│   │       ├── seasons.csv
│   │       ├── sprint_results.csv
│   │       ├── status.csv
│   │       └── driver_standings.csv
│   │
│   ├── jolpica/
│   │   └── seasons/
│   │       ├── 2025/
│   │       │   ├── round_01/
│   │       │   │   ├── results.json
│   │       │   │   ├── qualifying.json
│   │       │   │   ├── pitstops.json
│   │       │   │   ├── driver_standings.json
│   │       │   │   ├── constructor_standings.json
│   │       │   │   └── sprint.json           ← only exists for sprint weekends
│   │       │   ├── round_02/ ...
│   │       │   └── round_24/
│   │       └── 2026/
│   │           ├── round_01/
│   │           └── round_02/
│   │
│   └── openf1/
│       └── 2026/
│           └── meeting_1280/
│               └── race/
│                   ├── session_metadata.json
│                   ├── drivers.json
│                   ├── laps.json
│                   ├── stints.json
│                   └── pit.json
│
├── watermark/
│   ├── jolpica_watermark.json
│   └── openf1_watermark.json
│
└── validation/
    ├── jolpica/
    │   └── 2026/
    │       └── round_01/
    │           ├── results.json
    │           ├── qualifying.json
    │           └── ...
    └── openf1/
        └── 2026/
            └── meeting_1280/
                └── session_11245.json
```

---

## 4. Data Sources — Detailed

### 4.1 Kaggle (Historical — 1950 to 2024)

**Dataset:** `rohanrao/formula-1-world-championship-1950-2020` (actually updated to 2024)
**Format:** CSV
**Load type:** One-time backfill, already complete
**S3 path:** `raw/kaggle/2026-03-16/`

#### Key files and their join columns

| File | Primary Key | Key Join Columns | Notes |
|---|---|---|---|
| `drivers.csv` | `driverId` (INT) | `driverRef` (STRING), `number` (INT), `code` (CHAR3) | `driverRef` = bridge to Jolpica. `number` = bridge to OpenF1 |
| `races.csv` | `raceId` (INT) | `year`, `round`, `circuitId` | `year + round` = bridge to Jolpica |
| `results.csv` | `resultId` (INT) | `raceId`, `driverId`, `constructorId`, `statusId` | Core fact table source |
| `qualifying.csv` | `qualifyId` (INT) | `raceId`, `driverId`, `constructorId` | Q1/Q2/Q3 times as strings e.g. `"1:21.546"` |
| `lap_times.csv` | composite: `raceId + driverId + lap` | `raceId`, `driverId` | Lap time as milliseconds INT |
| `pit_stops.csv` | composite: `raceId + driverId + stop` | `raceId`, `driverId` | Duration as string e.g. `"23.124"` |
| `driver_standings.csv` | `driverStandingsId` | `raceId`, `driverId` | Points snapshot after each race |
| `constructor_standings.csv` | `constructorStandingsId` | `raceId`, `constructorId` | |
| `constructors.csv` | `constructorId` (INT) | `constructorRef` (STRING) | Bridge to Jolpica |
| `circuits.csv` | `circuitId` (INT) | `circuitRef`, `lat`, `lng`, `alt` | |
| `status.csv` | `statusId` (INT) | `status` (STRING) | e.g. "Finished", "Engine", "Accident" |
| `seasons.csv` | `year` | `url` | |
| `sprint_results.csv` | `resultId` | `raceId`, `driverId` | Only 2021 onwards |

#### Sample `drivers.csv` row
```
driverId,driverRef,number,code,forename,surname,dob,nationality,url
1,hamilton,44,HAM,Lewis,Hamilton,1985-01-07,British,http://en.wikipedia.org/wiki/Lewis_Hamilton
```

#### Sample `results.csv` row
```
resultId,raceId,driverId,constructorId,number,grid,position,positionText,positionOrder,
points,laps,time,milliseconds,fastestLap,rank,fastestLapTime,fastestLapSpeed,statusId
1,18,1,1,22,1,1,1,1,10,58,"1:34:50.616",5690616,39,2,"1:27.452","218.300",1
```

#### Data quality issues to handle in PySpark
- `position` and `grid` are `\N` (null string) for DNFs — must convert to NULL
- `milliseconds` is `\N` for DNFs
- `fastestLapTime` format is `"1:21.546"` string — needs parsing to milliseconds
- `time` format is `"1:34:50.616"` for race time — needs parsing
- `dob` in drivers is `YYYY-MM-DD` string — cast to DATE
- `lap_times.csv` milliseconds column is INT — no parsing needed

---

### 4.2 Jolpica API (Incremental — 2025 onwards)

**Base URL:** `https://api.jolpi.ca/ergast/f1`
**Format:** JSON (nested Ergast-style structure)
**Load type:** Incremental by season + round
**S3 path:** `raw/jolpica/seasons/{year}/round_{NN}/`
**Watermark:** `raw/watermark/jolpica_watermark.json`

#### Watermark format
```json
{
  "season": 2026,
  "last_round": 2,
  "updated_at": "2026-03-23"
}
```

#### Endpoints stored per round

| File | Endpoint | Key Fields |
|---|---|---|
| `results.json` | `/{season}/{round}/results.json` | `driverId`, `constructorId`, `grid`, `position`, `points`, `laps`, `Time`, `FastestLap`, `status` |
| `qualifying.json` | `/{season}/{round}/qualifying.json` | `driverId`, `constructorId`, `position`, `Q1`, `Q2`, `Q3` |
| `pitstops.json` | `/{season}/{round}/pitstops.json` | `driverId`, `lap`, `stop`, `time`, `duration` |
| `driver_standings.json` | `/{season}/{round}/driverStandings.json` | `driverId`, `position`, `points`, `wins` |
| `constructor_standings.json` | `/{season}/{round}/constructorStandings.json` | `constructorId`, `position`, `points`, `wins` |
| `sprint.json` | `/{season}/{round}/sprint.json` | Same as results — only exists for sprint weekends |

#### Sample `results.json` structure (heavily nested)
```json
{
  "MRData": {
    "RaceTable": {
      "season": "2026",
      "round": "1",
      "Races": [
        {
          "season": "2026",
          "round": "1",
          "raceName": "Australian Grand Prix",
          "Circuit": {
            "circuitId": "albert_park",
            "circuitName": "Albert Park Grand Prix Circuit"
          },
          "date": "2026-03-15",
          "Results": [
            {
              "number": "1",
              "position": "1",
              "positionText": "1",
              "points": "25",
              "Driver": {
                "driverId": "max_verstappen",
                "permanentNumber": "1",
                "code": "VER",
                "givenName": "Max",
                "familyName": "Verstappen",
                "dateOfBirth": "1997-09-30",
                "nationality": "Dutch"
              },
              "Constructor": {
                "constructorId": "red_bull",
                "name": "Red Bull",
                "nationality": "Austrian"
              },
              "grid": "1",
              "laps": "58",
              "status": "Finished",
              "Time": {
                "millis": "5690616",
                "time": "1:34:50.616"
              },
              "FastestLap": {
                "rank": "1",
                "lap": "39",
                "Time": {"time": "1:27.452"},
                "AverageSpeed": {"units": "kph", "speed": "218.300"}
              }
            }
          ]
        }
      ]
    }
  }
}
```

#### Key identifier — the bridge column
`Driver.driverId` in Jolpica = `driverRef` in Kaggle's `drivers.csv`
This is the join key between historical and incremental data.

#### Data quality issues to handle in PySpark
- All numeric fields are **strings** — `position`, `points`, `grid`, `laps`, `millis` all need casting
- `position` can be `"R"`, `"D"`, `"W"`, `"F"`, `"N"` for non-finishers — map to NULL INT + status flag
- `Q1`/`Q2`/`Q3` are time strings `"1:21.546"` or absent — parse to milliseconds, NULL if not set
- `duration` in pitstops is `"23.124"` string — parse to milliseconds
- Deep nesting — needs flattening: `MRData.RaceTable.Races[0].Results[*]`

---

### 4.3 OpenF1 API (Incremental — 2023 onwards)

**Base URL:** `https://api.openf1.org/v1`
**Format:** JSON (flat array, no nesting)
**Load type:** Incremental by session_key
**S3 path:** `raw/openf1/{year}/meeting_{meeting_key}/race/`
**Watermark:** `raw/watermark/openf1_watermark.json`

#### Watermark format
```json
{
  "year": 2026,
  "last_session_key": 11245,
  "updated_at": "2026-03-23"
}
```

#### Endpoints stored per session

| File | Endpoint | Records per race | Key Fields |
|---|---|---|---|
| `session_metadata.json` | `/sessions` | 1 | `session_key`, `meeting_key`, `year`, `date_start`, `date_end` |
| `drivers.json` | `/drivers` | ~22 | `driver_number`, `full_name`, `name_acronym`, `team_name` |
| `laps.json` | `/laps` | ~1000 (50 laps × 20 drivers) | `driver_number`, `lap_number`, `lap_duration`, `duration_sector_1/2/3`, `i1_speed`, `i2_speed`, `st_speed`, `is_pit_out_lap` |
| `stints.json` | `/stints` | ~40 | `driver_number`, `stint_number`, `lap_start`, `lap_end`, `compound`, `tyre_age_at_start` |
| `pit.json` | `/pit` | ~20 | `driver_number`, `lap_number`, `pit_duration`, `date` |

#### Sample `laps.json` row (flat — no nesting)
```json
{
  "date_start": "2026-03-15T06:12:34.123Z",
  "driver_number": 44,
  "duration_sector_1": 28.456,
  "duration_sector_2": 31.123,
  "duration_sector_3": 22.891,
  "i1_speed": 312,
  "i2_speed": 287,
  "is_pit_out_lap": false,
  "lap_duration": 82.470,
  "lap_number": 1,
  "meeting_key": 1280,
  "segments_sector_1": [2049, 2049, 2049, 2051, 2049],
  "segments_sector_2": [2049, 2051, 2049],
  "segments_sector_3": [2049, 2049, 2051],
  "session_key": 11245,
  "st_speed": 298
}
```

#### Sample `stints.json` row
```json
{
  "compound": "SOFT",
  "driver_number": 44,
  "lap_end": 22,
  "lap_start": 1,
  "meeting_key": 1280,
  "session_key": 11245,
  "stint_number": 1,
  "tyre_age_at_start": 0
}
```

#### Key identifier — the bridge column
`driver_number` in OpenF1 = `number` column in Kaggle's `drivers.csv`
This is the join key between OpenF1 and all other sources.

#### Data quality issues to handle in PySpark
- `lap_duration` is NULL for pit out laps — expected, not an error
- `segments_sector_*` arrays — drop these, not needed for the warehouse
- `date_start` is ISO 8601 with milliseconds — parse to timestamp
- `is_pit_out_lap` is boolean — already correct type
- `i1_speed`, `i2_speed`, `st_speed` are INT — already correct type
- `lap_duration`, `duration_sector_*` are FLOAT seconds — multiply × 1000 to get milliseconds INT

---

## 5. Identifier Bridge Map

This is critical for PySpark — you need to join three separate identifier systems:

| Source | Driver Identifier | How to join |
|---|---|---|
| Kaggle | `driverId` (INT), `driverRef` (STRING), `number` (INT) | Master reference |
| Jolpica | `driverId` (STRING) e.g. `"hamilton"` | = Kaggle `driverRef` |
| OpenF1 | `driver_number` (INT) e.g. `44` | = Kaggle `number` |

| Source | Race Identifier | How to join |
|---|---|---|
| Kaggle | `raceId` (INT) | Master reference |
| Jolpica | `season` (INT) + `round` (INT) | Join to Kaggle `races.csv` on `year + round` |
| OpenF1 | `session_key` (INT), `meeting_key` (INT) | Join via `session_metadata.json` date to Kaggle `races.csv` race date |

---

## 6. Ingestion Scripts

All scripts live in `ingestion/` and share a common logger module.

```
ingestion/
├── utils/
│   ├── __init__.py
│   └── logger.py              ← shared RotatingFileHandler, 5MB max, 3 backups
├── kaggle/
│   └── historical_load.py     ← one-time run, already complete
└── api/
    ├── jolpica.py             ← incremental by round, watermark in S3
    └── openf1.py              ← incremental by session_key, watermark in S3
```

### Run commands
```bash
# from project root with venv activated
python -m ingestion.kaggle.historical_load   # already done, don't re-run
python -m ingestion.api.jolpica              # run after each race
python -m ingestion.api.openf1               # run after each race
```

### Shared patterns across all scripts
- `fetch()` — HTTP GET with retry (3 attempts), exponential backoff, 429 rate limit handling, 404 raises immediately without retry
- `upload()` — boto3 `put_object` to S3, logs record count
- `read_watermark()` / `write_watermark()` — S3-based state management
- `validate_*()` — checks data completeness before uploading
- `write_validation_log()` — audit trail at `raw/validation/`
- `get_current_season()` — auto-detects year, Jan/Feb returns previous year

---

## 7. Validation Logs

Every ingestion writes a validation record to `raw/validation/`. These are intended to be checked by downstream processes before running.

### Jolpica validation log format
```json
{
  "season": 2026,
  "round": 1,
  "endpoint": "results",
  "is_valid": true,
  "reason": "ok",
  "checked_at": "2026-03-23"
}
```

### OpenF1 validation log format
```json
{
  "session_key": 11245,
  "year": 2026,
  "meeting_key": 1280,
  "is_valid": true,
  "reason": "ok",
  "checked_at": "2026-03-23"
}
```

---

## 8. Current Data State

| Source | Data Range | Rounds/Sessions | Status |
|---|---|---|---|
| Kaggle | 1950–2024 | All 14 CSVs | ✅ In S3 |
| Jolpica 2025 | Full season | 24/24 rounds | ✅ In S3 |
| Jolpica 2026 | Season in progress | 2/24 rounds | ✅ In S3 |
| OpenF1 2023 | Full season | Not yet ingested | 🔜 Pending first run |
| OpenF1 2024 | Full season | Not yet ingested | 🔜 Pending first run |
| OpenF1 2026 | Season in progress | 1 race (meeting_1280) | ✅ In S3 |

> Note: OpenF1 2023 and 2024 were not ingested in this session. The watermark starts from 2026. To backfill 2023/2024, change `START_YEAR = 2023` in `openf1.py` and reset the watermark manually.

---

## 9. Infrastructure

### AWS Resources (Terraform-managed)
| Resource | Name | Purpose |
|---|---|---|
| S3 Bucket | `f1-pipeline-raw-layer-034381339055` | Raw data + watermarks + validation |
| IAM User | `f1-pipeline-ingest-user` | Python scripts auth — S3 read/write only |
| IAM Policy | `s3-pipeline-access` | PutObject, GetObject, ListBucket on raw bucket |

### Terraform State
- State file: local (`terraform.tfstate`) — not remote yet
- To destroy compute: `terraform destroy` (S3 buckets have `prevent_destroy` behaviour — data persists)
- To pause: EC2 instance state toggle via `var.ec2_state = "stopped"`

### Credentials
- AWS credentials for `f1-pipeline-ingest-user` are in `.env` (gitignored)
- `.env` is never committed — `.env.example` documents required variables

---

## 10. What the PySpark Layer Needs to Do

This is the handoff scope for the next agent.

### 10.1 Read from S3
Read all raw files from:
- `raw/kaggle/2026-03-16/*.csv`
- `raw/jolpica/seasons/2025/round_*/*.json`
- `raw/jolpica/seasons/2026/round_*/*.json`
- `raw/openf1/2026/meeting_*/race/*.json`

### 10.2 Flatten Jolpica JSON
Jolpica responses are deeply nested. Each file needs to be flattened to a tabular structure:
- Navigate: `data["MRData"]["RaceTable"]["Races"][0]["Results"]`
- Extract race metadata (season, round, date, circuit) alongside each result row
- Add `_source_file` and `_loaded_at` audit columns

### 10.3 Type casting — priority fixes
| Column | Raw type | Target type | Notes |
|---|---|---|---|
| All Jolpica numeric fields | STRING | INT/DECIMAL | `position`, `points`, `grid`, `laps`, `millis` |
| Kaggle `\N` nulls | STRING `"\N"` | NULL | Replace before casting |
| Lap time strings | STRING `"1:21.546"` | INT milliseconds | Parse minutes + seconds |
| Pit stop duration | STRING `"23.124"` | INT milliseconds | Float seconds × 1000 |
| OpenF1 lap_duration | FLOAT seconds | INT milliseconds | × 1000 |
| Date columns | STRING | DATE | Multiple formats exist |
| OpenF1 segments arrays | ARRAY | DROP | Not needed downstream |

### 10.4 Deduplication
- Kaggle + Jolpica overlap for 2021–2024 seasons — deduplicate on natural keys
- Jolpica 2025/2026 laps endpoint is not available — use Kaggle lap_times for pre-2025, OpenF1 for 2023+

### 10.5 Load to Snowflake
Use Snowflake's `COPY INTO` from S3 stage. Target schema: `staging` (raw tables matching Kaggle column names, with added audit columns `_source`, `_loaded_at`, `_is_current`).

### 10.6 dbt picks up from staging
Once PySpark loads into `staging.*` tables, dbt handles all model building:
- `staging/` → views, type-safe, deduplicated
- `core/dimensions/` → `dim_driver`, `dim_race`, `dim_circuit`, `dim_constructor` etc.
- `core/facts/` → `fact_race_result`, `fact_qualifying`, `fact_lap_time`, `fact_pit_stop`
- `marts/` → pre-aggregated dashboard tables

Full dbt schema design is documented in `docs/star_schema_text.txt` in the repo.

---

## 11. Repo Structure

```
f1-pipeline/
├── ingestion/
│   ├── utils/
│   │   ├── __init__.py
│   │   └── logger.py
│   ├── kaggle/
│   │   └── historical_load.py
│   └── api/
│       ├── jolpica.py
│       └── openf1.py
├── processing/
│   └── glue_jobs/
│       └── raw_to_processed.py    ← placeholder, to be replaced with PySpark loader
├── warehouse/
│   └── dbt/
│       └── models/
│           ├── staging/
│           ├── intermediate/
│           ├── core/
│           └── marts/
├── orchestration/
│   └── dags/
│       └── f1_pipeline_dag.py     ← placeholder
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── logs/
│   └── pipeline.log               ← gitignored, rotating 5MB
├── .env                           ← gitignored
├── .env.example
├── requirements.txt
└── README.md
```

---

## 12. Environment Setup

```bash
# activate venv
venv\Scripts\activate   # Windows
source venv/bin/activate  # Mac/Linux

# install dependencies
pip install -r requirements.txt

# required .env variables
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=ap-south-1
S3_RAW_BUCKET=f1-pipeline-raw-layer-034381339055
KAGGLE_USERNAME=
KAGGLE_KEY=
SNOWFLAKE_ACCOUNT=      ← not yet configured
SNOWFLAKE_USER=         ← not yet configured
SNOWFLAKE_PASSWORD=     ← not yet configured
SNOWFLAKE_DATABASE=     ← not yet configured
SNOWFLAKE_WAREHOUSE=    ← not yet configured
```

---

*End of ingestion handoff report. Next stage: PySpark cleaning + Snowflake loader.*
