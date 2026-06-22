# dbt Layer Investigation Report

## 1. Project Structure
**Incremental Models (`incremental_models.txt`)**
```
models\marts\facts\fct_lap_time.sql:    materialized='incremental',
models\marts\facts\fct_pit_stop.sql:    materialized='incremental',
models\marts\facts\fct_qualifying.sql:    materialized='incremental',
models\marts\facts\fct_race_result.sql:    materialized='incremental',
models\staging\openf1\stg_openf1_laps.sql:    materialized='incremental',

```

**Unique Keys (`unique_keys.txt`)**
```
models\marts\facts\fct_lap_time.sql:    unique_key=['race_id', 'driver_key', 'lap_number'],
models\marts\facts\fct_pit_stop.sql:    unique_key=['race_key', 'driver_key', 'stop_number'],
models\marts\facts\fct_qualifying.sql:    unique_key='qualify_id',
models\marts\facts\fct_race_result.sql:    unique_key='result_id',
models\staging\jolpica\stg_jolpica_constructor_standings.sql:    unique_key=['season_year', 'round', 'constructor_id'],
models\staging\jolpica\stg_jolpica_driver_standings.sql:    unique_key=['season_year', 'round', 'driver_id'],
models\staging\jolpica\stg_jolpica_pitstops.sql:    unique_key=['season_year', 'round', 'driver_id', 'stop'],
models\staging\jolpica\stg_jolpica_qualifying.sql:    unique_key=['season_year', 'round', 'driver_id'],
models\staging\jolpica\stg_jolpica_results.sql:    unique_key=['season_year', 'round', 'driver_id'],
models\staging\jolpica\stg_jolpica_sprint.sql:    unique_key=['season_year', 'round', 'driver_id'],
models\staging\openf1\stg_openf1_drivers.sql:    unique_key=['year', 'driver_number'],
models\staging\openf1\stg_openf1_laps.sql:    unique_key=['year', 'driver_number', 'lap_number'],
models\staging\openf1\stg_openf1_pit.sql:    unique_key=['year', 'driver_number', 'lap_number'],
models\staging\openf1\stg_openf1_stints.sql:    unique_key=['year', 'driver_number', 'stint_number'],

```

**Partitions (`partitions.txt`)**
```
models\marts\facts\fct_lap_time.sql:    partition_by='season_year',
models\staging\jolpica\stg_jolpica_constructor_standings.sql:    partition_by='season_year'
models\staging\jolpica\stg_jolpica_driver_standings.sql:    partition_by='season_year'
models\staging\jolpica\stg_jolpica_pitstops.sql:    partition_by='season_year'
models\staging\jolpica\stg_jolpica_qualifying.sql:    partition_by='season'
models\staging\jolpica\stg_jolpica_results.sql:    partition_by='season'
models\staging\jolpica\stg_jolpica_sprint.sql:    partition_by='season_year'
models\staging\kaggle\stg_kaggle_constructor_standings.sql:    partition_by='year'
models\staging\kaggle\stg_kaggle_driver_standings.sql:    partition_by='year'
models\staging\kaggle\stg_kaggle_lap_times.sql:    partition_by='year'
models\staging\kaggle\stg_kaggle_qualifying.sql:    partition_by='year'
models\staging\kaggle\stg_kaggle_races.sql:    partition_by='year'
models\staging\kaggle\stg_kaggle_results.sql:    partition_by='year'
models\staging\openf1\stg_openf1_drivers.sql:    partition_by='year'
models\staging\openf1\stg_openf1_laps.sql:    partition_by='year'
models\staging\openf1\stg_openf1_pit.sql:    partition_by='year'
models\staging\openf1\stg_openf1_stints.sql:    partition_by='year'

```

## 2. Current Data Volumes (DuckDB)
**Staging:**
```text
('stg_kaggle_drivers', 861)\n('stg_kaggle_results', 26759)\n('stg_kaggle_qualifying', 10494)\n('stg_kaggle_lap_times', 589081)\n('stg_kaggle_pit_stops', 11371)\n('stg_openf1_laps', 1926)\n('stg_jolpica_results', 1134)\n('stg_jolpica_qualifying', 608)
```
**Intermediate:**
```text
('int_driver_bridge', 861)\n('int_results_unified', 27893)\n('int_qualifying_unified', 11102)\n('int_pitstops_unified', 12414)
```
**Gold:**
```text
('dim_driver', 861)\n('fct_race_result', 27893)
```

## 3. Data Freshness
```text
Binder Error: Referenced column "_dbt_loaded_at" not found in FROM clause!
Candidate bindings: "laps", "time"

LINE 4:     SELECT _dbt_loaded_at, 'stg_kaggle_results' as model_name FROM...
                   ^
```

## 4. Duplicate Check Results
**OpenF1 Laps Unique Key Violations (Phase 2):**
```text
(2026, 43, 2, 2)\n(2026, 16, 5, 2)\n(2026, 16, 24, 2)\n(2026, 41, 44, 2)\n(2026, 3, 22, 2)\n(2026, 55, 24, 2)\n(2026, 10, 26, 2)\n(2026, 44, 43, 2)\n(2026, 55, 5, 2)\n(2026, 87, 40, 2)
```
**NULL Foreign Keys (Phase 2):**
```text
('fct_race_result NULL driver_key', 176)
```

## 5. Incremental Models Currently Used
See Section 1 above. 
OpenF1 Duplicates detailed check (Phase 5):
```text
(2026, 16, 4, 2)\n(2026, 44, 35, 2)\n(2026, 43, 20, 2)\n(2026, 41, 42, 2)\n(2026, 87, 51, 2)\n(2026, 55, 39, 2)\n(2026, 55, 42, 2)\n(2026, 30, 44, 2)\n(2026, 10, 49, 2)\n(2026, 12, 53, 2)\n(2026, 10, 28, 2)\n(2026, 55, 7, 2)\n(2026, 30, 25, 2)\n(2026, 41, 39, 2)\n(2026, 44, 48, 2)\n(2026, 30, 5, 2)\n(2026, 41, 7, 2)\n(2026, 6, 8, 2)\n(2026, 10, 8, 2)\n(2026, 3, 8, 2)
```

## 6. Unique Key Definitions
See Section 1 above.

## 7. Test Coverage
Total tests (unique/not_null in YAMLs): 234
Staging Tests Output:
```text
dbt-fusion 2.0.0-preview.177
   Loading profiles.yml
   Started resolving packages from packages.yml
  Finished [  0.11s] resolving packages from packages.yml (1 items)
DOWNLOADING deferral manifest
DOWNLOADING deferral manifest
    Failed [  1.21s] test  source_not_null_processed_openf1_stints_driver_number (models\staging\sources.yml:425:17)
    Failed [  1.49s] test  source_not_null_processed_openf1_pit_lap_number (models\staging\sources.yml:413:17)
    Failed [  1.81s] test  source_not_null_processed_openf1_pit_year (models\staging\sources.yml:405:17)
    Failed [  2.11s] test  source_not_null_processed_jolpica_constructor_standings_round (models\staging\sources.yml:342:17)
    Failed [  2.41s] test  source_not_null_processed_jolpica_driver_standings_season (models\staging\sources.yml:318:17)
    Failed [  2.75s] test  source_not_null_processed_openf1_drivers_year (models\staging\sources.yml:377:17)
    Failed [  3.08s] test  unique_stg_kaggle_races_race_id (models\staging\kaggle\schema.yml:104:13)
    Failed [  3.35s] test  not_null_stg_kaggle_results_source_system (models\staging\kaggle\schema.yml:158:13)
    Failed [  3.64s] test  not_null_stg_kaggle_constructor_standings_year (models\staging\kaggle\schema.yml:275:13)
    Failed [  3.97s] test  source_unique_s3_kaggle_constructors_constructorId (models\staging\sources.yml:47:17)
    Failed [  4.25s] test  not_null_stg_openf1_drivers_year (models\staging\openf1\schema.yml:10:13)
    Failed [  4.58s] test  not_null_stg_jolpica_results_driver_id (models\staging\jolpica\schema.yml:18:13)
    Failed [  4.86s] test  source_not_null_processed_jolpica_driver_standings_wins (models\staging\sources.yml:330:17)
    Failed [  5.16s] test  not_null_stg_kaggle_circuits_circuit_id (models\staging\kaggle\schema.yml:11:13)
    Failed [  5.47s] test  source_not_null_processed_openf1_laps_driver_number (models\staging\sources.yml:393:17)
    Failed [  5.78s] test  source_not_null_processed_openf1_stints_stint_number (models\staging\sources.yml:429:17)
    Failed [  6.10s] test  not_null_stg_kaggle_qualifying_qualify_id (models\staging\kaggle\schema.yml:167:13)
    Failed [  6.42s] test  source_not_null_processed_jolpica_driver_standings_round (models\staging\sources.yml:322:17)
    Failed [  6.71s] test  not_null_stg_openf1_drivers_driver_number (models\staging\openf1\schema.yml:14:13)
    Failed [  7.05s] test  unique_stg_kaggle_status_status_id (models\staging\kaggle\schema.yml:287:13)
    Failed [  7.47s] test  source_not_null_processed_jolpica_pitstops_season (models\staging\sources.yml:306:17)
    Failed [  7.77s] test  not_null_stg_kaggle_qualifying_race_id (models\staging\kaggle\schema.yml:171:13)
    Failed [  8.09s] test  not_null_stg_jolpica_sprint_season_year (models\staging\jolpica\schema.yml:144:13)
    Failed [  8.39s] test  source_not_null_processed_openf1_pit_driver_number (models\staging\sources.yml:409:17)
    Failed [  8.69s] test  source_unique_s3_kaggle_drivers_driverId (models\staging\sources.yml:64:17)
    Failed [  8.97s] test  not_null_stg_kaggle_drivers_driver_ref (models\staging\kaggle\schema.yml:65:13)
    Failed [  9.27s] test  not_null_stg_jolpica_results_constructor_id (models\staging\jolpica\schema.yml:22:13)
    Failed [  9.56s] test  not_null_stg_jolpica_pitstops_stop (models\staging\jolpica\schema.yml:130:13)
    Failed [  9.84s] test  not_null_stg_jolpica_sprint_driver_id (models\staging\jolpica\schema.yml:152:13)
    Failed [ 10.16s] test  unique_stg_kaggle_drivers_driver_id (models\staging\kaggle\schema.yml:60:13)
    Failed [ 10.45s] test  source_not_null_processed_openf1_laps_year (models\staging\sources.yml:389:17)
    Failed [ 10.73s] test  not_null_stg_kaggle_constructor_standings_race_id (models\staging\kaggle\schema.yml:259:13)
    Failed [ 11.01s] test  not_null_stg_kaggle_lap_times_race_id (models\staging\kaggle\schema.yml:197:13)
    Failed [ 11.30s] test  not_null_stg_kaggle_driver_standings_source_system (models\staging\kaggle\schema.yml:246:13)
    Failed [ 11.59s] test  not_null_stg_kaggle_lap_times_source_system (models\staging\kaggle\schema.yml:215:13)
    Failed [ 11.88s] test  not_null_stg_kaggle_driver_standings_race_id (models\staging\kaggle\schema.yml:228:13)
    Failed [ 12.18s] test  source_not_null_processed_jolpica_qualifying_round (models\staging\sources.yml:294:17)
    Failed [ 12.48s] test  source_unique_s3_kaggle_races_raceId (models\staging\sources.yml:100:17)
    Failed [ 12.76s] test  not_null_stg_openf1_stints_year (models\staging\openf1\schema.yml:96:13)
    Failed [ 13.04s] test  not_null_stg_jolpica_constructor_standings_round (models\staging\jolpica\schema.yml:98:13)
    Failed [ 13.33s] test  source_not_null_s3_kaggle_seasons_year (models\staging\sources.yml:90:17)
    Failed [ 13.61s] test  not_null_stg_openf1_drivers_source_system (models\staging\openf1\schema.yml:28:13)
    Failed [ 13.91s] test  not_null_stg_jolpica_qualifying_season_year (models\staging\jolpica\schema.yml:40:13)
    Failed [ 14.21s] test  not_null_stg_kaggle_driver_standings_driver_id (models\staging\kaggle\schema.yml:232:13)
    Failed [ 14.52s] test  not_null_stg_jolpica_pitstops_driver_id (models\staging\jolpica\schema.yml:126:13)
    Failed [ 14.81s] test  not_null_stg_jolpica_pitstops_season_year (models\staging\jolpica\schema.yml:118:13)
    Failed [ 15.13s] test  not_null_stg_jolpica_sprint_constructor_id (models\staging\jolpica\schema.yml:156:13)
    Failed [ 15.43s] test  not_null_stg_kaggle_circuits_source_system (models\staging\kaggle\schema.yml:31:13)
    Failed [ 15.73s] test  not_null_stg_kaggle_races_source_system (models\staging\kaggle\schema.yml:125:13)
    Failed [ 16.04s] test  source_not_null_processed_jolpica_sprint_driver_id (models\staging\sources.yml:358:17)
    Failed [ 16.32s] test  not_null_stg_kaggle_constructors_source_system (models\staging\kaggle\schema.yml:52:13)
    Failed [ 16.61s] test  not_null_stg_kaggle_qualifying_constructor_id (models\staging\kaggle\schema.yml:179:13)
    Failed [ 16.92s] test  source_not_null_s3_kaggle_drivers_driverId (models\staging\sources.yml:65:17)
    Failed [ 17.20s] test  not_null_stg_kaggle_status_status_id (models\staging\kaggle\schema.yml:288:13)
    Failed [ 17.52s] test  source_not_null_s3_kaggle_races_raceId (models\staging\sources.yml:101:17)
    Failed [ 17.79s] test  not_null_stg_openf1_laps_year (models\staging\openf1\schema.yml:36:13)
    Failed [ 18.10s] test  unique_stg_kaggle_circuits_circuit_id (models\staging\kaggle\schema.yml:10:13)
    Failed [ 18.37s] test  not_null_stg_kaggle_results_year (models\staging\kaggle\schema.yml:150:13)
    Failed [ 18.68s] test  not_null_stg_kaggle_results_constructor_id (models\staging\kaggle\schema.yml:146:13)
    Failed [ 18.99s] test  not_null_stg_openf1_stints_stint_number (models\staging\openf1\schema.yml:104:13)
    Failed [ 19.29s] test  not_null_stg_kaggle_lap_times_year (models\staging\kaggle\schema.yml:211:13)
    Failed [ 19.58s] test  not_null_stg_openf1_pit_source_system (models\staging\openf1\schema.yml:88:13)
    Failed [ 19.88s] test  not_null_stg_jolpica_driver_standings_round (models\staging\jolpica\schema.yml:72:13)
    Failed [ 20.18s] test  not_null_stg_kaggle_seasons_season_year (models\staging\kaggle\schema.yml:92:13)
    Failed [ 20.49s] test  not_null_stg_openf1_laps_driver_number (models\staging\openf1\schema.yml:40:13)
    Failed [ 20.80s] test  not_null_stg_jolpica_driver_standings_season_year (models\staging\jolpica\schema.yml:68:13)
    Failed [ 21.08s] test  not_null_stg_openf1_pit_lap_number (models\staging\openf1\schema.yml:80:13)
    Failed [ 21.39s] test  source_not_null_processed_jolpica_qualifying_season (models\staging\sources.yml:290:17)
    Failed [ 21.71s] test  not_null_stg_kaggle_results_driver_id (models\staging\kaggle\schema.yml:142:13)
    Failed [ 22.00s] test  not_null_stg_kaggle_drivers_driver_id (models\staging\kaggle\schema.yml:61:13)
    Failed [ 22.33s] test  not_null_stg_kaggle_lap_times_lap_number (models\staging\kaggle\schema.yml:205:13)
    Failed [ 22.61s] test  not_null_stg_kaggle_lap_times_driver_id (models\staging\kaggle\schema.yml:201:13)
    Failed [ 22.88s] test  not_null_stg_kaggle_races_race_id (models\staging\kaggle\schema.yml:105:13)
    Failed [ 23.16s] test  not_null_stg_kaggle_qualifying_year (models\staging\kaggle\schema.yml:185:13)
    Failed [ 23.46s] test  not_null_stg_openf1_stints_source_system (models\staging\openf1\schema.yml:118:13)
    Failed [ 23.75s] test  source_not_null_processed_jolpica_results_driver_id (models\staging\sources.yml:282:17)
    Failed [ 24.02s] test  not_null_stg_kaggle_results_race_id (models\staging\kaggle\schema.yml:138:13)
    Failed [ 24.32s] test  source_not_null_processed_jolpica_driver_standings_driver_id (models\staging\sources.yml:326:17)
    Failed [ 24.62s] test  not_null_stg_openf1_stints_driver_number (models\staging\openf1\schema.yml:100:13)
    Failed [ 24.90s] test  not_null_stg_kaggle_races_round (models\staging\kaggle\schema.yml:113:13)
    Failed [ 25.18s] test  source_not_null_processed_openf1_drivers_driver_number (models\staging\sources.yml:381:17)
    Failed [ 25.47s] test  not_null_stg_jolpica_pitstops_source_system (models\staging\jolpica\schema.yml:136:13)
    Failed [ 25.73s] test  not_null_stg_kaggle_races_year (models\staging\kaggle\schema.yml:109:13)
    Failed [ 26.01s] test  not_null_stg_kaggle_driver_standings_year (models\staging\kaggle\schema.yml:242:13)
    Failed [ 26.32s] test  source_not_null_processed_openf1_laps_lap_number (models\staging\sources.yml:397:17)
    Failed [ 26.61s] test  not_null_stg_kaggle_qualifying_source_system (models\staging\kaggle\schema.yml:189:13)
    Failed [ 26.93s] test  not_null_stg_jolpica_results_source_system (models\staging\jolpica\schema.yml:32:13)
    Failed [ 27.27s] test  unique_stg_kaggle_qualifying_qualify_id (models\staging\kaggle\schema.yml:166:13)
    Failed [ 27.53s] test  not_null_stg_kaggle_driver_standings_driver_standings_id (models\staging\kaggle\schema.yml:224:13)
    Failed [ 27.80s] test  not_null_stg_openf1_laps_source_system (models\staging\openf1\schema.yml:64:13)
    Failed [ 28.13s] test  unique_stg_kaggle_constructor_standings_constructor_standings_id (models\staging\kaggle\schema.yml:254:13)
    Failed [ 28.45s] test  source_not_null_processed_jolpica_pitstops_round (models\staging\sources.yml:310:17)
    Failed [ 28.79s] test  source_not_null_processed_jolpica_constructor_standings_season (models\staging\sources.yml:338:17)
    Failed [ 29.11s] test  not_null_stg_kaggle_results_result_id (models\staging\kaggle\schema.yml:134:13)
    Failed [ 29.42s] test  not_null_stg_openf1_pit_year (models\staging\openf1\schema.yml:72:13)
    Failed [ 29.72s] test  not_null_stg_jolpica_results_round (models\staging\jolpica\schema.yml:14:13)
    Failed [ 30.04s] test  source_not_null_processed_openf1_stints_year (models\staging\sources.yml:421:17)
    Failed [ 30.34s] test  not_null_stg_jolpica_results_season_year (models\staging\jolpica\schema.yml:10:13)
    Failed [ 30.67s] test  not_null_stg_openf1_pit_driver_number (models\staging\openf1\schema.yml:76:13)
    Failed [ 31.02s] test  not_null_stg_kaggle_seasons_source_system (models\staging\kaggle\schema.yml:96:13)
    Failed [ 31.31s] test  not_null_stg_kaggle_constructor_standings_source_system (models\staging\kaggle\schema.yml:279:13)
    Failed [ 31.60s] test  not_null_stg_jolpica_qualifying_source_system (models\staging\jolpica\schema.yml:60:13)
    Failed [ 31.91s] test  not_null_stg_kaggle_circuits_circuit_ref (models\staging\kaggle\schema.yml:15:13)
    Failed [ 32.22s] test  not_null_stg_kaggle_constructors_constructor_ref (models\staging\kaggle\schema.yml:44:13)
    Failed [ 32.55s] test  unique_stg_kaggle_seasons_season_year (models\staging\kaggle\schema.yml:91:13)
    Failed [ 32.83s] test  unique_stg_kaggle_results_result_id (models\staging\kaggle\schema.yml:133:13)
    Failed [ 33.12s] test  source_not_null_s3_kaggle_circuits_circuitId (models\staging\sources.yml:23:17)
    Failed [ 33.48s] test  not_null_stg_jolpica_sprint_round (models\staging\jolpica\schema.yml:148:13)
    Failed [ 33.79s] test  source_not_null_processed_jolpica_sprint_season (models\staging\sources.yml:350:17)
    Failed [ 34.09s] test  not_null_stg_jolpica_qualifying_driver_id (models\staging\jolpica\schema.yml:48:13)
    Failed [ 34.38s] test  not_null_stg_jolpica_driver_standings_driver_id (models\staging\jolpica\schema.yml:76:13)
    Failed [ 34.68s] test  source_not_null_processed_jolpica_qualifying_driver_id (models\staging\sources.yml:298:17)
    Failed [ 34.96s] test  not_null_stg_jolpica_constructor_standings_season_year (models\staging\jolpica\schema.yml:94:13)
    Failed [ 35.24s] test  not_null_stg_jolpica_driver_standings_source_system (models\staging\jolpica\schema.yml:86:13)
    Failed [ 35.54s] test  source_not_null_s3_kaggle_constructors_constructorId (models\staging\sources.yml:48:17)
    Failed [ 35.83s] test  not_null_stg_kaggle_status_status_description (models\staging\kaggle\schema.yml:292:13)
    Failed [ 36.10s] test  not_null_stg_kaggle_constructors_constructor_id (models\staging\kaggle\schema.yml:40:13)
    Failed [ 36.37s] test  not_null_stg_kaggle_status_source_system (models\staging\kaggle\schema.yml:296:13)
    Failed [ 36.67s] test  source_unique_s3_kaggle_seasons_year (models\staging\sources.yml:89:17)
    Failed [ 36.94s] test  not_null_stg_openf1_laps_lap_number (models\staging\openf1\schema.yml:44:13)
    Failed [ 37.23s] test  not_null_stg_jolpica_pitstops_round (models\staging\jolpica\schema.yml:122:13)
    Failed [ 37.51s] test  not_null_stg_kaggle_constructor_standings_constructor_id (models\staging\kaggle\schema.yml:263:13)
    Failed [ 37.84s] test  not_null_stg_kaggle_constructor_standings_constructor_standings_id (models\staging\kaggle\schema.yml:255:13)
    Failed [ 38.13s] test  not_null_stg_kaggle_drivers_source_system (models\staging\kaggle\schema.yml:83:13)
    Failed [ 38.42s] test  not_null_stg_kaggle_qualifying_driver_id (models\staging\kaggle\schema.yml:175:13)
    Failed [ 38.70s] test  not_null_stg_jolpica_sprint_source_system (models\staging\jolpica\schema.yml:162:13)
    Failed [ 38.98s] test  not_null_stg_jolpica_constructor_standings_source_system (models\staging\jolpica\schema.yml:110:13)
    Failed [ 39.27s] test  unique_stg_kaggle_driver_standings_driver_standings_id (models\staging\kaggle\schema.yml:223:13)
    Failed [ 39.56s] test  source_not_null_processed_jolpica_results_round (models\staging\sources.yml:278:17)
    Failed [ 39.90s] test  source_unique_s3_kaggle_circuits_circuitId (models\staging\sources.yml:22:17)
    Failed [ 40.18s] test  not_null_stg_jolpica_qualifying_round (models\staging\jolpica\schema.yml:44:13)
    Failed [ 40.47s] test  not_null_stg_kaggle_races_circuit_id (models\staging\kaggle\schema.yml:117:13)
    Failed [ 40.78s] test  not_null_stg_jolpica_constructor_standings_constructor_id (models\staging\jolpica\schema.yml:102:13)
    Failed [ 41.09s] test  source_not_null_processed_jolpica_results_season (models\staging\sources.yml:274:17)
    Failed [ 41.41s] test  source_not_null_processed_jolpica_sprint_round (models\staging\sources.yml:354:17)
    Failed [ 41.70s] test  unique_stg_kaggle_constructors_constructor_id (models\staging\kaggle\schema.yml:39:13)
New version available 2.0.0-preview.190 (run `dbt system update`)

==================== Execution Summary =====================
Finished 'test' with 3 warnings and 272 errors for target 'dev' [44.7s]
Processed: 136 tests
Summary: 136 total | 136 error

=================== Errors and Warnings ====================
[warning] [PackageVersionMismatch (dbt1080)]: Package 'dbt_utils' may not be compatible with your dbt version: Required dbt version does not match current version. Check Package Hub (https://hub.getdbt.com) for compatible versions or contact the package maintainer.
[warning] [HttpError (dbt1203)]: Failed to download deferral manifest from the dbt platform for project 70506183136612, continuing without deferral. 404 Not Found: No deferral environment defined for project with ID 70506183136612 To target a specific environment, set `defer-env-id` in your dbt_project.yml under the `dbt-cloud` block.
[warning] [HttpError (dbt1203)]: Failed to download deferral manifest from the dbt platform for project 70506183136612, continuing without deferral. 404 Not Found: No deferral environment defined for project with ID 70506183136612 To target a specific environment, set `defer-env-id` in your dbt_project.yml under the `dbt-cloud` block.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_openf1_stints_driver_number.2698f04c60 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_stints_driver_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_stints_driver_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_openf1_pit_lap_number.7c463997b9 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_pit_lap_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_pit_lap_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_openf1_pit_year.83d38eef44 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_pit_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_pit_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_constructor_standings_round.3917f41f3e (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_constructor_standings_round.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_constructor_standings_round.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_driver_standings_season.f2b8786c3a (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_driver_standings_season.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_driver_standings_season.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_openf1_drivers_year.f2a25e5934 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_drivers_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_drivers_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.unique_stg_kaggle_races_race_id.32a2bbcf45 (target\run\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_races_race_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_races_race_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_results_source_system.618f2e91db (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_results_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_results_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_constructor_standings_year.5d8aa95602 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructor_standings_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructor_standings_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_unique_s3_kaggle_constructors_constructorId.85be24beba (target\run\f1_analytics_warehouse\target\generic_tests\source_unique_s3_kaggle_constructors_constructorId.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_unique_s3_kaggle_constructors_constructorId.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_drivers_year.fdd5650d6a (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_drivers_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_drivers_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_results_driver_id.1e4ebf22bd (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_results_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_results_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_driver_standings_wins.20ac21da80 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_driver_standings_wins.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_driver_standings_wins.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_circuits_circuit_id.37ce20d529 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_circuits_circuit_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_circuits_circuit_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_openf1_laps_driver_number.7ab8172d02 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_laps_driver_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_laps_driver_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_openf1_stints_stint_number.a143587018 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_stints_stint_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_stints_stint_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_qualifying_qualify_id.db752fb7b3 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_qualifying_qualify_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_qualifying_qualify_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_driver_standings_round.b1e8eca4b0 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_driver_standings_round.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_driver_standings_round.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_drivers_driver_number.9e514ab02c (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_drivers_driver_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_drivers_driver_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.unique_stg_kaggle_status_status_id.481d2e9dc7 (target\run\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_status_status_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_status_status_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_pitstops_season.90bfdb07e3 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_pitstops_season.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_pitstops_season.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_qualifying_race_id.ce324ddee6 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_qualifying_race_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_qualifying_race_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_sprint_season_year.11033f77e5 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_sprint_season_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_sprint_season_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_openf1_pit_driver_number.a14fa6180a (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_pit_driver_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_pit_driver_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_unique_s3_kaggle_drivers_driverId.f7eeff4931 (target\run\f1_analytics_warehouse\target\generic_tests\source_unique_s3_kaggle_drivers_driverId.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_unique_s3_kaggle_drivers_driverId.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_drivers_driver_ref.7c862d5648 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_drivers_driver_ref.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_drivers_driver_ref.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_results_constructor_id.bd062ff852 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_results_constructor_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_results_constructor_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_pitstops_stop.aa4b7cf592 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_pitstops_stop.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_pitstops_stop.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_sprint_driver_id.7d263d369b (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_sprint_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_sprint_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.unique_stg_kaggle_drivers_driver_id.de410a50d2 (target\run\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_drivers_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_drivers_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_openf1_laps_year.f506e1e308 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_laps_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_laps_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_constructor_standings_race_id.fa12f04707 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructor_standings_race_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructor_standings_race_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_lap_times_race_id.da7e036f74 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_lap_times_race_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_lap_times_race_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_driver_standings_source_system.03567ef70c (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_driver_standings_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_driver_standings_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_lap_times_source_system.71f8a9f0b5 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_lap_times_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_lap_times_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_driver_standings_race_id.4de65212ea (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_driver_standings_race_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_driver_standings_race_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_qualifying_round.edf8bce5d6 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_qualifying_round.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_qualifying_round.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_unique_s3_kaggle_races_raceId.afcc75a8fc (target\run\f1_analytics_warehouse\target\generic_tests\source_unique_s3_kaggle_races_raceId.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_unique_s3_kaggle_races_raceId.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_stints_year.ea477950c7 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_stints_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_stints_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_constructor_standings_round.478abb4754 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_constructor_standings_round.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_constructor_standings_round.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_s3_kaggle_seasons_year.ec636d3987 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_s3_kaggle_seasons_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_s3_kaggle_seasons_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_drivers_source_system.1d1f1b09cd (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_drivers_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_drivers_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_qualifying_season_year.7de56cc27b (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_qualifying_season_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_qualifying_season_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_driver_standings_driver_id.390bcf8fed (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_driver_standings_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_driver_standings_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_pitstops_driver_id.b92a4646f2 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_pitstops_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_pitstops_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_pitstops_season_year.948ce3321d (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_pitstops_season_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_pitstops_season_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_sprint_constructor_id.f211aac853 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_sprint_constructor_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_sprint_constructor_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_circuits_source_system.d84796db0d (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_circuits_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_circuits_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_races_source_system.58f0edc10f (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_races_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_races_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_sprint_driver_id.8e11e59a62 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_sprint_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_sprint_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_constructors_source_system.07f9251c7c (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructors_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructors_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_qualifying_constructor_id.2c207f3a90 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_qualifying_constructor_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_qualifying_constructor_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_s3_kaggle_drivers_driverId.f55e0e3166 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_s3_kaggle_drivers_driverId.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_s3_kaggle_drivers_driverId.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_status_status_id.2fb826964f (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_status_status_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_status_status_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_s3_kaggle_races_raceId.c227970716 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_s3_kaggle_races_raceId.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_s3_kaggle_races_raceId.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_laps_year.0faaebcf10 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_laps_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_laps_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.unique_stg_kaggle_circuits_circuit_id.27c97769d2 (target\run\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_circuits_circuit_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_circuits_circuit_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_results_year.02486157b9 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_results_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_results_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_results_constructor_id.dcaa13a24f (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_results_constructor_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_results_constructor_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_stints_stint_number.2d936e5b82 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_stints_stint_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_stints_stint_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_lap_times_year.8da74dce6f (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_lap_times_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_lap_times_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_pit_source_system.b5932d61ac (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_pit_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_pit_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_driver_standings_round.c56043cbf5 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_driver_standings_round.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_driver_standings_round.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_seasons_season_year.0b3bc98387 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_seasons_season_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_seasons_season_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_laps_driver_number.28fe8766df (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_laps_driver_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_laps_driver_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_driver_standings_season_year.5dc186cff7 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_driver_standings_season_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_driver_standings_season_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_pit_lap_number.6a556b0dee (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_pit_lap_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_pit_lap_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_qualifying_season.60eb43298d (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_qualifying_season.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_qualifying_season.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_results_driver_id.3a11bc53b8 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_results_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_results_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_drivers_driver_id.1c48f0fa64 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_drivers_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_drivers_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_lap_times_lap_number.0e3e27a06e (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_lap_times_lap_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_lap_times_lap_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_lap_times_driver_id.d1692d9039 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_lap_times_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_lap_times_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_races_race_id.4ded2d3839 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_races_race_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_races_race_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_qualifying_year.5fa8dce08f (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_qualifying_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_qualifying_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_stints_source_system.2b9adb5b5d (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_stints_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_stints_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_results_driver_id.ce8c7d8155 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_results_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_results_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_results_race_id.c66a6b348e (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_results_race_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_results_race_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_driver_standings_driver_id.d5db3d26e3 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_driver_standings_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_driver_standings_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_stints_driver_number.13ad9d5064 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_stints_driver_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_stints_driver_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_races_round.d7bc82baff (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_races_round.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_races_round.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_openf1_drivers_driver_number.60c202a76a (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_drivers_driver_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_drivers_driver_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_pitstops_source_system.d383293c9a (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_pitstops_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_pitstops_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_races_year.de36f1636d (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_races_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_races_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_driver_standings_year.0fec760be2 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_driver_standings_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_driver_standings_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_openf1_laps_lap_number.21d7c67cd9 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_laps_lap_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_laps_lap_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_qualifying_source_system.d843236454 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_qualifying_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_qualifying_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_results_source_system.49de4a8657 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_results_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_results_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.unique_stg_kaggle_qualifying_qualify_id.928c165def (target\run\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_qualifying_qualify_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_qualifying_qualify_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_driver_standings_driver_standings_id.e360cff68e (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_driver_standings_driver_standings_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_driver_standings_driver_standings_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_laps_source_system.6dbc11980d (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_laps_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_laps_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.unique_stg_kaggle_constructor_standings_constructor_standings_id.df63bc4d6f (target\run\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_constructor__3b5279953e0709f3d8e4b3a63b728252.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_constructor__3b5279953e0709f3d8e4b3a63b728252.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_pitstops_round.1aa3761de5 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_pitstops_round.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_pitstops_round.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_constructor_standings_season.5769bb7d14 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_constructor_standings_season.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_constructor_standings_season.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_results_result_id.b2bd2f2175 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_results_result_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_results_result_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_pit_year.ef5d5b391e (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_pit_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_pit_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_results_round.808055efff (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_results_round.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_results_round.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_openf1_stints_year.337fa8bef2 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_stints_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_openf1_stints_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_results_season_year.026cce281f (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_results_season_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_results_season_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_pit_driver_number.70b87f28cf (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_pit_driver_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_pit_driver_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_seasons_source_system.91957a65ac (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_seasons_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_seasons_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_constructor_standings_source_system.6f5c77c1a1 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructor_standings_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructor_standings_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_qualifying_source_system.7059233531 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_qualifying_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_qualifying_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_circuits_circuit_ref.bd66ae1484 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_circuits_circuit_ref.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_circuits_circuit_ref.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_constructors_constructor_ref.aff2f19e8e (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructors_constructor_ref.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructors_constructor_ref.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.unique_stg_kaggle_seasons_season_year.2b87ed611f (target\run\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_seasons_season_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_seasons_season_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.unique_stg_kaggle_results_result_id.35c51ac3c7 (target\run\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_results_result_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_results_result_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_s3_kaggle_circuits_circuitId.745c98b540 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_s3_kaggle_circuits_circuitId.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_s3_kaggle_circuits_circuitId.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_sprint_round.62d89a0e4d (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_sprint_round.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_sprint_round.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_sprint_season.fa69a7c4b8 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_sprint_season.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_sprint_season.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_qualifying_driver_id.aebe5248e7 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_qualifying_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_qualifying_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_driver_standings_driver_id.9eb5a9fc55 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_driver_standings_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_driver_standings_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_qualifying_driver_id.4072d673ba (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_qualifying_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_qualifying_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_constructor_standings_season_year.04063c8f74 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_constructor_standings_season_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_constructor_standings_season_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_driver_standings_source_system.e557fa1be8 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_driver_standings_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_driver_standings_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_s3_kaggle_constructors_constructorId.c04fc3312b (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_s3_kaggle_constructors_constructorId.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_s3_kaggle_constructors_constructorId.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_status_status_description.d2db533096 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_status_status_description.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_status_status_description.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_constructors_constructor_id.492a399a5d (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructors_constructor_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructors_constructor_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_status_source_system.6ce1cce9c6 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_status_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_status_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_unique_s3_kaggle_seasons_year.1737cdc5f7 (target\run\f1_analytics_warehouse\target\generic_tests\source_unique_s3_kaggle_seasons_year.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_unique_s3_kaggle_seasons_year.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_openf1_laps_lap_number.8272986be0 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_laps_lap_number.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_openf1_laps_lap_number.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_pitstops_round.b524f9e267 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_pitstops_round.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_pitstops_round.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_constructor_standings_constructor_id.e71617c23f (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructor_standings_constructor_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructor_standings_constructor_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_constructor_standings_constructor_standings_id.646ff21f6e (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructo_461e01a0083f93ab9ee41f4141b481ec.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_constructo_461e01a0083f93ab9ee41f4141b481ec.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_drivers_source_system.8ed62894f7 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_drivers_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_drivers_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_qualifying_driver_id.d9821c6179 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_qualifying_driver_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_qualifying_driver_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_sprint_source_system.9ceb54ba29 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_sprint_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_sprint_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_constructor_standings_source_system.f1779e9587 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_constructor_standings_source_system.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_constructor_standings_source_system.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.unique_stg_kaggle_driver_standings_driver_standings_id.12744a9de6 (target\run\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_driver_standings_driver_standings_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_driver_standings_driver_standings_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_results_round.f74fad6269 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_results_round.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_results_round.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_unique_s3_kaggle_circuits_circuitId.e76396d027 (target\run\f1_analytics_warehouse\target\generic_tests\source_unique_s3_kaggle_circuits_circuitId.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_unique_s3_kaggle_circuits_circuitId.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_qualifying_round.97db99f8bf (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_qualifying_round.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_qualifying_round.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_kaggle_races_circuit_id.44237d5109 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_races_circuit_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_kaggle_races_circuit_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.not_null_stg_jolpica_constructor_standings_constructor_id.d17f8aeff6 (target\run\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_constructor_standings_constructor_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\not_null_stg_jolpica_constructor_standings_constructor_id.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_results_season.176847d1a5 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_results_season.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_results_season.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.source_not_null_processed_jolpica_sprint_round.b50dbd5854 (target\run\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_sprint_round.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\source_not_null_processed_jolpica_sprint_round.sql:1:1). Treating as failing test.
[error]: Error materializing test test.f1_analytics_warehouse.unique_stg_kaggle_constructors_constructor_id.16345b7d6e (target\run\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_constructors_constructor_id.sql)
  IO Error: Cannot open file "f1_analytics.duckdb": The process cannot access the file because it is being used by another process.
  
  File is already open in 
  C:\Users\amazi\anaconda3\python.exe (PID 14300)
  (in compiled\f1_analytics_warehouse\target\generic_tests\unique_stg_kaggle_constructors_constructor_id.sql:1:1). Treating as failing test.

```

## 8. Known Issues
- `fct_race_result` has some NULL driver_key entries (due to 2025 Jolpica drivers not in Kaggle bridge).
- Any staging test failures will be shown above.

## 9. Questions Needing Clarification
- What is the exact expected `unique_key` for `stg_openf1_laps`? 
- Will BigQuery be handling history loads differently than DuckDB?
