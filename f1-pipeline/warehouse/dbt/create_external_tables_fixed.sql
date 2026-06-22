USE DATABASE f1_analytics_warehouse;
USE SCHEMA staging;

-- ==========================================
-- JOLPICA EXTERNAL TABLES (6 Tables)
-- ==========================================

CREATE OR REPLACE EXTERNAL TABLE staging.jolpica_constructor_standings (
    season INTEGER AS CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'season=(\\d+)', 1, 1, 'e', 1) AS INTEGER),
    round INTEGER AS (VALUE:round::INTEGER),
    constructor_id VARCHAR AS (VALUE:constructor_id::VARCHAR),
    constructor_name VARCHAR AS (VALUE:constructor_name::VARCHAR),
    position INTEGER AS (VALUE:position::INTEGER),
    position_text VARCHAR AS (VALUE:position_text::VARCHAR),
    points DOUBLE AS (VALUE:points::DOUBLE),
    wins INTEGER AS (VALUE:wins::INTEGER)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/jolpica/constructor_standings/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.jolpica_driver_standings (
    season INTEGER AS CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'season=(\\d+)', 1, 1, 'e', 1) AS INTEGER),
    round INTEGER AS (VALUE:round::INTEGER),
    driver_id VARCHAR AS (VALUE:driver_id::VARCHAR),
    position INTEGER AS (VALUE:position::INTEGER),
    points DOUBLE AS (VALUE:points::DOUBLE),
    wins INTEGER AS (VALUE:wins::INTEGER)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/jolpica/driver_standings/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.jolpica_pitstops (
    season INTEGER AS CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'season=(\\d+)', 1, 1, 'e', 1) AS INTEGER),
    round INTEGER AS (VALUE:round::INTEGER),
    driver_id VARCHAR AS (VALUE:driver_id::VARCHAR),
    stop INTEGER AS (VALUE:stop::INTEGER),
    lap INTEGER AS (VALUE:lap::INTEGER),
    time VARCHAR AS (VALUE:time::VARCHAR),
    duration_seconds DOUBLE AS (VALUE:duration_seconds::DOUBLE),
    duration_ms BIGINT AS (VALUE:duration_ms::BIGINT)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/jolpica/pitstops/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.jolpica_qualifying (
    season INTEGER AS CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'season=(\\d+)', 1, 1, 'e', 1) AS INTEGER),
    round INTEGER AS (VALUE:round::INTEGER),
    driver_id VARCHAR AS (VALUE:driver_id::VARCHAR),
    position INTEGER AS (VALUE:position::INTEGER),
    q1 VARCHAR AS (VALUE:q1::VARCHAR),
    q2 VARCHAR AS (VALUE:q2::VARCHAR),
    q3 VARCHAR AS (VALUE:q3::VARCHAR),
    q1_ms BIGINT AS (VALUE:q1_ms::BIGINT),
    q2_ms BIGINT AS (VALUE:q2_ms::BIGINT),
    q3_ms BIGINT AS (VALUE:q3_ms::BIGINT),
    driver_number INTEGER AS (VALUE:driver_number::INTEGER),
    driver_code VARCHAR AS (VALUE:driver_code::VARCHAR),
    constructor_id VARCHAR AS (VALUE:constructor_id::VARCHAR)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/jolpica/qualifying/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.jolpica_results (
    season INTEGER AS CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'season=(\\d+)', 1, 1, 'e', 1) AS INTEGER),
    round INTEGER AS (VALUE:round::INTEGER),
    driver_id VARCHAR AS (VALUE:driver_id::VARCHAR),
    driver_code VARCHAR AS (VALUE:driver_code::VARCHAR),
    driver_number INTEGER AS (VALUE:driver_number::INTEGER),
    constructor_id VARCHAR AS (VALUE:constructor_id::VARCHAR),
    grid INTEGER AS (VALUE:grid::INTEGER),
    position INTEGER AS (VALUE:position::INTEGER),
    position_text VARCHAR AS (VALUE:position_text::VARCHAR),
    laps INTEGER AS (VALUE:laps::INTEGER),
    points DOUBLE AS (VALUE:points::DOUBLE),
    time_millis BIGINT AS (VALUE:time_millis::BIGINT),
    time_text VARCHAR AS (VALUE:time_text::VARCHAR),
    fastest_lap_time_ms BIGINT AS (VALUE:fastest_lap_time_ms::BIGINT),
    fastest_lap_speed DOUBLE AS (VALUE:fastest_lap_speed::DOUBLE),
    status VARCHAR AS (VALUE:status::VARCHAR)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/jolpica/results/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.jolpica_sprint (
    season INTEGER AS CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'season=(\\d+)', 1, 1, 'e', 1) AS INTEGER),
    round INTEGER AS (VALUE:round::INTEGER),
    driver_id VARCHAR AS (VALUE:driver_id::VARCHAR),
    driver_code VARCHAR AS (VALUE:driver_code::VARCHAR),
    driver_number INTEGER AS (VALUE:driver_number::INTEGER),
    constructor_id VARCHAR AS (VALUE:constructor_id::VARCHAR),
    constructor_name VARCHAR AS (VALUE:constructor_name::VARCHAR),
    grid INTEGER AS (VALUE:grid::INTEGER),
    position INTEGER AS (VALUE:position::INTEGER),
    position_text VARCHAR AS (VALUE:position_text::VARCHAR),
    laps INTEGER AS (VALUE:laps::INTEGER),
    points DOUBLE AS (VALUE:points::DOUBLE),
    time_millis BIGINT AS (VALUE:time_millis::BIGINT),
    time_text VARCHAR AS (VALUE:time_text::VARCHAR),
    status VARCHAR AS (VALUE:status::VARCHAR)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/jolpica/sprint/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;


-- ==========================================
-- OPENF1 EXTERNAL TABLES (4 Tables)
-- ==========================================

CREATE OR REPLACE EXTERNAL TABLE staging.openf1_drivers (
    year INTEGER AS CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'year=(\\d+)', 1, 1, 'e', 1) AS INTEGER),
    driver_number INTEGER AS (VALUE:driver_number::INTEGER),
    full_name VARCHAR AS (VALUE:full_name::VARCHAR),
    name_acronym VARCHAR AS (VALUE:name_acronym::VARCHAR),
    country_code VARCHAR AS (VALUE:country_code::VARCHAR),
    team_name VARCHAR AS (VALUE:team_name::VARCHAR),
    team_colour VARCHAR AS (VALUE:team_colour::VARCHAR),
    broadcast_name VARCHAR AS (VALUE:broadcast_name::VARCHAR)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/openf1/drivers/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.openf1_laps (
    year INTEGER AS CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'year=(\\d+)', 1, 1, 'e', 1) AS INTEGER),
    driver_number INTEGER AS (VALUE:driver_number::INTEGER),
    lap_number INTEGER AS (VALUE:lap_number::INTEGER),
    lap_duration_ms BIGINT AS (VALUE:lap_duration_ms::BIGINT),
    duration_sector_1_ms BIGINT AS (VALUE:duration_sector_1_ms::BIGINT),
    duration_sector_2_ms BIGINT AS (VALUE:duration_sector_2_ms::BIGINT),
    duration_sector_3_ms BIGINT AS (VALUE:duration_sector_3_ms::BIGINT),
    i1_speed DOUBLE AS (VALUE:i1_speed::DOUBLE),
    i2_speed DOUBLE AS (VALUE:i2_speed::DOUBLE),
    st_speed DOUBLE AS (VALUE:st_speed::DOUBLE),
    is_pit_out_lap BOOLEAN AS (VALUE:is_pit_out_lap::BOOLEAN),
    date_start VARCHAR AS (VALUE:date_start::VARCHAR)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/openf1/laps/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.openf1_pit (
    year INTEGER AS CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'year=(\\d+)', 1, 1, 'e', 1) AS INTEGER),
    driver_number INTEGER AS (VALUE:driver_number::INTEGER),
    lap_number INTEGER AS (VALUE:lap_number::INTEGER),
    meeting_key BIGINT AS (VALUE:meeting_key::BIGINT),
    session_key BIGINT AS (VALUE:session_key::BIGINT),
    meeting_id INTEGER AS (VALUE:meeting_id::INTEGER),
    pit_duration DOUBLE AS (VALUE:pit_duration::DOUBLE),
    pit_duration_ms BIGINT AS (VALUE:pit_duration_ms::BIGINT),
    lane_duration DOUBLE AS (VALUE:lane_duration::DOUBLE),
    date VARCHAR AS (VALUE:date::VARCHAR)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/openf1/pit/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.openf1_stints (
    year INTEGER AS CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'year=(\\d+)', 1, 1, 'e', 1) AS INTEGER),
    driver_number INTEGER AS (VALUE:driver_number::INTEGER),
    stint_number BIGINT AS (VALUE:stint_number::BIGINT),
    compound VARCHAR AS (VALUE:compound::VARCHAR),
    lap_start INTEGER AS (VALUE:lap_start::INTEGER),
    lap_end INTEGER AS (VALUE:lap_end::INTEGER),
    tyre_age_at_start BIGINT AS (VALUE:tyre_age_at_start::BIGINT),
    meeting_key BIGINT AS (VALUE:meeting_key::BIGINT),
    session_key BIGINT AS (VALUE:session_key::BIGINT),
    meeting_id INTEGER AS (VALUE:meeting_id::INTEGER)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/openf1/stints/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;


-- ==========================================
-- KAGGLE EXTERNAL TABLES (12 Tables)
-- ==========================================

CREATE OR REPLACE EXTERNAL TABLE staging.kaggle_circuits (
    circuitId INTEGER AS (VALUE:circuitId::INTEGER),
    circuitRef VARCHAR AS (VALUE:circuitRef::VARCHAR),
    name VARCHAR AS (VALUE:name::VARCHAR),
    location VARCHAR AS (VALUE:location::VARCHAR),
    country VARCHAR AS (VALUE:country::VARCHAR),
    lat DOUBLE AS (VALUE:lat::DOUBLE),
    lng DOUBLE AS (VALUE:lng::DOUBLE),
    alt INTEGER AS (VALUE:alt::INTEGER),
    url VARCHAR AS (VALUE:url::VARCHAR)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/kaggle/circuits/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.kaggle_constructors (
    constructorId INTEGER AS (VALUE:constructorId::INTEGER),
    constructorRef VARCHAR AS (VALUE:constructorRef::VARCHAR),
    name VARCHAR AS (VALUE:name::VARCHAR),
    nationality VARCHAR AS (VALUE:nationality::VARCHAR),
    url VARCHAR AS (VALUE:url::VARCHAR)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/kaggle/constructors/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.kaggle_drivers (
    driverId INTEGER AS (VALUE:driverId::INTEGER),
    driverRef VARCHAR AS (VALUE:driverRef::VARCHAR),
    code VARCHAR AS (VALUE:code::VARCHAR),
    forename VARCHAR AS (VALUE:forename::VARCHAR),
    surname VARCHAR AS (VALUE:surname::VARCHAR),
    dob DATE AS (VALUE:dob::DATE),
    nationality VARCHAR AS (VALUE:nationality::VARCHAR),
    number VARCHAR AS (VALUE:number::VARCHAR),
    url VARCHAR AS (VALUE:url::VARCHAR)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/kaggle/drivers/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.kaggle_seasons (
    year INTEGER AS (VALUE:year::INTEGER),
    url VARCHAR AS (VALUE:url::VARCHAR)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/kaggle/seasons/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.kaggle_races (
    raceId INTEGER AS (VALUE:raceId::INTEGER),
    year INTEGER AS CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'year=(\\d+)', 1, 1, 'e', 1) AS INTEGER),
    round INTEGER AS (VALUE:round::INTEGER),
    circuitId INTEGER AS (VALUE:circuitId::INTEGER),
    name VARCHAR AS (VALUE:name::VARCHAR),
    date DATE AS (VALUE:date::DATE),
    time VARCHAR AS (VALUE:time::VARCHAR),
    url VARCHAR AS (VALUE:url::VARCHAR)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/kaggle/races/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.kaggle_results (
    resultId INTEGER AS (VALUE:resultId::INTEGER),
    raceId INTEGER AS (VALUE:raceId::INTEGER),
    driverId INTEGER AS (VALUE:driverId::INTEGER),
    constructorId INTEGER AS (VALUE:constructorId::INTEGER),
    number VARCHAR AS (VALUE:number::VARCHAR),
    position VARCHAR AS (VALUE:position::VARCHAR),
    milliseconds VARCHAR AS (VALUE:milliseconds::VARCHAR),
    fastestLap VARCHAR AS (VALUE:fastestLap::VARCHAR),
    rank VARCHAR AS (VALUE:rank::VARCHAR),
    fastestLapSpeed VARCHAR AS (VALUE:fastestLapSpeed::VARCHAR),
    positionText VARCHAR AS (VALUE:positionText::VARCHAR),
    time VARCHAR AS (VALUE:time::VARCHAR),
    fastestLapTime VARCHAR AS (VALUE:fastestLapTime::VARCHAR),
    grid INTEGER AS (VALUE:grid::INTEGER),
    positionOrder INTEGER AS (VALUE:positionOrder::INTEGER),
    points DOUBLE AS (VALUE:points::DOUBLE),
    laps INTEGER AS (VALUE:laps::INTEGER),
    statusId INTEGER AS (VALUE:statusId::INTEGER)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/kaggle/results/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.kaggle_qualifying (
    qualifyId INTEGER AS (VALUE:qualifyId::INTEGER),
    raceId INTEGER AS (VALUE:raceId::INTEGER),
    driverId INTEGER AS (VALUE:driverId::INTEGER),
    constructorId INTEGER AS (VALUE:constructorId::INTEGER),
    number INTEGER AS (VALUE:number::INTEGER),
    position INTEGER AS (VALUE:position::INTEGER),
    q1 VARCHAR AS (VALUE:q1::VARCHAR),
    q2 VARCHAR AS (VALUE:q2::VARCHAR),
    q3 VARCHAR AS (VALUE:q3::VARCHAR)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/kaggle/qualifying/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.kaggle_lap_times (
    raceId INTEGER AS (VALUE:raceId::INTEGER),
    driverId INTEGER AS (VALUE:driverId::INTEGER),
    lap INTEGER AS (VALUE:lap::INTEGER),
    position INTEGER AS (VALUE:position::INTEGER),
    milliseconds BIGINT AS (VALUE:milliseconds::BIGINT)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/kaggle/lap_times/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.kaggle_constructor_standings (
    constructorStandingsId INTEGER AS (VALUE:constructorStandingsId::INTEGER),
    raceId INTEGER AS (VALUE:raceId::INTEGER),
    constructorId INTEGER AS (VALUE:constructorId::INTEGER),
    points DOUBLE AS (VALUE:points::DOUBLE),
    position INTEGER AS (VALUE:position::INTEGER),
    positionText VARCHAR AS (VALUE:positionText::VARCHAR),
    wins INTEGER AS (VALUE:wins::INTEGER)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/kaggle/constructor_standings/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.kaggle_driver_standings (
    driverStandingsId INTEGER AS (VALUE:driverStandingsId::INTEGER),
    raceId INTEGER AS (VALUE:raceId::INTEGER),
    driverId INTEGER AS (VALUE:driverId::INTEGER),
    points DOUBLE AS (VALUE:points::DOUBLE),
    position INTEGER AS (VALUE:position::INTEGER),
    positionText VARCHAR AS (VALUE:positionText::VARCHAR),
    wins INTEGER AS (VALUE:wins::INTEGER)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/kaggle/driver_standings/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.kaggle_status (
    statusId INTEGER AS (VALUE:statusId::INTEGER),
    status VARCHAR AS (VALUE:status::VARCHAR)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/kaggle/status/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE staging.kaggle_pit_stops (
    raceId INTEGER AS (VALUE:raceId::INTEGER),
    driverId INTEGER AS (VALUE:driverId::INTEGER),
    stop INTEGER AS (VALUE:stop::INTEGER),
    lap INTEGER AS (VALUE:lap::INTEGER),
    time VARCHAR AS (VALUE:time::VARCHAR),
    duration VARCHAR AS (VALUE:duration::VARCHAR),
    milliseconds BIGINT AS (VALUE:milliseconds::BIGINT)
)
WITH LOCATION = @PUBLIC.s3_processed_stage/processed/kaggle/pit_stops/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;
