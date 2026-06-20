

/*
  int_pitstops_unified
  --------------------
  One row per pit stop event, all-time (2011–2026+).
  Kaggle pit_stops coverage starts from 2011 (when F1 began recording them).

  UNIONs:
    - Kaggle pit_stops source table (no separate staging model exists)
    - stg_jolpica_pitstops (2025+)

  Kaggle columns: raceId, driverId, stop, lap, time (HH:MM:SS), duration (string), milliseconds (bigint)
  Jolpica columns: season_year, round, driver_id (string), stop, lap, duration_seconds, duration_ms
*/

with kaggle_pitstops_raw as (
    select
        cast(ps.raceId as integer)          as race_id,
        cast(ps.driverId as integer)        as driver_id,
        cast(ps.stop as integer)            as stop_number,
        cast(ps.lap as integer)             as lap,
        cast(ps.time as varchar)            as pit_time_of_day,
        try_cast(ps.milliseconds as bigint) as duration_ms,
        cast(r.year as integer)             as year,
        'kaggle'                            as source_system
    from "f1_analytics"."processed_kaggle"."pit_stops" ps
    inner join "f1_analytics"."processed_kaggle"."races" r
        on ps.raceId = r.raceId
    where ps.raceId is not null
      and ps.driverId is not null
),

jolpica_pitstops as (
    select
        jp.season_year                      as year,
        jp.round,
        jp.driver_id                        as jolpica_driver_id,
        jp.stop                             as stop_number,
        jp.lap,
        jp.pit_time_of_day,
        jp.duration_ms,
        jp.source_system,

        -- Resolve integer keys via bridges
        db.driver_id                        as driver_id,
        rb.race_id                          as race_id
    from "f1_analytics"."main_staging"."stg_jolpica_pitstops" jp
    left join "f1_analytics"."main_intermediate"."int_driver_bridge" db
        on jp.driver_id = db.jolpica_driver_id
    left join "f1_analytics"."main_intermediate"."int_race_bridge" rb
        on jp.season_year = rb.year
        and jp.round = rb.round
),

kaggle_final as (
    select
        race_id,
        driver_id,
        year,
        stop_number,
        lap,
        pit_time_of_day,
        duration_ms,
        source_system
    from kaggle_pitstops_raw
),

jolpica_final as (
    select
        race_id,
        driver_id,
        year,
        stop_number,
        lap,
        pit_time_of_day,
        duration_ms,
        source_system
    from jolpica_pitstops
),

unified as (
    select * from kaggle_final
    union all
    select * from jolpica_final
)

select
    race_id,
    driver_id,
    year,
    stop_number,
    lap,
    pit_time_of_day,
    duration_ms,
    source_system,
    current_timestamp as _dbt_loaded_at
from unified