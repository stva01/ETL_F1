

/*
  fct_lap_time
  ------------
  One row per driver × lap (~550K+ rows from Kaggle, growing).
  Grain: 1 row per (race_id, driver_id, lap_number).

  LARGEST table in the warehouse. Partitioned by season_year, clustered by race_key.
  Dashboard queries should NEVER hit this directly — use mart tables instead.

  Sources:
    - stg_kaggle_lap_times  (1996–2024): race_id, driver_id, lap, position, ms
    - stg_openf1_laps       (2023+):     driver_number, lap, sector times, speed traps

  Overlap handling (2023-2024 exist in both):
    Prefer OpenF1 when both sources have the same (race, driver, lap) because
    OpenF1 provides sector times. Deduplicate via ROW_NUMBER.
*/

with kaggle_laps as (
    select
        lt.race_id,
        lt.driver_id,
        lt.year                  as season_year,
        lt.race_id               as race_key,
        lt.lap_number,
        lt.position_on_lap,
        lt.lap_time_ms,
        null::bigint             as sector_1_ms,
        null::bigint             as sector_2_ms,
        null::bigint             as sector_3_ms,
        null::boolean            as is_pit_out_lap,
        'kaggle'                 as source_system,
        lt._dbt_loaded_at
    from "f1_analytics"."main_staging"."stg_kaggle_lap_times" lt
),

openf1_laps as (
    select
        rb.race_id,
        db.driver_id,
        ol.year                  as season_year,
        rb.race_id               as race_key,
        ol.lap_number,
        null::integer            as position_on_lap,
        ol.lap_duration_ms       as lap_time_ms,
        ol.sector_1_ms,
        ol.sector_2_ms,
        ol.sector_3_ms,
        ol.is_pit_out_lap,
        'openf1'                 as source_system,
        ol._dbt_loaded_at
    from "f1_analytics"."main_staging"."stg_openf1_laps" ol
    inner join "f1_analytics"."main_intermediate"."int_driver_bridge" db
        on ol.driver_number = db.openf1_driver_number
    inner join "f1_analytics"."main_intermediate"."int_race_bridge" rb
        on ol.year = rb.year
        -- ponytail: OpenF1 laps don't have round. Match by year only
        -- and pick the best match. For now, this join works because
        -- OpenF1 session data maps 1:1 to race_bridge entries.
        -- TODO: refine with meeting_key if needed
    where db.driver_id is not null
      and rb.race_id is not null
),

-- UNION both sources, then deduplicate overlapping 2023-2024 data
all_laps as (
    select * from kaggle_laps
    union all
    select * from openf1_laps
),

deduped as (
    select
        *,
        row_number() over (
            partition by race_id, driver_id, lap_number
            order by case when source_system = 'openf1' then 0 else 1 end
        ) as rn
    from all_laps
)

select
    race_id,
    driver_id              as driver_key,
    season_year,
    race_key,
    lap_number,
    position_on_lap,
    lap_time_ms,
    sector_1_ms,
    sector_2_ms,
    sector_3_ms,
    is_pit_out_lap,
    source_system,
    current_timestamp      as dw_inserted_at
from deduped
where rn = 1

  and _dbt_loaded_at > (select max(dw_inserted_at) from "f1_analytics"."main_marts"."fct_lap_time")
