{{
  config(
    materialized='table',
    meta={'owner': 'data-eng', 'domain': 'f1_racing'},
    tags=['intermediate', 'unified', 'core_fact']
  )
}}

/*
  int_results_unified
  -------------------
  One row per race result, all-time (1950–2026+).
  UNIONs Kaggle (1950–2024) and Jolpica (2025+) into a shared schema
  with integer foreign keys throughout.

  Kaggle side: already has integer driver_id, constructor_id, race_id
  Jolpica side: joins through bridge tables to resolve to integer keys
*/

with kaggle_results as (
    select
        result_id,
        race_id,
        driver_id,
        constructor_id,
        year,
        grid_position,
        finishing_position,
        finishing_position_text,
        points_scored,
        laps_completed,
        race_time_ms,
        fastest_lap_rank,
        fastest_lap_speed_kmh,
        status_id,
        null::varchar  as race_status,   -- resolved later from stg_kaggle_status
        source_system
    from {{ ref('stg_kaggle_results') }}
),

jolpica_results as (
    select
        jr.season_year                                   as year,
        jr.round,
        jr.driver_id                                     as jolpica_driver_id,
        jr.constructor_id                                as jolpica_constructor_id,
        jr.grid_position,
        jr.finishing_position,
        jr.finishing_position_text,
        jr.points_scored,
        jr.laps_completed,
        jr.race_time_ms,
        null::integer                                    as fastest_lap_rank,
        jr.fastest_lap_speed_kmh,
        null::integer                                    as status_id,
        jr.race_status,
        jr.source_system,

        -- Resolve integer keys via bridge tables
        db.driver_id                                     as driver_id,
        cb.constructor_id                                as constructor_id,
        rb.race_id                                       as race_id
    from {{ ref('stg_jolpica_results') }} jr
    -- Resolve Jolpica driver_id string → Kaggle driver_id integer
    left join {{ ref('int_driver_bridge') }} db
        on jr.driver_id = db.jolpica_driver_id
    -- Resolve Jolpica constructor_id string → Kaggle constructor_id integer
    left join {{ ref('int_constructor_bridge') }} cb
        on jr.constructor_id = cb.jolpica_constructor_id
    -- Resolve (season_year, round) → integer race_id
    left join {{ ref('int_race_bridge') }} rb
        on jr.season_year = rb.year
        and jr.round = rb.round
),

kaggle_final as (
    select
        -- Use a synthetic result_id for Kaggle (already exists)
        result_id,
        race_id,
        driver_id,
        constructor_id,
        year,
        grid_position,
        finishing_position,
        finishing_position_text,
        points_scored,
        laps_completed,
        race_time_ms,
        fastest_lap_rank,
        fastest_lap_speed_kmh,
        status_id,
        race_status,
        source_system
    from kaggle_results
),

jolpica_final as (
    select
        -- Synthetic result_id for Jolpica: offset by 1000000 to avoid collision
        (1000000 + race_id * 100 + coalesce(
            row_number() over (partition by race_id order by driver_id),
        0))::integer                                     as result_id,
        race_id,
        driver_id,
        constructor_id,
        year,
        grid_position,
        finishing_position,
        finishing_position_text,
        points_scored,
        laps_completed,
        race_time_ms,
        fastest_lap_rank,
        fastest_lap_speed_kmh,
        status_id,
        race_status,
        source_system
    from jolpica_results
),

unified as (
    select * from kaggle_final
    union all
    select * from jolpica_final
)

select
    result_id,
    race_id,
    driver_id,
    constructor_id,
    year,
    grid_position,
    finishing_position,
    finishing_position_text,
    points_scored,
    laps_completed,
    race_time_ms,
    fastest_lap_rank,
    fastest_lap_speed_kmh,
    status_id,
    race_status,
    source_system,
    current_timestamp as _dbt_loaded_at
from unified
