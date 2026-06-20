{{
  config(
    materialized='incremental',
    unique_key='result_id',
    tags=['fact', 'gold', 'incremental']
  )
}}

/*
  fct_race_result
  ---------------
  One row per driver × race start (~28K rows).
  Grain: 1 row per result_id.
  Primary analytical fact table — most mart queries aggregate from here.

  Source: int_results_enriched (already fully joined with names, flags, status)
*/

select
    -- Keys
    r.result_id,
    r.race_id             as race_key,
    r.driver_id           as driver_key,
    r.constructor_id      as constructor_key,
    r.circuit_id          as circuit_key,
    r.year                as season_year,
    r.round               as round_number,

    -- Race result measures
    r.grid_position,
    r.finishing_position   as finish_position,
    r.finishing_position_text as finish_position_text,
    r.points_scored,
    r.laps_completed,
    r.race_time_ms,
    r.fastest_lap_rank,
    r.fastest_lap_speed_kmh as fastest_lap_speed_kph,
    r.race_status,
    r.qualifying_position,

    -- Derived measures
    case
        when r.grid_position is not null and r.finishing_position is not null
        then r.grid_position - r.finishing_position
        else null
    end                    as positions_gained,

    -- Pre-computed boolean flags
    r.is_winner,
    r.is_podium,
    coalesce(r.points_scored > 0, false) as is_points_finish,
    r.is_pole,
    r.is_fastest_lap,
    r.is_dnf,

    -- Metadata
    r.source_system,
    current_timestamp      as dw_inserted_at
from {{ ref('int_results_enriched') }} r
{% if is_incremental() %}
where r._dbt_loaded_at > (select max(dw_inserted_at) from {{ this }})
{% endif %}
