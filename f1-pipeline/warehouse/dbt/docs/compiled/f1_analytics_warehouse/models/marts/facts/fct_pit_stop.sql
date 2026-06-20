

/*
  fct_pit_stop
  ------------
  One row per pit stop event (~12K rows).
  Grain: 1 row per (race_id, driver_id, stop_number).
  Source: int_pitstops_unified
*/

select
    p.race_id             as race_key,
    p.driver_id           as driver_key,
    p.year                as season_year,
    p.stop_number,
    p.lap                 as lap_number,
    p.pit_time_of_day,
    p.duration_ms         as stop_duration_ms,

    -- Metadata
    p.source_system,
    current_timestamp     as dw_inserted_at
from "f1_analytics"."main_intermediate"."int_pitstops_unified" p
where p.race_id is not null
  and p.driver_id is not null

  and p._dbt_loaded_at > (select max(dw_inserted_at) from "f1_analytics"."main_marts"."fct_pit_stop")
