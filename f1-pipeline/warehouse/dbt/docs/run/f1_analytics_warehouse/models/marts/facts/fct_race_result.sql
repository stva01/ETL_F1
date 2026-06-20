
  
    
    

    create temporary table
      "fct_race_result__dbt_tmp_6022443f_f8d8_427d_b4f1_9122218c80ee"
  
    as (
      

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
from "f1_analytics"."main_intermediate"."int_results_enriched" r

where r._dbt_loaded_at > (select max(dw_inserted_at) from "f1_analytics"."main_marts"."fct_race_result")

    );
  
  ;

        
            delete from "f1_analytics"."main_marts"."fct_race_result"
            where (
                result_id) in (
                select (result_id)
                from "fct_race_result__dbt_tmp_6022443f_f8d8_427d_b4f1_9122218c80ee"
            );

        
    

    insert into "f1_analytics"."main_marts"."fct_race_result" ("result_id", "race_key", "driver_key", "constructor_key", "circuit_key", "season_year", "round_number", "grid_position", "finish_position", "finish_position_text", "points_scored", "laps_completed", "race_time_ms", "fastest_lap_rank", "fastest_lap_speed_kph", "race_status", "qualifying_position", "positions_gained", "is_winner", "is_podium", "is_points_finish", "is_pole", "is_fastest_lap", "is_dnf", "source_system", "dw_inserted_at")
    (
        select "result_id", "race_key", "driver_key", "constructor_key", "circuit_key", "season_year", "round_number", "grid_position", "finish_position", "finish_position_text", "points_scored", "laps_completed", "race_time_ms", "fastest_lap_rank", "fastest_lap_speed_kph", "race_status", "qualifying_position", "positions_gained", "is_winner", "is_podium", "is_points_finish", "is_pole", "is_fastest_lap", "is_dnf", "source_system", "dw_inserted_at"
        from "fct_race_result__dbt_tmp_6022443f_f8d8_427d_b4f1_9122218c80ee"
    )
  