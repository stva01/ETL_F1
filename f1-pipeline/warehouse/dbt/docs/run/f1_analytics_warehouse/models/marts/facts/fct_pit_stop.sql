
  
    
    

    create temporary table
      "fct_pit_stop__dbt_tmp_69034365_4241_455c_9012_49d7e30ce80c"
  
    as (
      

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

    );
  
  ;

        
            delete from "f1_analytics"."main_marts"."fct_pit_stop" as DBT_INCREMENTAL_TARGET
            using "fct_pit_stop__dbt_tmp_69034365_4241_455c_9012_49d7e30ce80c"
            where (
                
                    "fct_pit_stop__dbt_tmp_69034365_4241_455c_9012_49d7e30ce80c".race_key = DBT_INCREMENTAL_TARGET.race_key
                    and 
                
                    "fct_pit_stop__dbt_tmp_69034365_4241_455c_9012_49d7e30ce80c".driver_key = DBT_INCREMENTAL_TARGET.driver_key
                    and 
                
                    "fct_pit_stop__dbt_tmp_69034365_4241_455c_9012_49d7e30ce80c".stop_number = DBT_INCREMENTAL_TARGET.stop_number
                    
                
                
            );
        
    

    insert into "f1_analytics"."main_marts"."fct_pit_stop" ("race_key", "driver_key", "season_year", "stop_number", "lap_number", "pit_time_of_day", "stop_duration_ms", "source_system", "dw_inserted_at")
    (
        select "race_key", "driver_key", "season_year", "stop_number", "lap_number", "pit_time_of_day", "stop_duration_ms", "source_system", "dw_inserted_at"
        from "fct_pit_stop__dbt_tmp_69034365_4241_455c_9012_49d7e30ce80c"
    )
  