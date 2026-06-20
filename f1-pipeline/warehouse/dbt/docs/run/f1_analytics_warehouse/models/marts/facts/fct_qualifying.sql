
  
    
    

    create temporary table
      "fct_qualifying__dbt_tmp_86233cba_3074_4b21_a27f_4c4e0e024d1d"
  
    as (
      

/*
  fct_qualifying
  --------------
  One row per driver × qualifying session (~11K rows).
  Grain: 1 row per qualify_id.
  Source: int_qualifying_unified
*/

select
    q.qualify_id,
    q.race_id             as race_key,
    q.driver_id           as driver_key,
    q.constructor_id      as constructor_key,
    q.year                as season_year,

    -- Qualifying measures
    q.qualifying_position,
    q.q1_text,
    q.q2_text,
    q.q3_text,
    q.q1_ms,
    q.q2_ms,
    q.q3_ms,

    -- Derived flags
    case when q.qualifying_position = 1 then true else false end as is_pole_position,
    case when q.q2_text is not null then true else false end      as reached_q2,
    case when q.q3_text is not null then true else false end      as reached_q3,

    -- Metadata
    q.source_system,
    current_timestamp     as dw_inserted_at
from "f1_analytics"."main_intermediate"."int_qualifying_unified" q

where q._dbt_loaded_at > (select max(dw_inserted_at) from "f1_analytics"."main_marts"."fct_qualifying")

    );
  
  ;

        
            delete from "f1_analytics"."main_marts"."fct_qualifying"
            where (
                qualify_id) in (
                select (qualify_id)
                from "fct_qualifying__dbt_tmp_86233cba_3074_4b21_a27f_4c4e0e024d1d"
            );

        
    

    insert into "f1_analytics"."main_marts"."fct_qualifying" ("qualify_id", "race_key", "driver_key", "constructor_key", "season_year", "qualifying_position", "q1_text", "q2_text", "q3_text", "q1_ms", "q2_ms", "q3_ms", "is_pole_position", "reached_q2", "reached_q3", "source_system", "dw_inserted_at")
    (
        select "qualify_id", "race_key", "driver_key", "constructor_key", "season_year", "qualifying_position", "q1_text", "q2_text", "q3_text", "q1_ms", "q2_ms", "q3_ms", "is_pole_position", "reached_q2", "reached_q3", "source_system", "dw_inserted_at"
        from "fct_qualifying__dbt_tmp_86233cba_3074_4b21_a27f_4c4e0e024d1d"
    )
  