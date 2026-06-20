
  
    
    

    create  table
      "f1_analytics"."main_staging"."stg_jolpica_results__dbt_tmp"
  
    as (
      

with source as (
    select * from "f1_analytics"."processed_jolpica"."results"
),

renamed as (
    select
        cast(season as integer) as season_year,
        cast(round as integer) as round,
        cast(driver_id as varchar) as driver_id,
        cast(driver_code as varchar) as driver_code,
        cast(driver_number as integer) as driver_number,
        cast(constructor_id as varchar) as constructor_id,
        cast(grid as integer) as grid_position,
        cast(position as integer) as finishing_position,
        cast(position_text as varchar) as finishing_position_text,
        cast(laps as integer) as laps_completed,
        cast(points as double) as points_scored,
        cast(time_millis as bigint) as race_time_ms,
        cast(time_text as varchar) as race_time_text,
        cast(fastest_lap_time_ms as bigint) as fastest_lap_ms,
        cast(fastest_lap_speed as double) as fastest_lap_speed_kmh,
        cast(status as varchar) as race_status,
        
        now() as _dbt_loaded_at,
        'jolpica' as source_system
    from source
    where season is not null
      and round is not null
      and driver_id is not null
)

select * from renamed
    );
  
  