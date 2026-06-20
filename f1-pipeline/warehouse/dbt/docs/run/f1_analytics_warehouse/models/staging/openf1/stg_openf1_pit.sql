
  
    
    

    create  table
      "f1_analytics"."main_staging"."stg_openf1_pit__dbt_tmp"
  
    as (
      

with source as (
    select * from "f1_analytics"."processed_openf1"."pit"
),

renamed as (
    select
        cast(year as integer)           as year,
        cast(driver_number as integer)  as driver_number,
        cast(lap_number as integer)     as lap_number,
        cast(meeting_key as bigint)     as meeting_key,
        cast(session_key as bigint)     as session_key,
        cast(meeting_id as integer)     as meeting_id,
        cast(pit_duration as double)    as pit_duration_seconds,
        cast(pit_duration_ms as bigint) as pit_duration_ms,
        cast(lane_duration as double)   as lane_duration_seconds,
        cast(date as varchar)           as pit_timestamp,

        current_timestamp as _dbt_loaded_at,
        'openf1' as source_system
    from source
    where year is not null
      and driver_number is not null
      and lap_number is not null
)

select * from renamed
    );
  
  