
  
    
    

    create  table
      "f1_analytics"."main_staging"."stg_openf1_stints__dbt_tmp"
  
    as (
      

with source as (
    select * from "f1_analytics"."processed_openf1"."stints"
),

renamed as (
    select
        cast(year as integer)               as year,
        cast(driver_number as integer)      as driver_number,
        cast(stint_number as bigint)        as stint_number,
        cast(compound as varchar)           as tyre_compound,
        cast(lap_start as integer)          as lap_start,
        cast(lap_end as integer)            as lap_end,
        cast(lap_end - lap_start as integer) as stint_laps,
        cast(tyre_age_at_start as bigint)   as tyre_age_at_start_laps,
        cast(meeting_key as bigint)         as meeting_key,
        cast(session_key as bigint)         as session_key,
        cast(meeting_id as integer)         as meeting_id,

        current_timestamp as _dbt_loaded_at,
        'openf1' as source_system
    from source
    where year is not null
      and driver_number is not null
      and stint_number is not null
)

select * from renamed
    );
  
  