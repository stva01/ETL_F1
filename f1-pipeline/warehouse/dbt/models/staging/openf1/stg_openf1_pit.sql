{{
  config(
    materialized='table',
    unique_key=['year', 'driver_number', 'lap_number'],
    meta={'owner': 'data-eng', 'domain': 'f1_racing', 'source': 'openf1'},
    tags=['staging', 'openf1', 'telemetry'],
    partition_by='year'
  )
}}

with source as (
    select * from {{ source('processed_openf1', 'pit') }}
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
