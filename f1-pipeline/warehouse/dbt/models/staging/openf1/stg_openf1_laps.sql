{{
  config(
    materialized='incremental',
    unique_key=['year', 'driver_number', 'lap_number'],
    meta={'owner': 'data-eng', 'domain': 'f1_racing', 'source': 'openf1'},
    tags=['staging', 'openf1', 'telemetry', 'incremental', 'large'],
    partition_by='year'
  )
}}

with source as (
    select * from {{ source('processed_openf1', 'laps') }}
    where year is not null
      and driver_number is not null
      and lap_number is not null
),

renamed as (
    select
        cast(year as integer)                    as year,
        cast(driver_number as integer)           as driver_number,
        cast(lap_number as integer)              as lap_number,
        cast(lap_duration_ms as bigint)          as lap_duration_ms,
        cast(duration_sector_1_ms as bigint)     as sector_1_ms,
        cast(duration_sector_2_ms as bigint)     as sector_2_ms,
        cast(duration_sector_3_ms as bigint)     as sector_3_ms,
        cast(i1_speed as double)                 as speed_trap_1,
        cast(i2_speed as double)                 as speed_trap_2,
        cast(st_speed as double)                 as speed_trap_3,
        cast(is_pit_out_lap as boolean)          as is_pit_out_lap,
        current_timestamp                        as _dbt_loaded_at,
        'openf1'                                 as source_system
    from source
    {% if is_incremental() %}
    -- On incremental runs: only process (year, driver_number, lap_number) combos
    -- not already present in the target table
    where (year, driver_number, lap_number) not in (
        select year, driver_number, lap_number from {{ this }}
    )
    {% endif %}
)

select * from renamed