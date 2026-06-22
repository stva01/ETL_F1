with source as (
    select * from {{ s3_source('processed_jolpica', 'pitstops', 'jolpica/pitstops/*/*.parquet') }}
),

renamed as (
    select
        cast(season as integer)       as season_year,
        cast(round as integer)        as round,
        cast(driver_id as varchar)    as driver_id,
        cast(stop as integer)         as stop,
        cast(lap as integer)          as lap,
        cast(time as varchar)         as pit_time_of_day,
        cast(duration_seconds as double) as duration_seconds,
        cast(duration_ms as bigint)   as duration_ms,

        {{ dbt.current_timestamp() }} as _dbt_loaded_at,
        'jolpica' as source_system
    from source
    where season is not null
      and round is not null
      and driver_id is not null
)

select * from renamed
