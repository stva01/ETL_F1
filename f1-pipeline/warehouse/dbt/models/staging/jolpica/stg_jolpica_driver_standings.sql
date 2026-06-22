with source as (
    select * from {{ s3_source('processed_jolpica', 'driver_standings', 'jolpica/driver_standings/*/*.parquet') }}
),

renamed as (
    select
        cast(season as integer)    as season_year,
        cast(round as integer)     as round,
        cast(driver_id as varchar) as driver_id,
        cast(position as integer)  as position,
        cast(points as double)     as points,
        cast(wins as integer)      as wins,

        {{ dbt.current_timestamp() }} as _dbt_loaded_at,
        'jolpica' as source_system
    from source
    where season is not null
      and round is not null
      and driver_id is not null
)

select * from renamed