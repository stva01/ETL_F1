with source as (
    select * from {{ s3_source('processed_jolpica', 'qualifying', 'jolpica/qualifying/*/*.parquet') }}
),

renamed as (
    select
        cast(season as integer) as season_year,
        cast(round as integer) as round,
        cast(driver_id as varchar) as driver_id,
        cast(position as integer) as qualifying_position,
        cast(q1 as varchar) as q1_time,
        cast(q2 as varchar) as q2_time,
        cast(q3 as varchar) as q3_time,
        cast(q1_ms as bigint) as q1_ms,
        cast(q2_ms as bigint) as q2_ms,
        cast(q3_ms as bigint) as q3_ms,
        cast(driver_number as integer) as driver_number,
        cast(driver_code as varchar) as driver_code,
        cast(constructor_id as varchar) as constructor_id,
        
        {{ dbt.current_timestamp() }} as _dbt_loaded_at,
        'jolpica' as source_system
    from source
    where season is not null
      and round is not null
      and driver_id is not null
)

select * from renamed