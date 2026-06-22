with source as (
    select * from {{ s3_source('s3_kaggle', 'races', 'kaggle/races/*/*.parquet') }}
),

renamed as (
    select
        cast(raceId as integer) as race_id,
        cast(year as integer) as year,
        cast(round as integer) as round,
        cast(circuitId as integer) as circuit_id,
        cast(name as varchar) as gp_name,
        cast(date as date) as race_date,
        cast(time as varchar) as race_time_utc,
        cast(url as varchar) as wikipedia_url,
        {{ dbt.current_timestamp() }} as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where raceId is not null
      and year is not null
      and round is not null
)

select * from renamed