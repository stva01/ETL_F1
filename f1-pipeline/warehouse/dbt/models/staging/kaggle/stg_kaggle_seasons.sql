with source as (
    select * from {{ s3_source('s3_kaggle', 'seasons', 'kaggle/seasons/*.parquet') }}
),

renamed as (
    select
        cast(year as integer) as season_year,
        cast(url as varchar) as wikipedia_url,
        {{ dbt.current_timestamp() }} as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where year is not null
)

select * from renamed
