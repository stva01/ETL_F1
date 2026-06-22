with source as (
    select * from {{ s3_source('s3_kaggle', 'circuits', 'kaggle/circuits/*.parquet') }}
),

renamed as (
    select
        cast(circuitId as integer) as circuit_id,
        cast(circuitRef as varchar) as circuit_ref,
        cast(name as varchar) as circuit_name,
        cast(location as varchar) as location,
        cast(country as varchar) as country,
        cast(lat as double) as latitude,
        cast(lng as double) as longitude,
        cast(alt as integer) as altitude_meters,
        cast(url as varchar) as wikipedia_url,
        {{ dbt.current_timestamp() }} as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where circuitId is not null
)

select * from renamed