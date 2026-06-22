with source as (
    select * from {{ s3_source('s3_kaggle', 'constructors', 'kaggle/constructors/*.parquet') }}
),

renamed as (
    select
        cast(constructorId as integer) as constructor_id,
        cast(constructorRef as varchar) as constructor_ref,
        cast(name as varchar) as constructor_name,
        cast(nationality as varchar) as nationality,
        cast(url as varchar) as wikipedia_url,
        {{ dbt.current_timestamp() }} as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where constructorId is not null
)

select * from renamed