with source as (
    select * from {{ s3_source('s3_kaggle', 'status', 'kaggle/status/*.parquet') }}
),

renamed as (
    select
        cast(statusId as integer) as status_id,
        cast(status as varchar) as status_description,
        {{ dbt.current_timestamp() }} as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where statusId is not null
)

select * from renamed