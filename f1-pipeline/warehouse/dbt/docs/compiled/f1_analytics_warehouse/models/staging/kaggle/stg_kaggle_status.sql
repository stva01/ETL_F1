

with source as (
    select * from "f1_analytics"."processed_kaggle"."status"
),

renamed as (
    select
        cast(statusId as integer) as status_id,
        cast(status as varchar) as status_description,
        now() as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where statusId is not null
)

select * from renamed