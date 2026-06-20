

with source as (
    select * from "f1_analytics"."processed_kaggle"."constructors"
),

renamed as (
    select
        cast(constructorId as integer) as constructor_id,
        cast(constructorRef as varchar) as constructor_ref,
        cast(name as varchar) as constructor_name,
        cast(nationality as varchar) as nationality,
        cast(url as varchar) as wikipedia_url,
        now() as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where constructorId is not null
)

select * from renamed