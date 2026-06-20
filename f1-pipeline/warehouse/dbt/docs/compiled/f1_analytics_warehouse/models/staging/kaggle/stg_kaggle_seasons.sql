

with source as (
    select * from "f1_analytics"."processed_kaggle"."seasons"
),

renamed as (
    select
        cast(year as integer) as season_year,
        cast(url as varchar) as wikipedia_url,
        now() as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where year is not null
)

select * from renamed