{{
  config(
    materialized='table',
    meta={'owner': 'data-eng', 'domain': 'f1_racing', 'source': 'kaggle'},
    tags=['staging', 'kaggle', 'dimension']
  )
}}

with source as (
    select * from {{ source('s3_kaggle', 'seasons') }}
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
