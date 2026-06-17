{{
  config(
    materialized='table',
    meta={'owner': 'data-eng', 'domain': 'f1_racing', 'source': 'kaggle'},
    tags=['staging', 'kaggle', 'dimension']
  )
}}

with source as (
    select * from {{ source('s3_kaggle', 'circuits') }}
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
        now() as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where circuitId is not null
)

select * from renamed