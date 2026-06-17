{{
  config(
    materialized='table',
    meta={'owner': 'data-eng', 'domain': 'f1_racing', 'source': 'kaggle'},
    tags=['staging', 'kaggle', 'event'],
    partition_by='year'
  )
}}

with source as (
    select * from {{ source('s3_kaggle', 'races') }}
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
        now() as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where raceId is not null
      and year is not null
      and round is not null
)

select * from renamed