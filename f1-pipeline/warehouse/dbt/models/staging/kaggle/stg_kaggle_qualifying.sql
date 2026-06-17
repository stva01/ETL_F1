{{
  config(
    materialized='table',
    meta={'owner': 'data-eng', 'domain': 'f1_racing', 'source': 'kaggle'},
    tags=['staging', 'kaggle', 'event'],
    partition_by='year'
  )
}}

with source as (
    select
        q.*,
        cast(r.year as integer) as year
    from {{ source('s3_kaggle', 'qualifying') }} q
    inner join {{ source('s3_kaggle', 'races') }} r
        on q.raceId = r.raceId
),

renamed as (
    select
        cast(qualifyId as integer)     as qualify_id,
        cast(raceId as integer)        as race_id,
        cast(driverId as integer)      as driver_id,
        cast(constructorId as integer) as constructor_id,
        cast(number as integer)        as driver_number,
        cast(position as integer)      as qualifying_position,

        -- Qualifying times as provided (may be NULL or string format)
        cast(q1 as varchar) as q1_text,
        cast(q2 as varchar) as q2_text,
        cast(q3 as varchar) as q3_text,

        -- Partition column derived via JOIN (not correlated subquery)
        year,
        now() as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where raceId is not null
      and driverId is not null
      and constructorId is not null
)

select * from renamed