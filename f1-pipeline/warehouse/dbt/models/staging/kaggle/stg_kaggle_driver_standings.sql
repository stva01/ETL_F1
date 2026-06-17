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
        ds.*,
        cast(r.year as integer) as year
    from {{ source('s3_kaggle', 'driver_standings') }} ds
    inner join {{ source('s3_kaggle', 'races') }} r
        on ds.raceId = r.raceId
),

renamed as (
    select
        cast(driverStandingsId as integer) as driver_standings_id,
        cast(raceId as integer)            as race_id,
        cast(driverId as integer)          as driver_id,
        cast(points as double)             as points,
        cast(position as integer)          as position,
        cast(positionText as varchar)      as position_text,
        cast(wins as integer)              as wins,

        -- Partition column derived via JOIN (not correlated subquery)
        year,
        now() as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where raceId is not null
      and driverId is not null
)

select * from renamed