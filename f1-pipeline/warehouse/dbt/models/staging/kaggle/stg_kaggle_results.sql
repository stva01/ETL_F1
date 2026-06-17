{{
  config(
    materialized='table',
    meta={'owner': 'data-eng', 'domain': 'f1_racing', 'source': 'kaggle'},
    tags=['staging', 'kaggle', 'event', 'core_fact'],
    partition_by='year'
  )
}}

with source as (
    select
        res.*,
        cast(r.year as integer) as year
    from {{ source('s3_kaggle', 'results') }} res
    inner join {{ source('s3_kaggle', 'races') }} r
        on res.raceId = r.raceId
),

renamed as (
    select
        -- IDs (already numeric)
        cast(resultId as integer)      as result_id,
        cast(raceId as integer)        as race_id,
        cast(driverId as integer)      as driver_id,
        cast(constructorId as integer) as constructor_id,

        -- VARCHAR columns that need \N handling
        cast(nullif(number, '\N') as integer)          as driver_number,
        cast(nullif(position, '\N') as integer)        as finishing_position,
        cast(nullif(milliseconds, '\N') as bigint)     as race_time_ms,
        cast(nullif(fastestLap, '\N') as integer)      as fastest_lap_number,
        cast(nullif(rank, '\N') as integer)            as fastest_lap_rank,
        cast(nullif(fastestLapSpeed, '\N') as double)  as fastest_lap_speed_kmh,

        -- Columns that stay as strings
        cast(positionText as varchar)    as finishing_position_text,
        cast(time as varchar)            as race_time_text,
        cast(fastestLapTime as varchar)  as fastest_lap_time_text,

        -- Already numeric columns (clean)
        cast(grid as integer)            as grid_position,
        cast(positionOrder as integer)   as position_order,
        cast(points as double)           as points_scored,
        cast(laps as integer)            as laps_completed,
        cast(statusId as integer)        as status_id,

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