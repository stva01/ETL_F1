with source as (
    select
        ds.*,
        cast(r.year as integer) as year
    from {{ s3_source('s3_kaggle', 'driver_standings', 'kaggle/driver_standings/*/*.parquet') }} ds
    inner join {{ s3_source('s3_kaggle', 'races', 'kaggle/races/*/*.parquet') }} r
        on cast(ds.raceId as bigint) = cast(r.raceId as bigint)
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
        {{ dbt.current_timestamp() }} as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where raceId is not null
      and driverId is not null
)

select * from renamed