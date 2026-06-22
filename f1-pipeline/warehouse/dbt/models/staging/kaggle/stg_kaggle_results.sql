with source as (
    select
        res.*,
        cast(r.year as integer) as year
    from {{ s3_source('s3_kaggle', 'results', 'kaggle/results/*/*.parquet') }} res
    inner join {{ s3_source('s3_kaggle', 'races', 'kaggle/races/*/*.parquet') }} r
        on cast(res.raceId as bigint) = cast(r.raceId as bigint)
),

renamed as (
    select
        -- IDs (already numeric)
        cast(resultId as integer)      as result_id,
        cast(raceId as integer)        as race_id,
        cast(driverId as integer)      as driver_id,
        cast(constructorId as integer) as constructor_id,

        -- VARCHAR columns that need \N handling
        cast(nullif(cast(number as varchar), '\N') as integer)          as driver_number,
        cast(nullif(cast(position as varchar), '\N') as integer)        as finishing_position,
        cast(nullif(cast(milliseconds as varchar), '\N') as bigint)     as race_time_ms,
        cast(nullif(cast(fastestLap as varchar), '\N') as integer)      as fastest_lap_number,
        cast(nullif(cast(rank as varchar), '\N') as integer)            as fastest_lap_rank,
        cast(nullif(cast(fastestLapSpeed as varchar), '\N') as double)  as fastest_lap_speed_kmh,

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
        {{ dbt.current_timestamp() }} as _dbt_loaded_at,
        'kaggle' as source_system

    from source
    where raceId is not null
      and driverId is not null
      and constructorId is not null
)

select * from renamed