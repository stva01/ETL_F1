with source as (
    select
        cs.*,
        cast(r.year as integer) as year
    from {{ s3_source('s3_kaggle', 'constructor_standings', 'kaggle/constructor_standings/*/*.parquet') }} cs
    inner join {{ s3_source('s3_kaggle', 'races', 'kaggle/races/*/*.parquet') }} r
        on cast(cs.raceId as bigint) = cast(r.raceId as bigint)
),

renamed as (
    select
        cast(constructorStandingsId as integer) as constructor_standings_id,
        cast(raceId as integer)                 as race_id,
        cast(constructorId as integer)          as constructor_id,
        cast(points as double)                  as points,
        cast(position as integer)               as position,
        cast(positionText as varchar)           as position_text,
        cast(wins as integer)                   as wins,

        -- Partition column derived via JOIN (not correlated subquery)
        year,
        {{ dbt.current_timestamp() }} as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where raceId is not null
      and constructorId is not null
)

select * from renamed
