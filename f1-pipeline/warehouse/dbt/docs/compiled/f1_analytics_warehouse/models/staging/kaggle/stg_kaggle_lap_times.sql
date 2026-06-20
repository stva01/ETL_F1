

with source as (
    select
        lt.*,
        cast(r.year as integer) as year
    from "f1_analytics"."processed_kaggle"."lap_times" lt
    inner join "f1_analytics"."processed_kaggle"."races" r
        on lt.raceId = r.raceId
),

renamed as (
    select
        cast(raceId as integer)       as race_id,
        cast(driverId as integer)     as driver_id,
        cast(lap as integer)          as lap_number,
        cast(position as integer)     as position_on_lap,

        -- Lap time in milliseconds (from raw field)
        cast(milliseconds as bigint)  as lap_time_ms,

        -- Partition column derived via JOIN (not correlated subquery)
        year,
        now() as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where raceId is not null
      and driverId is not null
      and lap is not null
)

select * from renamed