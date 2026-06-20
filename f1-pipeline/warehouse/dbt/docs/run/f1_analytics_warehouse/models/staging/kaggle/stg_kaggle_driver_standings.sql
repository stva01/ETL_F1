
  
    
    

    create  table
      "f1_analytics"."main_staging"."stg_kaggle_driver_standings__dbt_tmp"
  
    as (
      

with source as (
    select
        ds.*,
        cast(r.year as integer) as year
    from "f1_analytics"."processed_kaggle"."driver_standings" ds
    inner join "f1_analytics"."processed_kaggle"."races" r
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
    );
  
  