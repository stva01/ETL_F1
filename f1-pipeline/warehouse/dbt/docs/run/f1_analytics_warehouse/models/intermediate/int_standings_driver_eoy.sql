
  
    
    

    create  table
      "f1_analytics"."main_intermediate"."int_standings_driver_eoy__dbt_tmp"
  
    as (
      

/*
  int_standings_driver_eoy
  ------------------------
  One row per driver per season — the FINAL championship standing after the
  last race of each year.

  Problem solved: raw standings tables have a snapshot after EVERY race.
  We only need the end-of-season snapshot (highest round per season).

  Sources:
    - stg_kaggle_driver_standings (1950–2024, keyed by race_id)
    - stg_jolpica_driver_standings (2025+, keyed by season_year + round)
*/

with kaggle_eoy as (
    select
        driver_id,
        year,
        position          as championship_position,
        points            as championship_points,
        wins              as season_wins
    from (
        select
            *,
            row_number() over (
                partition by year, driver_id
                order by race_id desc      -- highest race_id = last race of season
            ) as rn
        from "f1_analytics"."main_staging"."stg_kaggle_driver_standings"
    ) ranked
    where rn = 1
),

jolpica_eoy as (
    select
        db.driver_id,            -- resolved integer key
        ranked.season_year       as year,
        ranked.position          as championship_position,
        ranked.points            as championship_points,
        ranked.wins              as season_wins
    from (
        select
            *,
            row_number() over (
                partition by season_year, driver_id
                order by round desc        -- highest round = final round
            ) as rn
        from "f1_analytics"."main_staging"."stg_jolpica_driver_standings"
    ) ranked
    inner join "f1_analytics"."main_intermediate"."int_driver_bridge" db
        on ranked.driver_id = db.jolpica_driver_id
    where ranked.rn = 1
),

unified as (
    select driver_id, year, championship_position, championship_points, season_wins,
           'kaggle' as source_system
    from kaggle_eoy

    union all

    select driver_id, year, championship_position, championship_points, season_wins,
           'jolpica' as source_system
    from jolpica_eoy
)

select
    driver_id,
    year,
    championship_position,
    championship_points,
    season_wins,
    source_system,
    current_timestamp as _dbt_loaded_at
from unified
    );
  
  