

/*
  int_standings_constructor_eoy
  -----------------------------
  One row per constructor per season — the FINAL championship standing
  after the last race of each year.

  Sources:
    - stg_kaggle_constructor_standings (1958–2024)
    - stg_jolpica_constructor_standings (2025+)
*/

with kaggle_eoy as (
    select
        constructor_id,
        year,
        position          as championship_position,
        points            as championship_points,
        wins              as season_wins
    from (
        select
            *,
            row_number() over (
                partition by year, constructor_id
                order by race_id desc      -- highest race_id = last race of season
            ) as rn
        from "f1_analytics"."main_staging"."stg_kaggle_constructor_standings"
    ) ranked
    where rn = 1
),

jolpica_eoy as (
    select
        cb.constructor_id,       -- resolved integer key
        ranked.season_year       as year,
        ranked.position          as championship_position,
        ranked.points            as championship_points,
        ranked.wins              as season_wins
    from (
        select
            *,
            row_number() over (
                partition by season_year, constructor_id
                order by round desc        -- highest round = final round
            ) as rn
        from "f1_analytics"."main_staging"."stg_jolpica_constructor_standings"
    ) ranked
    inner join "f1_analytics"."main_intermediate"."int_constructor_bridge" cb
        on ranked.constructor_id = cb.jolpica_constructor_id
    where ranked.rn = 1
),

unified as (
    select constructor_id, year, championship_position, championship_points, season_wins,
           'kaggle' as source_system
    from kaggle_eoy

    union all

    select constructor_id, year, championship_position, championship_points, season_wins,
           'jolpica' as source_system
    from jolpica_eoy
)

select
    constructor_id,
    year,
    championship_position,
    championship_points,
    season_wins,
    source_system,
    current_timestamp as _dbt_loaded_at
from unified